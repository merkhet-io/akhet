//! Replay committed spool records to an HTTP receiver.
//!
//! Protocol `akhet-replay-v1` (implemented by `akhet receive`):
//! - `GET {url}/v1/identity` -> `{"receiver_id": "<stable id>"}`
//! - `POST {url}/v1/events` with the stored event bytes as body and headers
//!   `Idempotency-Key: <event_id>` and `Akhet-Body-SHA256: <hex>`.
//!   - `201` stored, `200` identical duplicate already stored: success.
//!   - `409` same event_id with different bytes, `400`/`413`/`422` and other
//!     4xx except 401/403/408/429: permanent failure, replay stops.
//!   - `401`/`403` (on either endpoint) and TLS certificate rejections:
//!     permanent authentication failure, replay stops without retrying.
//!   - `408`, `429`, `5xx`, connection errors and timeouts: transient, retried
//!     with bounded exponential backoff.
//!
//! Optional security: `https://` URLs use rustls with the platform trust store
//! or a configured CA bundle, optionally with a client certificate (mutual
//! TLS); a bearer token is sent as `Authorization: Bearer <token>`. A token is
//! refused over plain `http://` unless explicitly allowed.
//!
//! The local checkpoint advances only after a success response. A crash or a
//! lost response after the receiver committed causes the same event_id to be
//! sent again; the receiver must deduplicate it. This is at-least-once delivery
//! with receiver-side deduplication, not exactly-once.

use crate::event;
use crate::faults::{Faults, point};
use crate::spool::{SinkRow, Spool, SpoolError};
use crate::tls;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Base URL of the receiver, e.g. `http://127.0.0.1:8080`.
    pub url: String,
    /// Sink name; owns a checkpoint bound to (url, receiver_id).
    pub sink: String,
    pub batch: usize,
    pub request_timeout: Duration,
    /// Attempts per request before giving up on a transient failure.
    pub max_attempts: u32,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    /// Keep running and poll for new records at this interval.
    pub follow: Option<Duration>,
    /// Stop after delivering this many records.
    pub limit: Option<u64>,
    /// PEM CA bundle for `https://`; the platform trust store when absent.
    pub ca_file: Option<PathBuf>,
    /// PEM client certificate chain and private key (mutual TLS).
    pub client_cert: Option<(PathBuf, PathBuf)>,
    /// Bearer token for the receiver.
    pub token: Option<Token>,
    /// Permit sending `token` over plain `http://`.
    pub allow_insecure_token: bool,
}

/// A bearer token. Its `Debug` output is redacted so it cannot leak through
/// logged configuration.
#[derive(Clone)]
pub struct Token(String);

impl Token {
    /// Accepts RFC 6750 `b64token` characters only (no spaces or controls).
    pub fn new(s: String) -> Result<Token, String> {
        let ok = !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b));
        if ok {
            Ok(Token(s))
        } else {
            Err("token must be non-empty and use only A-Z a-z 0-9 - . _ ~ + / =".into())
        }
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

impl ReplayConfig {
    pub fn new(url: &str, sink: &str) -> Self {
        ReplayConfig {
            url: url.into(),
            sink: sink.into(),
            batch: 100,
            request_timeout: Duration::from_secs(10),
            max_attempts: 8,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            follow: None,
            limit: None,
            ca_file: None,
            client_cert: None,
            token: None,
            allow_insecure_token: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(transparent)]
    Spool(#[from] SpoolError),
    #[error("another replayer holds sink {sink:?} (lock file {lock})")]
    Locked { sink: String, lock: String },
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("receiver rejected seq {seq} (event {event_id}) permanently: HTTP {status}: {body}")]
    Rejected {
        seq: i64,
        event_id: String,
        status: u16,
        body: String,
    },
    #[error("receiver refused authentication on {endpoint}: HTTP {status}: {body} (not retried)")]
    Unauthorized {
        endpoint: &'static str,
        status: u16,
        body: String,
    },
    #[error("TLS failure for {url}: {detail} (not retried)")]
    Tls { url: String, detail: String },
    #[error("receiver unavailable after {attempts} attempts: {last}")]
    Unavailable { attempts: u32, last: String },
    #[error("receiver protocol error: {0}")]
    Protocol(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Default, serde::Serialize)]
pub struct ReplaySummary {
    pub sink: String,
    pub destination_url: String,
    pub receiver_id: String,
    pub delivered: u64,
    pub receiver_duplicates: u64,
    pub retries: u64,
    pub checkpoint_seq: i64,
    pub interrupted: bool,
}

/// Exclusive per-(spool, sink) lock. Held for the lifetime of the value and
/// released by the OS if the process dies.
pub struct SinkLock {
    _file: File,
    path: PathBuf,
}

impl SinkLock {
    pub fn acquire(spool_path: &Path, sink: &str) -> Result<SinkLock, ReplayError> {
        let tag = &event::sha256_hex(sink.as_bytes())[..16];
        let mut name = spool_path.as_os_str().to_owned();
        name.push(format!(".replay-{tag}.lock"));
        let path = PathBuf::from(name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(SinkLock { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(ReplayError::Locked {
                sink: sink.into(),
                lock: path.display().to_string(),
            }),
            Err(TryLockError::Error(e)) => Err(ReplayError::Io(e)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

enum Attempt {
    Success { duplicate: bool },
    Permanent { status: u16, body: String },
}

/// Outcome of a failed request attempt.
enum Step {
    Retry(String),
    Fail(ReplayError),
}

fn normalize_url(url: &str) -> Result<String, ReplayError> {
    let u = url.trim().trim_end_matches('/');
    if !(u.starts_with("http://") || u.starts_with("https://")) || u.len() < 10 {
        return Err(ReplayError::Config(format!(
            "receiver URL must be http(s)://host[:port][/path], got {url:?}"
        )));
    }
    Ok(u.to_string())
}

fn agent(cfg: &ReplayConfig, url: &str) -> Result<ureq::Agent, ReplayError> {
    let https = url.starts_with("https://");
    if !https && (cfg.ca_file.is_some() || cfg.client_cert.is_some()) {
        return Err(ReplayError::Config(
            "TLS options (CA file, client certificate) require an https:// URL".into(),
        ));
    }
    if !https && cfg.token.is_some() && !cfg.allow_insecure_token {
        return Err(ReplayError::Config(
            "refusing to send the bearer token over plain http://; use https:// \
             (or explicitly allow an insecure token)"
                .into(),
        ));
    }
    let builder = ureq::Agent::config_builder()
        .timeout_global(Some(cfg.request_timeout))
        .http_status_as_error(false)
        // The protocol has no redirects; never follow one with credentials.
        .max_redirects(0)
        .proxy(None);
    let builder = if https {
        builder.tls_config(tls_config(cfg)?)
    } else {
        builder
    };
    Ok(builder.build().into())
}

/// ureq TLS settings (ureq builds its rustls config from these).
fn tls_config(cfg: &ReplayConfig) -> Result<ureq::tls::TlsConfig, ReplayError> {
    use ureq::tls::{Certificate, ClientCert, PrivateKey, RootCerts, TlsConfig};
    let client = cfg
        .client_cert
        .as_ref()
        .map(|(c, k)| (c.as_path(), k.as_path()));
    // Fail early on unusable files or a key that does not match the
    // certificate, instead of on the first connection.
    tls::client_config(cfg.ca_file.as_deref(), client).map_err(ReplayError::Config)?;
    let roots = tls::trusted_certs(cfg.ca_file.as_deref()).map_err(ReplayError::Config)?;
    let roots = RootCerts::new_with_certs(
        &roots
            .iter()
            .map(|c| Certificate::from_der(c.as_ref()).to_owned())
            .collect::<Vec<_>>(),
    );
    let client_cert = match client {
        None => None,
        Some((cert, key)) => {
            let chain = tls::load_certs(cert, "client cert").map_err(ReplayError::Config)?;
            // Already validated by tls::client_config above. (ureq does not
            // export its key-kind enum, so it parses the PEM itself.)
            let pem = std::fs::read(key)
                .map_err(|e| ReplayError::Config(format!("reading client key: {e}")))?;
            let key = PrivateKey::from_pem(&pem)
                .map_err(|e| ReplayError::Config(format!("parsing client key: {e}")))?;
            let chain: Vec<_> = chain
                .iter()
                .map(|c| Certificate::from_der(c.as_ref()).to_owned())
                .collect();
            Some(ClientCert::new_with_certs(&chain, key))
        }
    };
    Ok(TlsConfig::builder()
        .root_certs(roots)
        .client_cert(client_cert)
        .build())
}

/// A TLS failure that retrying cannot fix: the server certificate is not
/// trusted or not valid for the host name, or the server refused our client
/// certificate. ureq reports rustls errors directly or wrapped in io::Error.
fn permanent_tls_failure(e: &ureq::Error) -> Option<String> {
    let rustls_err = match e {
        ureq::Error::Tls(m) => return Some((*m).to_string()),
        ureq::Error::Rustls(r) => Some(r),
        ureq::Error::Io(io) => io
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<rustls::Error>()),
        _ => None,
    }?;
    tls::is_certificate_error(rustls_err).then(|| match rustls_err {
        rustls::Error::AlertReceived(a) => {
            format!("server rejected the TLS handshake ({a:?}); check the client certificate")
        }
        other => format!("certificate verification failed: {other}"),
    })
}

fn request_failure(url: &str, e: ureq::Error) -> Step {
    match permanent_tls_failure(&e) {
        Some(detail) => Step::Fail(ReplayError::Tls {
            url: url.to_string(),
            detail,
        }),
        None => Step::Retry(e.to_string()),
    }
}

fn request<B>(req: ureq::RequestBuilder<B>, token: Option<&Token>) -> ureq::RequestBuilder<B> {
    match token {
        Some(t) => req.header("Authorization", format!("Bearer {}", t.expose())),
        None => req,
    }
}

fn read_body(resp: &mut ureq::http::Response<ureq::Body>) -> String {
    resp.body_mut()
        .with_config()
        .limit(4096)
        .read_to_string()
        .unwrap_or_default()
}

fn classify(status: u16, body: String) -> Result<Attempt, Step> {
    match status {
        201 => Ok(Attempt::Success { duplicate: false }),
        200 => Ok(Attempt::Success { duplicate: true }),
        401 | 403 => Err(Step::Fail(ReplayError::Unauthorized {
            endpoint: "/v1/events",
            status,
            body,
        })),
        408 | 429 | 500..=599 => Err(Step::Retry(format!("HTTP {status}: {body}"))),
        _ => Ok(Attempt::Permanent { status, body }),
    }
}

/// Sleep in small steps so a stop request is honored promptly.
fn sleep_interruptible(d: Duration, stop: &AtomicBool) -> bool {
    let end = Instant::now() + d;
    while Instant::now() < end {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        std::thread::sleep((end - Instant::now()).min(Duration::from_millis(50)));
    }
    !stop.load(Ordering::SeqCst)
}

/// Run `f` with bounded retries; Ok(None) means interrupted by stop.
fn with_retries<T>(
    cfg: &ReplayConfig,
    stop: &AtomicBool,
    retries: &mut u64,
    mut f: impl FnMut() -> Result<T, Step>,
) -> Result<Option<T>, ReplayError> {
    let mut delay = cfg.backoff_initial;
    let mut attempts = 0;
    loop {
        attempts += 1;
        match f() {
            Ok(v) => return Ok(Some(v)),
            Err(Step::Fail(e)) => return Err(e),
            Err(Step::Retry(e)) if attempts >= cfg.max_attempts => {
                return Err(ReplayError::Unavailable { attempts, last: e });
            }
            Err(Step::Retry(e)) => {
                *retries += 1;
                eprintln!(
                    "akhet: replay: transient failure ({e}); retry {attempts}/{} in {} ms",
                    cfg.max_attempts - 1,
                    delay.as_millis()
                );
                if !sleep_interruptible(delay, stop) {
                    return Ok(None);
                }
                delay = (delay * 2).min(cfg.backoff_max);
            }
        }
    }
}

fn fetch_identity(agent: &ureq::Agent, url: &str, token: Option<&Token>) -> Result<String, Step> {
    let mut resp = request(agent.get(format!("{url}/v1/identity")), token)
        .call()
        .map_err(|e| request_failure(url, e))?;
    let status = resp.status().as_u16();
    let body = read_body(&mut resp);
    let protocol = |m: String| Step::Fail(ReplayError::Protocol(m));
    match status {
        200 => {
            let v: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| Step::Retry(format!("identity response is not JSON: {e}")))?;
            match v.get("receiver_id").and_then(|r| r.as_str()) {
                Some(id) if !id.is_empty() => Ok(id.to_string()),
                _ => Err(protocol("identity response lacks receiver_id".into())),
            }
        }
        401 | 403 => Err(Step::Fail(ReplayError::Unauthorized {
            endpoint: "/v1/identity",
            status,
            body,
        })),
        408 | 429 | 500..=599 => Err(Step::Retry(format!("HTTP {status}"))),
        _ => Err(protocol(format!(
            "identity request failed: HTTP {status}: {body}"
        ))),
    }
}

/// Replay pending records. Blocking; run it on its own thread.
pub fn run(
    spool_path: &Path,
    max_db_bytes: u64,
    cfg: &ReplayConfig,
    faults: &Faults,
    stop: &AtomicBool,
) -> Result<ReplaySummary, ReplayError> {
    if cfg.sink.is_empty() || cfg.batch == 0 || cfg.max_attempts == 0 {
        return Err(ReplayError::Config(
            "sink must be non-empty; batch and max attempts must be positive".into(),
        ));
    }
    let url = normalize_url(&cfg.url)?;
    // Exclusive per sink, taken before touching the checkpoint.
    let _lock = SinkLock::acquire(spool_path, &cfg.sink)?;
    let mut spool = Spool::open(spool_path, max_db_bytes)?;
    let agent = agent(cfg, &url)?;
    let token = cfg.token.as_ref();
    let mut summary = ReplaySummary {
        sink: cfg.sink.clone(),
        destination_url: url.clone(),
        ..Default::default()
    };

    let identity = with_retries(cfg, stop, &mut summary.retries, || {
        fetch_identity(&agent, &url, token)
    })?;
    let Some(receiver_id) = identity else {
        summary.interrupted = true;
        return Ok(summary);
    };
    let sink: SinkRow = spool.bind_sink(&cfg.sink, &url, &receiver_id)?;
    summary.receiver_id = receiver_id;
    summary.checkpoint_seq = sink.checkpoint_seq;
    let events_url = format!("{url}/v1/events");

    'outer: loop {
        let batch = spool.read_after(summary.checkpoint_seq, cfg.batch)?;
        if batch.is_empty() {
            match cfg.follow {
                Some(interval) if sleep_interruptible(interval, stop) => continue,
                Some(_) => {
                    summary.interrupted = true;
                    break;
                }
                None => break,
            }
        }
        for rec in batch {
            if stop.load(Ordering::SeqCst) {
                summary.interrupted = true;
                break 'outer;
            }
            if cfg.limit.is_some_and(|l| summary.delivered >= l) {
                break 'outer;
            }
            faults.hit(point::REPLAY_BEFORE_SEND);
            let sha = hex::encode(rec.body_sha256);
            let attempt = with_retries(cfg, stop, &mut summary.retries, || {
                let r = request(agent.post(&events_url), token)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", &rec.event_id)
                    .header("Akhet-Body-SHA256", &sha)
                    .send(&rec.body[..]);
                let mut resp = r.map_err(|e| request_failure(&url, e))?;
                let status = resp.status().as_u16();
                let body = read_body(&mut resp);
                classify(status, body)
            })?;
            match attempt {
                None => {
                    summary.interrupted = true;
                    break 'outer;
                }
                Some(Attempt::Success { duplicate }) => {
                    faults.hit(point::REPLAY_AFTER_REMOTE_SUCCESS);
                    spool.advance_checkpoint(&sink, rec.seq)?;
                    faults.hit(point::REPLAY_AFTER_CHECKPOINT);
                    summary.checkpoint_seq = rec.seq;
                    summary.delivered += 1;
                    if duplicate {
                        summary.receiver_duplicates += 1;
                    }
                }
                Some(Attempt::Permanent { status, body }) => {
                    return Err(ReplayError::Rejected {
                        seq: rec.seq,
                        event_id: rec.event_id,
                        status,
                        body,
                    });
                }
            }
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_statuses_are_permanent_and_not_retried() {
        for status in [401, 403] {
            match classify(status, "no".into()) {
                Err(Step::Fail(ReplayError::Unauthorized { status: s, .. })) => {
                    assert_eq!(s, status)
                }
                _ => panic!("{status} must be a permanent auth failure"),
            }
        }
        assert!(matches!(classify(429, String::new()), Err(Step::Retry(_))));
        assert!(matches!(
            classify(409, String::new()),
            Ok(Attempt::Permanent { status: 409, .. })
        ));
    }

    #[test]
    fn token_is_validated_and_redacted() {
        let t = Token::new("abc.DEF-123_~+/=".into()).unwrap();
        assert_eq!(format!("{t:?}"), "Token(<redacted>)");
        let mut cfg = ReplayConfig::new("https://x", "s");
        cfg.token = Some(t);
        assert!(!format!("{cfg:?}").contains("abc.DEF"));
        for bad in ["", "a b", "a\r\nb", "tök"] {
            assert!(Token::new(bad.into()).is_err(), "{bad:?}");
        }
    }
}
