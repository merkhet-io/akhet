//! Replay committed spool records to an HTTP receiver.
//!
//! Protocol `akhet-replay-v1` (implemented by `akhet receive`):
//! - `GET {url}/v1/identity` -> `{"receiver_id": "<stable id>"}`
//! - `POST {url}/v1/events` with the stored event bytes as body and headers
//!   `Idempotency-Key: <event_id>` and `Akhet-Body-SHA256: <hex>`.
//!   - `201` stored, `200` identical duplicate already stored: success.
//!   - `409` same event_id with different bytes, `400`/`413`/`422` and other
//!     4xx except 408/429: permanent failure, replay stops.
//!   - `408`, `429`, `5xx`, connection errors and timeouts: transient, retried
//!     with bounded exponential backoff.
//!
//! The local checkpoint advances only after a success response. A crash or a
//! lost response after the receiver committed causes the same event_id to be
//! sent again; the receiver must deduplicate it. This is at-least-once delivery
//! with receiver-side deduplication, not exactly-once.

use crate::event;
use crate::faults::{Faults, point};
use crate::spool::{SinkRow, Spool, SpoolError};
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
    Transient(String),
    Permanent { status: u16, body: String },
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

fn agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .proxy(None)
        .build()
        .into()
}

fn read_body(resp: &mut ureq::http::Response<ureq::Body>) -> String {
    resp.body_mut()
        .with_config()
        .limit(4096)
        .read_to_string()
        .unwrap_or_default()
}

fn classify(status: u16, body: String) -> Attempt {
    match status {
        201 => Attempt::Success { duplicate: false },
        200 => Attempt::Success { duplicate: true },
        408 | 429 => Attempt::Transient(format!("HTTP {status}: {body}")),
        500..=599 => Attempt::Transient(format!("HTTP {status}: {body}")),
        _ => Attempt::Permanent { status, body },
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
    mut f: impl FnMut() -> Result<T, String>,
) -> Result<Option<T>, ReplayError> {
    let mut delay = cfg.backoff_initial;
    let mut attempts = 0;
    loop {
        attempts += 1;
        match f() {
            Ok(v) => return Ok(Some(v)),
            Err(e) if attempts >= cfg.max_attempts => {
                return Err(ReplayError::Unavailable { attempts, last: e });
            }
            Err(e) => {
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

fn fetch_identity(agent: &ureq::Agent, url: &str) -> Result<Result<String, String>, String> {
    let mut resp = agent
        .get(format!("{url}/v1/identity"))
        .call()
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let body = read_body(&mut resp);
    match status {
        200 => {
            let v: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| format!("identity response is not JSON: {e}"))?;
            match v.get("receiver_id").and_then(|r| r.as_str()) {
                Some(id) if !id.is_empty() => Ok(Ok(id.to_string())),
                _ => Ok(Err("identity response lacks receiver_id".into())),
            }
        }
        408 | 429 | 500..=599 => Err(format!("HTTP {status}")),
        _ => Ok(Err(format!(
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
    let agent = agent(cfg.request_timeout);
    let mut summary = ReplaySummary {
        sink: cfg.sink.clone(),
        destination_url: url.clone(),
        ..Default::default()
    };

    let identity = with_retries(cfg, stop, &mut summary.retries, || {
        fetch_identity(&agent, &url)
    })?;
    let receiver_id = match identity {
        None => {
            summary.interrupted = true;
            return Ok(summary);
        }
        Some(Ok(id)) => id,
        Some(Err(e)) => return Err(ReplayError::Protocol(e)),
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
                let r = agent
                    .post(&events_url)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", &rec.event_id)
                    .header("Akhet-Body-SHA256", &sha)
                    .send(&rec.body[..]);
                let mut resp = r.map_err(|e| e.to_string())?;
                let status = resp.status().as_u16();
                let body = read_body(&mut resp);
                match classify(status, body) {
                    Attempt::Transient(m) => Err(m),
                    other => Ok(other),
                }
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
                Some(Attempt::Transient(_)) => unreachable!("handled by with_retries"),
            }
        }
    }
    Ok(summary)
}
