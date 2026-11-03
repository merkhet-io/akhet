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
//!
//! With `--follow`, replay can also prune the spool periodically
//! ([`ReplayConfig::prune`]). Pruning uses the minimum checkpoint over *all*
//! sinks bound in the spool, not just this one, so a lagging sink holds back
//! deletion for every replayer.

//!
//! Optional extension `batch-v1` (compatible: a receiver that does not
//! advertise it only ever sees the requests above):
//! - The identity response may contain `"features": ["batch-v1", ...]`, and
//!   optionally `"batch_max_records"` and `"batch_max_bytes"` (the client
//!   stays below both).
//! - `POST {url}/v1/events/batch` with
//!   `Content-Type: application/vnd.akhet.event-batch.v1` and a body of N
//!   records, each `<event_id> <sha256-hex> <length>\n` followed by exactly
//!   `<length>` bytes of the stored event body and `\n`. Bodies are sent
//!   byte-exact, not re-encoded.
//! - The receiver checks every record exactly like a single POST, commits all
//!   records it accepts in ONE durable transaction and only then answers
//!   `200` with `{"results": [{"event_id": "...", "status_code": 201|200|409|
//!   400|413|422, ...}, ...]}`: one entry per record, in request order, with
//!   the single-POST meaning of each code. A record's failure does not stop
//!   the receiver from storing the others.
//! - A non-200 answer applies to the whole request: `408`, `429` and `5xx`
//!   (nothing stored) are retried like a single POST; other codes are
//!   permanent.
//! - The client advances its checkpoint only over the contiguous prefix of
//!   records with `201`/`200`. At the first other code it stops: permanent
//!   codes end replay as for a single POST (records after it may already be
//!   stored and are later acknowledged as duplicates); transient codes are
//!   retried from that record. A crash or lost response re-sends up to a whole
//!   batch, which the receiver deduplicates per record.
//!
//! Batching is opt-in (`records_per_request > 1`). Without it, or when the
//! receiver does not advertise `batch-v1`, every record is a single POST.

use crate::event;
use crate::faults::{Faults, point};
use crate::spool::{PruneOptions, SinkRow, Spool, SpoolError, StoredRecord};
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
    /// Prune acknowledged records at most once per interval (follow mode).
    pub prune: Option<(Duration, PruneOptions)>,
    /// Records per HTTP request when the receiver supports `batch-v1`.
    /// 1 (the default) always sends single POSTs.
    pub records_per_request: usize,
    /// Upper bound on one batch request body.
    pub max_request_bytes: usize,
}

/// Identity feature naming the batch extension.
pub const BATCH_FEATURE: &str = "batch-v1";
/// Content type of a `batch-v1` request body.
pub const BATCH_CONTENT_TYPE: &str = "application/vnd.akhet.event-batch.v1";

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
            prune: None,
            records_per_request: 1,
            max_request_bytes: 8 << 20,
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
    /// Records deleted by periodic pruning during this run.
    pub pruned_records: u64,
    /// True when records were sent with the `batch-v1` extension.
    pub batch_protocol: bool,
    /// HTTP requests that carried records (single or batch, without retries).
    pub requests: u64,
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

struct Identity {
    receiver_id: String,
    /// (max records, max bytes) when the receiver advertises `batch-v1`.
    batch: Option<(usize, usize)>,
}

fn fetch_identity(agent: &ureq::Agent, url: &str) -> Result<Result<Identity, String>, String> {
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
            let receiver_id = match v.get("receiver_id").and_then(|r| r.as_str()) {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => return Ok(Err("identity response lacks receiver_id".into())),
            };
            let has_batch = v
                .get("features")
                .and_then(|f| f.as_array())
                .is_some_and(|f| f.iter().any(|x| x.as_str() == Some(BATCH_FEATURE)));
            let limit = |k: &str| {
                v.get(k)
                    .and_then(|n| n.as_u64())
                    .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX))
            };
            Ok(Ok(Identity {
                receiver_id,
                batch: has_batch.then(|| (limit("batch_max_records"), limit("batch_max_bytes"))),
            }))
        }
        408 | 429 | 500..=599 => Err(format!("HTTP {status}")),
        _ => Ok(Err(format!(
            "identity request failed: HTTP {status}: {body}"
        ))),
    }
}

/// Bytes a record occupies in a `batch-v1` body, or None when its event_id
/// cannot be framed (contains whitespace or control characters).
fn framed_len(rec: &StoredRecord) -> Option<usize> {
    if rec.event_id.bytes().any(|b| b <= b' ' || b == 0x7f) {
        return None;
    }
    // id, space, 64 hex, space, decimal length, newline, body, newline
    Some(rec.event_id.len() + 1 + 64 + 1 + 20 + 1 + rec.body.len() + 1)
}

/// Encode records as a `batch-v1` request body.
pub fn encode_batch(records: &[StoredRecord]) -> Vec<u8> {
    let mut out = Vec::with_capacity(records.iter().filter_map(framed_len).sum());
    for r in records {
        out.extend_from_slice(
            format!(
                "{} {} {}\n",
                r.event_id,
                hex::encode(r.body_sha256),
                r.body.len()
            )
            .as_bytes(),
        );
        out.extend_from_slice(&r.body);
        out.push(b'\n');
    }
    out
}

/// Per-record entry of a batch response.
struct ItemResult {
    status: u16,
    detail: String,
}

fn is_transient(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

enum BatchAttempt {
    Results(Vec<ItemResult>),
    Permanent { status: u16, body: String },
    Protocol(String),
}

fn parse_batch_response(body: &str, sent: &[StoredRecord]) -> Result<Vec<ItemResult>, String> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("batch response is not JSON: {e}"))?;
    let results = v
        .get("results")
        .and_then(|r| r.as_array())
        .ok_or("batch response lacks a results array")?;
    if results.len() != sent.len() {
        return Err(format!(
            "batch response has {} results for {} records",
            results.len(),
            sent.len()
        ));
    }
    results
        .iter()
        .zip(sent)
        .map(|(r, rec)| {
            if r.get("event_id").and_then(|e| e.as_str()) != Some(rec.event_id.as_str()) {
                return Err(format!(
                    "batch response out of order at event {}",
                    rec.event_id
                ));
            }
            let status = r
                .get("status_code")
                .and_then(|s| s.as_u64())
                .and_then(|s| u16::try_from(s).ok())
                .ok_or("batch result lacks status_code")?;
            Ok(ItemResult {
                status,
                detail: r.to_string(),
            })
        })
        .collect()
}

/// Replay pending records. Blocking; run it on its own thread.
pub fn run(
    spool_path: &Path,
    max_db_bytes: u64,
    cfg: &ReplayConfig,
    faults: &Faults,
    stop: &AtomicBool,
) -> Result<ReplaySummary, ReplayError> {
    if cfg.sink.is_empty()
        || cfg.batch == 0
        || cfg.max_attempts == 0
        || cfg.records_per_request == 0
    {
        return Err(ReplayError::Config(
            "sink must be non-empty; batch, records per request and max attempts must be positive"
                .into(),
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
    let identity = match identity {
        None => {
            summary.interrupted = true;
            return Ok(summary);
        }
        Some(Ok(id)) => id,
        Some(Err(e)) => return Err(ReplayError::Protocol(e)),
    };
    let receiver_id = identity.receiver_id;
    // (records, bytes) per batch request, when both sides want batching.
    let batch_limits = identity
        .batch
        .filter(|_| cfg.records_per_request > 1)
        .map(|(r, b)| (r.min(cfg.records_per_request), b.min(cfg.max_request_bytes)));
    summary.batch_protocol = batch_limits.is_some();
    let sink: SinkRow = spool.bind_sink(&cfg.sink, &url, &receiver_id)?;
    summary.receiver_id = receiver_id;
    summary.checkpoint_seq = sink.checkpoint_seq;
    if sink.checkpoint_seq == 0
        && let Some(first) = spool.read_after(0, 1)?.first()
        && first.seq > 1
    {
        eprintln!(
            "akhet: replay: warning: sink {:?} starts at seq {}; records before it were pruned \
             and will not be delivered to this sink",
            cfg.sink, first.seq
        );
    }
    let events_url = format!("{url}/v1/events");
    let mut last_prune = Instant::now();
    let batch_url = format!("{url}/v1/events/batch");

    'outer: loop {
        if let Some((interval, opts)) = &cfg.prune
            && last_prune.elapsed() >= *interval
        {
            last_prune = Instant::now();
            let r = spool.prune(opts, faults, stop)?;
            if r.records > 0 {
                eprintln!(
                    "akhet: replay: pruned {} records (seq {}..={}); headroom {} pages",
                    r.records,
                    r.first_seq.unwrap_or(0),
                    r.last_seq.unwrap_or(0),
                    r.headroom_pages
                );
            }
            summary.pruned_records += r.records;
        }
        let batch = spool.read_after(
            summary.checkpoint_seq,
            cfg.batch.max(batch_limits.map_or(1, |(r, _)| r)),
        )?;
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
        let mut rest = &batch[..];
        while !rest.is_empty() {
            if stop.load(Ordering::SeqCst) {
                summary.interrupted = true;
                break 'outer;
            }
            if cfg.limit.is_some_and(|l| summary.delivered >= l) {
                break 'outer;
            }
            let n = batch_limits.map_or(0, |(max_records, max_bytes)| {
                let left = cfg
                    .limit
                    .map_or(usize::MAX, |l| (l - summary.delivered) as usize);
                let mut bytes = 0usize;
                rest.iter()
                    .take(max_records.min(left))
                    .take_while(|r| match framed_len(r) {
                        Some(len) if bytes + len <= max_bytes => {
                            bytes += len;
                            true
                        }
                        _ => false,
                    })
                    .count()
            });
            if n > 1 {
                let (chunk, tail) = rest.split_at(n);
                rest = tail;
                faults.hit(point::REPLAY_BEFORE_SEND);
                let body = encode_batch(chunk);
                let attempt = with_retries(cfg, stop, &mut summary.retries, || {
                    let r = agent
                        .post(&batch_url)
                        .header("Content-Type", BATCH_CONTENT_TYPE)
                        .send(&body[..]);
                    let mut resp = r.map_err(|e| e.to_string())?;
                    let status = resp.status().as_u16();
                    if status != 200 {
                        let body = read_body(&mut resp);
                        if is_transient(status) {
                            return Err(format!("HTTP {status}: {body}"));
                        }
                        return Ok(BatchAttempt::Permanent { status, body });
                    }
                    let text = resp
                        .body_mut()
                        .with_config()
                        .limit(16 << 20)
                        .read_to_string()
                        .map_err(|e| format!("reading batch response: {e}"))?;
                    match parse_batch_response(&text, chunk) {
                        // Nothing succeeded and the first record failed
                        // transiently: retry the whole request.
                        Ok(results) if is_transient(results[0].status) => Err(format!(
                            "first record of batch: HTTP {}: {}",
                            results[0].status, results[0].detail
                        )),
                        Ok(results) => Ok(BatchAttempt::Results(results)),
                        Err(e) => Ok(BatchAttempt::Protocol(e)),
                    }
                })?;
                let results = match attempt {
                    None => {
                        summary.interrupted = true;
                        break 'outer;
                    }
                    Some(BatchAttempt::Results(r)) => r,
                    Some(BatchAttempt::Protocol(e)) => return Err(ReplayError::Protocol(e)),
                    Some(BatchAttempt::Permanent { status, body }) => {
                        return Err(ReplayError::Rejected {
                            seq: chunk[0].seq,
                            event_id: chunk[0].event_id.clone(),
                            status,
                            body,
                        });
                    }
                };
                summary.requests += 1;
                // Advance over the contiguous prefix of successes only.
                let ok = results
                    .iter()
                    .take_while(|r| matches!(r.status, 200 | 201))
                    .count();
                if ok > 0 {
                    faults.hit(point::REPLAY_AFTER_REMOTE_SUCCESS);
                    spool.advance_checkpoint(&sink, chunk[ok - 1].seq)?;
                    faults.hit(point::REPLAY_AFTER_CHECKPOINT);
                    summary.checkpoint_seq = chunk[ok - 1].seq;
                    summary.delivered += ok as u64;
                    summary.receiver_duplicates +=
                        results[..ok].iter().filter(|r| r.status == 200).count() as u64;
                }
                if let Some(failed) = results.get(ok) {
                    if is_transient(failed.status) {
                        // Resend from the failed record (counted as a retry).
                        summary.retries += 1;
                        continue 'outer;
                    }
                    return Err(ReplayError::Rejected {
                        seq: chunk[ok].seq,
                        event_id: chunk[ok].event_id.clone(),
                        status: failed.status,
                        body: failed.detail.clone(),
                    });
                }
                continue;
            }
            let rec = &rest[0];
            rest = &rest[1..];
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
                    summary.requests += 1;
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
                        event_id: rec.event_id.clone(),
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
