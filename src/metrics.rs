//! Optional observability endpoint: Prometheus metrics, liveness, readiness.
//!
//! Collection and replay update plain atomics (a relaxed load/add per event;
//! no locks on the hot path). The HTTP server runs on its own thread and only
//! reads those atomics, so a slow or stuck scraper cannot block collection or
//! fsync. Spool gauges (size, headroom, pending records per sink) need SQLite
//! queries; a separate sampler thread reads them through its own read-only
//! connection at a fixed interval rather than per scrape, because in
//! `journal_mode=DELETE` every reader holds a SHARED lock that makes a
//! concurrent commit wait. Scrapes then serve the last sample.
//!
//! Endpoints (`--metrics-listen ADDR`):
//! - `GET /metrics`: Prometheus text exposition format 0.0.4.
//! - `GET /healthz`: liveness. `200` while the process and this server thread
//!   run; it does not prove that collection is making progress.
//! - `GET /readyz`: readiness, `200` or `503` with the failing conditions in
//!   the body. Definitions per mode are in [`Metrics::readiness`].

use crate::spool::SpoolError;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Response, Server};

/// Spool pages under the limit below which the spool counts as full. Matches
/// the margin the spool uses to classify `SQLITE_FULL` as the size limit.
const CAPACITY_MARGIN_PAGES: i64 = 64;

/// Upper bounds (seconds) of the commit-latency histogram buckets.
pub const COMMIT_BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Mqtt,
    Modbus,
    Replay,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Mqtt => "mqtt",
            Mode::Modbus => "modbus",
            Mode::Replay => "replay",
        }
    }
}

/// MQTT collector counters. Also the source of the exit-time `MqttSummary`.
#[derive(Debug, Default)]
pub struct MqttCounters {
    pub received: AtomicU64,
    pub committed: AtomicU64,
    pub source_duplicates: AtomicU64,
    pub acks_enqueued: AtomicU64,
    pub acks_skipped: AtomicU64,
    pub saturation: AtomicU64,
    pub dropped: AtomicU64,
    pub connections: AtomicU64,
    pub connection_errors: AtomicU64,
    pub qos0: AtomicU64,
    /// Spool transactions used by the collector (group commit: one per batch).
    pub transactions: AtomicU64,
    /// Largest number of messages committed in one transaction.
    pub max_batch: AtomicU64,
    /// Messages whose topic matched none of the configured filters.
    pub unmatched: AtomicU64,
    /// Messages currently queued between the socket and the committer.
    pub queue_depth: AtomicU64,
    pub queue_capacity: AtomicU64,
    /// CONNACK received and no connection error since.
    pub connected: AtomicBool,
    /// The configured filter set is active (SUBACK with QoS 1, or already
    /// held by an existing session per the spool record).
    pub subscribed: AtomicBool,
}

#[derive(Debug, Default)]
pub struct ModbusCounters {
    pub polls_ok: AtomicU64,
    pub polls_failed: AtomicU64,
    pub connections: AtomicU64,
    /// 0 = no poll yet, 1 = last poll ok, 2 = last poll failed.
    pub last_poll: AtomicU64,
}

#[derive(Debug, Default)]
pub struct ReplayCounters {
    pub delivered: AtomicU64,
    pub receiver_duplicates: AtomicU64,
    pub retries: AtomicU64,
    pub checkpoint_seq: AtomicI64,
    /// Receiver identity fetched and the sink binding verified.
    pub bound: AtomicBool,
    /// The most recent HTTP attempt failed (transiently or permanently).
    pub last_attempt_failed: AtomicBool,
}

/// Fixed-bucket histogram over [`COMMIT_BUCKETS`].
#[derive(Debug, Default)]
pub struct Histogram {
    buckets: [AtomicU64; COMMIT_BUCKETS.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, d: Duration) {
        let s = d.as_secs_f64();
        if let Some(i) = COMMIT_BUCKETS.iter().position(|b| s <= *b) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Last spool sample taken by the sampler thread.
#[derive(Debug, Clone, Default)]
pub struct SpoolSample {
    pub taken_unix_s: f64,
    pub page_size: i64,
    pub page_count: i64,
    pub max_page_count: i64,
    pub last_seq: i64,
    /// (sink name, records with seq above its checkpoint).
    pub pending: Vec<(String, i64)>,
}

impl SpoolSample {
    pub fn db_bytes_used(&self) -> i64 {
        self.page_size * self.page_count
    }

    pub fn headroom_bytes(&self) -> i64 {
        (self.max_page_count - self.page_count).max(0) * self.page_size
    }

    pub fn at_capacity(&self) -> bool {
        self.max_page_count - self.page_count < CAPACITY_MARGIN_PAGES
    }
}

/// Shared metrics state for one `akhet` command.
pub struct Metrics {
    mode: Mode,
    max_db_bytes: u64,
    /// Replay sink name (labels replay lag).
    sink: Option<String>,
    pub mqtt: Arc<MqttCounters>,
    pub modbus: ModbusCounters,
    pub replay: ReplayCounters,
    pub commit_latency: Histogram,
    /// An append failed because the spool or filesystem is full.
    pub capacity_exhausted: AtomicBool,
    errors: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    spool: Mutex<Option<SpoolSample>>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

fn now_unix_s() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Stable error-kind label for a spool error.
pub fn spool_error_kind(e: &SpoolError) -> &'static str {
    match e {
        SpoolError::CapacityExhausted { .. } => "capacity",
        SpoolError::DiskFull(_) => "disk_full",
        SpoolError::Conflict { .. } => "conflict",
        SpoolError::Corrupt { .. } => "corrupt",
        SpoolError::EventTooLarge { .. } => "event_too_large",
        SpoolError::Format(_) => "format",
        _ => "other",
    }
}

/// Escape a Prometheus label value.
fn label(v: &str) -> String {
    let mut s = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => s.push_str("\\\\"),
            '"' => s.push_str("\\\""),
            '\n' => s.push_str("\\n"),
            c => s.push(c),
        }
    }
    s
}

fn head(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn single(out: &mut String, name: &str, kind: &str, help: &str, v: impl std::fmt::Display) {
    head(out, name, kind, help);
    let _ = writeln!(out, "{name} {v}");
}

fn load(a: &AtomicU64) -> u64 {
    a.load(Ordering::Relaxed)
}

fn flag(a: &AtomicBool) -> u8 {
    u8::from(a.load(Ordering::Relaxed))
}

impl Metrics {
    pub fn new(mode: Mode, max_db_bytes: u64, sink: Option<&str>) -> Arc<Metrics> {
        Arc::new(Metrics {
            mode,
            max_db_bytes,
            sink: sink.map(str::to_string),
            mqtt: Arc::default(),
            modbus: ModbusCounters::default(),
            replay: ReplayCounters::default(),
            commit_latency: Histogram::default(),
            capacity_exhausted: AtomicBool::new(false),
            errors: Mutex::new(BTreeMap::new()),
            spool: Mutex::new(None),
        })
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Count one error of `kind` in `component`. Errors are rare; a mutex is fine.
    pub fn error(&self, component: &'static str, kind: &'static str) {
        let mut m = self.errors.lock().unwrap_or_else(|p| p.into_inner());
        *m.entry((component, kind)).or_insert(0) += 1;
    }

    pub fn errors(&self, component: &str, kind: &str) -> u64 {
        let m = self.errors.lock().unwrap_or_else(|p| p.into_inner());
        m.iter()
            .filter(|((c, k), _)| *c == component && *k == kind)
            .map(|(_, v)| *v)
            .sum()
    }

    /// Record the outcome of one collector spool append.
    pub fn observe_append(&self, d: Duration, err: Option<&SpoolError>) {
        self.commit_latency.observe(d);
        if let Some(e) = err {
            if e.is_capacity() {
                self.capacity_exhausted.store(true, Ordering::Relaxed);
            }
            self.error("spool", spool_error_kind(e));
        }
    }

    pub fn spool_sample(&self) -> Option<SpoolSample> {
        self.spool.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn spool_full(&self) -> bool {
        self.capacity_exhausted.load(Ordering::Relaxed)
            || self.spool_sample().is_some_and(|s| s.at_capacity())
    }

    /// Readiness: `Ok(())` or the list of failing conditions.
    ///
    /// - mqtt: connected (CONNACK, no connection error since), configured
    ///   filter set active (SUBACK with QoS 1, or held by an existing session),
    ///   and the spool is not full.
    /// - modbus: the most recent poll succeeded and the spool is not full.
    /// - replay: the receiver identity was fetched and the sink binding
    ///   verified, and the most recent HTTP attempt did not fail.
    ///
    /// "Spool full": an append failed with a capacity or filesystem-full
    /// error, or the last spool sample shows fewer than 64 free pages under
    /// `--max-db-bytes`.
    pub fn readiness(&self) -> Result<(), Vec<&'static str>> {
        let mut why = Vec::new();
        match self.mode {
            Mode::Mqtt => {
                if !self.mqtt.connected.load(Ordering::Relaxed) {
                    why.push("mqtt not connected");
                }
                if !self.mqtt.subscribed.load(Ordering::Relaxed) {
                    why.push("mqtt subscription not confirmed");
                }
                if self.spool_full() {
                    why.push("spool at capacity");
                }
            }
            Mode::Modbus => {
                match self.modbus.last_poll.load(Ordering::Relaxed) {
                    0 => why.push("no modbus poll completed yet"),
                    2 => why.push("last modbus poll failed"),
                    _ => {}
                }
                if self.spool_full() {
                    why.push("spool at capacity");
                }
            }
            Mode::Replay => {
                if !self.replay.bound.load(Ordering::Relaxed) {
                    why.push("receiver identity not bound");
                }
                if self.replay.last_attempt_failed.load(Ordering::Relaxed) {
                    why.push("last replay attempt failed");
                }
            }
        }
        if why.is_empty() { Ok(()) } else { Err(why) }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut o = String::with_capacity(8192);
        head(
            &mut o,
            "akhet_info",
            "gauge",
            "Build and mode of this process.",
        );
        let _ = writeln!(
            o,
            "akhet_info{{mode=\"{}\",version=\"{}\"}} 1",
            self.mode.as_str(),
            env!("CARGO_PKG_VERSION")
        );
        single(
            &mut o,
            "akhet_ready",
            "gauge",
            "1 when /readyz reports ready.",
            u8::from(self.readiness().is_ok()),
        );
        match self.mode {
            Mode::Mqtt => self.render_mqtt(&mut o),
            Mode::Modbus => self.render_modbus(&mut o),
            Mode::Replay => self.render_replay(&mut o),
        }
        if self.mode != Mode::Replay {
            self.render_latency(&mut o);
        }
        self.render_spool(&mut o);
        head(
            &mut o,
            "akhet_errors_total",
            "counter",
            "Errors by component and kind.",
        );
        let errors = self
            .errors
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        for ((c, k), v) in errors {
            let _ = writeln!(
                o,
                "akhet_errors_total{{component=\"{c}\",kind=\"{k}\"}} {v}"
            );
        }
        o
    }

    fn render_mqtt(&self, o: &mut String) {
        let m = &self.mqtt;
        let counters: [(&str, &str, &AtomicU64); 12] = [
            (
                "akhet_mqtt_messages_received_total",
                "PUBLISH packets received from the broker.",
                &m.received,
            ),
            (
                "akhet_mqtt_messages_committed_total",
                "Messages durably committed to the spool as new records.",
                &m.committed,
            ),
            (
                "akhet_mqtt_source_duplicates_total",
                "Redeliveries recognized by source identity and acknowledged without a new record.",
                &m.source_duplicates,
            ),
            (
                "akhet_mqtt_commit_transactions_total",
                "Spool transactions used by the collector (group commit: one per batch).",
                &m.transactions,
            ),
            (
                "akhet_mqtt_unmatched_messages_total",
                "Messages whose topic matched none of the configured filters (stored with an empty subscription).",
                &m.unmatched,
            ),
            (
                "akhet_mqtt_acks_enqueued_total",
                "PUBACKs enqueued after commit.",
                &m.acks_enqueued,
            ),
            (
                "akhet_mqtt_acks_skipped_after_reconnect_total",
                "PUBACKs not sent because the message arrived on an earlier connection.",
                &m.acks_skipped,
            ),
            (
                "akhet_mqtt_queue_saturation_events_total",
                "Times the socket-to-spool queue was full and socket reads paused.",
                &m.saturation,
            ),
            (
                "akhet_mqtt_dropped_unacknowledged_total",
                "Received messages dropped uncommitted and unacknowledged at shutdown.",
                &m.dropped,
            ),
            (
                "akhet_mqtt_qos0_messages_total",
                "Messages delivered at QoS 0 (no redelivery guarantee).",
                &m.qos0,
            ),
            (
                "akhet_mqtt_connections_total",
                "Successful broker connections (CONNACK).",
                &m.connections,
            ),
            (
                "akhet_mqtt_connection_errors_total",
                "Connection errors (each followed by a reconnect attempt or stop).",
                &m.connection_errors,
            ),
        ];
        for (name, help, a) in counters {
            single(o, name, "counter", help, load(a));
        }
        single(
            o,
            "akhet_mqtt_max_batch_committed",
            "gauge",
            "Largest number of messages committed in one spool transaction so far.",
            load(&m.max_batch),
        );
        single(
            o,
            "akhet_mqtt_queue_depth",
            "gauge",
            "Messages queued between socket and spool.",
            load(&m.queue_depth),
        );
        single(
            o,
            "akhet_mqtt_queue_capacity",
            "gauge",
            "Capacity of the socket-to-spool queue.",
            load(&m.queue_capacity),
        );
        single(
            o,
            "akhet_mqtt_connected",
            "gauge",
            "1 while connected to the broker.",
            flag(&m.connected),
        );
        single(
            o,
            "akhet_mqtt_subscribed",
            "gauge",
            "1 while the QoS 1 subscription is confirmed on the current connection.",
            flag(&m.subscribed),
        );
    }

    fn render_modbus(&self, o: &mut String) {
        let m = &self.modbus;
        head(
            o,
            "akhet_modbus_polls_total",
            "counter",
            "Completed polls by outcome (ok or read_failed, as recorded in the event).",
        );
        let _ = writeln!(
            o,
            "akhet_modbus_polls_total{{status=\"ok\"}} {}",
            load(&m.polls_ok)
        );
        let _ = writeln!(
            o,
            "akhet_modbus_polls_total{{status=\"read_failed\"}} {}",
            load(&m.polls_failed)
        );
        single(
            o,
            "akhet_modbus_connections_total",
            "counter",
            "Modbus TCP connections established.",
            load(&m.connections),
        );
        single(
            o,
            "akhet_modbus_last_poll_ok",
            "gauge",
            "1 if the most recent poll succeeded.",
            u8::from(load(&m.last_poll) == 1),
        );
    }

    fn render_replay(&self, o: &mut String) {
        let r = &self.replay;
        single(
            o,
            "akhet_replay_delivered_total",
            "counter",
            "Records acknowledged by the receiver (201 or 200) and checkpointed.",
            load(&r.delivered),
        );
        single(
            o,
            "akhet_replay_receiver_duplicates_total",
            "counter",
            "Deliveries the receiver reported as already stored (200).",
            load(&r.receiver_duplicates),
        );
        single(
            o,
            "akhet_replay_retries_total",
            "counter",
            "Retries after transient failures (identity and event requests).",
            load(&r.retries),
        );
        single(
            o,
            "akhet_replay_checkpoint_seq",
            "gauge",
            "Spool sequence number of the sink checkpoint.",
            r.checkpoint_seq.load(Ordering::Relaxed),
        );
        single(
            o,
            "akhet_replay_receiver_bound",
            "gauge",
            "1 once the receiver identity is fetched and the sink binding verified.",
            flag(&r.bound),
        );
        single(
            o,
            "akhet_replay_last_attempt_failed",
            "gauge",
            "1 if the most recent HTTP attempt failed.",
            flag(&r.last_attempt_failed),
        );
        let lag = self.spool_sample().and_then(|s| {
            let sink = self.sink.as_deref()?;
            s.pending.iter().find(|(n, _)| n == sink).map(|(_, p)| *p)
        });
        if let Some(lag) = lag {
            single(
                o,
                "akhet_replay_lag_records",
                "gauge",
                "Records above this sink's checkpoint, as of the last spool sample.",
                lag,
            );
        }
    }

    fn render_latency(&self, o: &mut String) {
        let h = &self.commit_latency;
        let name = "akhet_spool_commit_duration_seconds";
        head(
            o,
            name,
            "histogram",
            "Duration of collector spool appends (one fsync'd transaction each), including waiting for the spool lock.",
        );
        let mut cum = 0;
        for (b, n) in COMMIT_BUCKETS.iter().zip(&h.buckets) {
            cum += load(n);
            let _ = writeln!(o, "{name}_bucket{{le=\"{b}\"}} {cum}");
        }
        let count = load(&h.count);
        let _ = writeln!(o, "{name}_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(o, "{name}_sum {}", load(&h.sum_ns) as f64 / 1e9);
        let _ = writeln!(o, "{name}_count {count}");
    }

    fn render_spool(&self, o: &mut String) {
        single(
            o,
            "akhet_spool_max_db_bytes",
            "gauge",
            "Configured database-size limit (--max-db-bytes).",
            self.max_db_bytes,
        );
        let Some(s) = self.spool_sample() else {
            return;
        };
        single(
            o,
            "akhet_spool_db_bytes_used",
            "gauge",
            "SQLite pages in use times page size, as of the last spool sample.",
            s.db_bytes_used(),
        );
        single(
            o,
            "akhet_spool_headroom_bytes",
            "gauge",
            "Bytes left under the database-size limit, as of the last spool sample. Not free disk space.",
            s.headroom_bytes(),
        );
        single(
            o,
            "akhet_spool_last_seq",
            "gauge",
            "Highest committed sequence number, as of the last spool sample.",
            s.last_seq,
        );
        head(
            o,
            "akhet_spool_pending_records",
            "gauge",
            "Records above each sink's checkpoint, as of the last spool sample.",
        );
        for (sink, n) in &s.pending {
            let _ = writeln!(
                o,
                "akhet_spool_pending_records{{sink=\"{}\"}} {n}",
                label(sink)
            );
        }
        single(
            o,
            "akhet_spool_sample_timestamp_seconds",
            "gauge",
            "Unix time of the last successful spool sample.",
            s.taken_unix_s,
        );
    }
}

/// Read spool gauges through a read-only connection. Holds a SHARED lock for
/// the duration of a few index lookups plus one range count per sink
/// (proportional to its pending records).
pub fn sample_spool(path: &Path, max_db_bytes: u64) -> Result<SpoolSample, rusqlite::Error> {
    use rusqlite::{Connection, OpenFlags};
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(1))?;
    let tx = conn.unchecked_transaction()?;
    let page_size: i64 = tx.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let page_count: i64 = tx.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let last_seq: i64 =
        tx.query_row("SELECT coalesce(max(seq), 0) FROM events", [], |r| r.get(0))?;
    let mut stmt = tx.prepare(
        "SELECT s.name, (SELECT count(*) FROM events e WHERE e.seq > s.checkpoint_seq)
         FROM sinks s ORDER BY s.name",
    )?;
    let pending = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(String, i64)>>>()?;
    drop(stmt);
    tx.finish()?;
    // Same computation as Spool::open (a read-only connection does not see
    // the writer's per-connection max_page_count).
    let max_page_count =
        i64::try_from((max_db_bytes / page_size.max(1) as u64).max(1)).unwrap_or(i64::MAX);
    Ok(SpoolSample {
        taken_unix_s: now_unix_s(),
        page_size,
        page_count,
        max_page_count,
        last_seq,
        pending,
    })
}

/// Running metrics server plus spool sampler. Stops when dropped.
pub struct MetricsServer {
    addr: Option<std::net::SocketAddr>,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl MetricsServer {
    /// Bind `listen`, take a first spool sample, and start the server and
    /// sampler threads.
    pub fn start(
        listen: &str,
        metrics: Arc<Metrics>,
        spool_path: PathBuf,
        sample_interval: Duration,
    ) -> Result<MetricsServer, String> {
        let server = Server::http(listen).map_err(|e| format!("bind {listen}: {e}"))?;
        let addr = server.server_addr().to_ip();
        let stop = Arc::new(AtomicBool::new(false));
        let take_sample = {
            let m = metrics.clone();
            move || match sample_spool(&spool_path, m.max_db_bytes) {
                Ok(s) => *m.spool.lock().unwrap_or_else(|p| p.into_inner()) = Some(s),
                Err(_) => m.error("metrics", "spool_sample"),
            }
        };
        take_sample();
        let s2 = stop.clone();
        let sampler = std::thread::Builder::new()
            .name("akhet-metrics-sampler".into())
            .spawn(move || {
                let mut next = Instant::now() + sample_interval;
                while !s2.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    if now >= next {
                        take_sample();
                        next = Instant::now() + sample_interval;
                    }
                    std::thread::sleep(
                        next.saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(100)),
                    );
                }
            })
            .map_err(|e| e.to_string())?;
        let s3 = stop.clone();
        let http = std::thread::Builder::new()
            .name("akhet-metrics-http".into())
            .spawn(move || serve(server, &metrics, &s3))
            .map_err(|e| e.to_string())?;
        Ok(MetricsServer {
            addr,
            stop,
            threads: vec![sampler, http],
        })
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.addr
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn text(status: u16, body: String, content_type: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_data(body.into_bytes())
        .with_status_code(status)
        .with_header(
            Header::from_bytes(&b"Content-Type"[..], content_type.as_bytes())
                .expect("static header"),
        )
}

fn serve(server: Server, metrics: &Metrics, stop: &AtomicBool) {
    const PLAIN: &str = "text/plain; charset=utf-8";
    while !stop.load(Ordering::Relaxed) {
        let req = match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(_) => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        let path = req.url().split('?').next().unwrap_or("");
        let resp = match (req.method(), path) {
            (Method::Get, "/metrics") => text(
                200,
                metrics.render(),
                "text/plain; version=0.0.4; charset=utf-8",
            ),
            (Method::Get, "/healthz") => text(200, "ok\n".into(), PLAIN),
            (Method::Get, "/readyz") => match metrics.readiness() {
                Ok(()) => text(200, "ready\n".into(), PLAIN),
                Err(why) => text(503, format!("not ready: {}\n", why.join("; ")), PLAIN),
            },
            _ => text(404, "not found\n".into(), PLAIN),
        };
        let _ = req.respond(resp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_cumulative() {
        let m = Metrics::new(Mode::Modbus, 1 << 20, None);
        m.commit_latency.observe(Duration::from_micros(300));
        m.commit_latency.observe(Duration::from_millis(3));
        m.commit_latency.observe(Duration::from_secs(60));
        let out = m.render();
        assert!(out.contains("akhet_spool_commit_duration_seconds_bucket{le=\"0.0005\"} 1\n"));
        assert!(out.contains("akhet_spool_commit_duration_seconds_bucket{le=\"0.005\"} 2\n"));
        assert!(out.contains("akhet_spool_commit_duration_seconds_bucket{le=\"10\"} 2\n"));
        assert!(out.contains("akhet_spool_commit_duration_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("akhet_spool_commit_duration_seconds_count 3\n"));
    }

    #[test]
    fn every_family_has_help_and_type() {
        for mode in [Mode::Mqtt, Mode::Modbus, Mode::Replay] {
            let m = Metrics::new(mode, 1 << 20, Some("s"));
            m.error("spool", "capacity");
            *m.spool.lock().unwrap() = Some(SpoolSample {
                page_size: 4096,
                page_count: 10,
                max_page_count: 256,
                pending: vec![("a\"b".into(), 3)],
                ..Default::default()
            });
            let out = m.render();
            for line in out.lines().filter(|l| !l.starts_with('#')) {
                let name = line.split(['{', ' ']).next().unwrap();
                let family = ["_bucket", "_sum", "_count"]
                    .iter()
                    .find_map(|s| name.strip_suffix(s))
                    .filter(|f| out.contains(&format!("# TYPE {f} histogram")))
                    .unwrap_or(name);
                assert!(name.starts_with("akhet_"), "{line}");
                assert!(out.contains(&format!("# HELP {family} ")), "{line}");
                assert!(out.contains(&format!("# TYPE {family} ")), "{line}");
            }
            assert!(out.contains("akhet_spool_pending_records{sink=\"a\\\"b\"} 3"));
            assert!(out.contains("akhet_spool_headroom_bytes 1007616\n"));
        }
    }

    #[test]
    fn readiness_rules() {
        let m = Metrics::new(Mode::Mqtt, 1 << 20, None);
        assert_eq!(m.readiness().unwrap_err().len(), 2);
        m.mqtt.connected.store(true, Ordering::Relaxed);
        m.mqtt.subscribed.store(true, Ordering::Relaxed);
        assert!(m.readiness().is_ok());
        m.capacity_exhausted.store(true, Ordering::Relaxed);
        assert_eq!(m.readiness().unwrap_err(), vec!["spool at capacity"]);

        let r = Metrics::new(Mode::Replay, 1 << 20, Some("s"));
        assert!(r.readiness().is_err());
        r.replay.bound.store(true, Ordering::Relaxed);
        assert!(r.readiness().is_ok());
        r.replay.last_attempt_failed.store(true, Ordering::Relaxed);
        assert!(r.readiness().is_err());
    }
}
