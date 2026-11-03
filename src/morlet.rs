//! Replay sink into PostgreSQL with the `pg_morlet` extension (MorletDB).
//!
//! Pipeline per spooled MQTT record: decode (`decode`) -> grid placement per
//! waveform channel (`timing`) -> recordings and chunks in a MorletDB stream
//! collection; context signals (`context`) -> interval rows in the
//! collection's context timeline.
//!
//! Exactness: the sink keeps all derived state (grid state per channel,
//! context timelines, which context rows were written) in its own table in
//! the target database and updates it in the **same transaction** as the
//! samples, context rows and its spool checkpoint. After any crash or
//! connection loss it reloads that state and resumes from the committed
//! checkpoint, so no sample is lost or written twice. The local spool
//! checkpoint is advanced after the database commit; if it lags (crash in
//! between), it is moved forward from the database on the next start.
//!
//! Retries of an append that already committed are identical and accepted by
//! MorletDB (`morlet_append_chunk` verifies identical retries); a conflicting
//! append is a permanent error.

use crate::context::{ContextOutput, ContextTracker};
use crate::decode::DecoderConfig;
use crate::event;
use crate::faults::{Faults, point};
use crate::replay::{ReplayError, SinkLock};
use crate::spool::Spool;
use crate::timing::{ChannelGrid, GridOutput};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Version of the sink state stored in `akhet_morlet_sinks.state`.
pub const STATE_VERSION: u32 = 1;
/// Closed context intervals retained per key for recordings that start later.
const RETAINED_INTERVALS: usize = 64;

#[derive(Debug, Clone)]
pub struct MorletConfig {
    /// libpq-style connection string or URL. Never logged.
    pub conninfo: String,
    pub sink: String,
    /// Stream collection, `table` or `schema.table`.
    pub collection: String,
    /// Create the collection (and its context tables) if missing.
    pub create_collection: bool,
    pub decoder: DecoderConfig,
    /// Spool records per database transaction.
    pub records_per_tx: usize,
    pub window_size: i32,
    pub stride: i32,
    pub normalization: String,
    pub extractor: String,
    pub sample_quantum: f64,
    pub max_attempts: u32,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub follow: Option<Duration>,
    pub limit: Option<u64>,
}

impl MorletConfig {
    pub fn new(conninfo: &str, sink: &str, collection: &str, decoder: DecoderConfig) -> Self {
        MorletConfig {
            conninfo: conninfo.into(),
            sink: sink.into(),
            collection: collection.into(),
            create_collection: false,
            decoder,
            records_per_tx: 100,
            window_size: 512,
            stride: 256,
            normalization: "zscore".into(),
            extractor: "none".into(),
            sample_quantum: 1.0,
            max_attempts: 8,
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            follow: None,
            limit: None,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct MorletSummary {
    pub sink: String,
    pub destination: String,
    pub database_id: String,
    pub records: u64,
    pub transactions: u64,
    pub recordings_started: u64,
    pub chunks: u64,
    pub samples: u64,
    pub context_rows: u64,
    pub issues: u64,
    pub retries: u64,
    pub checkpoint_seq: i64,
    pub interrupted: bool,
}

/// Placement of a recording in time, for projecting context onto it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecSpan {
    anchor_ns: i64,
    rate_hz: f64,
    /// Sample count once the recording has ended.
    ended_samples: Option<u64>,
    last_unix_ns: i64,
    /// Per context key: sample offset up to which rows were written.
    written: BTreeMap<String, u64>,
}

impl RecSpan {
    /// First sample at or after `t`.
    fn sample_at(&self, t: i64) -> u64 {
        let x = ((t - self.anchor_ns) as f64 * self.rate_hz / 1e9).ceil();
        if x <= 0.0 { 0 } else { x as u64 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Interval {
    start_ns: i64,
    end_ns: Option<i64>,
    value: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SourceCtx {
    recordings: BTreeMap<String, RecSpan>,
    intervals: BTreeMap<String, Vec<Interval>>,
    latest_context_ns: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SinkState {
    version: u32,
    grids: BTreeMap<String, ChannelGrid>,
    trackers: BTreeMap<String, ContextTracker>,
    sources: BTreeMap<String, SourceCtx>,
}

#[derive(Debug, Serialize)]
struct ContextRow {
    recording_id: String,
    key: String,
    value: Value,
    start_sample: u64,
    end_sample: u64,
}

impl SourceCtx {
    /// Project intervals onto recordings; returns new rows.
    fn sync(&mut self) -> Vec<ContextRow> {
        let mut rows = Vec::new();
        for (rid, rec) in self.recordings.iter_mut() {
            for (key, ivs) in &self.intervals {
                for iv in ivs {
                    let s = rec.sample_at(iv.start_ns);
                    let e = match (iv.end_ns, rec.ended_samples) {
                        (Some(end), Some(n)) => rec.sample_at(end).min(n),
                        (Some(end), None) => rec.sample_at(end),
                        (None, Some(n)) => n,
                        (None, None) => continue,
                    };
                    let done = rec.written.get(key).copied().unwrap_or(0);
                    let s = s.max(done);
                    if e > s {
                        rows.push(ContextRow {
                            recording_id: rid.clone(),
                            key: key.clone(),
                            value: iv.value.clone(),
                            start_sample: s,
                            end_sample: e,
                        });
                        rec.written.insert(key.clone(), e);
                    }
                }
            }
        }
        // Forget ended recordings once context time has passed their end
        // (context arrives in time order, so nothing more can apply).
        if let Some(latest) = self.latest_context_ns {
            self.recordings
                .retain(|_, r| r.ended_samples.is_none() || r.last_unix_ns >= latest);
        }
        // Keep the most recent closed intervals for recordings whose samples
        // arrive later than the context (their anchors may precede them).
        for ivs in self.intervals.values_mut() {
            let closed = ivs.iter().filter(|i| i.end_ns.is_some()).count();
            let excess = closed.saturating_sub(RETAINED_INTERVALS);
            if excess > 0 {
                ivs.drain(..excess);
            }
        }
        rows
    }
}

#[derive(Debug, thiserror::Error)]
enum TxError {
    #[error("{0}")]
    Transient(String),
    #[error(transparent)]
    Fatal(#[from] ReplayError),
}

fn classify(e: postgres::Error) -> TxError {
    let transient = e.is_closed()
        || e.code().is_some_and(|c| {
            let s = c.code();
            s.starts_with("08")
                || s.starts_with("57P")
                || s == "40001"
                || s == "40P01"
                || s == "53300"
        })
        || e.code().is_none();
    let msg = match e.as_db_error() {
        Some(d) => format!("{}: {}", d.code().code(), d.message()),
        None => e.to_string(),
    };
    if transient {
        TxError::Transient(msg)
    } else {
        TxError::Fatal(ReplayError::Protocol(format!(
            "MorletDB rejected the write: {msg}"
        )))
    }
}

/// `host:port/dbname` from the connection string, without credentials.
fn display_destination(conninfo: &str, collection: &str) -> Result<String, ReplayError> {
    let cfg: postgres::Config = conninfo
        .parse()
        .map_err(|e| ReplayError::Config(format!("invalid MorletDB connection string: {e}")))?;
    let host = cfg
        .get_hosts()
        .first()
        .map(|h| match h {
            postgres::config::Host::Tcp(s) => s.clone(),
            #[cfg(unix)]
            postgres::config::Host::Unix(p) => p.display().to_string(),
        })
        .unwrap_or_else(|| "localhost".into());
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let db = cfg.get_dbname().unwrap_or("");
    Ok(format!("morletdb://{host}:{port}/{db}#{collection}"))
}

const SETUP_SQL: &str = "
CREATE TABLE IF NOT EXISTS akhet_morlet_identity(
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  database_id text NOT NULL);
CREATE TABLE IF NOT EXISTS akhet_morlet_sinks(
  sink text PRIMARY KEY,
  collection text NOT NULL,
  spool_id text NOT NULL,
  checkpoint_seq bigint NOT NULL,
  state jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS akhet_morlet_evidence(
  collection text NOT NULL,
  recording_id text NOT NULL,
  start_sample bigint NOT NULL,
  sample_count integer NOT NULL,
  first_unix_ns bigint NOT NULL,
  last_unix_ns bigint NOT NULL,
  max_abs_deviation_ns float8 NOT NULL,
  rms_deviation_ns float8 NOT NULL,
  spool_seq bigint NOT NULL,
  PRIMARY KEY(collection, recording_id, start_sample));
CREATE TABLE IF NOT EXISTS akhet_morlet_issues(
  sink text NOT NULL,
  spool_seq bigint NOT NULL,
  ordinal integer NOT NULL,
  source text NOT NULL,
  detail jsonb NOT NULL,
  PRIMARY KEY(sink, spool_seq, ordinal));
";

struct Session<'a> {
    cfg: &'a MorletConfig,
    client: postgres::Client,
}

impl<'a> Session<'a> {
    fn connect(cfg: &'a MorletConfig) -> Result<Self, TxError> {
        let client = postgres::Client::connect(&cfg.conninfo, postgres::NoTls).map_err(classify)?;
        Ok(Session { cfg, client })
    }

    /// Create helper tables (and optionally the collection); return the
    /// database identity.
    fn setup(&mut self) -> Result<String, TxError> {
        let mut tx = self.client.transaction().map_err(classify)?;
        tx.batch_execute(SETUP_SQL).map_err(classify)?;
        tx.execute(
            "INSERT INTO akhet_morlet_identity(database_id) VALUES($1) ON CONFLICT DO NOTHING",
            &[&uuid::Uuid::new_v4().to_string()],
        )
        .map_err(classify)?;
        let id: String = tx
            .query_one("SELECT database_id FROM akhet_morlet_identity", &[])
            .map_err(classify)?
            .get(0);
        let exists: bool = tx
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&self.cfg.collection],
            )
            .map_err(classify)?
            .get(0);
        if !exists {
            if !self.cfg.create_collection {
                return Err(TxError::Fatal(ReplayError::Config(format!(
                    "collection {:?} does not exist (use --create-collection, or create it with morlet_create_stream_collection and morlet_enable_context)",
                    self.cfg.collection
                ))));
            }
            let (schema, name) = match self.cfg.collection.split_once('.') {
                Some((s, n)) => (s.to_string(), n.to_string()),
                None => ("public".to_string(), self.cfg.collection.clone()),
            };
            tx.execute(
                "SELECT morlet_create_stream_collection($1, $2)",
                &[&schema, &name],
            )
            .map_err(classify)?;
            tx.execute(
                "SELECT morlet_enable_context($1::text::regclass)",
                &[&self.cfg.collection],
            )
            .map_err(classify)?;
        }
        tx.commit().map_err(classify)?;
        Ok(id)
    }

    /// Load (or create) the sink row. Returns (checkpoint, state).
    fn load(&mut self, spool_id: &str) -> Result<(i64, SinkState), TxError> {
        let mut tx = self.client.transaction().map_err(classify)?;
        let initial = serde_json::to_value(SinkState {
            version: STATE_VERSION,
            ..Default::default()
        })
        .expect("state serializes");
        tx.execute(
            "INSERT INTO akhet_morlet_sinks(sink, collection, spool_id, checkpoint_seq, state)
             VALUES($1,$2,$3,0,$4) ON CONFLICT(sink) DO NOTHING",
            &[&self.cfg.sink, &self.cfg.collection, &spool_id, &initial],
        )
        .map_err(classify)?;
        let row = tx
            .query_one(
                "SELECT collection, spool_id, checkpoint_seq, state FROM akhet_morlet_sinks WHERE sink=$1",
                &[&self.cfg.sink],
            )
            .map_err(classify)?;
        tx.commit().map_err(classify)?;
        let (collection, sid): (String, String) = (row.get(0), row.get(1));
        if sid != spool_id {
            return Err(TxError::Fatal(ReplayError::Spool(
                crate::spool::SpoolError::SinkBinding(format!(
                    "MorletDB sink {:?} belongs to spool {sid}, not this spool ({spool_id}); use a new sink name",
                    self.cfg.sink
                )),
            )));
        }
        if collection != self.cfg.collection {
            return Err(TxError::Fatal(ReplayError::Spool(
                crate::spool::SpoolError::SinkBinding(format!(
                    "MorletDB sink {:?} writes to collection {collection:?}, not {:?}; use a new sink name",
                    self.cfg.sink, self.cfg.collection
                )),
            )));
        }
        let state: SinkState = serde_json::from_value(row.get::<_, Value>(3)).map_err(|e| {
            TxError::Fatal(ReplayError::Protocol(format!("unreadable sink state: {e}")))
        })?;
        if state.version != STATE_VERSION {
            return Err(TxError::Fatal(ReplayError::Protocol(format!(
                "sink state version {} is not supported (this build: {STATE_VERSION})",
                state.version
            ))));
        }
        Ok((row.get(2), state))
    }
}

/// Counters for one transaction.
#[derive(Default)]
struct TxStats {
    recordings: u64,
    chunks: u64,
    samples: u64,
    context_rows: u64,
    issues: u64,
}

#[allow(clippy::too_many_arguments)]
fn write_batch(
    sess: &mut Session,
    state: &mut SinkState,
    expected_checkpoint: i64,
    records: &[crate::spool::StoredRecord],
    faults: &Faults,
    stats: &mut TxStats,
) -> Result<(), TxError> {
    let cfg = sess.cfg;
    let coll = cfg.collection.as_str();
    let mut tx = sess.client.transaction().map_err(classify)?;
    let cp: i64 = tx
        .query_one(
            "SELECT checkpoint_seq FROM akhet_morlet_sinks WHERE sink=$1 FOR UPDATE",
            &[&cfg.sink],
        )
        .map_err(classify)?
        .get(0);
    if cp != expected_checkpoint {
        return Err(TxError::Fatal(ReplayError::Protocol(format!(
            "MorletDB sink {:?} checkpoint moved to {cp} (expected {expected_checkpoint}); another writer?",
            cfg.sink
        ))));
    }
    let mut touched: Vec<String> = Vec::new();
    for rec in records {
        let ev =
            event::decode(&rec.body).map_err(|e| TxError::Fatal(ReplayError::Spool(e.into())))?;
        let Some(d) = cfg.decoder.decode_event(&ev) else {
            continue;
        };
        let mut ordinal = 0i32;
        let mut issue = |tx: &mut postgres::Transaction,
                         stats: &mut TxStats,
                         src: &str,
                         v: Value|
         -> Result<(), TxError> {
            tx.execute(
                "INSERT INTO akhet_morlet_issues(sink, spool_seq, ordinal, source, detail) VALUES($1,$2,$3,$4,$5)",
                &[&cfg.sink, &rec.seq, &ordinal, &src, &v],
            )
            .map_err(classify)?;
            ordinal += 1;
            stats.issues += 1;
            Ok(())
        };
        for i in &d.issues {
            issue(
                &mut tx,
                stats,
                "decode",
                serde_json::to_value(i).unwrap_or_default(),
            )?;
        }
        let spec = match &ev.data {
            event::EventData::MqttMessage(m) => cfg
                .decoder
                .rule_for(&m.topic)
                .and_then(|r| r.timing.clone())
                .unwrap_or_default(),
            _ => Default::default(),
        };
        for b in &d.batches {
            let key = b.channel.key();
            let grid = match state.grids.get_mut(&key) {
                Some(g) => g,
                None => {
                    let g = ChannelGrid::new(key.clone(), spec.clone())
                        .map_err(|e| TxError::Fatal(ReplayError::Config(e)))?;
                    state.grids.entry(key.clone()).or_insert(g)
                }
            };
            let outputs = grid.push(&b.timestamps_unix_ns, &b.values);
            let src = state.sources.entry(b.channel.source.clone()).or_default();
            if !touched.contains(&b.channel.source) {
                touched.push(b.channel.source.clone());
            }
            for o in outputs {
                match o {
                    GridOutput::Start(s) => {
                        let mut meta = b.metadata.clone();
                        meta.insert(
                            "akhet".into(),
                            json!({
                                "source": b.channel.source,
                                "group": b.channel.group,
                                "signal": b.channel.signal,
                                "anchor_unix_ns": s.anchor_unix_ns,
                                "rate_estimated": s.rate_estimated,
                                "tolerance_ns": s.tolerance_ns,
                                "discontinuity": s.discontinuity,
                            }),
                        );
                        tx.execute(
                            "SELECT morlet_ensure_recording($1::text::regclass,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                            &[
                                &coll,
                                &s.recording,
                                &s.rate_hz,
                                &key,
                                &b.unit,
                                &Value::Object(meta),
                                &cfg.window_size,
                                &cfg.stride,
                                &cfg.normalization,
                                &cfg.extractor,
                                &cfg.sample_quantum,
                            ],
                        )
                        .map_err(classify)?;
                        src.recordings.insert(
                            s.recording.clone(),
                            RecSpan {
                                anchor_ns: s.anchor_unix_ns,
                                rate_hz: s.rate_hz,
                                ended_samples: None,
                                last_unix_ns: s.anchor_unix_ns,
                                written: BTreeMap::new(),
                            },
                        );
                        stats.recordings += 1;
                    }
                    GridOutput::Chunk(c) => {
                        tx.execute(
                            "SELECT morlet_append_chunk($1::text::regclass,$2,$3,$4)",
                            &[&coll, &c.recording, &(c.start_sample as i64), &c.values],
                        )
                        .map_err(classify)?;
                        tx.execute(
                            "INSERT INTO akhet_morlet_evidence VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                             ON CONFLICT DO NOTHING",
                            &[
                                &coll,
                                &c.recording,
                                &(c.start_sample as i64),
                                &(c.values.len() as i32),
                                &c.evidence.first_unix_ns,
                                &c.evidence.last_unix_ns,
                                &c.evidence.max_abs_deviation_ns,
                                &c.evidence.rms_deviation_ns,
                                &rec.seq,
                            ],
                        )
                        .map_err(classify)?;
                        if let Some(r) = src.recordings.get_mut(&c.recording) {
                            r.last_unix_ns = c.evidence.last_unix_ns;
                        }
                        stats.chunks += 1;
                        stats.samples += c.values.len() as u64;
                    }
                    GridOutput::End {
                        recording,
                        samples,
                        last_unix_ns,
                    } => {
                        if let Some(r) = src.recordings.get_mut(&recording) {
                            r.ended_samples = Some(samples);
                            r.last_unix_ns = last_unix_ns;
                        }
                    }
                    GridOutput::Issue(i) => {
                        let mut v = serde_json::to_value(&i).unwrap_or_default();
                        v["channel"] = json!(b.channel);
                        issue(&mut tx, stats, "timing", v)?;
                    }
                }
            }
        }
        for b in &d.context {
            let key = b.channel.key();
            let tracker = state
                .trackers
                .entry(key)
                .or_insert_with(|| ContextTracker::new(b.channel.clone()));
            let outputs = tracker.push(&b.timestamps_unix_ns, &b.values, ev.collected_unix_ns);
            let src = state.sources.entry(b.channel.source.clone()).or_default();
            if !touched.contains(&b.channel.source) {
                touched.push(b.channel.source.clone());
            }
            if let Some(&t) = b.timestamps_unix_ns.last() {
                src.latest_context_ns = Some(src.latest_context_ns.map_or(t, |l| l.max(t)));
            }
            for o in outputs {
                match o {
                    ContextOutput::Change(c) => {
                        let ivs = src.intervals.entry(b.channel.signal.clone()).or_default();
                        if let Some(open) = ivs.last_mut()
                            && open.end_ns.is_none()
                        {
                            open.end_ns = Some(c.unix_ns);
                        }
                        ivs.push(Interval {
                            start_ns: c.unix_ns,
                            end_ns: None,
                            value: c.value,
                        });
                    }
                    ContextOutput::Issue(i) => {
                        issue(
                            &mut tx,
                            stats,
                            "context",
                            serde_json::to_value(&i).unwrap_or_default(),
                        )?;
                    }
                }
            }
        }
    }
    let mut rows = Vec::new();
    for s in &touched {
        if let Some(src) = state.sources.get_mut(s) {
            rows.extend(src.sync());
        }
    }
    if !rows.is_empty() {
        let first = records.first().map_or(0, |r| r.seq);
        let last = records.last().map_or(0, |r| r.seq);
        let batch_id = format!("akhet:{}:{first}-{last}", cfg.sink);
        tx.execute(
            "SELECT morlet_add_context($1::text::regclass,$2,$3)",
            &[
                &coll,
                &batch_id,
                &serde_json::to_value(&rows).unwrap_or_default(),
            ],
        )
        .map_err(classify)?;
        stats.context_rows += rows.len() as u64;
    }
    let last_seq = records.last().map_or(expected_checkpoint, |r| r.seq);
    let state_json = serde_json::to_value(&*state).expect("state serializes");
    tx.execute(
        "UPDATE akhet_morlet_sinks SET checkpoint_seq=$2, state=$3, updated_at=now() WHERE sink=$1",
        &[&cfg.sink, &last_seq, &state_json],
    )
    .map_err(classify)?;
    faults.hit(point::REPLAY_BEFORE_SEND);
    tx.commit().map_err(classify)?;
    Ok(())
}

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

/// Replay spooled records into MorletDB. Blocking; run it on its own thread
/// (the synchronous PostgreSQL client starts its own runtime).
pub fn run(
    spool_path: &Path,
    max_db_bytes: u64,
    cfg: &MorletConfig,
    faults: &Faults,
    stop: &AtomicBool,
) -> Result<MorletSummary, ReplayError> {
    if cfg.sink.is_empty() || cfg.records_per_tx == 0 || cfg.max_attempts == 0 {
        return Err(ReplayError::Config(
            "sink must be non-empty; records per transaction and max attempts must be positive"
                .into(),
        ));
    }
    let destination = display_destination(&cfg.conninfo, &cfg.collection)?;
    let _lock = SinkLock::acquire(spool_path, &cfg.sink)?;
    let mut spool = Spool::open(spool_path, max_db_bytes)?;
    let mut summary = MorletSummary {
        sink: cfg.sink.clone(),
        destination: destination.clone(),
        ..Default::default()
    };
    let mut attempts = 0u32;
    let mut delay = cfg.backoff_initial;
    // Outer loop: (re)connect, reload state from the database, resume.
    loop {
        match session_loop(&mut spool, cfg, faults, stop, &destination, &mut summary) {
            Ok(()) => return Ok(summary),
            Err(TxError::Fatal(e)) => return Err(e),
            Err(TxError::Transient(m)) => {
                attempts += 1;
                if attempts >= cfg.max_attempts {
                    return Err(ReplayError::Unavailable { attempts, last: m });
                }
                summary.retries += 1;
                eprintln!(
                    "akhet: morlet: transient failure ({m}); retry {attempts}/{} in {} ms",
                    cfg.max_attempts - 1,
                    delay.as_millis()
                );
                if !sleep_interruptible(delay, stop) {
                    summary.interrupted = true;
                    return Ok(summary);
                }
                delay = (delay * 2).min(cfg.backoff_max);
            }
        }
    }
}

fn session_loop(
    spool: &mut Spool,
    cfg: &MorletConfig,
    faults: &Faults,
    stop: &AtomicBool,
    destination: &str,
    summary: &mut MorletSummary,
) -> Result<(), TxError> {
    let mut sess = Session::connect(cfg)?;
    let db_id = sess.setup()?;
    summary.database_id = db_id.clone();
    let sink_row = spool
        .bind_sink(&cfg.sink, destination, &db_id)
        .map_err(|e| TxError::Fatal(e.into()))?;
    let (mut checkpoint, mut state) = sess.load(spool.spool_id())?;
    if checkpoint < sink_row.checkpoint_seq {
        return Err(TxError::Fatal(ReplayError::Protocol(format!(
            "MorletDB checkpoint {checkpoint} is behind the local checkpoint {} for sink {:?} \
             (database restored or rolled back?); refusing to continue",
            sink_row.checkpoint_seq, cfg.sink
        ))));
    }
    if checkpoint > sink_row.checkpoint_seq {
        // Crash after the database commit, before the local checkpoint.
        spool
            .advance_checkpoint(&sink_row, checkpoint)
            .map_err(|e| TxError::Fatal(e.into()))?;
    }
    summary.checkpoint_seq = checkpoint;
    loop {
        if stop.load(Ordering::SeqCst) {
            summary.interrupted = true;
            return Ok(());
        }
        let mut n = cfg.records_per_tx;
        if let Some(l) = cfg.limit {
            let left = l.saturating_sub(summary.records);
            if left == 0 {
                return Ok(());
            }
            n = n.min(left as usize);
        }
        let batch = spool
            .read_after(checkpoint, n)
            .map_err(|e| TxError::Fatal(e.into()))?;
        let Some(last) = batch.last().map(|r| r.seq) else {
            match cfg.follow {
                Some(i) if sleep_interruptible(i, stop) => continue,
                Some(_) => {
                    summary.interrupted = true;
                    return Ok(());
                }
                None => return Ok(()),
            }
        };
        let mut next = state.clone();
        let mut stats = TxStats::default();
        // On error nothing from this transaction is kept: the caller
        // reconnects and reloads the committed state from the database.
        write_batch(&mut sess, &mut next, checkpoint, &batch, faults, &mut stats)?;
        faults.hit(point::REPLAY_AFTER_REMOTE_SUCCESS);
        spool
            .advance_checkpoint(&sink_row, last)
            .map_err(|e| TxError::Fatal(e.into()))?;
        faults.hit(point::REPLAY_AFTER_CHECKPOINT);
        state = next;
        checkpoint = last;
        summary.checkpoint_seq = last;
        summary.records += batch.len() as u64;
        summary.transactions += 1;
        summary.recordings_started += stats.recordings;
        summary.chunks += stats.chunks;
        summary.samples += stats.samples;
        summary.context_rows += stats.context_rows;
        summary.issues += stats.issues;
    }
}
