use akhet::adapters::modbus::{self, ModbusConfig};
use akhet::adapters::mqtt::{self, MqttConfig, SourceIdentityConfig, TlsSettings};
use akhet::adapters::{AsyncSpool, CollectError, Stop};
use akhet::faults::Faults;
use akhet::receiver::Receiver;
use akhet::replay::{self, ReplayConfig, ReplayError};
use akhet::spool::{self, Spool, SpoolError};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Exit codes (see README).
mod exit {
    pub const ERROR: u8 = 1;
    pub const INTEGRITY: u8 = 3;
    pub const CAPACITY: u8 = 4;
    pub const REJECTED: u8 = 5;
    pub const UNAVAILABLE: u8 = 6;
    pub const LOCKED: u8 = 7;
}

#[derive(Parser)]
#[command(
    name = "akhet",
    version,
    about = "Durable MQTT / Modbus TCP collection with local spool and HTTP replay"
)]
struct Cli {
    /// Spool database file.
    #[arg(
        long,
        global = true,
        env = "AKHET_SPOOL",
        default_value = "data/akhet-spool.sqlite"
    )]
    spool: PathBuf,
    /// Database-size limit in bytes (SQLite pages). Not a disk-space guarantee.
    #[arg(long, global = true, default_value_t = spool::DEFAULT_MAX_DB_BYTES)]
    max_db_bytes: u64,
    /// Maximum encoded size of one event.
    #[arg(long, global = true, default_value_t = spool::DEFAULT_MAX_EVENT_BYTES)]
    max_event_bytes: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Collect MQTT messages (QoS 1, acknowledged after durable commit).
    Mqtt {
        #[arg(long, default_value = "localhost")]
        host: String,
        #[arg(long, default_value_t = 1883)]
        port: u16,
        /// Topic filter to subscribe to with QoS 1. Repeat for several filters
        /// (one SUBSCRIBE, one persistent session).
        #[arg(long, required = true)]
        topic: Vec<String>,
        /// Stable, unique client id (identifies the persistent broker session).
        #[arg(long)]
        client_id: String,
        /// Use TLS (platform trust store unless --tls-ca-file is given).
        #[arg(long)]
        tls: bool,
        #[arg(long)]
        tls_ca_file: Option<PathBuf>,
        #[arg(long, requires = "tls_client_key")]
        tls_client_cert: Option<PathBuf>,
        #[arg(long, requires = "tls_client_cert")]
        tls_client_key: Option<PathBuf>,
        #[arg(long, default_value_t = 30)]
        keep_alive_secs: u64,
        #[arg(long, default_value_t = 256 * 1024)]
        max_payload_bytes: usize,
        /// In-memory queue between network and spool (messages).
        #[arg(long, default_value_t = 64)]
        queue_capacity: usize,
        /// Group commit: most messages per spool transaction (1 = one
        /// transaction per message). Acknowledged only after that commit.
        #[arg(long, default_value_t = 64)]
        max_batch: usize,
        /// Group commit: after the first message, wait up to this long for
        /// more before committing (0 = only what is already queued).
        #[arg(long, default_value_t = 0)]
        max_linger_ms: u64,
        /// Namespace stating the meaning of --source-id-json-pointer.
        #[arg(long, requires = "source_id_json_pointer")]
        source_id_namespace: Option<String>,
        /// JSON pointer to a publisher-assigned message id in the payload.
        #[arg(long, requires = "source_id_namespace")]
        source_id_json_pointer: Option<String>,
        /// Stop after this many messages.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Poll Modbus TCP holding registers (function 0x03, read-only).
    Modbus {
        #[arg(long, default_value = "localhost")]
        host: String,
        #[arg(long, default_value_t = 502)]
        port: u16,
        /// Modbus unit identifier.
        #[arg(long, default_value_t = 1)]
        device_id: u8,
        /// Zero-based protocol address of the first holding register.
        #[arg(long)]
        address: u16,
        #[arg(long)]
        count: u16,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
        #[arg(long, default_value_t = 1000)]
        timeout_ms: u64,
        /// Stop after this many polls.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Replay committed records to an HTTP receiver.
    Replay {
        /// Receiver base URL (akhet-replay-v1 protocol).
        #[arg(long)]
        url: String,
        /// Sink name; its checkpoint is bound to this URL and receiver identity.
        #[arg(long)]
        sink: String,
        #[arg(long, default_value_t = 10_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 8)]
        max_attempts: u32,
        #[arg(long, default_value_t = 30_000)]
        max_backoff_ms: u64,
        /// Keep running and poll for new records.
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 1000)]
        follow_interval_ms: u64,
        #[arg(long)]
        limit: Option<u64>,
        /// Records per HTTP request when the receiver advertises the
        /// `batch-v1` extension (1 = one POST per record).
        #[arg(long, default_value_t = 1)]
        records_per_request: usize,
        /// Upper bound on one batch request body in bytes.
        #[arg(long, default_value_t = 8 << 20)]
        max_request_bytes: usize,
    },
    /// Replay decoded samples and context into MorletDB (PostgreSQL with
    /// pg_morlet). The connection string is read from AKHET_MORLET_URL only.
    Morlet {
        /// Sink name; its checkpoint is bound to the database identity.
        #[arg(long)]
        sink: String,
        /// Stream collection (`table` or `schema.table`).
        #[arg(long)]
        collection: String,
        /// Create the collection and its context tables if missing.
        #[arg(long)]
        create_collection: bool,
        /// Decoder config file (JSON) with timing and context_signals.
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 100)]
        records_per_tx: usize,
        /// Feature extractor for new recordings (`none` stores raw only).
        #[arg(long, default_value = "none")]
        extractor: String,
        #[arg(long, default_value_t = 512)]
        window_size: i32,
        #[arg(long, default_value_t = 256)]
        stride: i32,
        #[arg(long, default_value = "zscore")]
        normalization: String,
        /// Storage hint for the lossless chunk codec (see MorletDB docs).
        #[arg(long, default_value_t = 1.0)]
        sample_quantum: f64,
        #[arg(long, default_value_t = 8)]
        max_attempts: u32,
        #[arg(long, default_value_t = 30_000)]
        max_backoff_ms: u64,
        #[arg(long)]
        follow: bool,
        #[arg(long, default_value_t = 1000)]
        follow_interval_ms: u64,
        /// Stop after this many spool records.
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Show spool size, limits, record counts and sink checkpoints.
    Status,
    /// Print records as JSON lines (verifies checksums while reading).
    Inspect {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Verify SQLite structure and every record checksum and format version.
    Verify,
    /// Decode spooled MQTT payloads into per-channel sample batches (read-only).
    ///
    /// Prints one JSON line per MQTT record with its channel batches and any
    /// explicit decode issues, then a summary line. The spool is not modified.
    Decode {
        /// Decoder config file (JSON, see examples/decoder.json).
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        limit: Option<usize>,
        /// Include timestamp and value arrays (default: counts and ranges).
        #[arg(long)]
        samples: bool,
        /// Print only the summary line.
        #[arg(long, conflicts_with = "samples")]
        summary_only: bool,
    },
    /// Run the example HTTP receiver (stores into its own spool file).
    /// Place decoded samples on uniform sample grids (read-only).
    ///
    /// Prints recording starts/ends, timing issues and (with --chunks) chunks
    /// as JSON lines, then a summary with a timing report per channel. The
    /// streams are finished at the end of the selected records.
    Grid {
        /// Decoder config file (JSON); per-rule `timing` sets rate/tolerance.
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        limit: Option<usize>,
        /// Also print chunks with their timing evidence.
        #[arg(long)]
        chunks: bool,
        /// Include sample values in printed chunks.
        #[arg(long, requires = "chunks")]
        samples: bool,
    },
    /// Build context timelines (change events) from decoded context signals
    /// (read-only). Prints change events and issues as JSON lines, then a
    /// summary per channel.
    Context {
        /// Decoder config file (JSON); rules list `context_signals`.
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long)]
        limit: Option<usize>,
    },
    Receive {
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,
        /// Receiver database file.
        #[arg(long)]
        db: PathBuf,
    },
}

fn spool_exit(e: &SpoolError) -> u8 {
    match e {
        SpoolError::CapacityExhausted { .. } | SpoolError::DiskFull(_) => exit::CAPACITY,
        SpoolError::Corrupt { .. }
        | SpoolError::Conflict { .. }
        | SpoolError::Format(_)
        | SpoolError::NotASpool(..)
        | SpoolError::UnsupportedSchema { .. }
        | SpoolError::SinkBinding(_) => exit::INTEGRITY,
        _ => exit::ERROR,
    }
}

fn fail(code: u8, msg: impl std::fmt::Display) -> ExitCode {
    eprintln!("akhet: error: {msg}");
    ExitCode::from(code)
}

fn print_json(v: &impl serde::Serialize) {
    println!("{}", serde_json::to_string(v).unwrap_or_default());
}

/// Trigger `stop` on Ctrl-C or SIGTERM.
async fn watch_signals(stop: Stop, flag: Arc<AtomicBool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    eprintln!("akhet: shutdown requested");
    flag.store(true, Ordering::SeqCst);
    stop.trigger();
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let faults = match Faults::from_env() {
        Ok(f) => f,
        Err(e) => return fail(exit::ERROR, format!("AKHET_FAULT: {e}")),
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(4)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(exit::ERROR, e),
    };
    let stop = Stop::new();
    let flag = Arc::new(AtomicBool::new(false));
    let _signals = rt.spawn(watch_signals(stop.clone(), flag.clone()));

    let open = || Spool::open_with_limits(&cli.spool, cli.max_db_bytes, cli.max_event_bytes);

    match cli.command {
        Command::Mqtt {
            host,
            port,
            topic,
            client_id,
            tls,
            tls_ca_file,
            tls_client_cert,
            tls_client_key,
            keep_alive_secs,
            max_payload_bytes,
            queue_capacity,
            max_batch,
            max_linger_ms,
            source_id_namespace,
            source_id_json_pointer,
            limit,
        } => {
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let mut cfg = MqttConfig::with_topics(&host, port, &client_id, topic);
            cfg.username = std::env::var("AKHET_MQTT_USERNAME")
                .ok()
                .filter(|s| !s.is_empty());
            cfg.password = std::env::var("AKHET_MQTT_PASSWORD").ok();
            if tls || tls_ca_file.is_some() || tls_client_cert.is_some() {
                cfg.tls = Some(TlsSettings {
                    ca_file: tls_ca_file,
                    client_cert: tls_client_cert.zip(tls_client_key),
                });
            }
            cfg.keep_alive = Duration::from_secs(keep_alive_secs);
            cfg.max_payload_bytes = max_payload_bytes;
            cfg.queue_capacity = queue_capacity;
            cfg.max_batch = max_batch;
            cfg.max_linger = Duration::from_millis(max_linger_ms);
            cfg.source_identity =
                source_id_namespace
                    .zip(source_id_json_pointer)
                    .map(|(namespace, json_pointer)| SourceIdentityConfig {
                        namespace,
                        json_pointer,
                    });
            cfg.limit = limit;
            let spool = AsyncSpool::new(spool, faults);
            match rt.block_on(mqtt::run(cfg, spool, cli.max_event_bytes, stop)) {
                Ok(summary) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Err(e) => collect_fail(e),
            }
        }
        Command::Modbus {
            host,
            port,
            device_id,
            address,
            count,
            interval_ms,
            timeout_ms,
            limit,
        } => {
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let cfg = ModbusConfig {
                host,
                port,
                device_id,
                start_address: address,
                count,
                interval: Duration::from_millis(interval_ms),
                timeout: Duration::from_millis(timeout_ms),
                limit,
            };
            let spool = AsyncSpool::new(spool, faults);
            match rt.block_on(modbus::run(cfg, spool, stop)) {
                Ok(summary) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Err(e) => collect_fail(e),
            }
        }
        Command::Replay {
            url,
            sink,
            timeout_ms,
            max_attempts,
            max_backoff_ms,
            follow,
            follow_interval_ms,
            limit,
            records_per_request,
            max_request_bytes,
        } => {
            let mut cfg = ReplayConfig::new(&url, &sink);
            cfg.records_per_request = records_per_request;
            cfg.max_request_bytes = max_request_bytes;
            cfg.request_timeout = Duration::from_millis(timeout_ms);
            cfg.max_attempts = max_attempts;
            cfg.backoff_max = Duration::from_millis(max_backoff_ms);
            cfg.follow = follow.then(|| Duration::from_millis(follow_interval_ms));
            cfg.limit = limit;
            let path = cli.spool.clone();
            let max = cli.max_db_bytes;
            let f2 = flag.clone();
            let res = rt.block_on(async move {
                tokio::task::spawn_blocking(move || replay::run(&path, max, &cfg, &faults, &f2))
                    .await
            });
            match res {
                Err(e) => fail(exit::ERROR, e),
                Ok(Ok(summary)) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Ok(Err(e)) => {
                    let code = match &e {
                        ReplayError::Spool(s) => spool_exit(s),
                        ReplayError::Locked { .. } => exit::LOCKED,
                        ReplayError::Rejected { .. } | ReplayError::Protocol(_) => exit::REJECTED,
                        ReplayError::Unavailable { .. } => exit::UNAVAILABLE,
                        _ => exit::ERROR,
                    };
                    fail(code, e)
                }
            }
        }
        Command::Morlet {
            sink,
            collection,
            create_collection,
            config,
            records_per_tx,
            extractor,
            window_size,
            stride,
            normalization,
            sample_quantum,
            max_attempts,
            max_backoff_ms,
            follow,
            follow_interval_ms,
            limit,
        } => {
            let decoder = match akhet::decode::DecoderConfig::load(&config) {
                Ok(c) => c,
                Err(e) => return fail(2, e),
            };
            let Some(url) = std::env::var("AKHET_MORLET_URL")
                .ok()
                .filter(|s| !s.is_empty())
            else {
                return fail(
                    2,
                    "AKHET_MORLET_URL is not set (PostgreSQL connection string)",
                );
            };
            let mut cfg = akhet::morlet::MorletConfig::new(&url, &sink, &collection, decoder);
            cfg.create_collection = create_collection;
            cfg.records_per_tx = records_per_tx;
            cfg.extractor = extractor;
            cfg.window_size = window_size;
            cfg.stride = stride;
            cfg.normalization = normalization;
            cfg.sample_quantum = sample_quantum;
            cfg.max_attempts = max_attempts;
            cfg.backoff_max = Duration::from_millis(max_backoff_ms);
            cfg.follow = follow.then(|| Duration::from_millis(follow_interval_ms));
            cfg.limit = limit;
            let path = cli.spool.clone();
            let max = cli.max_db_bytes;
            let f2 = flag.clone();
            // The synchronous PostgreSQL client runs its own runtime: use a
            // plain thread, not the tokio blocking pool.
            let res =
                std::thread::spawn(move || akhet::morlet::run(&path, max, &cfg, &faults, &f2))
                    .join();
            match res {
                Err(_) => fail(exit::ERROR, "morlet sink thread panicked"),
                Ok(Ok(summary)) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Ok(Err(e)) => {
                    let code = match &e {
                        ReplayError::Spool(s) => spool_exit(s),
                        ReplayError::Locked { .. } => exit::LOCKED,
                        ReplayError::Rejected { .. } | ReplayError::Protocol(_) => exit::REJECTED,
                        ReplayError::Unavailable { .. } => exit::UNAVAILABLE,
                        ReplayError::Config(_) => 2,
                        _ => exit::ERROR,
                    };
                    fail(code, e)
                }
            }
        }
        Command::Status => match open().and_then(|s| s.status()) {
            Ok(st) => {
                println!("{}", serde_json::to_string_pretty(&st).unwrap_or_default());
                ExitCode::SUCCESS
            }
            Err(e) => fail(spool_exit(&e), e),
        },
        Command::Inspect { after, limit } => {
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let mut after = after;
            let mut left = limit.unwrap_or(usize::MAX);
            while left > 0 {
                let batch = match spool.read_after(after, left.min(256)) {
                    Ok(b) => b,
                    Err(e) => return fail(spool_exit(&e), e),
                };
                let Some(last) = batch.last() else { break };
                after = last.seq;
                left -= batch.len();
                for r in batch {
                    let ev: serde_json::Value = serde_json::from_slice(&r.body).unwrap_or_default();
                    print_json(&serde_json::json!({"seq": r.seq, "event": ev}));
                }
            }
            ExitCode::SUCCESS
        }
        Command::Verify => match open().and_then(|s| s.verify()) {
            Ok(n) => {
                print_json(&serde_json::json!({"verified_records": n, "status": "ok"}));
                ExitCode::SUCCESS
            }
            Err(e) => fail(spool_exit(&e), e),
        },
        Command::Decode {
            config,
            after,
            limit,
            samples,
            summary_only,
        } => {
            let cfg = match akhet::decode::DecoderConfig::load(&config) {
                Ok(c) => c,
                Err(e) => return fail(2, e),
            };
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            match decode_spool(&spool, &cfg, after, limit, samples, summary_only) {
                Ok(summary) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(spool_exit(&e), e),
            }
        }
        Command::Grid {
            config,
            after,
            limit,
            chunks,
            samples,
        } => {
            let cfg = match akhet::decode::DecoderConfig::load(&config) {
                Ok(c) => c,
                Err(e) => return fail(2, e),
            };
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            match grid_spool(&spool, &cfg, after, limit, chunks, samples) {
                Ok(summary) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(spool_exit(&e), e),
            }
        }
        Command::Context {
            config,
            after,
            limit,
        } => {
            let cfg = match akhet::decode::DecoderConfig::load(&config) {
                Ok(c) => c,
                Err(e) => return fail(2, e),
            };
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            match context_spool(&spool, &cfg, after, limit) {
                Ok(summary) => {
                    print_json(&summary);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(spool_exit(&e), e),
            }
        }
        Command::Receive { listen, db } => {
            let spool = match Spool::open_with_limits(&db, cli.max_db_bytes, cli.max_event_bytes) {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let receiver = match Receiver::bind(&listen, spool, faults) {
                Ok(r) => r,
                Err(e) => return fail(exit::ERROR, e),
            };
            eprintln!(
                "akhet: receiver {} listening on http://{} (example endpoint; no auth/TLS)",
                receiver.receiver_id(),
                receiver
                    .local_addr()
                    .map(|a| a.to_string())
                    .unwrap_or(listen)
            );
            let f2 = flag.clone();
            let _ = rt.block_on(async move {
                tokio::task::spawn_blocking(move || receiver.serve(&f2)).await
            });
            ExitCode::SUCCESS
        }
    }
}

#[derive(Default, serde::Serialize)]
struct DecodeSummary {
    summary: bool,
    records: u64,
    mqtt_records: u64,
    records_with_issues: u64,
    channel_batches: u64,
    samples: u64,
    context_batches: u64,
    context_observations: u64,
    issues_by_kind: std::collections::BTreeMap<String, u64>,
    last_seq: i64,
}

fn decode_spool(
    spool: &Spool,
    cfg: &akhet::decode::DecoderConfig,
    mut after: i64,
    limit: Option<usize>,
    samples: bool,
    summary_only: bool,
) -> Result<DecodeSummary, SpoolError> {
    let mut sum = DecodeSummary {
        summary: true,
        last_seq: after,
        ..Default::default()
    };
    let mut left = limit.unwrap_or(usize::MAX);
    while left > 0 {
        let batch = spool.read_after(after, left.min(256))?;
        let Some(last) = batch.last() else { break };
        after = last.seq;
        left -= batch.len();
        for r in batch {
            sum.records += 1;
            sum.last_seq = r.seq;
            let ev = akhet::event::decode(&r.body)?;
            let Some(d) = cfg.decode_event(&ev) else {
                continue;
            };
            sum.mqtt_records += 1;
            if !d.issues.is_empty() {
                sum.records_with_issues += 1;
            }
            for i in &d.issues {
                let k = serde_json::to_value(&i.kind)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_default();
                *sum.issues_by_kind.entry(k).or_default() += 1;
            }
            sum.channel_batches += d.batches.len() as u64;
            sum.context_batches += d.context.len() as u64;
            sum.context_observations +=
                d.context.iter().map(|b| b.values.len() as u64).sum::<u64>();
            sum.samples += d.batches.iter().map(|b| b.values.len() as u64).sum::<u64>();
            if summary_only {
                continue;
            }
            let batches: Vec<serde_json::Value> = d
                .batches
                .iter()
                .map(|b| {
                    if samples {
                        serde_json::to_value(b).unwrap_or_default()
                    } else {
                        serde_json::json!({
                            "channel": b.channel,
                            "unit": b.unit,
                            "samples": b.values.len(),
                            "first_unix_ns": b.timestamps_unix_ns.first(),
                            "last_unix_ns": b.timestamps_unix_ns.last(),
                        })
                    }
                })
                .collect();
            print_json(&serde_json::json!({
                "seq": r.seq,
                "event_id": r.event_id,
                "batches": batches,
                "context": d.context,
                "issues": d.issues,
            }));
        }
    }
    Ok(sum)
}

fn print_grid_output(seq: i64, o: &akhet::timing::GridOutput, chunks: bool, samples: bool) {
    use akhet::timing::GridOutput;
    match o {
        GridOutput::Chunk(c) if chunks => {
            let mut v = serde_json::to_value(o).unwrap_or_default();
            if !samples {
                v["values"] = serde_json::Value::Null;
                v["sample_count"] = c.values.len().into();
            }
            print_json(&serde_json::json!({"seq": seq, "output": v}));
        }
        GridOutput::Chunk(_) => {}
        other => print_json(&serde_json::json!({"seq": seq, "output": other})),
    }
}

fn grid_spool(
    spool: &Spool,
    cfg: &akhet::decode::DecoderConfig,
    mut after: i64,
    limit: Option<usize>,
    chunks: bool,
    samples: bool,
) -> Result<serde_json::Value, SpoolError> {
    use akhet::timing::ChannelGrid;
    use std::collections::BTreeMap;
    let mut grids: BTreeMap<akhet::decode::ChannelId, ChannelGrid> = BTreeMap::new();
    let mut decode_issues = 0u64;
    let mut last_seq = after;
    let mut left = limit.unwrap_or(usize::MAX);
    while left > 0 {
        let batch = spool.read_after(after, left.min(256))?;
        let Some(last) = batch.last() else { break };
        after = last.seq;
        left -= batch.len();
        for r in batch {
            last_seq = r.seq;
            let ev = akhet::event::decode(&r.body)?;
            let Some(d) = cfg.decode_event(&ev) else {
                continue;
            };
            decode_issues += d.issues.len() as u64;
            let akhet::event::EventData::MqttMessage(m) = &ev.data else {
                continue;
            };
            let spec = cfg
                .rule_for(&m.topic)
                .and_then(|r| r.timing.clone())
                .unwrap_or_default();
            for b in &d.batches {
                let g = grids.entry(b.channel.clone()).or_insert_with(|| {
                    ChannelGrid::new(b.channel.key(), spec.clone())
                        .expect("timing spec validated with the config")
                });
                for o in g.push(&b.timestamps_unix_ns, &b.values) {
                    print_grid_output(r.seq, &o, chunks, samples);
                }
            }
        }
    }
    let mut channels = Vec::new();
    for (id, mut g) in grids {
        for o in g.finish() {
            print_grid_output(last_seq, &o, chunks, samples);
        }
        channels.push(serde_json::json!({"channel": id, "report": g.report()}));
    }
    Ok(serde_json::json!({
        "summary": true,
        "last_seq": last_seq,
        "decode_issues": decode_issues,
        "channels": channels,
    }))
}

fn context_spool(
    spool: &Spool,
    cfg: &akhet::decode::DecoderConfig,
    mut after: i64,
    limit: Option<usize>,
) -> Result<serde_json::Value, SpoolError> {
    use akhet::context::ContextTracker;
    let mut trackers: std::collections::BTreeMap<akhet::decode::ChannelId, ContextTracker> =
        Default::default();
    let mut decode_issues = 0u64;
    let mut last_seq = after;
    let mut left = limit.unwrap_or(usize::MAX);
    while left > 0 {
        let batch = spool.read_after(after, left.min(256))?;
        let Some(last) = batch.last() else { break };
        after = last.seq;
        left -= batch.len();
        for r in batch {
            last_seq = r.seq;
            let ev = akhet::event::decode(&r.body)?;
            let Some(d) = cfg.decode_event(&ev) else {
                continue;
            };
            decode_issues += d.issues.len() as u64;
            for b in &d.context {
                let t = trackers
                    .entry(b.channel.clone())
                    .or_insert_with(|| ContextTracker::new(b.channel.clone()));
                for o in t.push(&b.timestamps_unix_ns, &b.values, ev.collected_unix_ns) {
                    print_json(&serde_json::json!({"seq": r.seq, "output": o}));
                }
            }
        }
    }
    let channels: Vec<serde_json::Value> = trackers
        .iter()
        .map(|(id, t)| serde_json::json!({"channel": id, "report": t.report()}))
        .collect();
    Ok(serde_json::json!({
        "summary": true,
        "last_seq": last_seq,
        "decode_issues": decode_issues,
        "channels": channels,
    }))
}

fn collect_fail(e: CollectError) -> ExitCode {
    let code = match &e {
        CollectError::Spool(s) => spool_exit(s),
        CollectError::Config(_) => 2,
        CollectError::Fatal(_) => exit::ERROR,
    };
    fail(code, e)
}
