use akhet::adapters::modbus::{self, ModbusConfig};
use akhet::adapters::mqtt::{self, MqttConfig, SourceIdentityConfig, TlsSettings};
use akhet::adapters::{AsyncSpool, CollectError, Stop};
use akhet::faults::Faults;
use akhet::metrics::{Metrics, MetricsServer, Mode};
use akhet::receiver::Receiver;
use akhet::replay::{self, ReplayConfig, ReplayError, Token};
use akhet::spool::{self, PruneOptions, Spool, SpoolError};
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
    pub const AUTH: u8 = 8;
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

/// Optional observability endpoint (mqtt, modbus, replay).
#[derive(clap::Args)]
struct MetricsArgs {
    /// Serve /metrics (Prometheus), /healthz and /readyz on this address,
    /// e.g. 127.0.0.1:9464. Off by default; no authentication or TLS, so bind
    /// to loopback or a trusted network.
    #[arg(long)]
    metrics_listen: Option<String>,
    /// How often the metrics endpoint samples spool size and pending records.
    #[arg(long, default_value_t = 5000, requires = "metrics_listen")]
    metrics_spool_interval_ms: u64,
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
        #[command(flatten)]
        metrics: MetricsArgs,
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
        #[command(flatten)]
        metrics: MetricsArgs,
    },
    /// Replay committed records to an HTTP receiver.
    ///
    /// A bearer token is read from AKHET_REPLAY_TOKEN (never from arguments).
    Replay {
        /// Receiver base URL (akhet-replay-v1 protocol), http:// or https://.
        #[arg(long)]
        url: String,
        /// PEM CA bundle for https (default: platform trust store).
        #[arg(long)]
        ca_file: Option<PathBuf>,
        /// PEM client certificate chain for mutual TLS.
        #[arg(long, requires = "client_key")]
        client_cert: Option<PathBuf>,
        /// PEM private key for --client-cert.
        #[arg(long, requires = "client_cert")]
        client_key: Option<PathBuf>,
        /// Send AKHET_REPLAY_TOKEN even over plain http:// (cleartext).
        #[arg(long)]
        allow_insecure_token: bool,
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
        #[command(flatten)]
        metrics: MetricsArgs,
        /// With --follow: prune records acknowledged by every bound sink at
        /// most once per this interval (e.g. 10m). Off by default.
        #[arg(long, requires = "follow", value_parser = parse_duration)]
        prune_interval: Option<Duration>,
        /// Minimum record age for --prune-interval (e.g. 30m, 24h, 7d).
        #[arg(long, default_value = "24h", value_parser = parse_duration)]
        prune_min_age: Duration,
        /// Records per HTTP request when the receiver advertises the
        /// `batch-v1` extension (1 = one POST per record).
        #[arg(long, default_value_t = 1)]
        records_per_request: usize,
        /// Upper bound on one batch request body in bytes.
        #[arg(long, default_value_t = 8 << 20)]
        max_request_bytes: usize,
    },
    /// Delete records that every bound sink has acknowledged and that are
    /// older than --min-age. Never deletes anything if no sink is bound.
    Prune {
        /// Minimum age since append (e.g. 0s, 30m, 24h, 7d).
        #[arg(long, default_value = "24h", value_parser = parse_duration)]
        min_age: Duration,
        /// Report what would be deleted without deleting.
        #[arg(long)]
        dry_run: bool,
        /// Records deleted per transaction.
        #[arg(long, default_value_t = spool::DEFAULT_PRUNE_BATCH, value_parser = parse_batch)]
        batch_size: usize,
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
    /// Run the example HTTP receiver (stores into its own spool file).
    ///
    /// If AKHET_RECEIVER_TOKEN is set, every request must carry it as a
    /// bearer token.
    Receive {
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,
        /// Receiver database file.
        #[arg(long)]
        db: PathBuf,
        /// PEM certificate chain; serve HTTPS.
        #[arg(long, requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        /// PEM private key for --tls-cert.
        #[arg(long, requires = "tls_cert")]
        tls_key: Option<PathBuf>,
        /// PEM CA bundle; require client certificates issued by it (mTLS).
        #[arg(long, requires = "tls_cert")]
        client_ca: Option<PathBuf>,
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

/// Start the metrics endpoint if `--metrics-listen` was given.
fn start_metrics(
    args: &MetricsArgs,
    mode: Mode,
    cli_spool: &std::path::Path,
    max_db_bytes: u64,
    sink: Option<&str>,
) -> Result<(Option<Arc<Metrics>>, Option<MetricsServer>), ExitCode> {
    let Some(listen) = &args.metrics_listen else {
        return Ok((None, None));
    };
    let metrics = Metrics::new(mode, max_db_bytes, sink);
    let server = MetricsServer::start(
        listen,
        metrics.clone(),
        cli_spool.to_path_buf(),
        Duration::from_millis(args.metrics_spool_interval_ms.max(100)),
    )
    .map_err(|e| fail(2, format!("metrics endpoint: {e}")))?;
    eprintln!(
        "akhet: metrics on http://{} (/metrics, /healthz, /readyz; no auth/TLS)",
        server
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| listen.clone())
    );
    Ok((Some(metrics), Some(server)))
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
            metrics,
        } => {
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let (metrics, _server) =
                match start_metrics(&metrics, Mode::Mqtt, &cli.spool, cli.max_db_bytes, None) {
                    Ok(m) => m,
                    Err(code) => return code,
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
            let mut spool = AsyncSpool::new(spool, faults);
            if let Some(m) = metrics {
                spool = spool.with_metrics(m);
            }
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
            metrics,
        } => {
            let spool = match open() {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let (metrics, _server) =
                match start_metrics(&metrics, Mode::Modbus, &cli.spool, cli.max_db_bytes, None) {
                    Ok(m) => m,
                    Err(code) => return code,
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
            let mut spool = AsyncSpool::new(spool, faults);
            if let Some(m) = metrics {
                spool = spool.with_metrics(m);
            }
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
            ca_file,
            client_cert,
            client_key,
            allow_insecure_token,
            sink,
            timeout_ms,
            max_attempts,
            max_backoff_ms,
            follow,
            follow_interval_ms,
            limit,
            metrics,
            prune_interval,
            prune_min_age,
            records_per_request,
            max_request_bytes,
        } => {
            let (metrics, _server) = match start_metrics(
                &metrics,
                Mode::Replay,
                &cli.spool,
                cli.max_db_bytes,
                Some(&sink),
            ) {
                Ok(m) => m,
                Err(code) => return code,
            };
            let mut cfg = ReplayConfig::new(&url, &sink);
            cfg.prune = prune_interval.map(|i| {
                (
                    i,
                    PruneOptions {
                        min_age: prune_min_age,
                        ..Default::default()
                    },
                )
            });
            cfg.records_per_request = records_per_request;
            cfg.max_request_bytes = max_request_bytes;
            cfg.request_timeout = Duration::from_millis(timeout_ms);
            cfg.max_attempts = max_attempts;
            cfg.backoff_max = Duration::from_millis(max_backoff_ms);
            cfg.follow = follow.then(|| Duration::from_millis(follow_interval_ms));
            cfg.limit = limit;
            cfg.metrics = metrics;
            cfg.ca_file = ca_file;
            cfg.client_cert = client_cert.zip(client_key);
            cfg.allow_insecure_token = allow_insecure_token;
            cfg.token = match token_from_env("AKHET_REPLAY_TOKEN") {
                Ok(t) => t,
                Err(e) => return fail(2, e),
            };
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
                        ReplayError::Unauthorized { .. } | ReplayError::Tls { .. } => exit::AUTH,
                        ReplayError::Config(_) => 2,
                        _ => exit::ERROR,
                    };
                    fail(code, e)
                }
            }
        }
        Command::Prune {
            min_age,
            dry_run,
            batch_size,
        } => {
            let opts = PruneOptions {
                min_age,
                batch: batch_size,
                dry_run,
            };
            match open().and_then(|mut s| s.prune(&opts, &faults, &flag)) {
                Ok(report) => {
                    print_json(&report);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(spool_exit(&e), e),
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
        Command::Receive {
            listen,
            db,
            tls_cert,
            tls_key,
            client_ca,
        } => {
            let token = match token_from_env("AKHET_RECEIVER_TOKEN") {
                Ok(t) => t,
                Err(e) => return fail(2, e),
            };
            let tls = match tls_cert.zip(tls_key) {
                None => None,
                Some((cert, key)) => {
                    match akhet::tls::server_config(&cert, &key, client_ca.as_deref()) {
                        Ok(c) => Some(c),
                        Err(e) => return fail(2, e),
                    }
                }
            };
            let spool = match Spool::open_with_limits(&db, cli.max_db_bytes, cli.max_event_bytes) {
                Ok(s) => s,
                Err(e) => return fail(spool_exit(&e), e),
            };
            let mut receiver = match Receiver::bind(&listen, spool, faults) {
                Ok(r) => r,
                Err(e) => return fail(exit::ERROR, e),
            };
            let scheme = if tls.is_some() { "https" } else { "http" };
            let auth = match (&token, &tls, client_ca.is_some()) {
                (None, _, false) => "no authentication",
                (None, _, true) => "client certificates required",
                (Some(_), _, true) => "bearer token and client certificates required",
                (Some(_), Some(_), false) => "bearer token required",
                (Some(_), None, false) => "bearer token required, sent in cleartext over http",
            };
            if let Some(t) = &token {
                receiver = receiver.with_token(t.expose());
            }
            if let Some(c) = tls {
                receiver = receiver.with_tls(c);
            }
            eprintln!(
                "akhet: receiver {} listening on {scheme}://{} ({auth}; example endpoint)",
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

/// A bearer token from the environment; unset or empty means none.
fn token_from_env(var: &str) -> Result<Option<Token>, String> {
    match std::env::var(var) {
        Ok(s) if s.is_empty() => Ok(None),
        Ok(s) => Token::new(s).map(Some).map_err(|e| format!("{var}: {e}")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{var} is not valid UTF-8")),
    }
}

fn parse_batch(s: &str) -> Result<usize, String> {
    match s.parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("batch size must be a positive integer, got {s:?}")),
    }
}

/// Parse `<n>[s|m|h|d]` (no unit means seconds).
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid duration {s:?} (use e.g. 90s, 30m, 24h, 7d)"))?;
    let mult = match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => return Err(format!("invalid duration unit in {s:?} (use s, m, h or d)")),
    };
    n.checked_mul(mult)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration {s:?} is too large"))
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

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        for bad in ["", "h", "-1s", "1.5h", "3w", "1h30m"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }
}
