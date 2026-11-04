//! Throughput benchmark: MQTT group commit and batched replay.
//!
//!     cargo run --release --locked --example throughput
//!     cargo run --release --locked --example throughput -- --runs 1 --quick
//!
//! Everything runs on loopback in one process: the embedded rumqttd broker
//! (in-memory), a QoS 1 publisher, the MQTT collector (a current-thread Tokio
//! runtime with at most 4 blocking threads, like the CLI) writing to a spool
//! in a temporary directory, and the example HTTP receiver. Spools are created
//! in the system temp directory unless `--dir` is given; put it on the disk
//! you want to measure.
//!
//! MQTT: the publisher sends N messages as fast as the broker accepts them,
//! so the collector is saturated; throughput is N divided by the time from
//! the first publish to the last spool commit. "Commit latency" is the wall
//! time of one spool transaction (inserts, COMMIT and fsync). `max_batch 1` is
//! one transaction per message (the behaviour before group commit). The paced
//! run publishes at the nominal rate of the source (10 messages/s) instead.
//!
//! Replay: the spool of the saturated group-commit run is replayed to a fresh
//! receiver with single POSTs and with the `batch-v1` extension.
//!
//! Spool size: the daq run with group commit, once with payloads stored as
//! base64 (event format version 1, the default) and once as raw bytes
//! (`--payload-storage raw`, format version 2). Reports spool bytes per
//! sample and checks that both spools return the published payloads bit for
//! bit.
//!
//! Payload "daq": 8 channels x 4000 samples per 100 ms as JSON (40 kHz per
//! channel), integer samples in -32768..=32767; about 200 KB per message.
//! Payload "small": a JSON object of about 40 bytes. Development tooling only.

#[path = "../tests/support/broker.rs"]
mod broker;

use akhet::adapters::mqtt::{self, MqttConfig};
use akhet::adapters::{AsyncSpool, CommitStat, Stop};
use akhet::event::{EventData, PayloadStorage};
use akhet::faults::Faults;
use akhet::receiver::Receiver;
use akhet::replay::{self, ReplayConfig};
use akhet::spool::{DEFAULT_MAX_DB_BYTES, DEFAULT_MAX_EVENT_BYTES, Spool};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CHANNELS: usize = 8;
const SAMPLES: usize = 4000;
const MAX_DB: u64 = 8 * DEFAULT_MAX_DB_BYTES;

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Daq,
    Small,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Daq => "daq 8x4000 JSON",
            Shape::Small => "small JSON",
        }
    }
}

/// Deterministic 8 x 4000 integer samples as JSON arrays (shared by all
/// messages; only the sequence number changes).
fn daq_channels() -> String {
    let mut x: u32 = 0x2545_f491;
    let mut out = String::from("[");
    for c in 0..CHANNELS {
        if c > 0 {
            out.push(',');
        }
        out.push('[');
        for s in 0..SAMPLES {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            if s > 0 {
                out.push(',');
            }
            out.push_str(&((x % 65536) as i32 - 32768).to_string());
        }
        out.push(']');
    }
    out.push(']');
    out
}

fn payload(shape: Shape, channels: &str, seq: usize) -> Vec<u8> {
    match shape {
        Shape::Daq => format!(
            r#"{{"seq":{seq},"rate_hz":40000,"samples_per_channel":{SAMPLES},"channels":{channels}}}"#
        )
        .into_bytes(),
        Shape::Small => format!(r#"{{"seq":{seq},"sensor":"s-01","value":{}}}"#, seq % 1000)
            .into_bytes(),
    }
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e3)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct MqttResult {
    messages: usize,
    payload_bytes: usize,
    elapsed: Duration,
    publish_elapsed: Duration,
    spool_bytes: u64,
    commits: Vec<CommitStat>,
}

/// Publish `n` messages (paced at `interval` if given) while a collector with
/// `max_batch` commits them; return when all are committed.
#[allow(clippy::too_many_arguments)]
fn run_mqtt(
    broker_port: u16,
    dir: &Path,
    tag: &str,
    shape: Shape,
    n: usize,
    max_batch: usize,
    interval: Option<Duration>,
    storage: PayloadStorage,
) -> MqttResult {
    let db = dir.join(format!("{tag}.sqlite"));
    let spool = Spool::open(&db, MAX_DB).unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let aspool = AsyncSpool::new(spool, Faults::none()).with_commit_log(log.clone());
    let topic_root = format!("bench/{tag}");
    let mut cfg = MqttConfig::new(
        "127.0.0.1",
        broker_port,
        &format!("akhet-bench-{tag}"),
        &format!("{topic_root}/#"),
    );
    cfg.max_payload_bytes = 512 * 1024;
    cfg.max_batch = max_batch;
    cfg.payload_storage = storage;
    cfg.limit = Some(n as u64);
    let ready = Arc::new(tokio::sync::Notify::new());
    cfg.ready = Some(ready.clone());

    // Collector on its own current-thread runtime, like the CLI.
    let collector = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(4)
            .build()
            .unwrap();
        rt.block_on(mqtt::run(cfg, aspool, DEFAULT_MAX_EVENT_BYTES, Stop::new()))
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let channels = if shape == Shape::Daq {
        daq_channels()
    } else {
        String::new()
    };
    let (t0, publish_elapsed, payload_bytes) = rt.block_on(async move {
        tokio::time::timeout(Duration::from_secs(20), ready.notified())
            .await
            .expect("collector subscribed");
        let mut o = MqttOptions::new(format!("bench-pub-{tag}"), "127.0.0.1", broker_port);
        o.set_max_packet_size(4 << 20, 4 << 20);
        o.set_keep_alive(Duration::from_secs(30));
        let (client, mut el) = AsyncClient::new(o, 256);
        let topic = format!("{topic_root}/data");
        let payloads: Vec<Vec<u8>> = (0..n).map(|i| payload(shape, &channels, i)).collect();
        let bytes: usize = payloads.iter().map(Vec::len).sum();
        let t0 = Instant::now();
        let publisher = tokio::spawn(async move {
            let mut tick = interval.map(tokio::time::interval);
            for p in payloads {
                if let Some(t) = tick.as_mut() {
                    t.tick().await;
                }
                client
                    .publish(topic.as_str(), QoS::AtLeastOnce, false, p)
                    .await
                    .unwrap();
            }
            client
        });
        let mut acked = 0;
        while acked < n {
            match el.poll().await {
                Ok(Event::Incoming(Packet::PubAck(_))) => acked += 1,
                Ok(_) => {}
                Err(e) => panic!("publisher: {e}"),
            }
        }
        let published = t0.elapsed();
        let client = publisher.await.unwrap();
        let _ = client.disconnect().await;
        let _ = tokio::time::timeout(Duration::from_millis(500), el.poll()).await;
        (t0, published, bytes)
    });
    let summary = collector.join().unwrap().expect("collector");
    assert_eq!(summary.committed as usize, n, "{summary:?}");
    let commits = std::mem::take(&mut *log.lock().unwrap());
    let last = commits.last().expect("commits").finished;
    MqttResult {
        messages: n,
        payload_bytes,
        elapsed: last - t0,
        publish_elapsed,
        spool_bytes: std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0),
        commits,
    }
}

struct ReplayResult {
    records: u64,
    bytes: u64,
    elapsed: Duration,
    requests: u64,
}

fn run_replay(spool: &Path, dir: &Path, tag: &str, records_per_request: usize) -> ReplayResult {
    let rx_db = dir.join(format!("{tag}-rx.sqlite"));
    let receiver = Receiver::bind(
        "127.0.0.1:0",
        Spool::open(&rx_db, MAX_DB).unwrap(),
        Faults::none(),
    )
    .unwrap();
    let url = format!("http://{}", receiver.local_addr().unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let s2 = stop.clone();
    let server = std::thread::spawn(move || receiver.serve(&s2));
    let bytes: u64 = Spool::open(spool, MAX_DB)
        .unwrap()
        .read_after(0, usize::MAX >> 1)
        .unwrap()
        .iter()
        .map(|r| r.body.len() as u64)
        .sum();
    let mut cfg = ReplayConfig::new(&url, tag);
    cfg.records_per_request = records_per_request;
    let t0 = Instant::now();
    let summary = replay::run(
        spool,
        MAX_DB,
        &cfg,
        &Faults::none(),
        &AtomicBool::new(false),
    )
    .expect("replay");
    let elapsed = t0.elapsed();
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    server.join().unwrap();
    ReplayResult {
        records: summary.delivered,
        bytes,
        elapsed,
        requests: summary.requests,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mut runs = 3;
    let mut quick = false;
    let mut dir: Option<PathBuf> = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--runs" => runs = args.next().and_then(|s| s.parse().ok()).expect("--runs N"),
            "--quick" => quick = true,
            "--dir" => dir = Some(args.next().expect("--dir PATH").into()),
            other => panic!("unknown argument {other:?}"),
        }
    }
    let tmp = match &dir {
        Some(d) => tempfile::tempdir_in(d).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let dir = tmp.path();
    let port = free_port();
    broker::start_broker(port);
    let scale = if quick { 5 } else { 1 };
    eprintln!("spool directory: {}", dir.display());

    println!("## MQTT ingest (saturated publisher), median of {runs} run(s)\n");
    println!(
        "| payload | max_batch | messages | msg/s | payload MB/s | samples/s | spool MB/s | \
         transactions | mean batch | commit p50 ms | commit p99 ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    let mut replay_sources: Vec<(Shape, PathBuf)> = Vec::new();
    for shape in [Shape::Daq, Shape::Small] {
        let n = match shape {
            Shape::Daq => 300 / scale,
            Shape::Small => 2000 / scale,
        };
        for max_batch in [1, 64] {
            let mut rates = Vec::new();
            let mut mbps = Vec::new();
            let mut spool_mbps = Vec::new();
            let mut latencies = Vec::new();
            let mut txns = Vec::new();
            for run in 0..runs {
                let tag = format!(
                    "{}-b{max_batch}-r{run}",
                    if shape == Shape::Daq { "daq" } else { "small" }
                );
                let r = run_mqtt(
                    port,
                    dir,
                    &tag,
                    shape,
                    n,
                    max_batch,
                    None,
                    PayloadStorage::Base64,
                );
                let secs = r.elapsed.as_secs_f64();
                eprintln!(
                    "{tag}: {} msgs in {secs:.2}s (publisher done after {:.2}s), {} txns",
                    r.messages,
                    r.publish_elapsed.as_secs_f64(),
                    r.commits.len()
                );
                rates.push(r.messages as f64 / secs);
                mbps.push(r.payload_bytes as f64 / secs / 1e6);
                spool_mbps.push(r.spool_bytes as f64 / secs / 1e6);
                txns.push(r.commits.len() as f64);
                latencies.extend(r.commits.iter().map(|c| c.duration));
                if max_batch > 1 && run == 0 {
                    replay_sources.push((shape, dir.join(format!("{tag}.sqlite"))));
                }
            }
            latencies.sort();
            let rate = median(rates);
            let samples = if shape == Shape::Daq {
                format!("{:.0}", rate * (CHANNELS * SAMPLES) as f64)
            } else {
                "-".into()
            };
            let t = median(txns);
            println!(
                "| {} | {max_batch} | {n} | {rate:.0} | {:.2} | {samples} | {:.2} | {t:.0} | {:.1} | {} | {} |",
                shape.name(),
                median(mbps),
                median(spool_mbps),
                n as f64 / t,
                ms(pct(&latencies, 0.5)),
                ms(pct(&latencies, 0.99)),
            );
        }
    }

    let paced_n = if quick { 20 } else { 100 };
    println!("\n## MQTT ingest at the source rate (daq, 10 msg/s, {paced_n} messages), one run\n");
    println!(
        "| max_batch | msg/s | transactions | commit p50 ms | commit p99 ms | commit max ms |"
    );
    println!("|---|---|---|---|---|---|");
    for max_batch in [1, 64] {
        let tag = format!("paced-b{max_batch}");
        let r = run_mqtt(
            port,
            dir,
            &tag,
            Shape::Daq,
            paced_n,
            max_batch,
            Some(Duration::from_millis(100)),
            PayloadStorage::Base64,
        );
        let mut l: Vec<Duration> = r.commits.iter().map(|c| c.duration).collect();
        l.sort();
        println!(
            "| {max_batch} | {:.1} | {} | {} | {} | {} |",
            r.messages as f64 / r.elapsed.as_secs_f64(),
            r.commits.len(),
            ms(pct(&l, 0.5)),
            ms(pct(&l, 0.99)),
            ms(*l.last().unwrap()),
        );
    }

    let size_n = 300 / scale;
    println!(
        "\n## Spool size per sample (daq, max_batch 64, {size_n} messages), median of {runs} run(s)\n"
    );
    println!(
        "| payload storage | event format | spool bytes | spool B/sample | spool / payload | msg/s | bit-exact |"
    );
    println!("|---|---|---|---|---|---|---|");
    let channels = daq_channels();
    let sent: Vec<Vec<u8>> = (0..size_n)
        .map(|i| payload(Shape::Daq, &channels, i))
        .collect();
    let samples = (size_n * CHANNELS * SAMPLES) as f64;
    for (storage, name, version) in [
        (PayloadStorage::Base64, "base64", 1),
        (PayloadStorage::Raw, "raw", 2),
    ] {
        let mut sizes = Vec::new();
        let mut rates = Vec::new();
        let mut payload_bytes = 0;
        let mut exact = true;
        for run in 0..runs {
            let tag = format!("size-{name}-r{run}");
            let r = run_mqtt(port, dir, &tag, Shape::Daq, size_n, 64, None, storage);
            payload_bytes = r.payload_bytes;
            sizes.push(r.spool_bytes as f64);
            rates.push(r.messages as f64 / r.elapsed.as_secs_f64());
            let spool = Spool::open(dir.join(format!("{tag}.sqlite")), MAX_DB).unwrap();
            let mut stored: Vec<Vec<u8>> = Vec::new();
            for rec in spool.read_after(0, size_n + 1).unwrap() {
                let (_, v) = akhet::event::check_body_version(&rec.body).unwrap();
                assert_eq!(v, version);
                let ev = akhet::event::decode(&rec.body).unwrap();
                let EventData::MqttMessage(m) = ev.data else {
                    panic!("not MQTT")
                };
                stored.push(m.payload().unwrap().into_owned());
            }
            // Redelivery could add duplicates; compare as sorted lists.
            stored.sort();
            let mut want = sent.clone();
            want.sort();
            exact &= stored == want;
        }
        let size = median(sizes);
        println!(
            "| {name} | {version} | {size:.0} | {:.2} | {:.3} | {:.0} | {} |",
            size / samples,
            size / payload_bytes as f64,
            median(rates),
            if exact { "yes" } else { "NO" },
        );
    }

    println!("\n## Replay to the example receiver, median of {runs} run(s)\n");
    println!("| payload | records/request | records | records/s | MB/s | requests |");
    println!("|---|---|---|---|---|---|");
    for (shape, spool) in &replay_sources {
        for rpr in [1, 100] {
            let mut rates = Vec::new();
            let mut mbps = Vec::new();
            let mut last = None;
            for run in 0..runs {
                let tag = format!(
                    "replay-{}-{rpr}-{run}",
                    if *shape == Shape::Daq { "daq" } else { "small" }
                );
                let r = run_replay(spool, dir, &tag, rpr);
                let secs = r.elapsed.as_secs_f64();
                eprintln!("{tag}: {} records in {secs:.2}s", r.records);
                rates.push(r.records as f64 / secs);
                mbps.push(r.bytes as f64 / secs / 1e6);
                last = Some(r);
            }
            let r = last.unwrap();
            println!(
                "| {} | {rpr} | {} | {:.0} | {:.2} | {} |",
                shape.name(),
                r.records,
                median(rates),
                median(mbps),
                r.requests
            );
        }
    }
}
