//! MorletDB sink benchmark: replay a synthetic spool into pg_morlet.
//!
//!     AKHET_MORLET_URL=postgresql://... cargo run --release --locked --example morlet_sink -- \
//!       --sources 1 --channels 8 --rate 25600 --samples 256 --messages 400 \
//!       --records-per-tx 100 --extractor auto
//!
//! Builds a spool in a temporary directory with `--messages` MQTT records per
//! source. Each record carries one group with `--channels` signals of
//! `--samples` samples at `--rate` Hz (nanosecond timestamps, deterministic
//! pseudo-random values). Then it replays the spool with `akhet::morlet::run`
//! into a fresh collection and prints one JSON line:
//! samples/s and chunks/s over the whole replay, transactions, mean time per
//! transaction, mean samples per chunk, and the size of the sink state in
//! `akhet_morlet_sinks` (text bytes and stored bytes) after the run.
//!
//! Development tooling only. It creates tables named `akhet_bench_*` in the
//! target database and drops them unless `--keep` is given; use a disposable
//! database.

use akhet::decode::DecoderConfig;
use akhet::event::{Event, EventData, MqttMessage};
use akhet::faults::Faults;
use akhet::morlet::{self, MorletConfig};
use akhet::spool::{DEFAULT_MAX_DB_BYTES, Spool};
use base64::Engine as _;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

struct Args {
    sources: usize,
    channels: usize,
    rate: f64,
    samples: usize,
    messages: usize,
    records_per_tx: usize,
    extractor: String,
    keep: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        sources: 1,
        channels: 8,
        rate: 25_600.0,
        samples: 256,
        messages: 400,
        records_per_tx: 100,
        extractor: "auto".into(),
        keep: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--keep" {
            a.keep = true;
            continue;
        }
        let v = it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        let num = || v.parse::<usize>().unwrap_or_else(|_| panic!("{flag}: {v}"));
        match flag.as_str() {
            "--sources" => a.sources = num(),
            "--channels" => a.channels = num(),
            "--samples" => a.samples = num(),
            "--messages" => a.messages = num(),
            "--records-per-tx" => a.records_per_tx = num(),
            "--rate" => a.rate = v.parse().expect("--rate"),
            "--extractor" => a.extractor = v,
            _ => panic!("unknown flag {flag}"),
        }
    }
    a
}

fn mqtt_event(topic: &str, payload: &[u8]) -> Event {
    Event::new(
        EventData::MqttMessage(MqttMessage {
            broker: "127.0.0.1:1883".into(),
            client_id: "bench".into(),
            subscription: "#".into(),
            subscriptions: vec!["#".into()],
            topic: topic.into(),
            qos: 1,
            retain: false,
            dup: false,
            payload_base64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_len: payload.len(),
            source_identity_error: None,
        }),
        1,
    )
}

/// Deterministic integer-valued samples (like ADC counts).
fn value(ch: usize, k: u64) -> f64 {
    let x = k
        .wrapping_mul(6364136223846793005)
        .wrapping_add(ch as u64 * 1442695040888963407);
    (((x >> 33) % 2001) as f64 - 1000.0) + (k as f64 / 37.0 + ch as f64).sin().round()
}

fn main() {
    let a = parse_args();
    let url = std::env::var("AKHET_MORLET_URL").expect("set AKHET_MORLET_URL");
    let dir = tempfile::tempdir().unwrap();
    let spool_path = dir.path().join("bench.sqlite");
    let t0_ns: i64 = 1_700_000_000_000_000_000;
    {
        let mut spool = Spool::open(&spool_path, DEFAULT_MAX_DB_BYTES).unwrap();
        for m in 0..a.messages {
            for src in 0..a.sources {
                let k0 = (m * a.samples) as u64;
                let ts: Vec<i64> = (k0..k0 + a.samples as u64)
                    .map(|k| t0_ns + (k as f64 * 1e9 / a.rate).round() as i64)
                    .collect();
                let mut signals = serde_json::Map::new();
                for c in 0..a.channels {
                    let ch = src * a.channels + c;
                    let v: Vec<f64> = (k0..k0 + a.samples as u64).map(|k| value(ch, k)).collect();
                    signals.insert(format!("c{c}"), v.into());
                }
                let body =
                    serde_json::json!({"groups":[{"name":"g","timestamps":ts,"signals":signals}]});
                spool
                    .append(
                        &mqtt_event(
                            &format!("bench/src{src}/batch"),
                            &serde_json::to_vec(&body).unwrap(),
                        ),
                        None,
                    )
                    .unwrap();
            }
        }
    }
    let decoder = DecoderConfig::from_json(
        serde_json::json!({"format_version":1,"rules":[{
            "name":"bench","topic":"bench/+/batch","source":"{topic.1}","groups":"/groups",
            "group":"{group:/name}","timestamps":"/timestamps","timestamp_unit":"ns",
            "signals":{"map":"/signals"},"default_unit":"counts",
            "timing":{"rate_hz":a.rate,"tolerance_ns":1000}}]})
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let id = &uuid::Uuid::new_v4().simple().to_string()[..10];
    let coll = format!("akhet_bench_{id}");
    let sink = format!("bench_{id}");
    let mut cfg = MorletConfig::new(&url, &sink, &coll, decoder);
    cfg.create_collection = true;
    cfg.records_per_tx = a.records_per_tx;
    cfg.extractor = a.extractor.clone();
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let s = morlet::run(
        &spool_path,
        DEFAULT_MAX_DB_BYTES,
        &cfg,
        &Faults::none(),
        &stop,
    )
    .expect("sink run");
    let secs = start.elapsed().as_secs_f64();
    let mut db = postgres::Client::connect(&url, postgres::NoTls).expect("connect for stats");
    let row = db
        .query_one(
            "SELECT octet_length(state::text)::bigint, pg_column_size(state)::bigint FROM akhet_morlet_sinks WHERE sink=$1",
            &[&sink],
        )
        .unwrap();
    let (state_text, state_stored): (i64, i64) = (row.get(0), row.get(1));
    if !a.keep {
        db.batch_execute(&format!("DROP TABLE {coll} CASCADE"))
            .unwrap();
        for t in [
            "_chunks",
            "_segments",
            "_ctx",
            "_ctx_segments",
            "_ctx_values",
            "_labels",
        ] {
            db.batch_execute(&format!("DROP TABLE IF EXISTS {coll}{t} CASCADE"))
                .unwrap();
        }
        db.execute("DELETE FROM akhet_morlet_sinks WHERE sink=$1", &[&sink])
            .unwrap();
        db.execute(
            "DELETE FROM akhet_morlet_evidence WHERE collection=$1",
            &[&coll],
        )
        .unwrap();
    }
    println!(
        "{}",
        serde_json::json!({
            "sources": a.sources,
            "channels": a.sources * a.channels,
            "rate_hz": a.rate,
            "samples_per_message": a.samples,
            "records": s.records,
            "records_per_tx": a.records_per_tx,
            "extractor": a.extractor,
            "samples": s.samples,
            "chunks": s.chunks,
            "transactions": s.transactions,
            "seconds": (secs * 1000.0).round() / 1000.0,
            "samples_per_s": (s.samples as f64 / secs).round(),
            "chunks_per_s": (s.chunks as f64 / secs).round(),
            "mean_tx_ms": (secs * 1e3 / s.transactions.max(1) as f64 * 100.0).round() / 100.0,
            "mean_chunk_samples": (s.samples as f64 / s.chunks.max(1) as f64 * 10.0).round() / 10.0,
            "state_text_bytes": state_text,
            "state_stored_bytes": state_stored,
        })
    );
}
