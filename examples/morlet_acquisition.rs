//! Acquisition read benchmark: `morlet_read_acquisition` on a source with
//! many gaps.
//!
//!     AKHET_MORLET_URL=postgresql://... cargo run --release --locked --example morlet_acquisition -- \
//!       --channels 4 --rate 1000 --samples 100 --messages 2000 --gap-every 2
//!
//! Reads the last `--window-ms` of data (default 1000) with
//! `morlet_read_acquisition`, one call per acquisition overlapping the
//! window, and reports the total time per window read.
//!
//! Builds a spool for one synthetic source: `--messages` messages of
//! `--samples` samples per channel at `--rate` Hz; every `--gap-every`-th
//! message is left out for all channels (a pause of the device), so the
//! source has messages / gap-every gaps. Replays it with the MorletDB sink
//! into a fresh collection, reads the window `--reads` times and prints one
//! JSON line: acquisitions, acquisitions read per window, channels in the
//! last acquisition, samples returned, median and p90 time per window read,
//! and the median of `SELECT 1` on the same connection as the round-trip
//! floor.
//!
//! Development tooling only; creates and drops `akhet_bench_*` tables.

use akhet::decode::DecoderConfig;
use akhet::event::{Event, EventData, MqttMessage};
use akhet::faults::Faults;
use akhet::morlet::{self, MorletConfig};
use akhet::spool::{DEFAULT_MAX_DB_BYTES, Spool};
use base64::Engine as _;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

struct Args {
    channels: usize,
    rate: f64,
    samples: usize,
    messages: usize,
    gap_every: usize,
    reads: usize,
    window_ms: i64,
    /// Extra `key=value` sink settings (see `configure`).
    sink: Vec<(String, String)>,
}

fn parse_args() -> Args {
    let mut a = Args {
        channels: 4,
        rate: 1000.0,
        samples: 100,
        messages: 2000,
        gap_every: 2,
        reads: 30,
        window_ms: 1000,
        sink: Vec::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        let num = || v.parse::<usize>().unwrap_or_else(|_| panic!("{flag}: {v}"));
        match flag.as_str() {
            "--channels" => a.channels = num(),
            "--samples" => a.samples = num(),
            "--messages" => a.messages = num(),
            "--gap-every" => a.gap_every = num(),
            "--reads" => a.reads = num(),
            "--window-ms" => a.window_ms = num() as i64,
            "--rate" => a.rate = v.parse().expect("--rate"),
            "--sink" => {
                let (k, val) = v.split_once('=').expect("--sink key=value");
                a.sink.push((k.into(), val.into()));
            }
            _ => panic!("unknown flag {flag}"),
        }
    }
    a
}

/// Sink settings beyond the defaults, by name.
fn configure(cfg: &mut MorletConfig, settings: &[(String, String)]) {
    for (k, v) in settings {
        match k.as_str() {
            "acquisition_period_s" => cfg.acquisition_period_s = Some(v.parse().expect("seconds")),
            _ => panic!("unknown sink setting {k}"),
        }
    }
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
            payload_base64: Some(base64::engine::general_purpose::STANDARD.encode(payload)),
            payload_raw: None,
            payload_len: payload.len(),
            source_identity_error: None,
        }),
        1,
    )
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn main() {
    let a = parse_args();
    let url = std::env::var("AKHET_MORLET_URL").expect("set AKHET_MORLET_URL");
    let dir = tempfile::tempdir().unwrap();
    let spool_path = dir.path().join("bench.sqlite");
    let t0_ns: i64 = 1_700_000_000_000_000_000;
    let period = 1e9 / a.rate;
    let mut last_ns = t0_ns;
    let mut gaps = 0;
    {
        let mut spool = Spool::open(&spool_path, DEFAULT_MAX_DB_BYTES).unwrap();
        for m in 0..a.messages {
            if a.gap_every > 0 && m % a.gap_every == a.gap_every - 1 {
                gaps += 1;
                continue;
            }
            let k0 = (m * a.samples) as u64;
            let ts: Vec<i64> = (k0..k0 + a.samples as u64)
                .map(|k| t0_ns + (k as f64 * period).round() as i64)
                .collect();
            last_ns = *ts.last().unwrap();
            let mut signals = serde_json::Map::new();
            for c in 0..a.channels {
                let v: Vec<f64> = (k0..k0 + a.samples as u64)
                    .map(|k| {
                        ((k as f64) / 7.0 + c as f64)
                            .sin()
                            .mul_add(1000.0, 0.0)
                            .round()
                    })
                    .collect();
                signals.insert(format!("c{c}"), v.into());
            }
            let body =
                serde_json::json!({"groups":[{"name":"g","timestamps":ts,"signals":signals}]});
            spool
                .append(
                    &mqtt_event("bench/src0/batch", &serde_json::to_vec(&body).unwrap()),
                    None,
                )
                .unwrap();
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
    cfg.extractor = "none".into();
    configure(&mut cfg, &a.sink);
    let start = Instant::now();
    let s = morlet::run(
        &spool_path,
        DEFAULT_MAX_DB_BYTES,
        &cfg,
        &Faults::none(),
        &AtomicBool::new(false),
    )
    .expect("sink run");
    let sink_s = start.elapsed().as_secs_f64();

    let mut db = postgres::Client::connect(&url, postgres::NoTls).expect("connect");
    let acquisitions: i64 = db
        .query_one(&format!("SELECT count(*) FROM {coll}_acq"), &[])
        .unwrap()
        .get(0);
    // All acquisitions overlapping the window [last - window, last]: one
    // morlet_read_acquisition call each (recordings of different
    // acquisitions do not overlap in time, so the samples add up).
    let acqs: Vec<(String, i64)> = db
        .query(
            &format!(
                "SELECT acquisition_id, (extract(epoch FROM origin_utc) * 1000000)::int8 * 1000
                 FROM {coll}_acq ORDER BY origin_utc"
            ),
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let (w1, w0) = (last_ns + 1, last_ns + 1 - a.window_ms * 1_000_000);
    let overlapping: Vec<&(String, i64)> = acqs
        .iter()
        .enumerate()
        .filter(|(i, (_, o))| *o < w1 && acqs.get(i + 1).is_none_or(|(_, next)| *next > w0))
        .map(|(_, x)| x)
        .collect();
    let channels: i64 = db
        .query_one(
            &format!("SELECT count(*) FROM {coll}_acq_channels WHERE acquisition_id=$1"),
            &[&overlapping.last().unwrap().0],
        )
        .unwrap()
        .get(0);
    let mut read_ms = Vec::new();
    let mut returned = 0i64;
    for _ in 0..a.reads {
        let t = Instant::now();
        returned = 0;
        for (acq, origin) in &overlapping {
            let rows = db
                .query(
                    "SELECT cardinality(samples)::int8 FROM morlet_read_acquisition($1::text::regclass, $2, $3, $4)",
                    &[&coll, acq, &(w0 - origin).max(0), &(w1 - origin)],
                )
                .unwrap();
            returned += rows.iter().map(|r| r.get::<_, i64>(0)).sum::<i64>();
        }
        read_ms.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let mut floor_ms: Vec<f64> = (0..a.reads)
        .map(|_| {
            let t = Instant::now();
            db.query_one("SELECT 1", &[]).unwrap();
            t.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    let p90 = {
        let mut v = read_ms.clone();
        v.sort_by(f64::total_cmp);
        v[(v.len() * 9 / 10).min(v.len() - 1)]
    };
    db.batch_execute(&format!("DROP TABLE {coll} CASCADE"))
        .unwrap();
    for t in [
        "_chunks",
        "_segments",
        "_ctx",
        "_ctx_segments",
        "_ctx_values",
        "_labels",
        "_acq",
        "_acq_channels",
    ] {
        db.batch_execute(&format!("DROP TABLE IF EXISTS {coll}{t} CASCADE"))
            .unwrap();
    }
    for t in [
        "akhet_morlet_sinks",
        "akhet_morlet_recent",
        "akhet_morlet_context_current",
    ] {
        db.batch_execute(&format!(
            "DO $$BEGIN IF to_regclass('{t}') IS NOT NULL THEN DELETE FROM {t} WHERE sink='{sink}'; END IF; END$$"
        ))
        .unwrap();
    }
    db.execute(
        "DELETE FROM akhet_morlet_evidence WHERE collection=$1",
        &[&coll],
    )
    .unwrap();
    println!(
        "{}",
        serde_json::json!({
            "channels_per_source": a.channels,
            "rate_hz": a.rate,
            "messages": a.messages - gaps,
            "gaps": gaps,
            "sink": a.sink.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>(),
            "recordings": s.recordings_started,
            "sink_seconds": (sink_s * 1000.0).round() / 1000.0,
            "acquisitions": acquisitions,
            "window_ms": a.window_ms,
            "acquisitions_read": overlapping.len(),
            "channels_in_last_acquisition": channels,
            "samples_returned": returned,
            "read_median_ms": (median(&mut read_ms) * 100.0).round() / 100.0,
            "read_p90_ms": (p90 * 100.0).round() / 100.0,
            "select1_median_ms": (median(&mut floor_ms) * 100.0).round() / 100.0,
        })
    );
}
