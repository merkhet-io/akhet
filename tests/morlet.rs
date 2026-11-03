//! MorletDB sink against a real PostgreSQL with pg_morlet.
//!
//! Ignored by default. Run with a disposable database, e.g.:
//! ```sh
//! ci/morletdb-test.sh /tmp/akhet-pgcerts
//! AKHET_TEST_MORLET_CERTS=/tmp/akhet-pgcerts \
//! AKHET_TEST_MORLET_URL=postgresql://morlet:morlet-test@127.0.0.1:54339/morlet \
//!   cargo test --locked --test morlet -- --ignored --test-threads=1
//! ```
//! `AKHET_TEST_MORLET_CERTS` (the CA files written by the script) is only
//! needed by the TLS test.

mod support;

use akhet::event::{Event, EventData, MqttMessage};
use akhet::spool::Spool;
use base64::Engine as _;
use std::collections::BTreeMap;
use std::path::Path;
use support::*;

const T0_US: i64 = 1_700_000_000_000_000;
const N: usize = 400; // samples per batch, 40 kHz => 10 ms

fn url() -> String {
    std::env::var("AKHET_TEST_MORLET_URL").expect("set AKHET_TEST_MORLET_URL")
}

fn mqtt_event(topic: &str, payload: &[u8]) -> Event {
    Event::new(
        EventData::MqttMessage(MqttMessage {
            broker: "127.0.0.1:1883".into(),
            client_id: "test".into(),
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

fn value(signal: usize, i: i64) -> f64 {
    // Awkward doubles, including negative zero, to check bit-exactness.
    if i % 97 == 0 {
        return -0.0;
    }
    ((i as f64) * 0.37 + signal as f64).sin() * 1e3 / 7.0
}

/// One sensor batch: samples [k0, k0+N) at 40 kHz, two signals.
fn sensor_batch(k0: i64) -> Vec<u8> {
    let ts: Vec<i64> = (k0..k0 + N as i64).map(|k| T0_US + k * 25).collect();
    let sig = |s| -> Vec<f64> { (k0..k0 + N as i64).map(|k| value(s, k)).collect() };
    serde_json::to_vec(&serde_json::json!({"groups":[
        {"name":"vib","timestamps":ts,"signals":{"accel_x":sig(0),"accel_y":sig(1)}}]}))
    .unwrap()
}

fn controller(t_us: &[i64], program: &[i64]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"t": t_us.iter().map(|t| t / 1000).collect::<Vec<_>>(), "program": program})).unwrap()
}

const DECODER: &str = r#"{"format_version":1,"rules":[
  {"name":"hub","topic":"sensors/+/batch","source":"{topic.1}","groups":"/groups",
   "group":"{group:/name}","timestamps":"/timestamps","timestamp_unit":"us",
   "signals":{"map":"/signals"},"units":{"accel_x":"g","accel_y":"g"},
   "timing":{"rate_hz":40000,"tolerance_ns":2000}},
  {"name":"ctrl","topic":"plant/+/controller","source":"{topic.1}","group":"state",
   "timestamps":"/t","timestamp_unit":"ms","signals":{"list":[{"name":"program","pointer":"/program"}]},
   "context_signals":["program"]},
  {"name":"temp","topic":"plant/+/temp","source":"{topic.1}","group":"thermal",
   "timestamps":"/t","timestamp_unit":"us","signals":{"list":[{"name":"temp","pointer":"/temp"}]},
   "units":{"temp":"degC"},"timing":{"rate_hz":1000,"tolerance_ns":2000}}]}"#;

/// Spool: batches 0..6 regular, batch 3 redelivered, a 100-sample gap,
/// then batches 7..9 regular; program changes 11 -> 2011 -> 11.
/// Returns the expected samples per (recording anchor k, signal).
fn build_spool(path: &Path) -> BTreeMap<(i64, usize), Vec<f64>> {
    let mut spool = Spool::open(path, MAX_DB).unwrap();
    let mut add = |topic: &str, p: Vec<u8>| {
        spool.append(&mqtt_event(topic, &p), None).unwrap();
    };
    let ms = |k: i64| (T0_US + k * 25) * 1000; // sample k -> ns, used for context
    add("plant/hub-1/controller", controller(&[ms(0) / 1000], &[11]));
    for b in 0..6 {
        add("sensors/hub-1/batch", sensor_batch(b * N as i64));
        if b == 3 {
            add("sensors/hub-1/batch", sensor_batch(3 * N as i64));
        }
        if b == 2 {
            add(
                "plant/hub-1/controller",
                controller(&[ms(1000) / 1000], &[2011]),
            );
        }
    }
    let gap_start = 6 * N as i64 + 100;
    for b in 0..3 {
        add(
            "sensors/hub-1/batch",
            sensor_batch(gap_start + b * N as i64),
        );
    }
    add(
        "plant/hub-1/controller",
        controller(&[ms(gap_start + 780) / 1000], &[11]),
    );
    let mut want = BTreeMap::new();
    for s in 0..2 {
        want.insert((0, s), (0..6 * N as i64).map(|k| value(s, k)).collect());
        want.insert(
            (gap_start, s),
            (gap_start..gap_start + 3 * N as i64)
                .map(|k| value(s, k))
                .collect(),
        );
    }
    want
}

fn decoder_file(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("decoder.json");
    std::fs::write(&p, DECODER).unwrap();
    p
}

fn morlet_run(
    spool: &Path,
    cfg: &Path,
    sink: &str,
    coll: &str,
    fault: Option<&str>,
) -> std::process::Output {
    morlet_run_args(spool, cfg, sink, coll, fault, &[])
}

fn morlet_run_args(
    spool: &Path,
    cfg: &Path,
    sink: &str,
    coll: &str,
    fault: Option<&str>,
    extra: &[&str],
) -> std::process::Output {
    morlet_run_url(&url(), spool, cfg, sink, coll, fault, extra)
}

fn morlet_run_url(
    url: &str,
    spool: &Path,
    cfg: &Path,
    sink: &str,
    coll: &str,
    fault: Option<&str>,
    extra: &[&str],
) -> std::process::Output {
    let mut args = vec![
        "morlet",
        "--sink",
        sink,
        "--collection",
        coll,
        "--create-collection",
        "--config",
        cfg.to_str().unwrap(),
        "--records-per-tx",
        "2",
        "--max-attempts",
        "2",
    ];
    if extra.contains(&"--records-per-tx") {
        // Overridden by the caller.
        let i = args.iter().position(|a| *a == "--records-per-tx").unwrap();
        args.drain(i..i + 2);
    }
    args.extend_from_slice(extra);
    let mut c = akhet(spool, &args, fault);
    c.env("AKHET_MORLET_URL", url);
    let out = c.output().unwrap();
    eprintln!(
        "morlet -> {:?}\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

/// Read every recording of the collection back, bit-exact.
fn read_back(coll: &str) -> BTreeMap<(i64, String), Vec<f64>> {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let rows = db
        .query(
            &format!(
                "SELECT r.recording_id, (r.metadata->'akhet'->>'anchor_unix_ns')::bigint, r.metadata->'akhet'->>'signal',
                        (s.received_samples) FROM {coll} r, morlet_stream_status($1::text::regclass, r.recording_id) s"
            ),
            &[&coll],
        )
        .unwrap();
    let mut out = BTreeMap::new();
    for r in rows {
        let (id, anchor, signal, n): (String, i64, String, i64) =
            (r.get(0), r.get(1), r.get(2), r.get(3));
        let mut vals = Vec::new();
        let mut s = 0;
        while s < n {
            let e = (s + 32768).min(n);
            let row = db
                .query_one(
                    "SELECT samples FROM morlet_read_interval($1::text::regclass, $2, $3, $4)",
                    &[&coll, &id, &s, &e],
                )
                .unwrap();
            vals.extend(row.get::<_, Vec<f64>>(0));
            s = e;
        }
        let k = (anchor / 1000 - T0_US) / 25;
        out.insert((k, signal), vals);
    }
    out
}

fn assert_exact(coll: &str, want: &BTreeMap<(i64, usize), Vec<f64>>) {
    let got = read_back(coll);
    assert_eq!(
        got.len(),
        want.len(),
        "recordings: {:?}",
        got.keys().collect::<Vec<_>>()
    );
    for ((k, s), w) in want {
        let sig = ["accel_x", "accel_y"][*s];
        let g = &got[&(*k, sig.to_string())];
        assert_eq!(g.len(), w.len(), "sample count of {k}/{sig}");
        let bad: Vec<(usize, f64, f64)> = g
            .iter()
            .zip(w)
            .enumerate()
            .filter(|(_, (a, b))| a.to_bits() != b.to_bits())
            .map(|(i, (a, b))| (i, *a, *b))
            .take(5)
            .collect();
        assert!(bad.is_empty(), "bit-exact {k}/{sig}: {bad:?}");
    }
}

fn context_rows(coll: &str) -> Vec<(String, i64, i64, serde_json::Value)> {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    db.query(
        &format!(
            "SELECT r.metadata->'akhet'->>'signal' || '@' || ((r.metadata->'akhet'->>'anchor_unix_ns')::bigint/1000 - {T0_US})/25,
                    c.start_sample, c.end_sample, c.value
             FROM {coll}_ctx c JOIN {coll} r USING(recording_id) WHERE c.key='program'
             ORDER BY 1, 2"
        ),
        &[],
    )
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn replays_bit_exact_with_gaps_duplicates_and_context() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    let want = build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_t");
    let sink = unique("s");
    let out = morlet_run(&spool, &cfg, &sink, &coll, None);
    assert!(out.status.success());
    let s = stdout_json(&out);
    assert_eq!(s["samples"], 2 * 9 * N as u64);
    assert_eq!(s["recordings_started"], 4);
    assert_exact(&coll, &want);

    // Context: program 11 on [0,1000), 2011 from 1000 through the end of the
    // first recording; in the second recording (anchor 2500) 2011 until
    // sample 780 (82 ms, millisecond context clock), then 11 (still open:
    // not written yet).
    let rows = context_rows(&coll);
    for sig in ["accel_x", "accel_y"] {
        let r: Vec<_> = rows
            .iter()
            .filter(|r| r.0.starts_with(sig))
            .cloned()
            .collect();
        assert_eq!(
            r,
            vec![
                (format!("{sig}@0"), 0, 1000, 11.into()),
                (format!("{sig}@0"), 1000, 2400, 2011.into()),
                (format!("{sig}@2500"), 0, 780, 2011.into()),
            ]
        );
    }

    assert_searchable(&coll);

    // The value in effect (11 since the last change, still open and so not
    // in the context timeline) is visible in akhet_morlet_context_current.
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let cur = db
        .query(
            "SELECT collection, source, key, value, since_unix_ns, spool_seq FROM akhet_morlet_context_current WHERE sink=$1",
            &[&sink],
        )
        .unwrap();
    assert_eq!(cur.len(), 1);
    let last_change_ms = (T0_US + (6 * N as i64 + 100 + 780) * 25) / 1000;
    let (c, src, key, v, since, seq): (String, String, String, serde_json::Value, i64, i64) = (
        cur[0].get(0),
        cur[0].get(1),
        cur[0].get(2),
        cur[0].get(3),
        cur[0].get(4),
        cur[0].get(5),
    );
    assert_eq!(
        (c.as_str(), src.as_str(), key.as_str(), v, since),
        (
            coll.as_str(),
            "hub-1",
            "program",
            11.into(),
            last_change_ms * 1_000_000
        )
    );
    assert_eq!(seq, s["checkpoint_seq"].as_i64().unwrap(), "last record");

    // Nothing left: a second run writes nothing and changes nothing.
    let out = morlet_run(&spool, &cfg, &sink, &coll, None);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["records"], 0);
    assert_exact(&coll, &want);

    // Timing evidence and binding.
    let ev = evidence(&coll);
    assert!(ev.iter().all(|e| e.max_abs_deviation_ns == 0.0));
    assert_eq!(chunk_count(&coll), ev.len());
    // Chunks are merged within a transaction (2 records here), so there are
    // fewer than the 18 messages x channels.
    assert!(ev.len() < 2 * 9, "{} chunks", ev.len());
    let st = run(&spool, &["status"], None);
    let v: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
    assert_eq!(v["sinks"][0]["pending_records"], 0);
    assert!(
        v["sinks"][0]["destination_url"]
            .as_str()
            .unwrap()
            .starts_with("morletdb://")
    );
    assert!(
        !v["sinks"][0]["destination_url"]
            .as_str()
            .unwrap()
            .contains("morlet-test"),
        "no password"
    );
}

#[derive(Debug)]
struct Evidence {
    recording: String,
    start: i64,
    count: i64,
    first_ns: i64,
    last_ns: i64,
    max_abs_deviation_ns: f64,
}

/// Evidence rows in sample order; checks they tile every recording exactly
/// (no gap, no overlap, sample counts add up) and that their timestamps are
/// the original ones of the first and last sample.
fn evidence(coll: &str) -> Vec<Evidence> {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let ev: Vec<Evidence> = db
        .query(
            "SELECT recording_id, start_sample, sample_count, first_unix_ns, last_unix_ns, max_abs_deviation_ns
             FROM akhet_morlet_evidence WHERE collection=$1 ORDER BY 1, 2",
            &[&coll],
        )
        .unwrap()
        .iter()
        .map(|r| Evidence {
            recording: r.get(0),
            start: r.get(1),
            count: r.get::<_, i32>(2) as i64,
            first_ns: r.get(3),
            last_ns: r.get(4),
            max_abs_deviation_ns: r.get(5),
        })
        .collect();
    let lens: BTreeMap<String, i64> = db
        .query(
            &format!(
                "SELECT r.recording_id, s.received_samples FROM {coll} r, morlet_stream_status($1::text::regclass, r.recording_id) s"
            ),
            &[&coll],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    for (rec, n) in &lens {
        let mut next = 0;
        for e in ev.iter().filter(|e| &e.recording == rec) {
            assert_eq!(e.start, next, "{rec}: evidence must tile the recording");
            // 40 kHz, exact grid: sample k of the recording is 25 us after k-1.
            assert_eq!(
                e.last_ns - e.first_ns,
                (e.count - 1) * 25_000,
                "{rec}@{}",
                e.start
            );
            next += e.count;
        }
        assert_eq!(next, *n, "{rec}: evidence covers all samples");
    }
    ev
}

fn chunk_count(coll: &str) -> usize {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let n: i64 = db
        .query_one(&format!("SELECT count(*) FROM {coll}_chunks"), &[])
        .unwrap()
        .get(0);
    n as usize
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn one_transaction_writes_one_chunk_per_recording() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    let want = build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_one");
    let out = morlet_run_args(
        &spool,
        &cfg,
        &unique("s"),
        &coll,
        None,
        &["--records-per-tx", "100"],
    );
    assert!(out.status.success());
    let s = stdout_json(&out);
    assert_eq!(
        (s["transactions"].as_u64(), s["chunks"].as_u64()),
        (Some(1), Some(4))
    );
    assert_exact(&coll, &want);
    let ev = evidence(&coll);
    assert_eq!(ev.len(), 4);
    assert_eq!(chunk_count(&coll), 4);
    assert_searchable(&coll);
}

/// Default extractor (`auto`): every 40 kHz recording has
/// `morlet-energy-v1` feature segments, and a window cut from a recording
/// finds itself at distance 0 by feature search.
fn assert_searchable(coll: &str) {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let rows = db
        .query(
            &format!(
                "SELECT r.recording_id, s.extractor_version, s.indexed_segments, s.received_samples
                 FROM {coll} r, morlet_stream_status($1::text::regclass, r.recording_id) s"
            ),
            &[&coll],
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    for r in &rows {
        let (id, ext, segs, n): (String, String, i64, i64) =
            (r.get(0), r.get(1), r.get(2), r.get(3));
        assert_eq!(ext, "morlet-energy-v1", "{id}");
        // window 512, stride 256
        assert_eq!(segs, (n - 512) / 256 + 1, "{id}: segments for {n} samples");
        let hit = db
            .query_one(
                "SELECT recording_id, start_sample, end_sample, distance
                 FROM morlet_search_interval($1::text::regclass, $2, 256, 768, '{}', 1, exclude_source => false)",
                &[&coll, &id],
            )
            .unwrap();
        let (hid, hs, he, d): (String, i64, i64, f64) =
            (hit.get(0), hit.get(1), hit.get(2), hit.get(3));
        assert_eq!((hid.as_str(), hs, he, d), (id.as_str(), 256, 768, 0.0));
    }
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn extractor_none_stores_raw_samples_only() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    let want = build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_raw");
    let out = morlet_run_args(
        &spool,
        &cfg,
        &unique("s"),
        &coll,
        None,
        &["--extractor", "none"],
    );
    assert!(out.status.success());
    assert_exact(&coll, &want);
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let row = db
        .query_one(
            &format!(
                "SELECT count(*), sum(s.indexed_segments)::bigint, bool_and(s.extractor_version = 'none')
                 FROM {coll} r, morlet_stream_status($1::text::regclass, r.recording_id) s"
            ),
            &[&coll],
        )
        .unwrap();
    let (n, segs, none): (i64, i64, bool) = (row.get(0), row.get(1), row.get(2));
    assert_eq!((n, segs, none), (4, 0, true));
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn crashes_lose_and_duplicate_nothing() {
    for fault in [
        "abort:replay.before_send",
        "abort:replay.after_remote_success",
        "abort:replay.after_checkpoint",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("s.sqlite");
        let want = build_spool(&spool);
        let cfg = decoder_file(dir.path());
        let coll = unique("akhet_c");
        let sink = unique("s");
        let mut crashes = 0;
        loop {
            let out = morlet_run(&spool, &cfg, &sink, &coll, Some(fault));
            if out.status.success() {
                break;
            }
            assert!(out.status.code().is_none(), "{fault}: expected an abort");
            crashes += 1;
            assert!(crashes < 50, "no progress under {fault}");
            if fault.ends_with("before_send") {
                // The aborted transaction rolled back; make progress with
                // one clean transaction before crashing at the next one.
                let out = morlet_run_args(&spool, &cfg, &sink, &coll, None, &["--limit", "2"]);
                assert!(out.status.success());
            }
        }
        assert!(crashes >= 5, "{fault}: only {crashes} crashes");
        assert_exact(&coll, &want);
        assert_eq!(context_rows(&coll).len(), 6, "{fault}");
        assert_searchable(&coll);
        evidence(&coll);
        eprintln!("{fault}: {crashes} crashes, exact");
    }
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn binding_and_conflicts_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_b");
    let sink = unique("s");
    assert!(
        morlet_run(&spool, &cfg, &sink, &coll, None)
            .status
            .success()
    );
    // Same sink name from another spool: refused (integrity).
    let other = dir.path().join("other.sqlite");
    build_spool(&other);
    let out = morlet_run(&other, &cfg, &sink, &coll, None);
    assert_eq!(out.status.code(), Some(3));
    // Same sink, other collection: refused (integrity).
    let out = morlet_run(&spool, &cfg, &sink, &unique("akhet_o"), None);
    assert_eq!(out.status.code(), Some(3));
    // A different sink writing different samples at offsets that already
    // exist in the same recording: permanent conflict, nothing overwritten.
    let third = dir.path().join("third.sqlite");
    {
        let mut s = Spool::open(&third, MAX_DB).unwrap();
        let mut p: serde_json::Value = serde_json::from_slice(&sensor_batch(0)).unwrap();
        p["groups"][0]["signals"]["accel_x"][5] = 42.0.into();
        s.append(
            &mqtt_event("sensors/hub-1/batch", &serde_json::to_vec(&p).unwrap()),
            None,
        )
        .unwrap();
    }
    let out = morlet_run(&third, &cfg, &unique("s"), &coll, None);
    assert_eq!(out.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&out.stderr).contains("conflicting replay"));
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL and AKHET_TEST_MORLET_CERTS (ci/morletdb-test.sh)"]
fn tls_modes_and_certificate_checks() {
    let certs = std::path::PathBuf::from(
        std::env::var("AKHET_TEST_MORLET_CERTS").expect("set AKHET_TEST_MORLET_CERTS"),
    );
    let ca = certs.join("ca.pem");
    let other_ca = certs.join("other-ca.pem");
    let base = url();
    assert!(base.contains("@127.0.0.1:"), "test URL must use 127.0.0.1");
    // The server certificate names only `localhost`.
    let by_name = base.replace("@127.0.0.1:", "@localhost:");
    let with = |u: &str, q: &str| format!("{u}?{q}");
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    let want = build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let run_with = |u: &str| {
        let coll = unique("akhet_tls");
        let out = morlet_run_url(u, &spool, &cfg, &unique("s"), &coll, None, &[]);
        (out, coll)
    };

    // verify-full with the right CA and host name: exact replay.
    let (out, coll) = run_with(&with(
        &by_name,
        &format!("sslmode=verify-full&sslrootcert={}", ca.display()),
    ));
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["sslmode"], "verify-full");
    assert_exact(&coll, &want);

    // Wrong host name or untrusted CA: permanent TLS failure, not retried.
    for (u, what) in [
        (&base, ca.display().to_string()),
        (&by_name, other_ca.display().to_string()),
    ] {
        let (out, _) = run_with(&with(u, &format!("sslmode=verify-full&sslrootcert={what}")));
        assert_eq!(out.status.code(), Some(8), "{u} with {what}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("certificate"), "{err}");
        assert!(!err.contains("retry"), "not retried: {err}");
        assert!(!err.contains("morlet-test"), "no password in errors");
    }
    // Without the CA the platform trust store does not know the test CA.
    let (out, _) = run_with(&with(&by_name, "sslmode=verify-full"));
    assert_eq!(out.status.code(), Some(8));

    // verify-ca checks the chain only: the IP address is accepted.
    let (out, coll) = run_with(&with(
        &base,
        &format!("sslmode=verify-ca&sslrootcert={}", ca.display()),
    ));
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["sslmode"], "verify-ca");
    assert_exact(&coll, &want);
    // ...but not an untrusted chain.
    let (out, _) = run_with(&with(
        &base,
        &format!("sslmode=verify-ca&sslrootcert={}", other_ca.display()),
    ));
    assert_eq!(out.status.code(), Some(8));

    // Loopback without sslmode stays plain; prefer is refused.
    let (out, _) = run_with(&base);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["sslmode"], "disable");
    let (out, _) = run_with(&with(&base, "sslmode=prefer"));
    assert_eq!(out.status.code(), Some(2));
}

fn timing_issues(sink: &str) -> i64 {
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    db.query_one(
        "SELECT count(*) FROM akhet_morlet_issues WHERE sink=$1 AND source='timing'",
        &[&sink],
    )
    .unwrap()
    .get(0)
}

/// The duplicate-detection windows live in `akhet_morlet_recent`, not in
/// the state row, and survive restarts: the redelivered batch after a
/// restart is skipped as a duplicate, not reported. A version 1 state (with
/// the windows inside the row) is migrated on the next write.
#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn dedup_windows_are_stored_incrementally_and_migrated() {
    for migrate in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("s.sqlite");
        let want = build_spool(&spool);
        let cfg = decoder_file(dir.path());
        let coll = unique("akhet_w");
        let sink = unique("s");
        // Records: ctrl, b0, b1, b2, ctrl, b3 | b3 again, b4, ...
        let out = morlet_run_args(&spool, &cfg, &sink, &coll, None, &["--limit", "6"]);
        assert!(out.status.success());
        let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
        let state: serde_json::Value = db
            .query_one(
                "SELECT state FROM akhet_morlet_sinks WHERE sink=$1",
                &[&sink],
            )
            .unwrap()
            .get(0);
        assert_eq!(state["version"], 2);
        for g in state["grids"].as_object().unwrap().values() {
            assert_eq!(g["recent"], serde_json::json!([]), "no window in the row");
        }
        let blocks = db
            .query(
                "SELECT channel, placed FROM akhet_morlet_recent WHERE sink=$1 ORDER BY channel, first_unix_ns",
                &[&sink],
            )
            .unwrap();
        assert!(!blocks.is_empty());
        if migrate {
            // Rewrite as a version 1 state: windows back into the row.
            let mut st = state.clone();
            for b in &blocks {
                let ch: String = b.get(0);
                let bytes: Vec<u8> = b.get(1);
                let w = st["grids"][&ch]["recent"].as_array_mut().unwrap();
                for e in bytes.chunks(16) {
                    let t = i64::from_le_bytes(e[..8].try_into().unwrap());
                    let v = f64::from_bits(u64::from_le_bytes(e[8..].try_into().unwrap()));
                    w.push(serde_json::json!([t, v]));
                }
            }
            st["version"] = 1.into();
            db.execute(
                "UPDATE akhet_morlet_sinks SET state=$2 WHERE sink=$1",
                &[&sink, &st],
            )
            .unwrap();
            db.execute("DELETE FROM akhet_morlet_recent WHERE sink=$1", &[&sink])
                .unwrap();
        }
        let out = morlet_run(&spool, &cfg, &sink, &coll, None);
        assert!(out.status.success(), "migrate={migrate}");
        assert_exact(&coll, &want);
        if migrate {
            // A version 1 row is jsonb, which stores -0.0 as 0: the migrated
            // window cannot tell them apart, so the redelivered -0.0 samples
            // are reported as conflicts (samples are unaffected). Version 2
            // stores the window in binary and keeps -0.0.
            let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
            let other: i64 = db
                .query_one(
                    "SELECT count(*) FROM akhet_morlet_issues WHERE sink=$1 AND source='timing'
                     AND detail->>'message' <> 'value -0 differs from 0 already placed at this timestamp'",
                    &[&sink],
                )
                .unwrap()
                .get(0);
            assert_eq!(other, 0);
        } else {
            assert_eq!(timing_issues(&sink), 0, "redelivery skipped as duplicate");
        }
        let v: i64 = db
            .query_one(
                "SELECT (state->>'version')::int8 FROM akhet_morlet_sinks WHERE sink=$1",
                &[&sink],
            )
            .unwrap()
            .get(0);
        assert_eq!(v, 2);
        // Each placed sample is stored once (9 batches of 400 per channel,
        // below the 8192-sample window, so nothing was dropped yet).
        let row = db
            .query_one(
                "SELECT count(DISTINCT channel), max(n)::int8 FROM (SELECT channel, sum(length(placed))/16 AS n
                 FROM akhet_morlet_recent WHERE sink=$1 GROUP BY channel) x",
                &[&sink],
            )
            .unwrap();
        let (chans, max_n): (i64, i64) = (row.get(0), row.get(1));
        assert_eq!((chans, max_n), (2, 9 * N as i64));
    }
}

/// One source with two rates: the 40 kHz vibration recordings and a 1 kHz
/// temperature recording starting 5 ms later are channels of one
/// acquisition, and a time-interval read returns for every channel the
/// samples of its own recording at the right offsets.
#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn sources_are_acquisitions_read_on_one_time_axis() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    build_spool(&spool);
    {
        let mut sp = Spool::open(&spool, MAX_DB).unwrap();
        for m in 0..3i64 {
            let t: Vec<i64> = (0..20)
                .map(|i| T0_US + 5_000 + (m * 20 + i) * 1_000)
                .collect();
            let temp: Vec<f64> = (0..20).map(|i| 20.0 + (m * 20 + i) as f64 / 8.0).collect();
            let p = serde_json::to_vec(&serde_json::json!({"t": t, "temp": temp})).unwrap();
            sp.append(&mqtt_event("plant/hub-1/temp", &p), None)
                .unwrap();
        }
    }
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_acq");
    let out = morlet_run_args(
        &spool,
        &cfg,
        &unique("s"),
        &coll,
        None,
        &["--records-per-tx", "100"],
    );
    assert!(out.status.success());
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let acq = db
        .query(
            &format!(
                "SELECT acquisition_id, machine, (extract(epoch FROM origin_utc) * 1000000)::int8 FROM {coll}_acq"
            ),
            &[],
        )
        .unwrap();
    assert_eq!(acq.len(), 1);
    let (id, machine, origin_us): (String, String, i64) =
        (acq[0].get(0), acq[0].get(1), acq[0].get(2));
    assert_eq!(
        (id.as_str(), machine.as_str(), origin_us),
        ("hub-1", "hub-1", T0_US)
    );
    let gap_ns = (6 * N as i64 + 100) * 25_000;
    let t0_ns = T0_US * 1000;
    let channels: Vec<(String, i64, String, i64)> = db
        .query(
            &format!(
                "SELECT role, offset_ns, clock_source, offset_uncertainty_ns FROM {coll}_acq_channels
                 WHERE acquisition_id='hub-1' ORDER BY role"
            ),
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    let ch = |role: String, off: i64, tol: i64| (role, off, "daq".to_string(), tol);
    assert_eq!(
        channels,
        vec![
            ch(
                format!("thermal/temp@{}", t0_ns + 5_000_000),
                5_000_000,
                2000
            ),
            ch(format!("vib/accel_x@{t0_ns}"), 0, 2000),
            ch(format!("vib/accel_x@{}", t0_ns + gap_ns), gap_ns, 2000),
            ch(format!("vib/accel_y@{t0_ns}"), 0, 2000),
            ch(format!("vib/accel_y@{}", t0_ns + gap_ns), gap_ns, 2000),
        ]
    );

    // [10 ms, 70 ms) in acquisition time.
    let rows = db
        .query(
            "SELECT role, recording_id, start_sample, end_sample, samples
             FROM morlet_read_acquisition($1::text::regclass, 'hub-1', 10000000, 70000000) ORDER BY role",
            &[&coll],
        )
        .unwrap();
    let got: Vec<(String, i64, i64)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(2), r.get(3)))
        .collect();
    assert_eq!(
        got,
        vec![
            // 1 kHz from 5 ms: samples 5..60 (the recording has 60).
            (format!("thermal/temp@{}", t0_ns + 5_000_000), 5, 60),
            // 40 kHz from 0: samples 400..2400 (the recording has 2400).
            (format!("vib/accel_x@{t0_ns}"), 400, 2400),
            // 40 kHz from 62.5 ms: samples 0..300.
            (format!("vib/accel_x@{}", t0_ns + gap_ns), 0, 300),
            (format!("vib/accel_y@{t0_ns}"), 400, 2400),
            (format!("vib/accel_y@{}", t0_ns + gap_ns), 0, 300),
        ]
    );
    for r in &rows {
        let (rec, s, e, samples): (String, i64, i64, Vec<f64>) =
            (r.get(1), r.get(2), r.get(3), r.get(4));
        let direct: Vec<f64> = db
            .query_one(
                "SELECT samples FROM morlet_read_interval($1::text::regclass, $2, $3, $4)",
                &[&coll, &rec, &s, &e],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            samples.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            direct.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            "{rec}"
        );
    }
    let temp: Vec<f64> = rows[0].get(4);
    assert_eq!(temp[0], 20.0 + 5.0 / 8.0);
}

#[test]
#[ignore = "needs AKHET_TEST_MORLET_URL"]
fn acquisitions_can_be_turned_off() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("s.sqlite");
    let want = build_spool(&spool);
    let cfg = decoder_file(dir.path());
    let coll = unique("akhet_noacq");
    let out = morlet_run_args(
        &spool,
        &cfg,
        &unique("s"),
        &coll,
        None,
        &["--no-acquisitions"],
    );
    assert!(out.status.success());
    assert_exact(&coll, &want);
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let exists: bool = db
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&format!("{coll}_acq")],
        )
        .unwrap()
        .get(0);
    assert!(!exists);
}
