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
   "context_signals":["program"]}]}"#;

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

    // Nothing left: a second run writes nothing and changes nothing.
    let out = morlet_run(&spool, &cfg, &sink, &coll, None);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["records"], 0);
    assert_exact(&coll, &want);

    // Timing evidence and binding.
    let mut db = postgres::Client::connect(&url(), postgres::NoTls).unwrap();
    let n: i64 = db
        .query_one(
            "SELECT count(*) FROM akhet_morlet_evidence WHERE collection=$1 AND max_abs_deviation_ns=0",
            &[&coll],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 2 * 9);
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
