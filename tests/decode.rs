//! Decoding spooled MQTT payloads: library round trip and the `decode` CLI.

mod support;

use akhet::decode::{DecoderConfig, IssueKind};
use akhet::event::{Event, EventData, MqttMessage};
use akhet::spool::Spool;
use base64::Engine as _;
use support::*;

fn mqtt_event(topic: &str, payload: &[u8]) -> Event {
    Event::new(
        EventData::MqttMessage(MqttMessage {
            broker: "127.0.0.1:1883".into(),
            client_id: "test".into(),
            subscription: "#".into(),
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

/// Synthetic batch: `groups` signal groups, `n` samples each, 3 signals.
fn batch_payload(t0_us: i64, n: usize, seed: f64) -> Vec<u8> {
    let groups: Vec<serde_json::Value> = ["vib", "elec"]
        .iter()
        .enumerate()
        .map(|(gi, name)| {
            let ts: Vec<i64> = (0..n as i64).map(|i| t0_us + i * 25).collect();
            let sig = |k: f64| -> Vec<f64> {
                (0..n).map(|i| (seed + k + i as f64 * 0.37).sin() * 1e3 / 7.0).collect()
            };
            if gi == 0 {
                serde_json::json!({"name": name, "timestamps": ts,
                    "signals": {"accel_x": sig(0.0), "accel_y": sig(1.0), "accel_z": sig(2.0)}})
            } else {
                serde_json::json!({"name": name, "timestamps": ts, "signals": {"current": sig(3.0)}})
            }
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({"groups": groups})).unwrap()
}

fn example_config() -> DecoderConfig {
    DecoderConfig::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/decoder.json"),
    )
    .unwrap()
}

#[test]
fn spool_roundtrip_multiple_groups_bit_exact() {
    let dir = tempfile::tempdir().unwrap();
    let mut spool = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    let cfg = example_config();
    let payloads: Vec<Vec<u8>> = (0..5)
        .map(|k| batch_payload(1_700_000_000_000_000 + k * 4000 * 25, 4000, k as f64))
        .collect();
    for p in &payloads {
        spool
            .append(&mqtt_event("sensors/hub-1/batch", p), None)
            .unwrap();
    }
    let recs = spool.read_after(0, 100).unwrap();
    assert_eq!(recs.len(), 5);
    for (rec, payload) in recs.iter().zip(&payloads) {
        let ev = akhet::event::decode(&rec.body).unwrap();
        let d = cfg.decode_event(&ev).unwrap();
        assert!(d.issues.is_empty(), "{:?}", d.issues);
        assert_eq!(d.batches.len(), 4, "3 vib signals + 1 elec signal");
        let src: serde_json::Value = serde_json::from_slice(payload).unwrap();
        for b in &d.batches {
            assert_eq!(b.channel.source, "hub-1");
            let g = src["groups"]
                .as_array()
                .unwrap()
                .iter()
                .find(|g| g["name"] == b.channel.group.as_str())
                .unwrap();
            let want: Vec<f64> =
                serde_json::from_value(g["signals"][&b.channel.signal].clone()).unwrap();
            assert_eq!(b.values.len(), want.len());
            assert!(
                b.values
                    .iter()
                    .zip(&want)
                    .all(|(a, w)| a.to_bits() == w.to_bits())
            );
            let ts: Vec<i64> = serde_json::from_value(g["timestamps"].clone()).unwrap();
            assert_eq!(
                b.timestamps_unix_ns,
                ts.iter().map(|t| t * 1000).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn malformed_payloads_yield_explicit_issues() {
    let cfg = example_config();
    let cases: [(&str, &[u8], IssueKind); 4] = [
        ("sensors/hub-1/batch", b"\xff\xfe", IssueKind::NotJson),
        (
            "sensors/hub-1/batch",
            br#"{"groups":{}}"#,
            IssueKind::BadStructure,
        ),
        (
            "sensors/hub-1/batch",
            br#"{"groups":[{"name":"vib","timestamps":[1,2],"signals":{"accel_x":[1,"x"]}}]}"#,
            IssueKind::NonFiniteValue,
        ),
        ("unrelated/topic", b"{}", IssueKind::NoMatchingRule),
    ];
    for (topic, p, kind) in cases {
        let d = cfg.decode_event(&mqtt_event(topic, p)).unwrap();
        assert!(d.batches.is_empty());
        assert_eq!(d.issues.len(), 1);
        assert_eq!(d.issues[0].kind, kind);
    }
}

#[test]
fn decode_cli_reports_batches_and_issues() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sqlite");
    {
        let mut spool = Spool::open(&path, MAX_DB).unwrap();
        spool
            .append(
                &mqtt_event("sensors/hub-1/batch", &batch_payload(1000, 10, 0.0)),
                None,
            )
            .unwrap();
        spool
            .append(
                &mqtt_event("sensors/hub-1/batch", b"{\"groups\":[{\"name\":\"vib\"}]}"),
                None,
            )
            .unwrap();
    }
    let cfg = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/decoder.json");
    let out = run(&path, &["decode", "--config", cfg.to_str().unwrap()], None);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["batches"].as_array().unwrap().len(), 4);
    assert_eq!(lines[0]["batches"][0]["samples"], 10);
    assert_eq!(lines[1]["issues"][0]["kind"], "bad_structure");
    let s = &lines[2];
    assert_eq!(s["summary"], true);
    assert_eq!(s["mqtt_records"], 2);
    assert_eq!(s["records_with_issues"], 1);
    assert_eq!(s["samples"], 40);
    assert_eq!(s["issues_by_kind"]["bad_structure"], 1);

    let bad = dir.path().join("bad.json");
    std::fs::write(&bad, br#"{"format_version":1,"rules":[]}"#).unwrap();
    let out = run(&path, &["decode", "--config", bad.to_str().unwrap()], None);
    assert_eq!(out.status.code(), Some(2));
}
