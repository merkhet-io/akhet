//! Redelivery scenarios against an external MQTT broker (e.g. Mosquitto in CI).
//!
//! Ignored by default. Run with a broker listening on 127.0.0.1:
//!
//!     AKHET_TEST_BROKER_PORT=1883 cargo test --test mqtt_external -- --ignored

mod support;

use akhet::faults::point;
use std::process::Stdio;
use std::time::Duration;
use support::*;

fn broker_port() -> u16 {
    std::env::var("AKHET_TEST_BROKER_PORT")
        .expect("set AKHET_TEST_BROKER_PORT")
        .parse()
        .expect("port")
}

fn args<'a>(port: &'a str, client: &'a str, topic: &'a str) -> Vec<&'a str> {
    vec![
        "mqtt",
        "--host",
        "127.0.0.1",
        "--port",
        port,
        "--client-id",
        client,
        "--topic",
        topic,
        "--keep-alive-secs",
        "5",
    ]
}

/// Start the collector, wait for its subscription to exist, stop it (SIGTERM).
fn establish(db: &std::path::Path, port: &str, client: &str, topic: &str) {
    let child = akhet(db, &args(port, client, topic), None)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(2000));
    std::process::Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(child.wait_with_output().unwrap().status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs an external broker (AKHET_TEST_BROKER_PORT)"]
async fn external_broker_redelivers_after_crash_between_commit_and_ack() {
    let port = broker_port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let topic = format!("akhet-ext/{}", std::process::id());
    let filter = format!("{topic}/#");
    establish(&db, &p, "akhet-ext-crash", &filter);
    publish(
        port,
        "akhet-ext-pub",
        &[(&format!("{topic}/a"), b"x1".to_vec(), false)],
    )
    .await;

    let out = run(
        &db,
        &args(&p, "akhet-ext-crash", &filter),
        Some(&format!("abort:{}", point::MQTT_AFTER_COMMIT)),
    );
    assert!(killed_by_signal(&out));
    assert_eq!(count(&db), 1);

    let mut a = args(&p, "akhet-ext-crash", &filter);
    a.extend_from_slice(&["--limit", "1"]);
    let mut child = akhet(&db, &a, None).stdout(Stdio::piped()).spawn().unwrap();
    let done = wait_until(Duration::from_secs(20), || {
        child.try_wait().unwrap().is_some()
    });
    if !done {
        let _ = child.kill();
    }
    assert!(done, "broker did not redeliver the unacknowledged message");
    let evs = events(&db);
    assert_eq!(evs.len(), 2, "at-least-once: stored twice");
    eprintln!("external broker redelivery dup flag: {}", evs[1]["dup"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs an external broker (AKHET_TEST_BROKER_PORT)"]
async fn external_broker_redelivers_messages_left_unacked_by_graceful_shutdown() {
    let port = broker_port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let topic = format!("akhet-ext2/{}", std::process::id());
    let filter = format!("{topic}/#");
    establish(&db, &p, "akhet-ext-shut", &filter);
    let msgs: Vec<(String, Vec<u8>, bool)> = (0..10)
        .map(|i| (format!("{topic}/x"), format!("s{i}").into_bytes(), false))
        .collect();
    let refs: Vec<(&str, Vec<u8>, bool)> = msgs
        .iter()
        .map(|(t, b, r)| (t.as_str(), b.clone(), *r))
        .collect();
    publish(port, "akhet-ext-pub2", &refs).await;

    // Slow commits; SIGTERM after the first commit. Small group-commit
    // batches keep messages queued behind the slow commits (with the default
    // batch size one transaction would take all ten).
    let mut slow = args(&p, "akhet-ext-shut", &filter);
    slow.extend(["--max-batch", "2"]);
    let child = akhet(
        &db,
        &slow,
        Some(&format!("delay:{}:300", point::COLLECTOR_COMMIT)),
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    assert!(wait_until(Duration::from_secs(10), || count(&db) >= 1));
    std::process::Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let first = count(&db);
    assert!(first < 10);

    let n = (10 - first).to_string();
    let mut a = args(&p, "akhet-ext-shut", &filter);
    a.extend_from_slice(&["--limit", &n]);
    let mut child = akhet(&db, &a, None).stdout(Stdio::piped()).spawn().unwrap();
    let done = wait_until(Duration::from_secs(20), || {
        child.try_wait().unwrap().is_some()
    });
    if !done {
        let _ = child.kill();
    }
    assert!(done, "unacknowledged messages were not redelivered");
    let mut payloads: Vec<String> = events(&db)
        .iter()
        .map(|e| e["payload_base64"].as_str().unwrap().to_string())
        .collect();
    payloads.sort();
    payloads.dedup();
    assert_eq!(
        payloads.len(),
        10,
        "no message lost across graceful shutdown"
    );
}

fn multi_args(port: &str, client: &str, topics: &[&str]) -> Vec<String> {
    let mut a: Vec<String> = ["mqtt", "--host", "127.0.0.1", "--port", port]
        .iter()
        .map(|s| s.to_string())
        .collect();
    a.extend(["--client-id".to_string(), client.to_string()]);
    for t in topics {
        a.extend(["--topic".to_string(), t.to_string()]);
    }
    a.extend(["--keep-alive-secs".to_string(), "5".to_string()]);
    a
}

fn sigterm(child: std::process::Child) -> serde_json::Value {
    std::process::Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{:?}", out.status);
    stdout_json(&out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs an external broker (AKHET_TEST_BROKER_PORT)"]
async fn external_broker_applies_changed_topic_set_to_existing_session() {
    use akhet::adapters::mqtt::{MqttConfig, SubscriptionRecord, subscription_key};
    let port = broker_port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let pid = std::process::id();
    let client = "akhet-ext-change";
    let line_t = format!("akhet-ext3-{pid}/plant/line1/spindle");
    let line = format!("akhet-ext3-{pid}/plant/line1/#");
    let hub = format!("akhet-ext3-hub-{pid}/sensors/hub-1");
    let cell_t = format!("akhet-ext3-cell-{pid}/7/temp");
    let cell = format!("akhet-ext3-cell-{pid}/7/+");

    // Session holding {line, hub}.
    let a = multi_args(&p, client, &[&line, &hub]);
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let child = akhet(&db, &a, None).stdout(Stdio::piped()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(2000));
    sigterm(child);
    publish(
        port,
        "akhet-ext-pub3",
        &[
            (&line_t, b"l0".to_vec(), false),
            (&hub, b"h0".to_vec(), false),
        ],
    )
    .await;

    // Restart with {line, cell}: hub is unsubscribed, cell subscribed. h0 was
    // queued for the session before the UNSUBSCRIBE and is still delivered,
    // stored with an empty `subscription`.
    let a = multi_args(&p, client, &[&line, &cell]);
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let child = akhet(&db, &a, None).stdout(Stdio::piped()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(2000));
    publish(
        port,
        "akhet-ext-pub4",
        &[
            (&hub, b"h1".to_vec(), false),
            (&cell_t, b"c1".to_vec(), false),
            (&line_t, b"l1".to_vec(), false),
        ],
    )
    .await;
    assert!(wait_until(Duration::from_secs(20), || count(&db) >= 4));
    std::thread::sleep(Duration::from_millis(1000));
    let summary = sigterm(child);
    assert_eq!(summary["unmatched_messages"], 1, "{summary}");

    let evs = events(&db);
    let payloads = |t: &str| -> Vec<String> {
        let mut v: Vec<String> = evs
            .iter()
            .filter(|e| e["topic"] == t)
            .map(|e| e["payload_base64"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    use base64::Engine as _;
    let b64 = |s: &[u8]| base64::engine::general_purpose::STANDARD.encode(s);
    assert_eq!(payloads(&hub), vec![b64(b"h0")], "h1 must not be delivered");
    assert_eq!(payloads(&cell_t), vec![b64(b"c1")]);
    assert_eq!(payloads(&line_t), vec![b64(b"l0"), b64(b"l1")]);
    let h0 = evs.iter().find(|e| e["topic"] == hub.as_str()).unwrap();
    assert_eq!(h0["subscription"], "");

    let cfg = MqttConfig::with_topics("127.0.0.1", port, client, vec![]);
    let spool = akhet::spool::Spool::open(&db, MAX_DB).unwrap();
    let rec: SubscriptionRecord =
        serde_json::from_str(&spool.meta_get(&subscription_key(&cfg)).unwrap().unwrap()).unwrap();
    let want: std::collections::BTreeSet<String> = [line, cell].into_iter().collect();
    assert_eq!(rec.confirmed, want);
    assert_eq!(rec.possible, want, "UNSUBACK recorded");
}
