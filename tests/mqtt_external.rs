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

    // Slow commits; SIGTERM after the first commit.
    let child = akhet(
        &db,
        &args(&p, "akhet-ext-shut", &filter),
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
