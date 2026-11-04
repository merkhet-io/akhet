//! Broker restarts with a persistent Mosquitto (#12).
//!
//! Ignored by default: the tests restart the broker, so they need their own
//! Mosquitto 2 container with `persistence true`, run one at a time:
//!
//!     docker run -d --name akhet-mosquitto-persist -p 127.0.0.1:1884:1883 \
//!       -v "$PWD/ci/mosquitto-persist.conf:/mosquitto/config/mosquitto.conf:ro" \
//!       eclipse-mosquitto:2
//!     AKHET_TEST_PERSISTENT_BROKER_PORT=1884 \
//!     AKHET_TEST_BROKER_CONTAINER=akhet-mosquitto-persist \
//!       cargo test --test mqtt_broker_restart -- --ignored --test-threads=1
//!
//! What they show: QoS 1 messages queued for Akhet's persistent session, and
//! messages Akhet committed but did not acknowledge, survive a broker restart
//! and are delivered afterwards; a running collector reconnects to the
//! restarted broker without losing messages. Mosquitto writes its state on a
//! clean shutdown and every `autosave_interval`; after a hard kill only what
//! was saved before survives, which the last test makes explicit.

mod support;

use akhet::faults::point;
use std::process::{Command, Stdio};
use std::time::Duration;
use support::*;

fn port() -> u16 {
    std::env::var("AKHET_TEST_PERSISTENT_BROKER_PORT")
        .expect("set AKHET_TEST_PERSISTENT_BROKER_PORT")
        .parse()
        .expect("port")
}

fn container() -> String {
    std::env::var("AKHET_TEST_BROKER_CONTAINER").expect("set AKHET_TEST_BROKER_CONTAINER")
}

fn docker(args: &[&str]) {
    let st = Command::new("docker")
        .args(args)
        .stdout(Stdio::null())
        .status()
        .expect("docker");
    assert!(st.success(), "docker {args:?}");
}

/// Wait until the broker answers an MQTT CONNECT (a TCP accept through the
/// Docker port mapping is not enough).
async fn broker_up(port: u16) {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (client, mut el) =
            AsyncClient::new(MqttOptions::new("akhet-probe", "127.0.0.1", port), 10);
        let r = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match el.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => return true,
                    Ok(_) => {}
                    Err(_) => return false,
                }
            }
        })
        .await;
        drop(client);
        if r == Ok(true) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker did not come back"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Clean restart: Mosquitto saves its persistence file on SIGTERM.
async fn restart_broker() {
    docker(&["restart", "-t", "10", &container()]);
    broker_up(port()).await;
}

/// Hard restart: SIGKILL, so only the last autosave survives.
async fn kill_and_start_broker() {
    docker(&["kill", &container()]);
    docker(&["start", &container()]);
    broker_up(port()).await;
}

fn args(port: &str, client: &str, filter: &str) -> Vec<String> {
    [
        "mqtt",
        "--host",
        "127.0.0.1",
        "--port",
        port,
        "--client-id",
        client,
        "--topic",
        filter,
        "--keep-alive-secs",
        "5",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn refs(a: &[String]) -> Vec<&str> {
    a.iter().map(String::as_str).collect()
}

/// Start the collector, wait for its subscription, stop it (SIGTERM): the
/// broker now holds a persistent session that queues QoS 1 messages.
fn establish(db: &std::path::Path, port: &str, client: &str, filter: &str) {
    let a = args(port, client, filter);
    let child = akhet(db, &refs(&a), None)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(2000));
    sigterm(&child);
    assert!(child.wait_with_output().unwrap().status.success());
}

fn sigterm(child: &std::process::Child) {
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
}

/// Run the collector until it has taken `n` messages; fail after 30 s.
fn collect(db: &std::path::Path, port: &str, client: &str, filter: &str, n: usize) {
    let mut a = args(port, client, filter);
    a.extend(["--limit".to_string(), n.to_string()]);
    let mut child = akhet(db, &refs(&a), None)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let done = wait_until(Duration::from_secs(30), || {
        child.try_wait().unwrap().is_some()
    });
    if !done {
        let _ = child.kill();
    }
    assert!(done, "collector did not receive {n} messages");
    assert!(child.wait_with_output().unwrap().status.success());
}

fn messages(topic: &str, tag: &str, n: usize) -> Vec<(String, Vec<u8>, bool)> {
    (0..n)
        .map(|i| {
            (
                format!("{topic}/x"),
                format!("{tag}-{i}").into_bytes(),
                false,
            )
        })
        .collect()
}

async fn publish_all(port: u16, client: &str, msgs: &[(String, Vec<u8>, bool)]) {
    let r: Vec<(&str, Vec<u8>, bool)> = msgs
        .iter()
        .map(|(t, b, k)| (t.as_str(), b.clone(), *k))
        .collect();
    publish(port, client, &r).await;
}

/// Distinct payloads in the spool, and the number of records.
fn stored(db: &std::path::Path) -> (Vec<String>, usize) {
    let evs = events(db);
    let mut p: Vec<String> = evs
        .iter()
        .map(|e| e["payload_base64"].as_str().unwrap().to_string())
        .collect();
    p.sort();
    p.dedup();
    (p, evs.len())
}

fn b64(s: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn expected(tag: &str, n: usize) -> Vec<String> {
    let mut v: Vec<String> = (0..n).map(|i| b64(&format!("{tag}-{i}"))).collect();
    v.sort();
    v
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}-{}", std::process::id(), event_now())
}

fn event_now() -> i64 {
    akhet::event::now_unix_ns() % 1_000_000_000
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "restarts a persistent broker (AKHET_TEST_PERSISTENT_BROKER_PORT)"]
async fn queued_messages_survive_a_broker_restart() {
    let port = port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let client = unique("akhet-restart-q");
    let topic = format!("akhet-restart/{client}");
    let filter = format!("{topic}/#");
    establish(&db, &p, &client, &filter);

    // Published while the collector is down: queued in its session.
    publish_all(port, "akhet-restart-pub", &messages(&topic, "q", 50)).await;
    restart_broker().await;

    collect(&db, &p, &client, &filter, 50);
    let (distinct, records) = stored(&db);
    assert_eq!(
        distinct,
        expected("q", 50),
        "every queued message delivered"
    );
    assert_eq!(records, 50);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "restarts a persistent broker (AKHET_TEST_PERSISTENT_BROKER_PORT)"]
async fn committed_but_unacknowledged_messages_are_redelivered_after_a_broker_restart() {
    let port = port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let client = unique("akhet-restart-c");
    let topic = format!("akhet-restart/{client}");
    let filter = format!("{topic}/#");
    establish(&db, &p, &client, &filter);
    publish_all(port, "akhet-restart-pub", &messages(&topic, "c", 10)).await;

    // Commit, then die before any PUBACK reaches the broker.
    let a = args(&p, &client, &filter);
    let out = run(
        &db,
        &refs(&a),
        Some(&format!("abort:{}", point::MQTT_AFTER_COMMIT)),
    );
    assert!(killed_by_signal(&out));
    let before = count(&db) as usize;
    assert!(before >= 1);

    restart_broker().await;
    collect(&db, &p, &client, &filter, 10);
    let (distinct, records) = stored(&db);
    assert_eq!(distinct, expected("c", 10), "nothing lost");
    // At-least-once: the committed-but-unacknowledged ones are stored again.
    assert_eq!(records, before + 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "restarts a persistent broker (AKHET_TEST_PERSISTENT_BROKER_PORT)"]
async fn a_running_collector_reconnects_across_a_broker_restart() {
    let port = port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let client = unique("akhet-restart-r");
    let topic = format!("akhet-restart/{client}");
    let filter = format!("{topic}/#");
    establish(&db, &p, &client, &filter);

    let mut a = args(&p, &client, &filter);
    a.extend(["--limit".to_string(), "40".to_string()]);
    let mut child = akhet(&db, &refs(&a), None)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    publish_all(port, "akhet-restart-pub", &messages(&topic, "before", 20)).await;
    assert!(wait_until(Duration::from_secs(20), || count(&db) == 20));

    restart_broker().await;
    publish_all(port, "akhet-restart-pub", &messages(&topic, "after", 20)).await;
    let done = wait_until(Duration::from_secs(60), || {
        child.try_wait().unwrap().is_some()
    });
    if !done {
        let _ = child.kill();
    }
    assert!(done, "collector did not resume after the broker restart");
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let summary = stdout_json(&out);
    assert!(summary["connections"].as_u64().unwrap() >= 2, "{summary}");

    let (distinct, _) = stored(&db);
    let mut want = expected("before", 20);
    want.extend(expected("after", 20));
    want.sort();
    assert_eq!(distinct, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "restarts a persistent broker (AKHET_TEST_PERSISTENT_BROKER_PORT)"]
async fn queued_messages_survive_a_broker_kill_once_autosaved() {
    let port = port();
    let p = port.to_string();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let client = unique("akhet-restart-k");
    let topic = format!("akhet-restart/{client}");
    let filter = format!("{topic}/#");
    establish(&db, &p, &client, &filter);
    publish_all(port, "akhet-restart-pub", &messages(&topic, "k", 20)).await;

    // ci/mosquitto-persist.conf saves every second; wait for two autosaves,
    // then SIGKILL. Messages newer than the last autosave would be lost by
    // the broker, and Akhet could not tell.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    kill_and_start_broker().await;

    collect(&db, &p, &client, &filter, 20);
    let (distinct, records) = stored(&db);
    assert_eq!(distinct, expected("k", 20));
    assert_eq!(records, 20);
}
