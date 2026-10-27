//! MQTT integration tests against a real loopback broker (rumqttd).

mod support;

use akhet::adapters::mqtt::{self, MqttConfig, SourceIdentityConfig};
use akhet::adapters::{AsyncSpool, Stop};
use akhet::faults::{Faults, point};
use akhet::spool::{DEFAULT_MAX_EVENT_BYTES, Spool};
use base64::Engine as _;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use support::*;

fn payloads(db: &Path) -> Vec<Vec<u8>> {
    events(db)
        .iter()
        .map(|e| {
            base64::engine::general_purpose::STANDARD
                .decode(e["payload_base64"].as_str().unwrap())
                .unwrap()
        })
        .collect()
}

fn multiset(v: Vec<Vec<u8>>) -> BTreeMap<Vec<u8>, usize> {
    let mut m = BTreeMap::new();
    for p in v {
        *m.entry(p).or_insert(0) += 1;
    }
    m
}

fn cfg(port: u16, client: &str, topic: &str) -> MqttConfig {
    let mut c = MqttConfig::new("127.0.0.1", port, client, topic);
    c.keep_alive = Duration::from_secs(5);
    c.reconnect_min = Duration::from_millis(50);
    c.reconnect_max = Duration::from_millis(500);
    c
}

/// Attach a readiness signal to `c`; await the returned future after spawning
/// the collector to know its subscription is active.
fn ready(c: &mut MqttConfig) -> impl std::future::Future<Output = ()> + use<> {
    let n = std::sync::Arc::new(tokio::sync::Notify::new());
    c.ready = Some(n.clone());
    async move {
        tokio::time::timeout(Duration::from_secs(20), n.notified())
            .await
            .expect("subscription became active");
    }
}

async fn run_collector(
    c: MqttConfig,
    db: &Path,
    faults: Faults,
    stop: Stop,
) -> Result<mqtt::MqttSummary, akhet::adapters::CollectError> {
    let spool = AsyncSpool::new(Spool::open(db, MAX_DB).unwrap(), faults);
    mqtt::run(c, spool, DEFAULT_MAX_EVENT_BYTES, stop).await
}

/// Start a collector to create the persistent session and subscription, then
/// stop it gracefully. Messages published afterwards are queued by the broker.
async fn establish_session(port: u16, client: &str, topic: &str, db: &Path) {
    let stop = Stop::new();
    let s2 = stop.clone();
    let db2 = db.to_path_buf();
    let mut c = cfg(port, client, topic);
    let r = ready(&mut c);
    let h = tokio::spawn(async move { run_collector(c, &db2, Faults::none(), s2).await });
    r.await;
    stop.trigger();
    h.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn captures_payload_bytes_topic_and_flags() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    // Retained message published before the collector subscribes.
    publish(
        port,
        "pub-r",
        &[("plant/a/retained", b"retained-1".to_vec(), true)],
    )
    .await;

    let mut c = cfg(port, "akhet-capture", "plant/#");
    let r = ready(&mut c);
    c.limit = Some(4);
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, Faults::none(), Stop::new()).await });
    r.await;
    let binary: Vec<u8> = (0u8..=255).collect();
    publish(
        port,
        "pub-1",
        &[
            ("plant/a/raw", binary.clone(), false),
            ("plant/b/json", br#"{"v":[1,2,3]}"#.to_vec(), false),
            ("plant/a/empty", Vec::new(), false),
        ],
    )
    .await;
    let summary = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .expect("collector finished")
        .unwrap()
        .unwrap();
    assert_eq!(summary.committed, 4, "{summary:?}");
    assert_eq!(summary.acks_enqueued, 4);

    let evs = events(&db);
    assert_eq!(evs.len(), 4);
    let by_topic: BTreeMap<String, serde_json::Value> = evs
        .iter()
        .map(|e| (e["topic"].as_str().unwrap().to_string(), e.clone()))
        .collect();
    let raw = &by_topic["plant/a/raw"];
    assert_eq!(raw["kind"], "mqtt_message");
    assert_eq!(raw["format_version"], 1);
    assert_eq!(raw["qos"], 1);
    assert_eq!(raw["retain"], false);
    assert_eq!(raw["payload_len"], 256);
    assert_eq!(raw["client_id"], "akhet-capture");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(raw["payload_base64"].as_str().unwrap())
            .unwrap(),
        binary
    );
    assert_eq!(by_topic["plant/a/retained"]["retain"], true);
    assert_eq!(by_topic["plant/a/empty"]["payload_len"], 0);
    assert!(raw["collected_unix_ns"].as_i64().unwrap() > 0);
    assert!(raw.get("packet_id").is_none());
}

/// Run the CLI collector as a separate process that aborts at `fault`.
fn crash_collector(port: u16, client: &str, db: &Path, fault: &str, extra: &[&str]) {
    let port_s = port.to_string();
    let mut args = vec![
        "mqtt",
        "--host",
        "127.0.0.1",
        "--port",
        &port_s,
        "--client-id",
        client,
        "--topic",
        "crash/#",
        "--keep-alive-secs",
        "5",
    ];
    args.extend_from_slice(extra);
    let out = run(db, &args, Some(fault));
    assert!(
        killed_by_signal(&out),
        "collector should have aborted: {:?}",
        out.status
    );
}

fn collect_n(port: u16, client: &str, db: &Path, n: u64, extra: &[&str]) -> serde_json::Value {
    let port_s = port.to_string();
    let n_s = n.to_string();
    let mut args = vec![
        "mqtt",
        "--host",
        "127.0.0.1",
        "--port",
        &port_s,
        "--client-id",
        client,
        "--topic",
        "crash/#",
        "--keep-alive-secs",
        "5",
        "--limit",
        &n_s,
    ];
    args.extend_from_slice(extra);
    let mut child = akhet(db, &args, None)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let ok = wait_until(Duration::from_secs(20), || {
        child.try_wait().unwrap().is_some()
    });
    if !ok {
        let _ = child.kill();
        panic!("collector did not reach limit {n}");
    }
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{:?}", out.status);
    stdout_json(&out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_commit_before_ack_redelivers_as_duplicate_event() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    establish_session(port, "akhet-crash-a", "crash/#", &db).await;
    publish(port, "pub-ca", &[("crash/x", b"m1".to_vec(), false)]).await;

    // Commits m1, then dies before the PUBACK is queued.
    crash_collector(
        port,
        "akhet-crash-a",
        &db,
        &format!("abort:{}", point::MQTT_AFTER_COMMIT),
        &[],
    );
    assert_eq!(count(&db), 1, "commit before crash is durable");

    // Restart with the same client id: the broker redelivers m1.
    let summary = collect_n(port, "akhet-crash-a", &db, 1, &[]);
    assert_eq!(summary["committed"], 1);
    let evs = events(&db);
    assert_eq!(
        evs.len(),
        2,
        "at-least-once: redelivery stored as a second event"
    );
    assert_eq!(payloads(&db), vec![b"m1".to_vec(), b"m1".to_vec()]);
    assert_ne!(evs[0]["event_id"], evs[1]["event_id"]);
    eprintln!("redelivered dup flag: {}", evs[1]["dup"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_commit_with_source_identity_is_deduplicated() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let ident = [
        "--source-id-namespace",
        "test-publisher-seq",
        "--source-id-json-pointer",
        "/id",
    ];
    establish_session(port, "akhet-crash-b", "crash/#", &db).await;
    publish(
        port,
        "pub-cb",
        &[("crash/x", br#"{"id":7,"v":1}"#.to_vec(), false)],
    )
    .await;
    crash_collector(
        port,
        "akhet-crash-b",
        &db,
        &format!("abort:{}", point::MQTT_AFTER_COMMIT),
        &ident,
    );
    assert_eq!(count(&db), 1);
    let summary = collect_n(port, "akhet-crash-b", &db, 1, &ident);
    assert_eq!(summary["source_duplicates"], 1, "{summary}");
    assert_eq!(summary["committed"], 0);
    let evs = events(&db);
    assert_eq!(
        evs.len(),
        1,
        "redelivery recognized by configured source identity"
    );
    assert_eq!(evs[0]["source_identity"]["key"], "7");
    assert_eq!(evs[0]["source_identity"]["namespace"], "test-publisher-seq");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_source_identity_stops_without_ack() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let mut c = cfg(port, "akhet-conflict", "crash/#");
    let r = ready(&mut c);
    c.source_identity = Some(SourceIdentityConfig {
        namespace: "ns".into(),
        json_pointer: "/id".into(),
    });
    let db2 = db.clone();
    let c2 = c.clone();
    let h = tokio::spawn(async move { run_collector(c2, &db2, Faults::none(), Stop::new()).await });
    r.await;
    publish(
        port,
        "pub-conf",
        &[
            ("crash/x", br#"{"id":1,"v":"a"}"#.to_vec(), false),
            ("crash/x", br#"{"id":1,"v":"DIFFERENT"}"#.to_vec(), false),
        ],
    )
    .await;
    let res = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap();
    let err = res.expect_err("conflict must stop the collector");
    assert!(
        err.to_string()
            .contains("conflicting reuse of source identity"),
        "{err}"
    );
    assert_eq!(
        count(&db),
        1,
        "first message kept, conflicting one not stored"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_commit_loses_nothing() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    establish_session(port, "akhet-crash-c", "crash/#", &db).await;
    publish(port, "pub-cc", &[("crash/x", b"before".to_vec(), false)]).await;
    crash_collector(
        port,
        "akhet-crash-c",
        &db,
        &format!("abort:{}", point::MQTT_BEFORE_COMMIT),
        &[],
    );
    assert_eq!(count(&db), 0);
    collect_n(port, "akhet-crash-c", &db, 1, &[]);
    assert_eq!(payloads(&db), vec![b"before".to_vec()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_saturation_applies_backpressure_without_loss() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let mut c = cfg(port, "akhet-sat", "sat/#");
    let r = ready(&mut c);
    c.queue_capacity = 1;
    c.limit = Some(20);
    let faults = Faults::none().with_delay(point::COLLECTOR_COMMIT, Duration::from_millis(40));
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, faults, Stop::new()).await });
    r.await;
    let msgs: Vec<(&str, Vec<u8>, bool)> = (0..20)
        .map(|i| ("sat/x", format!("m{i}").into_bytes(), false))
        .collect();
    publish(port, "pub-sat", &msgs).await;
    let summary = tokio::time::timeout(Duration::from_secs(30), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(summary.queue_saturation_events > 0, "{summary:?}");
    assert_eq!(summary.committed, 20);
    let got = multiset(payloads(&db));
    assert_eq!(got.len(), 20);
    assert!(got.values().all(|n| *n == 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_does_not_ack_queued_uncommitted_messages() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    establish_session(port, "akhet-shut", "shut/#", &db).await;
    let msgs: Vec<(&str, Vec<u8>, bool)> = (0..10)
        .map(|i| ("shut/x", format!("s{i}").into_bytes(), false))
        .collect();
    publish(port, "pub-shut", &msgs).await;

    // Slow commits so most messages sit in the queue when shutdown arrives.
    let mut c = cfg(port, "akhet-shut", "shut/#");
    c.queue_capacity = 16;
    let faults = Faults::none().with_delay(point::COLLECTOR_COMMIT, Duration::from_millis(300));
    let stop = Stop::new();
    let s2 = stop.clone();
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, faults, s2).await });
    assert!(wait_until(Duration::from_secs(10), || count(&db) >= 1));
    stop.trigger();
    let summary = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let first = count(&db);
    assert!(
        first < 10,
        "shutdown should interrupt the slow pipeline: {summary:?}"
    );
    assert!(
        summary.dropped_unacknowledged_on_shutdown > 0,
        "{summary:?}"
    );
    assert_eq!(summary.acks_enqueued as i64, first);

    // Restart: everything not committed must be redelivered (it was never acked).
    let mut c = cfg(port, "akhet-shut", "shut/#");
    c.limit = Some(10 - first as u64);
    let s = tokio::time::timeout(
        Duration::from_secs(20),
        run_collector(c, &db, Faults::none(), Stop::new()),
    )
    .await
    .expect("redelivery of unacknowledged messages")
    .unwrap();
    eprintln!("restart summary: {s:?}");
    let got = multiset(payloads(&db));
    assert_eq!(
        got.len(),
        10,
        "no message lost across graceful shutdown: {got:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnects_reconnect_and_redeliver_without_loss() {
    let port = free_port();
    broker::start_broker(port);
    let proxy = Proxy::start(port).await;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let mut c = cfg(proxy.port, "akhet-disc", "disc/#");
    let r = ready(&mut c);
    c.queue_capacity = 4;
    let faults = Faults::none().with_delay(point::COLLECTOR_COMMIT, Duration::from_millis(30));
    let stop = Stop::new();
    let s2 = stop.clone();
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, faults, s2).await });
    r.await;
    // Publish in three batches and sever the collector's TCP connection after
    // each of the first two, while acknowledgments may still be outstanding.
    for batch in 0..3 {
        let msgs: Vec<(String, Vec<u8>, bool)> = (0..10)
            .map(|i| {
                (
                    "disc/x".to_string(),
                    format!("d{}", batch * 10 + i).into_bytes(),
                    false,
                )
            })
            .collect();
        let refs: Vec<(&str, Vec<u8>, bool)> = msgs
            .iter()
            .map(|(t, p, r)| (t.as_str(), p.clone(), *r))
            .collect();
        publish(port, &format!("pub-disc-{batch}"), &refs).await;
        if batch < 2 {
            let want = (batch + 1) * 10;
            assert!(wait_until(Duration::from_secs(20), || multiset(payloads(
                &db
            ))
            .len()
                >= want));
            proxy.sever();
        }
    }
    let all = wait_until(Duration::from_secs(30), || {
        multiset(payloads(&db)).len() == 30
    });
    stop.trigger();
    let summary = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    eprintln!("disconnect summary: {summary:?}");
    assert!(
        all,
        "all 30 messages captured: {:?}",
        multiset(payloads(&db)).len()
    );
    assert!(summary.connections >= 3, "{summary:?}");
    assert!(summary.connection_errors >= 2, "{summary:?}");
    let dupes: usize = multiset(payloads(&db)).values().map(|n| n - 1).sum();
    eprintln!("duplicates from redelivery: {dupes}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversize_payload_is_fatal_not_silently_dropped() {
    let port = free_port();
    broker::start_broker(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let mut c = cfg(port, "akhet-big", "big/#");
    let r = ready(&mut c);
    c.max_payload_bytes = 1000;
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, Faults::none(), Stop::new()).await });
    r.await;
    publish(port, "pub-big", &[("big/x", vec![7u8; 200_000], false)]).await;
    let res = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap();
    let err = res.expect_err("oversize must stop collection");
    assert!(err.to_string().contains("exceeds"), "{err}");
    assert_eq!(count(&db), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_spanning_a_reconnect_are_not_acked_on_the_new_connection() {
    let port = free_port();
    broker::start_broker(port);
    let proxy = Proxy::start(port).await;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let mut c = cfg(proxy.port, "akhet-span", "span/#");
    let r = ready(&mut c);
    c.queue_capacity = 8;
    // Slow commits: messages are still queued when the connection drops.
    let faults = Faults::none().with_delay(point::COLLECTOR_COMMIT, Duration::from_millis(200));
    let stop = Stop::new();
    let s2 = stop.clone();
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, faults, s2).await });
    r.await;
    let msgs: Vec<(&str, Vec<u8>, bool)> = (0..5)
        .map(|i| ("span/x", format!("p{i}").into_bytes(), false))
        .collect();
    publish(port, "pub-span", &msgs).await;
    assert!(wait_until(Duration::from_secs(10), || count(&db) >= 1));
    proxy.sever();
    // Every message must be stored at least once; those committed after the
    // disconnect were redelivered by the broker and stored again.
    assert!(wait_until(Duration::from_secs(30), || {
        let m = multiset(payloads(&db));
        m.len() == 5 && count(&db) as usize > 5
    }));
    stop.trigger();
    let summary = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    eprintln!("span summary: {summary:?}");
    assert!(summary.acks_skipped_after_reconnect >= 1, "{summary:?}");
    assert!(summary.connections >= 2);
}
