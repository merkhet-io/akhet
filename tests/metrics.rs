//! Metrics, liveness and readiness endpoints, scraped from real `akhet`
//! processes while they collect (loopback broker, Modbus simulator) and
//! replay (example receiver).

mod support;

use akhet::event::{Event, EventData, ModbusOutcome, ModbusPoll};
use akhet::spool::Spool;
use std::path::Path;
use std::process::{Child, Stdio};
use std::time::Duration;
use support::*;

const WAIT: Duration = Duration::from_secs(30);

/// GET `url`; `(0, "")` when the server is not reachable.
fn get(url: &str) -> (u16, String) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .http_status_as_error(false)
        .proxy(None)
        .build()
        .into();
    match agent.get(url).call() {
        Ok(mut r) => (
            r.status().as_u16(),
            r.body_mut().read_to_string().unwrap_or_default(),
        ),
        Err(_) => (0, String::new()),
    }
}

/// Value of the sample `series` (name plus labels, exactly as rendered).
fn value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

struct Endpoint(String);

impl Endpoint {
    fn new(port: u16) -> Endpoint {
        Endpoint(format!("http://127.0.0.1:{port}"))
    }

    fn metric(&self, series: &str) -> Option<f64> {
        let (status, body) = get(&format!("{}/metrics", self.0));
        if status != 200 {
            return None;
        }
        value(&body, series)
    }

    fn wait_metric(&self, series: &str, pred: impl Fn(f64) -> bool) -> f64 {
        let mut last = None;
        let ok = wait_until(WAIT, || {
            last = self.metric(series);
            last.is_some_and(&pred)
        });
        assert!(ok, "{series} never satisfied the condition (last {last:?})");
        last.unwrap()
    }

    fn ready(&self) -> (u16, String) {
        get(&format!("{}/readyz", self.0))
    }

    fn wait_ready(&self, want: u16) -> String {
        let mut last = (0, String::new());
        let ok = wait_until(WAIT, || {
            last = self.ready();
            last.0 == want
        });
        assert!(ok, "/readyz never returned {want} (last {last:?})");
        last.1
    }

    fn wait_live(&self) {
        assert!(
            wait_until(WAIT, || get(&format!("{}/healthz", self.0)).0 == 200),
            "/healthz not reachable"
        );
    }
}

fn sigterm_and_wait(child: Child) -> serde_json::Value {
    assert!(
        std::process::Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status()
            .unwrap()
            .success()
    );
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{:?}", out.status);
    stdout_json(&out)
}

fn keys(v: &serde_json::Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mqtt_metrics_move_and_readiness_follows_broker() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    // Broker port chosen but not started yet: the collector cannot connect.
    let port = free_port();
    let mport = free_port();
    let (p, m) = (port.to_string(), format!("127.0.0.1:{mport}"));
    let child = akhet(
        &db,
        &[
            "mqtt",
            "--host",
            "127.0.0.1",
            "--port",
            &p,
            "--client-id",
            "metrics-mqtt",
            "--topic",
            "m/#",
            "--keep-alive-secs",
            "5",
            "--metrics-listen",
            &m,
            "--metrics-spool-interval-ms",
            "100",
        ],
        None,
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    let ep = Endpoint::new(mport);
    ep.wait_live();
    let (status, body) = ep.ready();
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("mqtt not connected"), "{body}");
    ep.wait_metric("akhet_mqtt_connection_errors_total", |v| v >= 1.0);
    ep.wait_metric("akhet_errors_total{component=\"mqtt\",kind=\"io\"}", |v| {
        v >= 1.0
    });
    assert_eq!(ep.metric("akhet_ready"), Some(0.0));
    assert_eq!(ep.metric("akhet_mqtt_queue_capacity"), Some(64.0));
    assert_eq!(ep.metric("akhet_spool_last_seq"), Some(0.0));

    broker::start_broker(port);
    ep.wait_ready(200);
    assert_eq!(ep.metric("akhet_mqtt_connected"), Some(1.0));
    assert_eq!(ep.metric("akhet_mqtt_subscribed"), Some(1.0));
    assert_eq!(ep.metric("akhet_mqtt_connections_total"), Some(1.0));

    publish(
        port,
        "metrics-pub",
        &[
            ("m/a", b"one".to_vec(), false),
            ("m/b", b"two".to_vec(), false),
            ("m/c", b"three".to_vec(), false),
        ],
    )
    .await;
    ep.wait_metric("akhet_mqtt_acks_enqueued_total", |v| v == 3.0);
    assert_eq!(ep.metric("akhet_mqtt_messages_received_total"), Some(3.0));
    assert_eq!(ep.metric("akhet_mqtt_messages_committed_total"), Some(3.0));
    assert_eq!(ep.metric("akhet_mqtt_queue_depth"), Some(0.0));
    assert_eq!(
        ep.metric("akhet_spool_commit_duration_seconds_count"),
        Some(3.0)
    );
    assert_eq!(
        ep.metric("akhet_spool_commit_duration_seconds_bucket{le=\"+Inf\"}"),
        Some(3.0)
    );
    // Spool gauges come from the sampler, so they follow with a short delay.
    ep.wait_metric("akhet_spool_last_seq", |v| v == 3.0);
    let used = ep.metric("akhet_spool_db_bytes_used").unwrap();
    let headroom = ep.metric("akhet_spool_headroom_bytes").unwrap();
    let max = ep.metric("akhet_spool_max_db_bytes").unwrap();
    assert!(used > 0.0 && headroom > 0.0, "{used} {headroom}");
    assert!(used + headroom <= max, "{used} + {headroom} > {max}");
    assert_eq!(ep.metric("akhet_ready"), Some(1.0));

    // The exit-time JSON summary is unchanged.
    let summary = sigterm_and_wait(child);
    assert_eq!(
        keys(&summary),
        [
            "acks_enqueued",
            "acks_skipped_after_reconnect",
            "committed",
            "connection_errors",
            "connections",
            "dropped_unacknowledged_on_shutdown",
            "qos0_messages",
            "queue_saturation_events",
            "received",
            "source_duplicates",
        ]
    );
    assert_eq!(summary["committed"], 3);
    assert_eq!(count(&db), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modbus_metrics_move_and_readiness_follows_device() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let port = free_port();
    let mport = free_port();
    let (p, m) = (port.to_string(), format!("127.0.0.1:{mport}"));
    let child = akhet(
        &db,
        &[
            "modbus",
            "--host",
            "127.0.0.1",
            "--port",
            &p,
            "--address",
            "0",
            "--count",
            "2",
            "--interval-ms",
            "100",
            "--timeout-ms",
            "500",
            "--metrics-listen",
            &m,
            "--metrics-spool-interval-ms",
            "100",
        ],
        None,
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    let ep = Endpoint::new(mport);
    ep.wait_live();
    ep.wait_metric("akhet_modbus_polls_total{status=\"read_failed\"}", |v| {
        v >= 2.0
    });
    let body = ep.wait_ready(503);
    assert!(body.contains("last modbus poll failed"), "{body}");
    ep.wait_metric(
        "akhet_errors_total{component=\"modbus\",kind=\"connect\"}",
        |v| v >= 2.0,
    );
    assert_eq!(
        ep.metric("akhet_modbus_polls_total{status=\"ok\"}"),
        Some(0.0)
    );
    assert_eq!(ep.metric("akhet_modbus_last_poll_ok"), Some(0.0));

    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let _sim = tokio::spawn(modbus_sim::serve(l, Default::default()));
    ep.wait_ready(200);
    ep.wait_metric("akhet_modbus_polls_total{status=\"ok\"}", |v| v >= 3.0);
    assert_eq!(ep.metric("akhet_modbus_connections_total"), Some(1.0));
    assert_eq!(ep.metric("akhet_modbus_last_poll_ok"), Some(1.0));
    ep.wait_metric("akhet_spool_commit_duration_seconds_count", |v| v >= 5.0);
    ep.wait_metric("akhet_spool_last_seq", |v| v >= 5.0);

    let summary = sigterm_and_wait(child);
    assert_eq!(keys(&summary), ["connections", "failed", "ok", "polls"]);
    assert_eq!(summary["connections"], 1);
}

fn seed(db: &Path, from: u16, n: u16) {
    let mut s = Spool::open(db, MAX_DB).unwrap();
    for i in from..from + n {
        let e = Event::new(
            EventData::ModbusPoll(ModbusPoll {
                endpoint: "sim:502".into(),
                device_id: 1,
                function: "read_holding_registers".into(),
                start_address: 0,
                count: 1,
                poll_index: u64::from(i),
                nominal_interval_ms: 1000,
                started_unix_ns: 1,
                completed_unix_ns: 2,
                duration_ns: 1,
                new_connection: false,
                outcome: ModbusOutcome::Ok { registers: vec![i] },
            }),
            2,
        );
        s.append(&e, None).unwrap();
    }
}

#[test]
fn replay_metrics_move_and_readiness_follows_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 0, 5);
    // Receiver port chosen but not listening yet.
    let rx_port = free_port();
    let mport = free_port();
    let url = format!("http://127.0.0.1:{rx_port}");
    let m = format!("127.0.0.1:{mport}");
    let child = akhet(
        &db,
        &[
            "replay",
            "--url",
            &url,
            "--sink",
            "main",
            "--follow",
            "--follow-interval-ms",
            "100",
            "--max-attempts",
            "1000",
            "--max-backoff-ms",
            "100",
            "--timeout-ms",
            "1000",
            "--metrics-listen",
            &m,
            "--metrics-spool-interval-ms",
            "100",
        ],
        None,
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    let ep = Endpoint::new(mport);
    ep.wait_live();
    let (status, body) = ep.ready();
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("receiver identity not bound"), "{body}");
    ep.wait_metric("akhet_replay_retries_total", |v| v >= 2.0);
    ep.wait_metric(
        "akhet_errors_total{component=\"replay\",kind=\"transient\"}",
        |v| v >= 2.0,
    );
    assert_eq!(ep.metric("akhet_replay_receiver_bound"), Some(0.0));
    assert_eq!(ep.metric("akhet_replay_delivered_total"), Some(0.0));
    // Lag is reported once the sink exists in the spool.
    assert_eq!(ep.metric("akhet_replay_lag_records"), None);

    let rx = TestReceiver::start_at(
        &dir.path().join("rx.sqlite"),
        &format!("127.0.0.1:{rx_port}"),
    );
    ep.wait_ready(200);
    ep.wait_metric("akhet_replay_delivered_total", |v| v == 5.0);
    assert_eq!(ep.metric("akhet_replay_checkpoint_seq"), Some(5.0));
    assert_eq!(
        ep.metric("akhet_replay_receiver_duplicates_total"),
        Some(0.0)
    );
    assert_eq!(ep.metric("akhet_replay_last_attempt_failed"), Some(0.0));
    ep.wait_metric("akhet_replay_lag_records", |v| v == 0.0);
    assert_eq!(
        ep.metric("akhet_spool_pending_records{sink=\"main\"}"),
        Some(0.0)
    );

    // New records while following: counters and checkpoint advance.
    seed(&db, 5, 2);
    ep.wait_metric("akhet_replay_delivered_total", |v| v == 7.0);
    ep.wait_metric("akhet_replay_checkpoint_seq", |v| v == 7.0);
    assert_eq!(rx.count(), 7);

    // Receiver goes away and a new record is pending: not ready.
    drop(rx);
    seed(&db, 7, 1);
    let body = ep.wait_ready(503);
    assert!(body.contains("last replay attempt failed"), "{body}");
    assert_eq!(ep.metric("akhet_replay_last_attempt_failed"), Some(1.0));
    ep.wait_metric("akhet_replay_lag_records", |v| v == 1.0);

    let summary = sigterm_and_wait(child);
    assert_eq!(
        keys(&summary),
        [
            "checkpoint_seq",
            "delivered",
            "destination_url",
            "interrupted",
            "receiver_duplicates",
            "receiver_id",
            "retries",
            "sink",
        ]
    );
    assert_eq!(summary["delivered"], 7);
    assert_eq!(summary["interrupted"], true);
}

#[test]
fn metrics_bind_failure_is_a_configuration_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 0, 1);
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = busy.local_addr().unwrap().to_string();
    let out = run(
        &db,
        &[
            "replay",
            "--url",
            "http://127.0.0.1:9",
            "--sink",
            "x",
            "--metrics-listen",
            &addr,
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("metrics endpoint"));
}
