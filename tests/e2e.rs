//! End-to-end CLI run over real loopback protocols: an MQTT broker (rumqttd),
//! a Modbus TCP server (tokio-modbus) and the example HTTP receiver, with
//! collectors and replay as separate `akhet` processes.

mod support;

use std::process::Stdio;
use std::time::Duration;
use support::*;

fn sigterm(pid: u32) {
    assert!(
        std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .unwrap()
            .success()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mqtt_and_modbus_to_spool_then_replay_to_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let rx_db = dir.path().join("receiver.sqlite");

    let mqtt_port = free_port();
    broker::start_broker(mqtt_port);
    let modbus_port = free_port();
    let l = tokio::net::TcpListener::bind(("127.0.0.1", modbus_port))
        .await
        .unwrap();
    let _sim = tokio::spawn(modbus_sim::serve(l, Default::default()));

    // Long-running collectors, stopped later with SIGTERM.
    let mp = mqtt_port.to_string();
    let mqtt = akhet(
        &db,
        &[
            "mqtt",
            "--host",
            "127.0.0.1",
            "--port",
            &mp,
            "--client-id",
            "e2e-collector",
            "--topic",
            "line/#",
        ],
        None,
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    let bp = modbus_port.to_string();
    let modbus = akhet(
        &db,
        &[
            "modbus",
            "--host",
            "127.0.0.1",
            "--port",
            &bp,
            "--address",
            "10",
            "--count",
            "3",
            "--interval-ms",
            "100",
        ],
        None,
    )
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    assert!(wait_until(Duration::from_secs(10), || count(&db) >= 1));
    // Retained publications reach the collector whether its subscription is
    // established before or after they are published (exactly once either way).
    publish(
        mqtt_port,
        "e2e-pub",
        &[
            ("line/1/vib", vec![0, 1, 2, 3, 255], true),
            ("line/2/temp", b"21.5".to_vec(), true),
        ],
    )
    .await;
    let mqtt_events = || {
        events(&db)
            .iter()
            .filter(|e| e["kind"] == "mqtt_message")
            .count()
    };
    assert!(wait_until(Duration::from_secs(10), || mqtt_events() == 2));

    sigterm(mqtt.id());
    sigterm(modbus.id());
    let mqtt_out = mqtt.wait_with_output().unwrap();
    let modbus_out = modbus.wait_with_output().unwrap();
    assert!(
        mqtt_out.status.success(),
        "graceful MQTT shutdown: {:?}",
        mqtt_out.status
    );
    assert!(
        modbus_out.status.success(),
        "graceful Modbus shutdown: {:?}",
        modbus_out.status
    );
    let ms = stdout_json(&mqtt_out);
    assert_eq!(ms["committed"], 2);
    assert_eq!(ms["acks_enqueued"], 2);
    let bs = stdout_json(&modbus_out);
    assert!(bs["ok"].as_u64().unwrap() >= 1);
    assert_eq!(bs["failed"], 0);

    let total = count(&db);
    let st = run(&db, &["status"], None);
    assert!(st.status.success());
    let status: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
    assert_eq!(status["records"], total);
    assert!(run(&db, &["verify"], None).status.success());

    // Example receiver as its own process; replay twice.
    let rx_port = free_port();
    let listen = format!("127.0.0.1:{rx_port}");
    let mut receiver = akhet(
        &db,
        &[
            "receive",
            "--listen",
            &listen,
            "--db",
            rx_db.to_str().unwrap(),
        ],
        None,
    )
    .spawn()
    .unwrap();
    wait_port(rx_port);
    let url = format!("http://{listen}");
    let out = run(&db, &["replay", "--url", &url, "--sink", "e2e"], None);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["delivered"], total);
    let out = run(&db, &["replay", "--url", &url, "--sink", "e2e"], None);
    assert_eq!(stdout_json(&out)["delivered"], 0);
    sigterm(receiver.id());
    assert!(receiver.wait().unwrap().success());

    assert_eq!(count(&rx_db), total);
    assert_eq!(bodies(&rx_db), bodies(&db));
    let inspect = run(&rx_db, &["inspect", "--limit", "1"], None);
    assert!(inspect.status.success());
    assert_eq!(String::from_utf8_lossy(&inspect.stdout).lines().count(), 1);
}

#[test]
fn modbus_collector_stops_visibly_when_spool_limit_is_reached() {
    let port = free_port();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _srv = rt.spawn(async move {
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let _ = modbus_sim::serve(l, Default::default()).await;
    });
    wait_port(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let p = port.to_string();
    let out = run(
        &db,
        &[
            "--max-db-bytes",
            "65536",
            "modbus",
            "--host",
            "127.0.0.1",
            "--port",
            &p,
            "--address",
            "0",
            "--count",
            "100",
            "--interval-ms",
            "10",
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(4), "capacity exit code");
    assert!(String::from_utf8_lossy(&out.stderr).contains("database-size limit reached"));
    let n = count(&db);
    assert!(n > 0);
    let v = run(&db, &["--max-db-bytes", "65536", "verify"], None);
    assert!(v.status.success());
    assert_eq!(stdout_json(&v)["verified_records"], n);
}
