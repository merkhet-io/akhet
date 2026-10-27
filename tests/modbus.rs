//! Modbus TCP integration tests against a real loopback tokio-modbus server.

mod support;

use akhet::adapters::modbus::{self, ModbusConfig};
use akhet::adapters::{AsyncSpool, Stop};
use akhet::faults::Faults;
use akhet::spool::Spool;
use std::path::Path;
use std::time::{Duration, Instant};
use support::modbus_sim::{self, Sim};
use support::*;
use tokio::io::AsyncReadExt;

fn cfg(
    port: u16,
    address: u16,
    count: u16,
    interval_ms: u64,
    timeout_ms: u64,
    limit: u64,
) -> ModbusConfig {
    ModbusConfig {
        host: "127.0.0.1".into(),
        port,
        device_id: 1,
        start_address: address,
        count,
        interval: Duration::from_millis(interval_ms),
        timeout: Duration::from_millis(timeout_ms),
        limit: Some(limit),
    }
}

async fn collect(c: ModbusConfig, db: &Path) -> modbus::ModbusSummary {
    let spool = AsyncSpool::new(Spool::open(db, MAX_DB).unwrap(), Faults::none());
    modbus::run(c, spool, Stop::new()).await.unwrap()
}

async fn start_sim(port: u16) -> (tokio::task::JoinHandle<()>, Sim) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let sim = Sim::default();
    let s2 = sim.clone();
    let h = tokio::spawn(async move {
        let _ = modbus_sim::serve(listener, s2).await;
    });
    (h, sim)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_raw_registers_with_collector_timing() {
    let port = free_port();
    let (_srv, sim) = start_sim(port).await;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let s = collect(cfg(port, 3, 4, 50, 1000, 3), &db).await;
    assert_eq!((s.polls, s.ok, s.failed, s.connections), (3, 3, 0, 1));
    assert_eq!(sim.reads.load(std::sync::atomic::Ordering::Relaxed), 3);
    let evs = events(&db);
    assert_eq!(evs.len(), 3);
    for (i, e) in evs.iter().enumerate() {
        assert_eq!(e["kind"], "modbus_poll");
        assert_eq!(e["status"], "ok");
        assert_eq!(e["function"], "read_holding_registers");
        assert_eq!(e["registers"], serde_json::json!([103, 104, 105, 106]));
        assert_eq!(e["start_address"], 3);
        assert_eq!(e["poll_index"], i as u64);
        assert_eq!(e["nominal_interval_ms"], 50);
        assert_eq!(e["new_connection"], i == 0);
        assert!(e["completed_unix_ns"].as_i64() >= e["started_unix_ns"].as_i64());
        assert!(e["duration_ns"].as_u64().unwrap() > 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exception_is_recorded_as_failure_without_values() {
    let port = free_port();
    let (_srv, _) = start_sim(port).await;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    // Simulator has 100 registers; 98 + 4 is out of range.
    let s = collect(cfg(port, 98, 4, 30, 1000, 2), &db).await;
    assert_eq!(
        (s.ok, s.failed, s.connections),
        (0, 2, 1),
        "exception keeps the connection"
    );
    for e in events(&db) {
        assert_eq!(e["status"], "read_failed");
        assert_eq!(e["error_kind"], "exception");
        assert!(e.get("registers").is_none(), "failed reads carry no values");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_then_reconnect_records_explicit_failures() {
    // A server that accepts TCP but never answers Modbus requests.
    let port = free_port();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let silent = tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 256];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                }
            });
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let db2 = db.clone();
    let stop = Stop::new();
    let s2 = stop.clone();
    let mut c = cfg(port, 0, 2, 100, 150, 1_000);
    c.limit = None;
    let collector = tokio::spawn(async move {
        let spool = AsyncSpool::new(Spool::open(&db2, MAX_DB).unwrap(), Faults::none());
        modbus::run(c, spool, s2).await.unwrap()
    });
    assert!(wait_until(Duration::from_secs(10), || count(&db) >= 2));
    // Replace the silent server with a working simulator on the same port.
    silent.abort();
    let _ = silent.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (_srv, _) = start_sim(port).await;
    assert!(wait_until(Duration::from_secs(10), || {
        events(&db).iter().any(|e| e["status"] == "ok")
    }));
    stop.trigger();
    let s = collector.await.unwrap();
    let evs = events(&db);
    let timeouts = evs.iter().filter(|e| e["error_kind"] == "timeout").count();
    assert!(timeouts >= 2, "timeouts recorded: {evs:?}");
    for e in &evs {
        if e["status"] == "read_failed" {
            assert!(e.get("registers").is_none());
        }
    }
    let first_ok = evs.iter().find(|e| e["status"] == "ok").unwrap();
    assert_eq!(first_ok["registers"], serde_json::json!([100, 101]));
    assert_eq!(first_ok["new_connection"], true);
    assert!(s.connections >= 2, "reconnected after timeouts: {s:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_device_is_polled_at_interval_not_busy_looped() {
    let port = free_port(); // nothing listening
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let t0 = Instant::now();
    let s = collect(cfg(port, 0, 1, 100, 100, 5), &db).await;
    let elapsed = t0.elapsed();
    assert_eq!((s.ok, s.failed), (0, 5));
    // 5 polls at a 100 ms interval: first tick is immediate, so >= 400 ms.
    assert!(elapsed >= Duration::from_millis(390), "{elapsed:?}");
    for e in events(&db) {
        assert_eq!(e["status"], "read_failed");
        assert_eq!(e["error_kind"], "connect");
        assert!(e.get("registers").is_none());
    }
}

#[test]
fn invalid_ranges_are_rejected() {
    for (addr, count) in [(0u16, 0u16), (0, 126), (65_535, 2)] {
        let c = cfg(502, addr, count, 1000, 1000, 1);
        assert!(c.validate().is_err(), "{addr}+{count}");
    }
    assert!(cfg(502, 65_535, 1, 1000, 1000, 1).validate().is_ok());
    assert!(cfg(502, 0, 125, 1000, 1000, 1).validate().is_ok());
    assert!(
        cfg(502, 0, 1, 1, 1000, 1).validate().is_err(),
        "interval floor"
    );
    assert!(
        cfg(502, 0, 1, 1000, 0, 1).validate().is_err(),
        "zero timeout"
    );
}
