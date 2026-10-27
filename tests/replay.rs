//! Replay to the example HTTP receiver over real loopback HTTP, including
//! process-crash fault injection at each step of the delivery protocol.

mod support;

use akhet::event::{Event, EventData, ModbusOutcome, ModbusPoll};
use akhet::faults::point;
use akhet::replay::SinkLock;
use akhet::spool::Spool;
use rusqlite::params;
use std::path::Path;
use std::time::Duration;
use support::*;

const EXIT_INTEGRITY: i32 = 3;
const EXIT_REJECTED: i32 = 5;
const EXIT_UNAVAILABLE: i32 = 6;
const EXIT_LOCKED: i32 = 7;

fn seed(db: &Path, n: u16) -> Vec<Event> {
    let mut s = Spool::open(db, MAX_DB).unwrap();
    (0..n)
        .map(|i| {
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
            e
        })
        .collect()
}

fn replay(
    db: &Path,
    url: &str,
    sink: &str,
    fault: Option<&str>,
    extra: &[&str],
) -> std::process::Output {
    let mut args = vec![
        "replay",
        "--url",
        url,
        "--sink",
        sink,
        "--max-attempts",
        "3",
        "--max-backoff-ms",
        "100",
    ];
    args.extend_from_slice(extra);
    run(db, &args, fault)
}

fn checkpoint(db: &Path, sink: &str) -> i64 {
    Spool::open(db, MAX_DB)
        .unwrap()
        .sink(sink)
        .unwrap()
        .map(|s| s.checkpoint_seq)
        .unwrap_or(0)
}

#[test]
fn replays_byte_identical_and_repeated_replay_creates_no_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 5);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let out = replay(&db, &rx.url, "main", None, &[]);
    assert!(out.status.success());
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 5);
    assert_eq!(s["receiver_id"], rx.receiver_id.as_str());
    assert_eq!(
        bodies(&rx.db),
        bodies(&db),
        "receiver stores the exact spooled bytes"
    );
    assert_eq!(checkpoint(&db, "main"), 5);

    // Re-running delivers nothing new.
    let out = replay(&db, &rx.url, "main", None, &[]);
    assert_eq!(stdout_json(&out)["delivered"], 0);
    // A new sink name to the same receiver re-sends everything; the receiver
    // deduplicates by event_id.
    let out = replay(&db, &rx.url, "second", None, &[]);
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 5);
    assert_eq!(s["receiver_duplicates"], 5);
    assert_eq!(rx.count(), 5);
}

#[test]
fn crash_before_send_resumes_without_loss() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 3);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let out = replay(
        &db,
        &rx.url,
        "s",
        Some(&format!("abort:{}", point::REPLAY_BEFORE_SEND)),
        &[],
    );
    assert!(killed_by_signal(&out));
    assert_eq!(rx.count(), 0);
    assert_eq!(checkpoint(&db, "s"), 0);
    assert!(replay(&db, &rx.url, "s", None, &[]).status.success());
    assert_eq!(rx.count(), 3);
}

#[test]
fn crash_after_remote_commit_before_checkpoint_redelivers_and_receiver_dedups() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 3);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let out = replay(
        &db,
        &rx.url,
        "s",
        Some(&format!("abort:{}", point::REPLAY_AFTER_REMOTE_SUCCESS)),
        &[],
    );
    assert!(killed_by_signal(&out));
    assert_eq!(rx.count(), 1, "receiver committed the first event");
    assert_eq!(checkpoint(&db, "s"), 0, "checkpoint not advanced");
    let out = replay(&db, &rx.url, "s", None, &[]);
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 3);
    assert_eq!(s["receiver_duplicates"], 1, "first event was sent twice");
    assert_eq!(rx.count(), 3, "no duplicate receiver records");
    assert_eq!(checkpoint(&db, "s"), 3);
}

#[test]
fn crash_after_checkpoint_does_not_resend() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 3);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let out = replay(
        &db,
        &rx.url,
        "s",
        Some(&format!("abort:{}", point::REPLAY_AFTER_CHECKPOINT)),
        &[],
    );
    assert!(killed_by_signal(&out));
    assert_eq!(checkpoint(&db, "s"), 1);
    let s = stdout_json(&replay(&db, &rx.url, "s", None, &[]));
    assert_eq!(s["delivered"], 2);
    assert_eq!(s["receiver_duplicates"], 0);
    assert_eq!(rx.count(), 3);
}

#[test]
fn receiver_crash_after_commit_is_transient_then_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let rx_db = dir.path().join("rx.sqlite");
    seed(&db, 3);
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let url = format!("http://{listen}");
    // Receiver process that dies after committing, before responding.
    let mut child = akhet(
        &db,
        &[
            "receive",
            "--listen",
            &listen,
            "--db",
            rx_db.to_str().unwrap(),
        ],
        Some(&format!("abort:{}", point::RECEIVER_AFTER_COMMIT)),
    )
    .spawn()
    .unwrap();
    wait_port(port);
    let out = replay(&db, &url, "s", None, &[]);
    assert_eq!(
        out.status.code(),
        Some(EXIT_UNAVAILABLE),
        "lost response then refused connections"
    );
    let _ = child.wait();
    assert_eq!(count(&rx_db), 1, "receiver committed before dying");
    assert_eq!(checkpoint(&db, "s"), 0);

    // Restart the receiver (same database, same receiver identity).
    let mut child = akhet(
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
    wait_port(port);
    let out = replay(&db, &url, "s", None, &[]);
    let _ = child.kill();
    let _ = child.wait();
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 3);
    assert_eq!(s["receiver_duplicates"], 1);
    assert_eq!(count(&rx_db), 3);
}

#[test]
fn receiver_conflict_is_a_permanent_failure() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let evs = seed(&db, 3);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    // The receiver already holds a *different* body under event 2's id.
    {
        let mut v: serde_json::Value = serde_json::from_slice(&evs[1].encode()).unwrap();
        v["registers"] = serde_json::json!([4242]);
        let mut r = Spool::open(&rx.db, MAX_DB).unwrap();
        r.append_body(&serde_json::to_vec(&v).unwrap(), None)
            .unwrap();
    }
    let out = replay(&db, &rx.url, "s", None, &[]);
    assert_eq!(out.status.code(), Some(EXIT_REJECTED));
    assert!(String::from_utf8_lossy(&out.stderr).contains("HTTP 409"));
    assert_eq!(
        checkpoint(&db, "s"),
        1,
        "stops before the conflicting record"
    );
}

#[test]
fn unreachable_receiver_retries_with_bounded_backoff_then_fails() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 1);
    let url = format!("http://127.0.0.1:{}", free_port());
    let t0 = std::time::Instant::now();
    let out = replay(&db, &url, "s", None, &[]);
    assert_eq!(out.status.code(), Some(EXIT_UNAVAILABLE));
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(err.matches("transient failure").count(), 2, "{err}");
    assert!(t0.elapsed() < Duration::from_secs(10));
}

#[test]
fn concurrent_replayers_for_one_sink_are_excluded() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let lock = SinkLock::acquire(&db, "s").unwrap();
    let out = replay(&db, &rx.url, "s", None, &[]);
    assert_eq!(out.status.code(), Some(EXIT_LOCKED));
    assert_eq!(rx.count(), 0);
    // A different sink is independent.
    assert!(replay(&db, &rx.url, "other", None, &[]).status.success());
    drop(lock);
    assert!(replay(&db, &rx.url, "s", None, &[]).status.success());
}

#[test]
fn sink_checkpoint_cannot_be_reused_for_another_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    let rx1 = TestReceiver::start(&dir.path().join("rx1.sqlite"));
    assert!(replay(&db, &rx1.url, "s", None, &[]).status.success());
    let url1 = rx1.url.clone();
    drop(rx1);
    // A different receiver (new identity) on another URL, and then on the same URL.
    let rx2 = TestReceiver::start(&dir.path().join("rx2.sqlite"));
    let out = replay(&db, &rx2.url, "s", None, &[]);
    assert_eq!(out.status.code(), Some(EXIT_INTEGRITY));
    assert!(String::from_utf8_lossy(&out.stderr).contains("is bound to"));
    let port: u16 = url1.rsplit(':').next().unwrap().parse().unwrap();
    let rx_db3 = dir.path().join("rx3.sqlite");
    let mut child = akhet(
        &db,
        &[
            "receive",
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--db",
            rx_db3.to_str().unwrap(),
        ],
        None,
    )
    .spawn()
    .unwrap();
    wait_port(port);
    let out = replay(&db, &url1, "s", None, &[]);
    let _ = child.kill();
    let _ = child.wait();
    assert_eq!(out.status.code(), Some(EXIT_INTEGRITY));
    assert!(String::from_utf8_lossy(&out.stderr).contains("now reports receiver"));
    assert_eq!(count(&rx_db3), 0);
}

#[test]
fn corrupted_record_stops_replay_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 3);
    {
        let s = Spool::open(&db, MAX_DB).unwrap();
        s.connection()
            .execute(
                "UPDATE events SET body_sha256=zeroblob(32) WHERE seq=2",
                params![],
            )
            .unwrap();
    }
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let out = replay(&db, &rx.url, "s", None, &[]);
    assert_eq!(out.status.code(), Some(EXIT_INTEGRITY));
    assert!(String::from_utf8_lossy(&out.stderr).contains("corruption at seq 2"));
    assert_eq!(rx.count(), 0, "batch is verified before sending");
    assert_eq!(
        run(&db, &["verify"], None).status.code(),
        Some(EXIT_INTEGRITY)
    );
    assert_eq!(
        run(&db, &["inspect"], None).status.code(),
        Some(EXIT_INTEGRITY)
    );
}

#[test]
fn unsupported_schema_is_rejected_by_cli() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 1);
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("PRAGMA user_version=99")
        .unwrap();
    let out = run(&db, &["status"], None);
    assert_eq!(out.status.code(), Some(EXIT_INTEGRITY));
    assert!(String::from_utf8_lossy(&out.stderr).contains("unsupported spool schema version 99"));
}

#[test]
fn follow_mode_stops_gracefully_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let child = akhet(
        &db,
        &[
            "replay",
            "--url",
            &rx.url,
            "--sink",
            "f",
            "--follow",
            "--follow-interval-ms",
            "50",
        ],
        None,
    )
    .stdout(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    assert!(wait_until(Duration::from_secs(10), || rx.count() == 2));
    seed(&db, 1);
    assert!(
        wait_until(Duration::from_secs(10), || rx.count() == 3),
        "follow picks up new records"
    );
    sigterm(child.id());
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{:?}", out.status);
    let s = stdout_json(&out);
    assert_eq!(s["interrupted"], true);
    assert_eq!(s["delivered"], 3);
}

fn sigterm(pid: u32) {
    let st = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .unwrap();
    assert!(st.success());
}
