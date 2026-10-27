//! Spool durability, integrity and versioning tests (in-process and
//! subprocess restart).

mod support;

use akhet::event::{Event, EventData, ModbusOutcome, ModbusPoll, sha256};
use akhet::spool::{AppendOutcome, SourceClaim, Spool, SpoolError};
use rusqlite::params;
use support::*;

fn ev(regs: Vec<u16>) -> Event {
    Event::new(
        EventData::ModbusPoll(ModbusPoll {
            endpoint: "127.0.0.1:502".into(),
            device_id: 1,
            function: "read_holding_registers".into(),
            start_address: 0,
            count: regs.len() as u16,
            poll_index: 0,
            nominal_interval_ms: 1000,
            started_unix_ns: 1,
            completed_unix_ns: 2,
            duration_ns: 1,
            new_connection: false,
            outcome: ModbusOutcome::Ok { registers: regs },
        }),
        2,
    )
}

#[test]
fn records_survive_reopen_and_keep_stable_ids() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    let e1 = ev(vec![1, 2]);
    let e2 = ev(vec![3]);
    {
        let mut s = Spool::open(&db, MAX_DB).unwrap();
        assert_eq!(
            s.append(&e1, None).unwrap(),
            AppendOutcome::Inserted { seq: 1 }
        );
        assert_eq!(
            s.append(&e2, None).unwrap(),
            AppendOutcome::Inserted { seq: 2 }
        );
    }
    let s = Spool::open(&db, MAX_DB).unwrap();
    let recs = s.read_after(0, 10).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].event_id, e1.event_id);
    assert_eq!(recs[0].body, e1.encode());
    assert_eq!(akhet::event::decode(&recs[1].body).unwrap(), e2);
    assert_eq!(s.verify().unwrap(), 2);
}

#[test]
fn identical_retry_is_idempotent_and_conflicting_reuse_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    let e = ev(vec![1]);
    assert_eq!(
        s.append(&e, None).unwrap(),
        AppendOutcome::Inserted { seq: 1 }
    );
    assert_eq!(
        s.append(&e, None).unwrap(),
        AppendOutcome::DuplicateEvent { seq: 1 }
    );
    let mut conflicting = ev(vec![999]);
    conflicting.event_id = e.event_id.clone();
    match s.append(&conflicting, None) {
        Err(SpoolError::Conflict {
            what: "event_id",
            existing_seq: 1,
            ..
        }) => {}
        other => panic!("expected conflict, got {other:?}"),
    }
    // Source identity: same key + same content = duplicate, different = conflict.
    let claim = SourceClaim {
        key: "k".into(),
        content_sha256: sha256(b"a"),
    };
    assert_eq!(
        s.append(&ev(vec![5]), Some(&claim)).unwrap(),
        AppendOutcome::Inserted { seq: 2 }
    );
    assert_eq!(
        s.append(&ev(vec![5]), Some(&claim)).unwrap(),
        AppendOutcome::DuplicateSource { seq: 2 }
    );
    let other = SourceClaim {
        key: "k".into(),
        content_sha256: sha256(b"b"),
    };
    assert!(matches!(
        s.append(&ev(vec![6]), Some(&other)),
        Err(SpoolError::Conflict {
            what: "source identity",
            ..
        })
    ));
    let recs = s.read_after(0, 10).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].body, e.encode(), "original record untouched");
}

#[test]
fn corrupted_body_fails_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    let mut s = Spool::open(&db, MAX_DB).unwrap();
    for i in 0..3 {
        s.append(&ev(vec![i]), None).unwrap();
    }
    // Flip one byte of record 2 behind the spool's back.
    let body: Vec<u8> = s
        .connection()
        .query_row("SELECT body FROM events WHERE seq=2", [], |r| r.get(0))
        .unwrap();
    let mut bad = body.clone();
    let pos = bad.iter().position(|b| *b == b'[').unwrap() + 1;
    bad[pos] = if bad[pos] == b'7' { b'8' } else { b'7' };
    s.connection()
        .execute("UPDATE events SET body=?1 WHERE seq=2", params![bad])
        .unwrap();
    assert_eq!(
        s.read_after(0, 1).unwrap().len(),
        1,
        "records before corruption readable"
    );
    match s.read_after(0, 10) {
        Err(SpoolError::Corrupt { seq: 2, .. }) => {}
        other => panic!("expected corruption error, got {other:?}"),
    }
    assert!(matches!(
        s.verify(),
        Err(SpoolError::Corrupt { seq: 2, .. })
    ));
}

#[test]
fn unsupported_schema_and_foreign_files_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    drop(Spool::open(&db, MAX_DB).unwrap());
    {
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute_batch("PRAGMA user_version=2").unwrap();
    }
    assert!(matches!(
        Spool::open(&db, MAX_DB),
        Err(SpoolError::UnsupportedSchema { found: 2 })
    ));

    // A SQLite file from something else (e.g. the old Python prototype).
    let foreign = dir.path().join("foreign.sqlite");
    {
        let c = rusqlite::Connection::open(&foreign).unwrap();
        c.execute_batch("CREATE TABLE records(seq INTEGER); PRAGMA user_version=1;")
            .unwrap();
    }
    assert!(matches!(
        Spool::open(&foreign, MAX_DB),
        Err(SpoolError::NotASpool(..))
    ));
}

#[test]
fn unsupported_event_format_is_rejected_on_append_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    let mut v: serde_json::Value = serde_json::from_slice(&ev(vec![1]).encode()).unwrap();
    v["format_version"] = 2.into();
    let body = serde_json::to_vec(&v).unwrap();
    assert!(matches!(
        s.append_body(&body, None),
        Err(SpoolError::Format(_))
    ));
    // Even a record with a valid checksum but unknown version is not replayed.
    s.connection()
        .execute(
            "INSERT INTO events(event_id, format_version, body, body_sha256, appended_unix_ns)
             VALUES('x', 2, ?1, ?2, 0)",
            params![body, &sha256(&body)[..]],
        )
        .unwrap();
    assert!(matches!(s.read_after(0, 10), Err(SpoolError::Format(_))));
}

#[test]
fn capacity_exhaustion_is_visible_and_preserves_committed_records() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    let mut s = Spool::open(&db, 64 * 1024).unwrap();
    let mut committed = Vec::new();
    let err = loop {
        let e = ev(vec![7; 125]);
        match s.append(&e, None) {
            Ok(_) => committed.push(e.event_id.clone()),
            Err(e) => break e,
        }
        assert!(committed.len() < 10_000, "limit never reached");
    };
    assert!(matches!(err, SpoolError::CapacityExhausted { .. }), "{err}");
    assert!(err.is_capacity());
    assert!(!committed.is_empty());
    // Still full on retry; nothing overwritten.
    assert!(s.append(&ev(vec![1; 125]), None).is_err());
    drop(s);
    let s = Spool::open(&db, 64 * 1024).unwrap();
    let ids: Vec<String> = s
        .read_after(0, 100_000)
        .unwrap()
        .into_iter()
        .map(|r| r.event_id)
        .collect();
    assert_eq!(ids, committed);
    assert_eq!(s.verify().unwrap() as usize, committed.len());
    let st = s.status().unwrap();
    assert!(st.db_bytes_used <= 64 * 1024, "{st:?}");
    // Raising the limit allows appends again (no data was dropped meanwhile).
    drop(s);
    let mut s = Spool::open(&db, 1 << 20).unwrap();
    s.append(&ev(vec![1]), None).unwrap();
}

#[test]
fn sink_binding_is_exclusive_to_destination_and_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    for i in 0..3 {
        s.append(&ev(vec![i]), None).unwrap();
    }
    let sink = s.bind_sink("a", "http://h:1", "r1").unwrap();
    s.advance_checkpoint(&sink, 2).unwrap();
    s.advance_checkpoint(&sink, 1).unwrap(); // never moves backwards
    assert_eq!(s.sink("a").unwrap().unwrap().checkpoint_seq, 2);
    assert!(matches!(
        s.bind_sink("a", "http://other:1", "r1"),
        Err(SpoolError::SinkBinding(_))
    ));
    assert!(matches!(
        s.bind_sink("a", "http://h:1", "r2"),
        Err(SpoolError::SinkBinding(_))
    ));
    assert_eq!(
        s.bind_sink("a", "http://h:1", "r1").unwrap().checkpoint_seq,
        2
    );
    let st = s.status().unwrap();
    assert_eq!(st.sinks[0].pending_records, 1);
}

#[test]
fn process_exit_right_after_commit_keeps_the_record() {
    // A separate process commits one Modbus observation and aborts
    // immediately afterwards (no destructors, no clean SQLite close).
    let port = free_port();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _srv = rt.spawn(async move {
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let _ = support::modbus_sim::serve(l, Default::default()).await;
    });
    wait_port(port);
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    let p = port.to_string();
    let out = run(
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
            "50",
        ],
        Some("abort:modbus.after_commit"),
    );
    assert!(killed_by_signal(&out));
    let evs = events(&db);
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0]["registers"], serde_json::json!([100, 101]));
    let out = run(&db, &["verify"], None);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["verified_records"], 1);
}
