//! Retention: pruning acknowledged records, freelist reuse under the
//! database-size limit, capacity warnings, a bounded-size soak test and
//! process-crash tests during deletion.

mod support;

use akhet::event::{Event, EventData, ModbusOutcome, ModbusPoll, MqttMessage};
use akhet::faults::{Faults, point};
use akhet::replay::{self, ReplayConfig};
use akhet::spool::{AppendOutcome, PruneLimit, PruneOptions, Spool, SpoolError};
use rusqlite::params;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use support::*;

const SMALL_DB: u64 = 64 * 1024;

fn ev(i: u64, regs: usize) -> Event {
    Event::new(
        EventData::ModbusPoll(ModbusPoll {
            endpoint: "sim:502".into(),
            device_id: 1,
            function: "read_holding_registers".into(),
            start_address: 0,
            count: regs as u16,
            poll_index: i,
            nominal_interval_ms: 1000,
            started_unix_ns: 1,
            completed_unix_ns: 2,
            duration_ns: 1,
            new_connection: false,
            outcome: ModbusOutcome::Ok {
                registers: vec![7; regs],
            },
        }),
        2,
    )
}

/// MQTT event with a payload of `len` bytes (sizes vary to fragment pages).
fn mqtt_ev(i: u64, len: usize) -> Event {
    use base64::Engine as _;
    let payload: Vec<u8> = (0..len).map(|j| (i as usize + j) as u8).collect();
    Event::new(
        EventData::MqttMessage(MqttMessage {
            broker: "127.0.0.1:1883".into(),
            client_id: "soak".into(),
            subscription: "s/#".into(),
            topic: format!("s/{i}"),
            qos: 1,
            retain: false,
            dup: false,
            payload_base64: base64::engine::general_purpose::STANDARD.encode(&payload),
            payload_len: len,
            source_identity_error: None,
        }),
        2,
    )
}

fn seed(s: &mut Spool, n: u64) -> Vec<i64> {
    (0..n)
        .map(|i| s.append(&ev(i, 4), None).unwrap().seq())
        .collect()
}

fn no_stop() -> AtomicBool {
    AtomicBool::new(false)
}

fn prune(
    s: &mut Spool,
    min_age: Duration,
    batch: usize,
    dry_run: bool,
) -> akhet::spool::PruneReport {
    s.prune(
        &PruneOptions {
            min_age,
            batch,
            dry_run,
        },
        &Faults::none(),
        &no_stop(),
    )
    .unwrap()
}

fn seqs(s: &Spool) -> Vec<i64> {
    s.read_after(0, 1_000_000)
        .unwrap()
        .into_iter()
        .map(|r| r.seq)
        .collect()
}

#[test]
fn nothing_is_pruned_without_a_bound_sink() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    seed(&mut s, 5);
    let r = prune(&mut s, Duration::ZERO, 100, false);
    assert_eq!(r.records, 0);
    assert_eq!(r.sinks_bound, 0);
    assert_eq!(r.safe_seq, None);
    assert_eq!(r.limited_by, PruneLimit::NoSinkBound);
    assert_eq!(seqs(&s), vec![1, 2, 3, 4, 5]);
    assert_eq!(s.status().unwrap().prune_safe_seq, None);
}

#[test]
fn prune_respects_the_minimum_checkpoint_over_all_sinks() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    seed(&mut s, 10);
    let a = s.bind_sink("a", "http://a:1", "ra").unwrap();
    let b = s.bind_sink("b", "http://b:1", "rb").unwrap();
    s.advance_checkpoint(&a, 7).unwrap();
    s.advance_checkpoint(&b, 3).unwrap();
    assert_eq!(s.status().unwrap().prune_safe_seq, Some(3));

    // Dry run reports without deleting.
    let r = prune(&mut s, Duration::ZERO, 100, true);
    assert!(r.dry_run);
    assert_eq!((r.records, r.first_seq, r.last_seq), (3, Some(1), Some(3)));
    assert_eq!(r.batches, 0);
    assert_eq!(seqs(&s).len(), 10);

    // Default minimum age (24 h) protects these fresh records.
    let r = s
        .prune(&PruneOptions::default(), &Faults::none(), &no_stop())
        .unwrap();
    assert_eq!(r.records, 0);
    assert_eq!(r.limited_by, PruneLimit::MinAge);

    let r = prune(&mut s, Duration::ZERO, 2, false);
    assert_eq!((r.records, r.first_seq, r.last_seq), (3, Some(1), Some(3)));
    assert_eq!(r.batches, 2, "batch size 2: 1..=2, then 3");
    assert_eq!(r.limited_by, PruneLimit::MinCheckpoint);
    assert_eq!(r.safe_seq, Some(3));
    assert_eq!(seqs(&s), (4..=10).collect::<Vec<_>>());

    // Reads, verify, status and per-sink pending counts work with a gap.
    let st = s.status().unwrap();
    assert_eq!(
        (st.records, st.first_seq, st.last_seq),
        (7, Some(4), Some(10))
    );
    assert_eq!(st.sinks[0].pending_records, 3, "sink a: 8..=10");
    assert_eq!(st.sinks[1].pending_records, 7, "sink b: 4..=10");
    assert_eq!(s.verify().unwrap(), 7);
    assert_eq!(s.read_after(0, 1).unwrap()[0].seq, 4);
    assert_eq!(s.read_after(7, 10).unwrap()[0].seq, 8);

    // Everything acknowledged: the spool may become empty, and seq stays
    // monotonic afterwards (AUTOINCREMENT never reuses a deleted seq).
    s.advance_checkpoint(&b, 10).unwrap();
    s.advance_checkpoint(&a, 10).unwrap();
    let r = prune(&mut s, Duration::ZERO, 100, false);
    assert_eq!(r.records, 7);
    assert!(seqs(&s).is_empty());
    let st = s.status().unwrap();
    assert_eq!((st.records, st.first_seq, st.last_seq), (0, None, None));
    assert_eq!(
        s.append(&ev(99, 4), None).unwrap(),
        AppendOutcome::Inserted { seq: 11 }
    );
    drop(s);
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    assert_eq!(s.append(&ev(100, 4), None).unwrap().seq(), 12);
}

#[test]
fn prune_deletes_only_a_contiguous_prefix_older_than_min_age() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), MAX_DB).unwrap();
    seed(&mut s, 6);
    let a = s.bind_sink("a", "http://a:1", "ra").unwrap();
    s.advance_checkpoint(&a, 6).unwrap();
    // Records 1, 2 and 4 are two hours old; 3, 5 and 6 are fresh.
    let old = akhet::event::now_unix_ns() - 2 * 3600 * 1_000_000_000;
    s.connection()
        .execute(
            "UPDATE events SET appended_unix_ns=?1 WHERE seq IN (1,2,4)",
            params![old],
        )
        .unwrap();
    let r = prune(&mut s, Duration::from_secs(3600), 100, false);
    assert_eq!((r.records, r.first_seq, r.last_seq), (2, Some(1), Some(2)));
    assert_eq!(r.limited_by, PruneLimit::MinAge);
    assert_eq!(
        seqs(&s),
        vec![3, 4, 5, 6],
        "stops at the first young record"
    );
}

#[test]
fn freed_pages_are_reused_under_the_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Spool::open(dir.path().join("s.sqlite"), 2 * SMALL_DB).unwrap();
    let max_pages = s.capacity().unwrap().max_page_count;
    let fill = |s: &mut Spool, start: u64| -> (u64, i64) {
        let mut n = 0;
        let mut last = 0;
        loop {
            match s.append(&ev(start + n, 125), None) {
                Ok(o) => {
                    n += 1;
                    last = o.seq();
                }
                Err(e @ SpoolError::CapacityExhausted { .. }) => {
                    assert!(e.is_capacity());
                    return (n, last);
                }
                Err(e) => panic!("{e}"),
            }
            assert!(n < 10_000, "limit never reached");
        }
    };
    let (first_fill, last) = fill(&mut s, 0);
    assert!(first_fill > 5);
    assert_eq!(s.capacity_warning_percent(), 95, "warned before the limit");
    let st = s.status().unwrap();
    assert_eq!(st.page_count, max_pages);
    assert!(st.headroom_pages <= 1, "{st:?}");

    // Deletion works in a full database and returns pages to the freelist.
    let a = s.bind_sink("a", "http://a:1", "ra").unwrap();
    s.advance_checkpoint(&a, last).unwrap();
    let r = prune(&mut s, Duration::ZERO, 1000, false);
    assert_eq!(r.records, first_fill);
    assert!(r.freelist_count > 0);
    let st = s.status().unwrap();
    assert_eq!(st.page_count, max_pages, "file does not shrink");
    assert!(st.headroom_pages >= st.freelist_count);
    assert!(st.used_percent < 50.0, "{st:?}");

    // New appends reuse freelist pages; page_count never exceeds the limit
    // and about the same number of records fits again.
    s.append(&ev(10_000, 125), None).unwrap();
    assert_eq!(s.capacity_warning_percent(), 0, "warning re-armed");
    let (second_fill, _) = fill(&mut s, 20_000);
    let st = s.status().unwrap();
    assert_eq!(st.page_count, max_pages);
    assert!(
        second_fill + 1 + 2 >= first_fill,
        "freelist reuse: {first_fill} then {second_fill}"
    );
    assert_eq!(s.verify().unwrap(), second_fill + 1);
}

/// Bounded-size soak: append, replay to the example receiver, prune; repeat.
/// The total data written is many times the size limit, so without pruning
/// the spool would be full long before the end. `AKHET_SOAK_CYCLES` raises
/// the number of cycles for a longer local run.
#[test]
fn soak_spool_size_stays_bounded_while_sink_keeps_up() {
    let cycles: u64 = std::env::var("AKHET_SOAK_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    const PER_CYCLE: u64 = 3;
    const LIMIT: u64 = 2 * SMALL_DB;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    let mut s = Spool::open(&db, LIMIT).unwrap();
    let max_pages = s.capacity().unwrap().max_page_count;
    let mut cfg = ReplayConfig::new(&rx.url, "soak");
    cfg.backoff_initial = Duration::from_millis(10);
    let stop = no_stop();
    let mut appended_bytes = 0u64;
    let mut page_counts = Vec::new();
    for c in 0..cycles {
        for i in 0..PER_CYCLE {
            let n = c * PER_CYCLE + i;
            let e = mqtt_ev(n, 1000 + (n as usize * 1777) % 12_000);
            appended_bytes += e.encode().len() as u64;
            s.append(&e, None)
                .unwrap_or_else(|e| panic!("cycle {c}: {e}"));
        }
        let sum = replay::run(&db, LIMIT, &cfg, &Faults::none(), &stop).unwrap();
        assert_eq!(sum.delivered, PER_CYCLE);
        let r = prune(&mut s, Duration::ZERO, 7, false);
        assert_eq!(r.records, PER_CYCLE, "cycle {c}");
        let st = s.status().unwrap();
        assert_eq!(st.records, 0);
        assert!(st.page_count <= max_pages);
        page_counts.push(st.page_count);
    }
    let total = cycles * PER_CYCLE;
    eprintln!(
        "soak: {cycles} cycles, {total} records, {appended_bytes} bytes appended, \
         limit {LIMIT} bytes ({max_pages} pages), page_count per cycle {page_counts:?}"
    );
    assert_eq!(
        rx.count(),
        total as i64,
        "every record reached the receiver"
    );
    if cycles >= 10 {
        assert!(
            appended_bytes > 3 * LIMIT,
            "soak wrote {appended_bytes} bytes"
        );
    }
    // After warm-up the file stops growing: freed pages are reused.
    let (early, late) = page_counts.split_at(page_counts.len() / 2);
    if let (Some(e), Some(l)) = (early.iter().max(), late.iter().max()) {
        assert!(
            l <= e,
            "page_count grew in the second half: {page_counts:?}"
        );
    }
    assert_eq!(
        s.append(&ev(u64::MAX, 1), None).unwrap().seq(),
        total as i64 + 1
    );
}

fn bind_and_ack(db: &Path, sink: &str, through: i64) {
    let mut s = Spool::open(db, MAX_DB).unwrap();
    let row = s.bind_sink(sink, "http://unused:1", "r").unwrap();
    s.advance_checkpoint(&row, through).unwrap();
}

#[test]
fn crash_inside_a_prune_transaction_rolls_back_that_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    seed(&mut Spool::open(&db, MAX_DB).unwrap(), 10);
    bind_and_ack(&db, "a", 6);
    let out = run(
        &db,
        &["prune", "--min-age", "0s", "--batch-size", "4"],
        Some(&format!("abort:{}", point::PRUNE_BEFORE_COMMIT)),
    );
    assert!(killed_by_signal(&out));
    let out = run(&db, &["verify"], None);
    assert!(out.status.success());
    assert_eq!(stdout_json(&out)["verified_records"], 10, "nothing deleted");

    let out = run(
        &db,
        &["prune", "--min-age", "0s", "--batch-size", "4"],
        Some(&format!("abort:{}", point::PRUNE_AFTER_COMMIT)),
    );
    assert!(killed_by_signal(&out));
    let out = run(&db, &["verify"], None);
    assert!(out.status.success());
    assert_eq!(
        stdout_json(&out)["verified_records"],
        6,
        "first batch committed"
    );
    assert_eq!(
        seqs(&Spool::open(&db, MAX_DB).unwrap()),
        (5..=10).collect::<Vec<_>>()
    );

    // A clean run finishes the job; unacknowledged records 7..=10 survive.
    let out = run(
        &db,
        &["prune", "--min-age", "0s", "--batch-size", "4"],
        None,
    );
    assert!(out.status.success());
    let r = stdout_json(&out);
    assert_eq!(r["records"], 2);
    assert_eq!(r["first_seq"], 5);
    assert_eq!(r["last_seq"], 6);
    assert_eq!(r["limited_by"], "min_checkpoint");
    let s = Spool::open(&db, MAX_DB).unwrap();
    assert_eq!(seqs(&s), vec![7, 8, 9, 10]);
    assert_eq!(s.verify().unwrap(), 4);
}

#[test]
fn prune_cli_dry_run_default_age_and_no_sink() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.sqlite");
    seed(&mut Spool::open(&db, MAX_DB).unwrap(), 3);
    let out = run(&db, &["prune", "--min-age", "0s"], None);
    assert!(out.status.success());
    let r = stdout_json(&out);
    assert_eq!(r["records"], 0);
    assert_eq!(r["limited_by"], "no_sink_bound");

    bind_and_ack(&db, "a", 3);
    let r = stdout_json(&run(&db, &["prune"], None));
    assert_eq!(r["records"], 0, "default --min-age 24h");
    assert_eq!(r["min_age_secs"], 86_400);
    assert_eq!(r["limited_by"], "min_age");
    let r = stdout_json(&run(&db, &["prune", "--min-age", "0s", "--dry-run"], None));
    assert_eq!(r["dry_run"], true);
    assert_eq!(r["records"], 3);
    assert_eq!(count(&db), 3);
    assert_eq!(
        run(&db, &["prune", "--min-age", "5x"], None).status.code(),
        Some(2)
    );
    let st: serde_json::Value =
        serde_json::from_slice(&run(&db, &["status"], None).stdout).unwrap();
    assert_eq!(st["prune_safe_seq"], 3);
    assert!(st["headroom_pages"].as_i64().unwrap() > 0);
    assert!(st["freelist_count"].is_i64());
}

#[test]
fn follow_replay_prunes_up_to_the_slowest_sink() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&mut Spool::open(&db, MAX_DB).unwrap(), 6);
    let rx = TestReceiver::start(&dir.path().join("rx.sqlite"));
    // A second sink that has acknowledged only seq 1 and 2.
    let out = run(
        &db,
        &["replay", "--url", &rx.url, "--sink", "slow", "--limit", "2"],
        None,
    );
    assert!(out.status.success());
    let child = akhet(
        &db,
        &[
            "replay",
            "--url",
            &rx.url,
            "--sink",
            "fast",
            "--follow",
            "--follow-interval-ms",
            "50",
            "--prune-interval",
            "0s",
            "--prune-min-age",
            "0s",
        ],
        None,
    )
    .stdout(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    let first_seq = || {
        Spool::open(&db, MAX_DB)
            .unwrap()
            .status()
            .unwrap()
            .first_seq
    };
    assert!(wait_until(Duration::from_secs(10), || first_seq() == Some(3)));
    // The slow sink catches up; the follower's next prune removes the rest.
    assert!(
        run(&db, &["replay", "--url", &rx.url, "--sink", "slow"], None)
            .status
            .success()
    );
    assert!(wait_until(Duration::from_secs(10), || first_seq().is_none()));
    let st = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(st.success());
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 6);
    assert_eq!(s["pruned_records"], 6);
    assert_eq!(rx.count(), 6);

    // A sink bound after pruning only sees what is left, and says so.
    seed(&mut Spool::open(&db, MAX_DB).unwrap(), 1);
    let out = run(&db, &["replay", "--url", &rx.url, "--sink", "late"], None);
    assert_eq!(stdout_json(&out)["delivered"], 1);
    assert!(String::from_utf8_lossy(&out.stderr).contains("starts at seq 7"));
}
