//! Durable local spool on SQLite.
//!
//! Durability model: every append (a single event, or a group of events with
//! [`Spool::append_batch`]) is one explicit `BEGIN IMMEDIATE` ... `COMMIT`
//! transaction with `journal_mode=DELETE`, `synchronous=FULL` and
//! (on macOS) `fullfsync=ON`. When an append returns `Ok`, SQLite has asked the
//! OS to flush the rollback journal and database file to stable storage. Whether
//! the data survives a power failure depends on the filesystem and device
//! honoring those flushes; this has not been power-failure tested.
//!
//! Retention: records are only deleted by [`Spool::prune`], which removes a
//! contiguous prefix of records that every bound sink has acknowledged and
//! that are older than a minimum age. Freed pages go to SQLite's freelist and
//! are reused by later appends, so `page_count` stays bounded once pruning
//! keeps up; the file is never shrunk automatically.

use crate::event::{self, FormatError};
use crate::faults::{Faults, point};
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// SQLite `application_id` marking an Akhet spool ("AKHT").
pub const APPLICATION_ID: i64 = 0x414B_4854;
/// Database schema version written by this build (`PRAGMA user_version`).
pub const SCHEMA_VERSION: i64 = 1;
/// Default database-size limit: 1 GiB of SQLite pages.
pub const DEFAULT_MAX_DB_BYTES: u64 = 1 << 30;
/// Default maximum encoded event size.
pub const DEFAULT_MAX_EVENT_BYTES: usize = 1 << 20;
/// Smallest accepted database-size limit.
pub const MIN_MAX_DB_BYTES: u64 = 64 * 1024;
/// Usage levels (percent of `max_page_count`, freelist pages counted as free)
/// at which appends log a capacity warning.
pub const CAPACITY_WARN_PERCENT: [u8; 2] = [80, 95];
/// A warning level is re-armed once usage falls this many points below it.
const CAPACITY_WARN_HYSTERESIS: f64 = 5.0;
/// Default number of records deleted per prune transaction.
pub const DEFAULT_PRUNE_BATCH: usize = 1000;

#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0} is not an Akhet spool (SQLite application_id {1:#x})")]
    NotASpool(PathBuf, i64),
    #[error("unsupported spool schema version {found} (this build supports {SCHEMA_VERSION})")]
    UnsupportedSchema { found: i64 },
    #[error("invalid event: {0}")]
    Format(#[from] FormatError),
    #[error("encoded event is {size} bytes, limit is {max}")]
    EventTooLarge { size: usize, max: usize },
    #[error(
        "conflicting reuse of {what} {id:?}: already stored at seq {existing_seq} with different content"
    )]
    Conflict {
        what: &'static str,
        id: String,
        existing_seq: i64,
    },
    #[error("spool corruption at seq {seq}: {detail}")]
    Corrupt { seq: i64, detail: String },
    #[error(
        "spool database-size limit reached ({page_count} of {max_page_count} pages, limit {max_db_bytes} bytes); \
         committed records are intact, new records are refused"
    )]
    CapacityExhausted {
        max_db_bytes: u64,
        page_count: i64,
        max_page_count: i64,
    },
    #[error("filesystem full while below the spool size limit: {0}")]
    DiskFull(String),
    #[error("sink binding: {0}")]
    SinkBinding(String),
}

impl SpoolError {
    /// True when the spool (database-size limit) or the filesystem is full.
    pub fn is_capacity(&self) -> bool {
        matches!(
            self,
            SpoolError::CapacityExhausted { .. } | SpoolError::DiskFull(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, SpoolError>;

/// A source-defined identity for deduplication across upstream redelivery.
#[derive(Debug, Clone)]
pub struct SourceClaim {
    /// Fully qualified key (namespace + source-specific parts).
    pub key: String,
    /// Digest of the content the source identity is supposed to identify.
    pub content_sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Inserted {
        seq: i64,
    },
    /// Identical event_id and identical bytes already stored.
    DuplicateEvent {
        seq: i64,
    },
    /// Same configured source identity with identical content already stored.
    DuplicateSource {
        seq: i64,
    },
}

impl AppendOutcome {
    pub fn seq(&self) -> i64 {
        match *self {
            AppendOutcome::Inserted { seq }
            | AppendOutcome::DuplicateEvent { seq }
            | AppendOutcome::DuplicateSource { seq } => seq,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub seq: i64,
    pub event_id: String,
    pub body: Vec<u8>,
    pub body_sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SinkRow {
    pub name: String,
    pub destination_url: String,
    pub receiver_id: String,
    pub checkpoint_seq: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct SpoolStatus {
    pub path: String,
    pub spool_id: String,
    pub schema_version: i64,
    pub records: i64,
    pub first_seq: Option<i64>,
    pub last_seq: Option<i64>,
    pub page_size: i64,
    pub page_count: i64,
    pub freelist_count: i64,
    pub max_page_count: i64,
    pub max_db_bytes: u64,
    pub db_bytes_used: i64,
    pub file_bytes_on_disk: u64,
    /// Pages holding data: `page_count - freelist_count`.
    pub used_pages: i64,
    /// `used_pages` as a percentage of `max_page_count`.
    pub used_percent: f64,
    /// Pages still available for new records (`max_page_count - used_pages`):
    /// unallocated pages plus freelist pages, which SQLite reuses first.
    pub headroom_pages: i64,
    pub headroom_bytes: i64,
    /// Highest seq that `prune` may delete (minimum checkpoint over all bound
    /// sinks); `null` when no sink is bound, in which case nothing is pruned.
    pub prune_safe_seq: Option<i64>,
    pub sinks: Vec<SinkStatus>,
}

/// Page accounting against the database-size limit.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Capacity {
    pub page_size: i64,
    pub page_count: i64,
    pub freelist_count: i64,
    pub max_page_count: i64,
    pub used_pages: i64,
    pub headroom_pages: i64,
    pub used_percent: f64,
}

#[derive(Debug, Clone)]
pub struct PruneOptions {
    /// Only records whose `appended_unix_ns` is at least this old are deleted.
    pub min_age: Duration,
    /// Records deleted per transaction (one commit each).
    pub batch: usize,
    /// Report what would be deleted without deleting anything.
    pub dry_run: bool,
}

impl Default for PruneOptions {
    fn default() -> Self {
        PruneOptions {
            min_age: Duration::from_secs(24 * 3600),
            batch: DEFAULT_PRUNE_BATCH,
            dry_run: false,
        }
    }
}

/// What stopped a prune run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PruneLimit {
    /// No sink is bound; nothing is ever deleted then.
    NoSinkBound,
    /// Every record up to the minimum sink checkpoint was eligible.
    MinCheckpoint,
    /// The next record at or below the minimum checkpoint is younger than
    /// the minimum age.
    MinAge,
    /// Stop was requested between batches.
    Interrupted,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PruneReport {
    pub dry_run: bool,
    pub min_age_secs: u64,
    /// Records appended at or before this instant were old enough.
    pub cutoff_unix_ns: i64,
    pub sinks_bound: i64,
    /// Minimum checkpoint over all bound sinks (at the last batch).
    pub safe_seq: Option<i64>,
    /// Records deleted (or, with `dry_run`, that would be deleted).
    pub records: u64,
    pub first_seq: Option<i64>,
    pub last_seq: Option<i64>,
    /// Committed delete transactions (0 for a dry run).
    pub batches: u64,
    pub limited_by: PruneLimit,
    pub page_count: i64,
    pub freelist_count: i64,
    pub headroom_pages: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct SinkStatus {
    #[serde(flatten)]
    pub sink: SinkRow,
    pub pending_records: i64,
}

pub struct Spool {
    conn: Connection,
    path: PathBuf,
    spool_id: String,
    max_db_bytes: u64,
    max_event_bytes: usize,
    /// Highest capacity warning level (index into CAPACITY_WARN_PERCENT,
    /// plus one) already logged by this connection; 0 = none.
    warn_level: usize,
}

const SCHEMA: &str = "
CREATE TABLE meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
CREATE TABLE events(
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL UNIQUE,
    format_version  INTEGER NOT NULL,
    source_key      TEXT UNIQUE,
    source_sha256   BLOB,
    body            BLOB NOT NULL,
    body_sha256     BLOB NOT NULL,
    appended_unix_ns INTEGER NOT NULL
) STRICT;
CREATE TABLE sinks(
    name             TEXT PRIMARY KEY,
    destination_url  TEXT NOT NULL,
    receiver_id      TEXT NOT NULL,
    checkpoint_seq   INTEGER NOT NULL DEFAULT 0,
    created_unix_ns  INTEGER NOT NULL,
    updated_unix_ns  INTEGER NOT NULL
) STRICT;
";

fn pragma_i64(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))
}

impl Spool {
    pub fn open(path: impl AsRef<Path>, max_db_bytes: u64) -> Result<Spool> {
        Self::open_with_limits(path, max_db_bytes, DEFAULT_MAX_EVENT_BYTES)
    }

    pub fn open_with_limits(
        path: impl AsRef<Path>,
        max_db_bytes: u64,
        max_event_bytes: usize,
    ) -> Result<Spool> {
        let path = path.as_ref().to_path_buf();
        if max_db_bytes < MIN_MAX_DB_BYTES {
            return Err(SpoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("max_db_bytes must be at least {MIN_MAX_DB_BYTES}"),
            )));
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(10))?;
        // Rollback journal: no separately growing WAL file. FULL sync on every
        // commit. fullfsync requests F_FULLFSYNC on macOS (no-op elsewhere).
        let mode: String = conn.query_row("PRAGMA journal_mode=DELETE", [], |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("delete") {
            return Err(SpoolError::Io(std::io::Error::other(format!(
                "could not set journal_mode=DELETE (got {mode})"
            ))));
        }
        conn.execute_batch("PRAGMA synchronous=FULL; PRAGMA fullfsync=ON;")?;

        let mut spool = Spool {
            conn,
            path,
            spool_id: String::new(),
            max_db_bytes,
            max_event_bytes,
            warn_level: 0,
        };
        spool.init_or_check_schema()?;
        let page_size = pragma_i64(&spool.conn, "page_size")?;
        let max_pages = (max_db_bytes / page_size as u64).max(1);
        spool.conn.query_row(
            &format!("PRAGMA max_page_count={max_pages}"),
            [],
            |_| Ok(()),
        )?;
        spool.spool_id =
            spool
                .conn
                .query_row("SELECT value FROM meta WHERE key='spool_id'", [], |r| {
                    r.get(0)
                })?;
        Ok(spool)
    }

    fn init_or_check_schema(&mut self) -> Result<()> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let app_id = pragma_i64(&tx, "application_id")?;
        let version = pragma_i64(&tx, "user_version")?;
        let objects: i64 = tx.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))?;
        if app_id == 0 && version == 0 && objects == 0 {
            tx.execute_batch(SCHEMA)?;
            tx.execute_batch(&format!(
                "PRAGMA application_id={APPLICATION_ID}; PRAGMA user_version={SCHEMA_VERSION};"
            ))?;
            tx.execute(
                "INSERT INTO meta(key,value) VALUES('spool_id',?1),('created_unix_ns',?2),('schema_version',?3)",
                params![
                    uuid::Uuid::new_v4().to_string(),
                    event::now_unix_ns().to_string(),
                    SCHEMA_VERSION.to_string()
                ],
            )?;
            tx.commit()?;
            return Ok(());
        }
        if app_id != APPLICATION_ID {
            return Err(SpoolError::NotASpool(self.path.clone(), app_id));
        }
        if version != SCHEMA_VERSION {
            return Err(SpoolError::UnsupportedSchema { found: version });
        }
        tx.commit()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Random identity generated when the spool file was created.
    pub fn spool_id(&self) -> &str {
        &self.spool_id
    }

    pub fn max_event_bytes(&self) -> usize {
        self.max_event_bytes
    }

    fn map_full(&self, e: rusqlite::Error) -> SpoolError {
        if let rusqlite::Error::SqliteFailure(f, msg) = &e
            && f.code == ErrorCode::DiskFull
        {
            let page_count = pragma_i64(&self.conn, "page_count").unwrap_or(-1);
            let max_page_count = pragma_i64(&self.conn, "max_page_count").unwrap_or(-1);
            // SQLITE_FULL is reported both for max_page_count and for a full
            // filesystem. Distinguish by how close the file is to the limit.
            let headroom = max_page_count - page_count;
            if page_count >= 0 && max_page_count >= 0 && headroom <= 64 {
                return SpoolError::CapacityExhausted {
                    max_db_bytes: self.max_db_bytes,
                    page_count,
                    max_page_count,
                };
            }
            return SpoolError::DiskFull(msg.clone().unwrap_or_else(|| f.to_string()));
        }
        SpoolError::Sqlite(e)
    }

    /// Append an event. Returns only after the transaction committed.
    pub fn append(
        &mut self,
        ev: &event::Event,
        source: Option<&SourceClaim>,
    ) -> Result<AppendOutcome> {
        self.append_body(&ev.encode(), source)
    }

    /// Append an already encoded event body (used by the receiver).
    pub fn append_body(
        &mut self,
        body: &[u8],
        source: Option<&SourceClaim>,
    ) -> Result<AppendOutcome> {
        let item = [BatchItem { body, source }];
        let mut results = self.append_batch(&item, ItemErrors::StopAtFirst)?;
        results.pop().expect("one result per attempted item")
    }

    /// Append several encoded event bodies in ONE transaction (group commit).
    ///
    /// Each item gets exactly the checks and dedup/conflict semantics of
    /// [`Spool::append_body`], applied in order and seeing the items inserted
    /// before it in the same batch. Returns only after the transaction
    /// committed (outer `Ok`) or rolled back (outer `Err`: SQLite, I/O or
    /// capacity failure, nothing from the batch is stored).
    ///
    /// Item-level failures (size limit, invalid format, conflicting identity)
    /// insert nothing for that item and are reported in the inner `Result`.
    /// With [`ItemErrors::StopAtFirst`] the items before the first failure are
    /// committed, later items are not attempted and the returned vector ends
    /// with that failure. With [`ItemErrors::Independent`] every item is
    /// attempted and the vector has one entry per item.
    pub fn append_batch(
        &mut self,
        items: &[BatchItem<'_>],
        policy: ItemErrors,
    ) -> Result<Vec<Result<AppendOutcome>>> {
        self.append_batch_observed(items, policy, |_| {})
    }

    /// [`Spool::append_batch`] with a callback after each inserted row, before
    /// the commit (fault injection for crash tests).
    #[doc(hidden)]
    pub fn append_batch_observed(
        &mut self,
        items: &[BatchItem<'_>],
        policy: ItemErrors,
        mut after_insert: impl FnMut(usize),
    ) -> Result<Vec<Result<AppendOutcome>>> {
        let max_event_bytes = self.max_event_bytes;
        let res = (|| -> Result<Vec<Result<AppendOutcome>>> {
            let tx =
                rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
            let mut out = Vec::with_capacity(items.len());
            let mut inserted = 0;
            for item in items {
                // Item-level errors are detected before the INSERT, so a
                // failed item leaves nothing behind in the transaction.
                match append_in_tx(&tx, max_event_bytes, item) {
                    Ok(o) => {
                        if matches!(o, AppendOutcome::Inserted { .. }) {
                            after_insert(inserted);
                            inserted += 1;
                        }
                        out.push(Ok(o));
                    }
                    Err(e @ (SpoolError::Sqlite(_) | SpoolError::Io(_))) => return Err(e),
                    Err(e) => {
                        out.push(Err(e));
                        if policy == ItemErrors::StopAtFirst {
                            break;
                        }
                    }
                }
            }
            tx.commit()?;
            Ok(out)
        })();
        let res = res.map_err(|e| match e {
            SpoolError::Sqlite(e) => self.map_full(e),
            other => other,
        });
        if let Ok(items) = &res
            && items
                .iter()
                .any(|r| matches!(r, Ok(AppendOutcome::Inserted { .. })))
        {
            self.note_capacity();
        }
        res
    }

    /// Page usage against the limit. Freelist pages count as free because
    /// SQLite reuses them before growing the file.
    pub fn capacity(&self) -> Result<Capacity> {
        let page_size = pragma_i64(&self.conn, "page_size")?;
        let page_count = pragma_i64(&self.conn, "page_count")?;
        let freelist_count = pragma_i64(&self.conn, "freelist_count")?;
        let max_page_count = pragma_i64(&self.conn, "max_page_count")?;
        let used_pages = page_count - freelist_count;
        Ok(Capacity {
            page_size,
            page_count,
            freelist_count,
            max_page_count,
            used_pages,
            headroom_pages: max_page_count - used_pages,
            used_percent: used_pages as f64 * 100.0 / max_page_count.max(1) as f64,
        })
    }

    /// Highest capacity warning threshold (percent) currently armed as
    /// crossed by this connection, or 0.
    pub fn capacity_warning_percent(&self) -> u8 {
        match self.warn_level {
            0 => 0,
            n => CAPACITY_WARN_PERCENT[n - 1],
        }
    }

    /// Log once per threshold crossing; re-arm when usage drops again.
    fn note_capacity(&mut self) {
        let Ok(c) = self.capacity() else { return };
        let reached = CAPACITY_WARN_PERCENT
            .iter()
            .filter(|p| c.used_percent >= f64::from(**p))
            .count();
        if reached > self.warn_level {
            self.warn_level = reached;
            eprintln!(
                "akhet: warning: spool at {:.1}% of its database-size limit \
                 ({} of {} pages used, {} bytes headroom); prune replayed records \
                 or raise --max-db-bytes before appends are refused",
                c.used_percent,
                c.used_pages,
                c.max_page_count,
                c.headroom_pages * c.page_size
            );
        } else {
            while self.warn_level > 0
                && c.used_percent
                    < f64::from(CAPACITY_WARN_PERCENT[self.warn_level - 1])
                        - CAPACITY_WARN_HYSTERESIS
            {
                self.warn_level -= 1;
            }
        }
    }

    /// Delete records that every bound sink has acknowledged
    /// (`seq <= min(checkpoint_seq)`) and that were appended at least
    /// `min_age` ago. Only a contiguous prefix of the spool is deleted: the
    /// run stops at the first record that is too young or above the minimum
    /// checkpoint. With no bound sink, nothing is deleted.
    ///
    /// Deletion happens in batches of `opts.batch` records, one
    /// `BEGIN IMMEDIATE ... COMMIT` each, re-reading the sink checkpoints
    /// inside every transaction. A crash rolls back at most the batch in
    /// progress. `stop` is checked between batches.
    pub fn prune(
        &mut self,
        opts: &PruneOptions,
        faults: &Faults,
        stop: &AtomicBool,
    ) -> Result<PruneReport> {
        if opts.batch == 0 {
            return Err(SpoolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "prune batch size must be positive",
            )));
        }
        let min_age_ns = i64::try_from(opts.min_age.as_nanos()).unwrap_or(i64::MAX);
        let cutoff = event::now_unix_ns().saturating_sub(min_age_ns);
        let mut report = PruneReport {
            dry_run: opts.dry_run,
            min_age_secs: opts.min_age.as_secs(),
            cutoff_unix_ns: cutoff,
            sinks_bound: 0,
            safe_seq: None,
            records: 0,
            first_seq: None,
            last_seq: None,
            batches: 0,
            limited_by: PruneLimit::NoSinkBound,
            page_count: 0,
            freelist_count: 0,
            headroom_pages: 0,
        };
        // Records at or below `cursor` have been deleted (or, in a dry run,
        // counted) already.
        let mut cursor = 0i64;
        loop {
            if stop.load(Ordering::SeqCst) {
                report.limited_by = PruneLimit::Interrupted;
                break;
            }
            let behavior = if opts.dry_run {
                TransactionBehavior::Deferred
            } else {
                TransactionBehavior::Immediate
            };
            let step = (|| -> Result<Option<(usize, i64, i64, bool)>> {
                let tx = rusqlite::Transaction::new_unchecked(&self.conn, behavior)?;
                let (sinks, safe): (i64, Option<i64>) =
                    tx.query_row("SELECT count(*), min(checkpoint_seq) FROM sinks", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
                report.sinks_bound = sinks;
                report.safe_seq = safe;
                let Some(safe) = safe else {
                    report.limited_by = PruneLimit::NoSinkBound;
                    return Ok(None);
                };
                let rows: Vec<(i64, i64)> = {
                    let mut stmt = tx.prepare_cached(
                        "SELECT seq, appended_unix_ns FROM events
                         WHERE seq > ?1 AND seq <= ?2 ORDER BY seq LIMIT ?3",
                    )?;
                    stmt.query_map(params![cursor, safe, opts.batch as i64], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?
                    .collect::<rusqlite::Result<_>>()?
                };
                let take = rows.iter().take_while(|(_, t)| *t <= cutoff).count();
                let too_young = take < rows.len();
                report.limited_by = if too_young {
                    PruneLimit::MinAge
                } else {
                    PruneLimit::MinCheckpoint
                };
                if take == 0 {
                    return Ok(None);
                }
                let last = rows[take - 1].0;
                if !opts.dry_run {
                    let deleted = tx.execute(
                        "DELETE FROM events WHERE seq > ?1 AND seq <= ?2",
                        params![cursor, last],
                    )?;
                    if deleted != take {
                        return Err(SpoolError::Corrupt {
                            seq: last,
                            detail: format!(
                                "prune expected to delete {take} records, deleted {deleted}"
                            ),
                        });
                    }
                    faults.hit(point::PRUNE_BEFORE_COMMIT);
                }
                tx.commit()?;
                let more = !too_young && rows.len() == opts.batch;
                Ok(Some((take, rows[0].0, last, more)))
            })();
            let step = step.map_err(|e| match e {
                SpoolError::Sqlite(e) => self.map_full(e),
                other => other,
            })?;
            let Some((n, first, last, more)) = step else {
                break;
            };
            report.first_seq.get_or_insert(first);
            if !opts.dry_run {
                report.batches += 1;
                faults.hit(point::PRUNE_AFTER_COMMIT);
            }
            report.records += n as u64;
            report.last_seq = Some(last);
            cursor = last;
            if !more {
                break;
            }
        }
        let c = self.capacity()?;
        report.page_count = c.page_count;
        report.freelist_count = c.freelist_count;
        report.headroom_pages = c.headroom_pages;
        if !opts.dry_run {
            self.note_capacity();
        }
        Ok(report)
    }

    /// Read up to `limit` records with seq > `after`, verifying checksums and
    /// the event format. Fails on the first corrupt or unsupported record.
    pub fn read_after(&self, after: i64, limit: usize) -> Result<Vec<StoredRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT seq, event_id, format_version, body, body_sha256 FROM events
             WHERE seq > ?1 ORDER BY seq LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![after, limit as i64], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, Vec<u8>>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, event_id, fv, body, sha) = row?;
            out.push(verify_row(seq, event_id, fv, body, sha)?);
        }
        Ok(out)
    }

    pub fn sink(&self, name: &str) -> Result<Option<SinkRow>> {
        Ok(self
            .conn
            .query_row(
                "SELECT name, destination_url, receiver_id, checkpoint_seq FROM sinks WHERE name=?1",
                [name],
                |r| {
                    Ok(SinkRow {
                        name: r.get(0)?,
                        destination_url: r.get(1)?,
                        receiver_id: r.get(2)?,
                        checkpoint_seq: r.get(3)?,
                    })
                },
            )
            .optional()?)
    }

    /// Create the sink binding or verify that an existing binding matches.
    /// A sink name is permanently bound to one destination URL and one
    /// receiver identity; reusing it for another endpoint is refused.
    pub fn bind_sink(&mut self, name: &str, url: &str, receiver_id: &str) -> Result<SinkRow> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let now = event::now_unix_ns();
        tx.execute(
            "INSERT INTO sinks(name, destination_url, receiver_id, checkpoint_seq, created_unix_ns, updated_unix_ns)
             VALUES(?1,?2,?3,0,?4,?4) ON CONFLICT(name) DO NOTHING",
            params![name, url, receiver_id, now],
        )
        .map_err(|e| self.map_full(e))?;
        let row: SinkRow = tx.query_row(
            "SELECT name, destination_url, receiver_id, checkpoint_seq FROM sinks WHERE name=?1",
            [name],
            |r| {
                Ok(SinkRow {
                    name: r.get(0)?,
                    destination_url: r.get(1)?,
                    receiver_id: r.get(2)?,
                    checkpoint_seq: r.get(3)?,
                })
            },
        )?;
        tx.commit().map_err(|e| self.map_full(e))?;
        if row.destination_url != url {
            return Err(SpoolError::SinkBinding(format!(
                "sink {name:?} is bound to {:?}, refusing to replay its checkpoint to {url:?}; use a new sink name",
                row.destination_url
            )));
        }
        if row.receiver_id != receiver_id {
            return Err(SpoolError::SinkBinding(format!(
                "sink {name:?} was bound to receiver {:?} but {url:?} now reports receiver {receiver_id:?} \
                 (receiver reset or different endpoint); refusing to reuse the checkpoint; use a new sink name",
                row.receiver_id
            )));
        }
        Ok(row)
    }

    /// Advance the checkpoint of a bound sink. Never moves it backwards.
    pub fn advance_checkpoint(&mut self, sink: &SinkRow, seq: i64) -> Result<()> {
        let tx = rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let changed = tx
            .execute(
                "UPDATE sinks SET checkpoint_seq=?1, updated_unix_ns=?2
                 WHERE name=?3 AND destination_url=?4 AND receiver_id=?5 AND checkpoint_seq < ?1",
                params![
                    seq,
                    event::now_unix_ns(),
                    sink.name,
                    sink.destination_url,
                    sink.receiver_id
                ],
            )
            .map_err(|e| self.map_full(e))?;
        if changed == 0 {
            let current: Option<(String, String, i64)> = tx
                .query_row(
                    "SELECT destination_url, receiver_id, checkpoint_seq FROM sinks WHERE name=?1",
                    [&sink.name],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            match current {
                Some((u, r, _)) if u == sink.destination_url && r == sink.receiver_id => {}
                _ => {
                    return Err(SpoolError::SinkBinding(format!(
                        "binding of sink {:?} changed during replay",
                        sink.name
                    )));
                }
            }
        }
        tx.commit().map_err(|e| self.map_full(e))?;
        Ok(())
    }

    pub fn status(&self) -> Result<SpoolStatus> {
        let (records, first_seq, last_seq): (i64, Option<i64>, Option<i64>) =
            self.conn
                .query_row("SELECT count(*), min(seq), max(seq) FROM events", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        let cap = self.capacity()?;
        let prune_safe_seq: Option<i64> =
            self.conn
                .query_row("SELECT min(checkpoint_seq) FROM sinks", [], |r| r.get(0))?;
        let mut stmt = self.conn.prepare(
            "SELECT s.name, s.destination_url, s.receiver_id, s.checkpoint_seq,
                    (SELECT count(*) FROM events e WHERE e.seq > s.checkpoint_seq)
             FROM sinks s ORDER BY s.name",
        )?;
        let sinks = stmt
            .query_map([], |r| {
                Ok(SinkStatus {
                    sink: SinkRow {
                        name: r.get(0)?,
                        destination_url: r.get(1)?,
                        receiver_id: r.get(2)?,
                        checkpoint_seq: r.get(3)?,
                    },
                    pending_records: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(SpoolStatus {
            path: self.path.display().to_string(),
            spool_id: self.spool_id.clone(),
            schema_version: SCHEMA_VERSION,
            records,
            first_seq,
            last_seq,
            page_size: cap.page_size,
            page_count: cap.page_count,
            freelist_count: cap.freelist_count,
            max_page_count: cap.max_page_count,
            max_db_bytes: self.max_db_bytes,
            db_bytes_used: cap.page_size * cap.page_count,
            file_bytes_on_disk: std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0),
            used_pages: cap.used_pages,
            used_percent: (cap.used_percent * 10.0).round() / 10.0,
            headroom_pages: cap.headroom_pages,
            headroom_bytes: cap.headroom_pages * cap.page_size,
            prune_safe_seq,
            sinks,
        })
    }

    /// Full verification: SQLite structural check plus every record checksum
    /// and format version. Returns the number of verified records.
    pub fn verify(&self) -> Result<u64> {
        let check: String = self
            .conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(SpoolError::Corrupt {
                seq: -1,
                detail: format!("sqlite quick_check: {check}"),
            });
        }
        let mut after = 0;
        let mut n = 0u64;
        loop {
            let batch = self.read_after(after, 256)?;
            let Some(last) = batch.last() else { break };
            after = last.seq;
            n += batch.len() as u64;
        }
        Ok(n)
    }

    /// Read a value from the spool's `meta` table (collector bookkeeping, not
    /// events; never replayed).
    pub fn meta_get(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()?)
    }

    /// Durably insert or replace a value in the `meta` table (one autocommit
    /// transaction, same synchronous settings as appends).
    pub fn meta_set(&mut self, key: &str, value: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO meta(key,value) VALUES(?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(|e| self.map_full(e))?;
        Ok(())
    }

    /// Direct access for tests and fault injection.
    #[doc(hidden)]
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// One item of [`Spool::append_batch`].
#[derive(Debug, Clone, Copy)]
pub struct BatchItem<'a> {
    /// Encoded event body (stored byte-exact).
    pub body: &'a [u8],
    pub source: Option<&'a SourceClaim>,
}

/// How [`Spool::append_batch`] continues after an item-level failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemErrors {
    /// Commit the items before the first failure; do not attempt later ones.
    StopAtFirst,
    /// Attempt every item; commit all that succeeded.
    Independent,
}

/// Checks and dedup for one event inside an open write transaction.
fn append_in_tx(
    tx: &Connection,
    max_event_bytes: usize,
    item: &BatchItem<'_>,
) -> Result<AppendOutcome> {
    let BatchItem { body, source } = *item;
    if body.len() > max_event_bytes {
        return Err(SpoolError::EventTooLarge {
            size: body.len(),
            max: max_event_bytes,
        });
    }
    let (event_id, format_version) = event::check_body_version(body)?;
    let digest = event::sha256(body);
    let existing: Option<(i64, Vec<u8>)> = tx
        .prepare_cached("SELECT seq, body_sha256 FROM events WHERE event_id=?1")?
        .query_row([&event_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    if let Some((seq, sha)) = existing {
        if sha == digest {
            return Ok(AppendOutcome::DuplicateEvent { seq });
        }
        return Err(SpoolError::Conflict {
            what: "event_id",
            id: event_id,
            existing_seq: seq,
        });
    }
    if let Some(src) = source {
        let existing: Option<(i64, Option<Vec<u8>>)> = tx
            .prepare_cached("SELECT seq, source_sha256 FROM events WHERE source_key=?1")?
            .query_row([&src.key], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        if let Some((seq, sha)) = existing {
            if sha.as_deref() == Some(&src.content_sha256[..]) {
                return Ok(AppendOutcome::DuplicateSource { seq });
            }
            return Err(SpoolError::Conflict {
                what: "source identity",
                id: src.key.clone(),
                existing_seq: seq,
            });
        }
    }
    tx.prepare_cached(
        "INSERT INTO events(event_id, format_version, source_key, source_sha256, body, body_sha256, appended_unix_ns)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
    )?
    .execute(params![
        event_id,
        format_version,
        source.map(|s| s.key.as_str()),
        source.map(|s| &s.content_sha256[..]),
        body,
        &digest[..],
        event::now_unix_ns()
    ])?;
    Ok(AppendOutcome::Inserted {
        seq: tx.last_insert_rowid(),
    })
}

fn verify_row(
    seq: i64,
    event_id: String,
    format_version: i64,
    body: Vec<u8>,
    sha: Vec<u8>,
) -> Result<StoredRecord> {
    let digest = event::sha256(&body);
    if sha.len() != 32 || sha != digest {
        return Err(SpoolError::Corrupt {
            seq,
            detail: "body checksum mismatch".into(),
        });
    }
    if !event::SUPPORTED_EVENT_FORMAT_VERSIONS
        .iter()
        .any(|v| i64::from(*v) == format_version)
    {
        return Err(SpoolError::Format(FormatError::UnsupportedVersion(
            u32::try_from(format_version).unwrap_or(u32::MAX),
        )));
    }
    let body_id = event::check_body(&body)?;
    if body_id != event_id {
        return Err(SpoolError::Corrupt {
            seq,
            detail: "event_id column does not match body".into(),
        });
    }
    Ok(StoredRecord {
        seq,
        event_id,
        body,
        body_sha256: digest,
    })
}
