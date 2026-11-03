//! Acquisition adapters (MQTT, Modbus TCP) and the shared async plumbing.

pub mod modbus;
pub mod mqtt;

use crate::event::Event;
use crate::faults::Faults;
use crate::spool::{AppendOutcome, BatchItem, ItemErrors, SourceClaim, Spool, SpoolError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Spool shared by async tasks. All SQLite work runs on the blocking pool so
/// fsync never stalls the async runtime.
#[derive(Clone)]
pub struct AsyncSpool {
    inner: Arc<Mutex<Spool>>,
    faults: Faults,
    commit_log: Option<Arc<Mutex<Vec<CommitStat>>>>,
}

/// Timing of one spool transaction, recorded when a commit log is attached.
#[derive(Debug, Clone, Copy)]
pub struct CommitStat {
    /// Events in the transaction.
    pub events: usize,
    /// Wall time of the SQLite transaction (inserts, commit and fsync).
    pub duration: Duration,
    /// When the transaction finished.
    pub finished: Instant,
}

impl AsyncSpool {
    pub fn new(spool: Spool, faults: Faults) -> Self {
        AsyncSpool {
            inner: Arc::new(Mutex::new(spool)),
            faults,
            commit_log: None,
        }
    }

    /// Record the timing of every successful batch transaction (benchmarks).
    #[doc(hidden)]
    pub fn with_commit_log(mut self, log: Arc<Mutex<Vec<CommitStat>>>) -> Self {
        self.commit_log = Some(log);
        self
    }

    /// Durably append; resolves only after commit (or failure).
    pub async fn append(
        &self,
        ev: Event,
        claim: Option<SourceClaim>,
    ) -> Result<AppendOutcome, SpoolError> {
        let inner = self.inner.clone();
        let faults = self.faults.clone();
        tokio::task::spawn_blocking(move || {
            faults.delay_blocking(crate::faults::point::COLLECTOR_COMMIT);
            let mut s = inner.lock().unwrap_or_else(|p| p.into_inner());
            s.append(&ev, claim.as_ref())
        })
        .await
        .map_err(|e| SpoolError::Io(std::io::Error::other(format!("spool task failed: {e}"))))?
    }

    /// Durably append a group of events in one transaction (group commit);
    /// resolves only after commit (or failure). Item semantics are those of
    /// [`Spool::append_batch`] with [`ItemErrors::StopAtFirst`]: the result
    /// has one entry per attempted event and ends at the first item failure.
    pub async fn append_batch(
        &self,
        events: Vec<(Event, Option<SourceClaim>)>,
    ) -> Result<Vec<Result<AppendOutcome, SpoolError>>, SpoolError> {
        let inner = self.inner.clone();
        let faults = self.faults.clone();
        let commit_log = self.commit_log.clone();
        tokio::task::spawn_blocking(move || {
            use crate::faults::point;
            let bodies: Vec<Vec<u8>> = events.iter().map(|(ev, _)| ev.encode()).collect();
            let items: Vec<BatchItem<'_>> = bodies
                .iter()
                .zip(&events)
                .map(|(body, (_, claim))| BatchItem {
                    body,
                    source: claim.as_ref(),
                })
                .collect();
            faults.delay_blocking(point::COLLECTOR_COMMIT);
            let mut s = inner.lock().unwrap_or_else(|p| p.into_inner());
            let t0 = Instant::now();
            let res = s.append_batch_observed(&items, ItemErrors::StopAtFirst, |i| {
                if i == 1 {
                    faults.hit(point::COLLECTOR_BATCH_PARTIAL);
                }
            });
            if let (Ok(_), Some(log)) = (&res, &commit_log) {
                let finished = Instant::now();
                log.lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(CommitStat {
                        events: items.len(),
                        duration: finished - t0,
                        finished,
                    });
            }
            res
        })
        .await
        .map_err(|e| SpoolError::Io(std::io::Error::other(format!("spool task failed: {e}"))))?
    }

    /// Read a `meta` value on the blocking pool.
    pub async fn meta_get(&self, key: &str) -> Result<Option<String>, SpoolError> {
        let inner = self.inner.clone();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let s = inner.lock().unwrap_or_else(|p| p.into_inner());
            s.meta_get(&key)
        })
        .await
        .map_err(|e| SpoolError::Io(std::io::Error::other(format!("spool task failed: {e}"))))?
    }

    /// Durably write a `meta` value on the blocking pool.
    pub async fn meta_set(&self, key: &str, value: &str) -> Result<(), SpoolError> {
        let inner = self.inner.clone();
        let (key, value) = (key.to_string(), value.to_string());
        tokio::task::spawn_blocking(move || {
            let mut s = inner.lock().unwrap_or_else(|p| p.into_inner());
            s.meta_set(&key, &value)
        })
        .await
        .map_err(|e| SpoolError::Io(std::io::Error::other(format!("spool task failed: {e}"))))?
    }

    pub fn faults(&self) -> &Faults {
        &self.faults
    }
}

/// Cooperative stop signal (Ctrl-C, SIGTERM, limits, fatal errors).
#[derive(Clone)]
pub struct Stop {
    tx: Arc<watch::Sender<bool>>,
    rx: watch::Receiver<bool>,
}

impl Default for Stop {
    fn default() -> Self {
        Self::new()
    }
}

impl Stop {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Stop {
            tx: Arc::new(tx),
            rx,
        }
    }

    pub fn trigger(&self) {
        let _ = self.tx.send(true);
    }

    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolves once stop has been triggered.
    pub async fn wait(&self) {
        let mut rx = self.rx.clone();
        let _ = rx.wait_for(|v| *v).await;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error(transparent)]
    Spool(#[from] SpoolError),
    #[error("{0}")]
    Fatal(String),
    #[error("invalid configuration: {0}")]
    Config(String),
}

pub(crate) fn log(msg: impl AsRef<str>) {
    eprintln!("akhet: {}", msg.as_ref());
}
