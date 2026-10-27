//! Acquisition adapters (MQTT, Modbus TCP) and the shared async plumbing.

pub mod modbus;
pub mod mqtt;

use crate::event::Event;
use crate::faults::Faults;
use crate::spool::{AppendOutcome, SourceClaim, Spool, SpoolError};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Spool shared by async tasks. All SQLite work runs on the blocking pool so
/// fsync never stalls the async runtime.
#[derive(Clone)]
pub struct AsyncSpool {
    inner: Arc<Mutex<Spool>>,
    faults: Faults,
}

impl AsyncSpool {
    pub fn new(spool: Spool, faults: Faults) -> Self {
        AsyncSpool {
            inner: Arc::new(Mutex::new(spool)),
            faults,
        }
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
