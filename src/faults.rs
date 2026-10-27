//! Fault injection for crash testing.
//!
//! Faults are read from the `AKHET_FAULT` environment variable by the CLI only
//! (comma-separated). They exist to test process-crash behavior: `abort`
//! terminates the process immediately (no destructors, no buffered output, no
//! further SQLite or network activity), similar to `kill -9`. This is NOT a
//! power-failure simulation: the OS page cache survives a process abort.
//!
//! Supported entries:
//! - `abort:<point>`: abort when execution reaches `<point>`.
//! - `delay:<point>:<millis>`: sleep at `<point>` (slow-disk simulation).

use std::time::Duration;

/// Named points where faults can be injected.
pub mod point {
    /// MQTT message received, before the spool transaction starts.
    pub const MQTT_BEFORE_COMMIT: &str = "mqtt.before_commit";
    /// Spool append committed, before the MQTT acknowledgment is queued.
    pub const MQTT_AFTER_COMMIT: &str = "mqtt.after_commit";
    /// Before each spool commit in a collector (delay to simulate slow disks).
    pub const COLLECTOR_COMMIT: &str = "collector.commit";
    /// Modbus observation committed.
    pub const MODBUS_AFTER_COMMIT: &str = "modbus.after_commit";
    /// Replay: HTTP request about to be sent.
    pub const REPLAY_BEFORE_SEND: &str = "replay.before_send";
    /// Replay: receiver returned success, local checkpoint not yet advanced.
    pub const REPLAY_AFTER_REMOTE_SUCCESS: &str = "replay.after_remote_success";
    /// Replay: local checkpoint advanced.
    pub const REPLAY_AFTER_CHECKPOINT: &str = "replay.after_checkpoint";
    /// Receiver: event committed, response not yet sent.
    pub const RECEIVER_AFTER_COMMIT: &str = "receiver.after_commit";
}

#[derive(Debug, Clone, Default)]
pub struct Faults {
    aborts: Vec<String>,
    delays: Vec<(String, Duration)>,
}

impl Faults {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut f = Faults::default();
        for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let parts: Vec<&str> = item.split(':').collect();
            match parts.as_slice() {
                ["abort", p] => f.aborts.push((*p).to_string()),
                ["delay", p, ms] => {
                    let ms: u64 = ms.parse().map_err(|_| format!("bad delay in {item:?}"))?;
                    f.delays.push(((*p).to_string(), Duration::from_millis(ms)));
                }
                _ => return Err(format!("unrecognized fault {item:?}")),
            }
        }
        Ok(f)
    }

    pub fn from_env() -> Result<Self, String> {
        match std::env::var("AKHET_FAULT") {
            Ok(s) => Self::parse(&s),
            Err(_) => Ok(Self::none()),
        }
    }

    pub fn with_delay(mut self, point: &str, d: Duration) -> Self {
        self.delays.push((point.to_string(), d));
        self
    }

    /// Abort the process if an abort fault is set for `point`.
    pub fn hit(&self, point: &str) {
        if self.aborts.iter().any(|p| p == point) {
            eprintln!("akhet: fault injection: abort at {point}");
            std::process::abort();
        }
    }

    /// Blocking delay configured for `point`, if any (call from blocking code).
    pub fn delay_blocking(&self, point: &str) {
        for (p, d) in &self.delays {
            if p == point {
                std::thread::sleep(*d);
            }
        }
    }
}
