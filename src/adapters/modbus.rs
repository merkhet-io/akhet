//! Read-only Modbus TCP holding-register polling (function code 0x03).
//!
//! Each poll produces exactly one event: either the raw registers or an
//! explicit `read_failed` record. Failed reads never carry register values.
//! Polls are scheduled at a nominal interval with `MissedTickBehavior::Delay`
//! (no catch-up bursts); real poll instants are irregular and recorded with
//! collector timestamps. Connection attempts happen at most once per interval,
//! so an unreachable device cannot cause a busy loop.

use super::{AsyncSpool, CollectError, Stop, log};
use crate::event::{self, Event, EventData, ModbusOutcome, ModbusPoll};
use crate::faults::point;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio_modbus::client::{Context, Reader};
use tokio_modbus::prelude::*;

/// Modbus limit for one read-holding-registers request.
pub const MAX_REGISTERS_PER_READ: u16 = 125;

#[derive(Debug, Clone)]
pub struct ModbusConfig {
    pub host: String,
    pub port: u16,
    pub device_id: u8,
    pub start_address: u16,
    pub count: u16,
    pub interval: Duration,
    pub timeout: Duration,
    /// Stop after this many polls (successful or failed).
    pub limit: Option<u64>,
}

impl ModbusConfig {
    pub fn validate(&self) -> Result<(), CollectError> {
        let c = |m: String| Err(CollectError::Config(m));
        if self.count == 0 || self.count > MAX_REGISTERS_PER_READ {
            return c(format!("count must be 1..={MAX_REGISTERS_PER_READ}"));
        }
        if u32::from(self.start_address) + u32::from(self.count) > 65_536 {
            return c("address + count must not exceed 65536".into());
        }
        if self.interval < Duration::from_millis(10) {
            return c("interval must be at least 10 ms".into());
        }
        if self.timeout.is_zero() {
            return c("timeout must be positive".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default, serde::Serialize)]
pub struct ModbusSummary {
    pub polls: u64,
    pub ok: u64,
    pub failed: u64,
    pub connections: u64,
}

async fn resolve(host: &str, port: u16) -> std::io::Result<SocketAddr> {
    tokio::net::lookup_host((host, port))
        .await?
        .next()
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "host resolved to no address")
        })
}

struct Failure {
    kind: &'static str,
    message: String,
    disconnect: bool,
}

async fn poll_once(
    cfg: &ModbusConfig,
    ctx: &mut Option<Context>,
    connections: &mut u64,
) -> (Result<Vec<u16>, Failure>, bool) {
    let mut new_connection = false;
    if ctx.is_none() {
        let addr = match tokio::time::timeout(cfg.timeout, resolve(&cfg.host, cfg.port)).await {
            Ok(Ok(a)) => a,
            Ok(Err(e)) => {
                return (
                    Err(Failure {
                        kind: "resolve",
                        message: e.to_string(),
                        disconnect: true,
                    }),
                    false,
                );
            }
            Err(_) => {
                return (
                    Err(Failure {
                        kind: "timeout",
                        message: "address resolution timed out".into(),
                        disconnect: true,
                    }),
                    false,
                );
            }
        };
        let slave = Slave(cfg.device_id);
        match tokio::time::timeout(cfg.timeout, tcp::connect_slave(addr, slave)).await {
            Ok(Ok(c)) => {
                *ctx = Some(c);
                *connections += 1;
                new_connection = true;
            }
            Ok(Err(e)) => {
                return (
                    Err(Failure {
                        kind: "connect",
                        message: e.to_string(),
                        disconnect: true,
                    }),
                    false,
                );
            }
            Err(_) => {
                return (
                    Err(Failure {
                        kind: "timeout",
                        message: format!("connect timed out after {} ms", cfg.timeout.as_millis()),
                        disconnect: true,
                    }),
                    false,
                );
            }
        }
    }
    let c = ctx.as_mut().expect("connected");
    let r = tokio::time::timeout(
        cfg.timeout,
        c.read_holding_registers(cfg.start_address, cfg.count),
    )
    .await;
    let r = match r {
        Err(_) => Err(Failure {
            kind: "timeout",
            message: format!("read timed out after {} ms", cfg.timeout.as_millis()),
            disconnect: true,
        }),
        Ok(Err(e)) => Err(Failure {
            kind: "transport",
            message: e.to_string(),
            disconnect: true,
        }),
        Ok(Ok(Err(exc))) => Err(Failure {
            kind: "exception",
            message: format!("Modbus exception: {exc}"),
            disconnect: false,
        }),
        Ok(Ok(Ok(regs))) if regs.len() != usize::from(cfg.count) => Err(Failure {
            kind: "response_length",
            message: format!("expected {} registers, got {}", cfg.count, regs.len()),
            disconnect: true,
        }),
        Ok(Ok(Ok(regs))) => Ok(regs),
    };
    (r, new_connection)
}

pub async fn run(
    cfg: ModbusConfig,
    spool: AsyncSpool,
    stop: Stop,
) -> Result<ModbusSummary, CollectError> {
    cfg.validate()?;
    let mut summary = ModbusSummary::default();
    let mut ctx: Option<Context> = None;
    let mut ticker = tokio::time::interval(cfg.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let endpoint = format!("{}:{}", cfg.host, cfg.port);
    let mut consecutive_failures = 0u64;
    loop {
        if cfg.limit.is_some_and(|l| summary.polls >= l) {
            break;
        }
        tokio::select! {
            biased;
            _ = stop.wait() => break,
            _ = ticker.tick() => {}
        }
        let started_unix_ns = event::now_unix_ns();
        let t0 = Instant::now();
        let (result, new_connection) = poll_once(&cfg, &mut ctx, &mut summary.connections).await;
        let duration_ns = u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let completed_unix_ns = event::now_unix_ns();
        let outcome = match result {
            Ok(registers) => {
                summary.ok += 1;
                if consecutive_failures > 0 {
                    log(format!(
                        "modbus: reads recovered after {consecutive_failures} failures"
                    ));
                }
                consecutive_failures = 0;
                ModbusOutcome::Ok { registers }
            }
            Err(f) => {
                summary.failed += 1;
                consecutive_failures += 1;
                if consecutive_failures == 1 || consecutive_failures.is_power_of_two() {
                    log(format!(
                        "modbus: read failed ({}): {}; recorded as read_failed (consecutive: {consecutive_failures})",
                        f.kind, f.message
                    ));
                }
                if f.disconnect {
                    // Drop the connection; the next tick reconnects.
                    ctx = None;
                }
                ModbusOutcome::ReadFailed {
                    error_kind: f.kind.to_string(),
                    message: f.message,
                }
            }
        };
        let poll = ModbusPoll {
            endpoint: endpoint.clone(),
            device_id: cfg.device_id,
            function: "read_holding_registers".into(),
            start_address: cfg.start_address,
            count: cfg.count,
            poll_index: summary.polls,
            nominal_interval_ms: u64::try_from(cfg.interval.as_millis()).unwrap_or(u64::MAX),
            started_unix_ns,
            completed_unix_ns,
            duration_ns,
            new_connection,
            outcome,
        };
        summary.polls += 1;
        let ev = Event::new(EventData::ModbusPoll(poll), completed_unix_ns);
        spool.append(ev, None).await?;
        spool.faults().hit(point::MODBUS_AFTER_COMMIT);
    }
    Ok(summary)
}
