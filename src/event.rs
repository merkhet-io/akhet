//! Versioned event format.
//!
//! Every spooled record is one JSON object encoded once at capture time. The
//! encoded bytes are what gets checksummed, stored and replayed; they are never
//! re-encoded, so the receiver sees exactly the bytes the collector committed.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// Event format version written by this build.
pub const EVENT_FORMAT_VERSION: u32 = 1;
/// Event format versions this build can read.
pub const SUPPORTED_EVENT_FORMAT_VERSIONS: &[u32] = &[1];

/// One captured observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub format_version: u32,
    /// Stable identity of this record. Generated once at capture and never
    /// changed by retries; used as the HTTP idempotency key.
    pub event_id: String,
    /// Collector wall clock (UTC, ns since Unix epoch) when the observation was
    /// received or the poll completed. Not a sensor sampling instant.
    pub collected_unix_ns: i64,
    /// Present only when the operator configured how the source identifies
    /// its own messages (see `SourceIdentity`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_identity: Option<SourceIdentity>,
    #[serde(flatten)]
    pub data: EventData,
}

/// An identity defined by the source, with explicitly configured semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Operator-chosen namespace stating what the key means.
    pub namespace: String,
    /// The key extracted from the source message.
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventData {
    MqttMessage(MqttMessage),
    ModbusPoll(ModbusPoll),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MqttMessage {
    /// Broker address as configured (`host:port`).
    pub broker: String,
    pub client_id: String,
    pub subscription: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    /// DUP flag as received. Indicates a redelivery attempt by the broker; it
    /// is not a reliable duplicate detector.
    pub dup: bool,
    /// Raw payload bytes, base64 (standard alphabet, padded).
    pub payload_base64: String,
    pub payload_len: usize,
    /// Set when a source identity was configured but could not be extracted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_identity_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModbusPoll {
    /// `host:port` as configured.
    pub endpoint: String,
    /// Modbus unit identifier.
    pub device_id: u8,
    /// Always `read_holding_registers` (function code 0x03).
    pub function: String,
    /// Zero-based protocol address of the first register.
    pub start_address: u16,
    pub count: u16,
    /// Poll counter within this collector run (starts at 0).
    pub poll_index: u64,
    /// Configured nominal interval. Actual poll instants are irregular.
    pub nominal_interval_ms: u64,
    pub started_unix_ns: i64,
    pub completed_unix_ns: i64,
    /// Monotonic duration of connect (if needed) plus the read request.
    pub duration_ns: u64,
    /// True when this poll had to open a new TCP connection first (its
    /// duration then includes connection setup).
    pub new_connection: bool,
    #[serde(flatten)]
    pub outcome: ModbusOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ModbusOutcome {
    /// Raw unsigned 16-bit register values, no scaling or interpretation.
    Ok { registers: Vec<u16> },
    /// The read failed. There are deliberately no register values.
    ReadFailed { error_kind: String, message: String },
}

impl Event {
    pub fn new(data: EventData, collected_unix_ns: i64) -> Self {
        Event {
            format_version: EVENT_FORMAT_VERSION,
            event_id: uuid::Uuid::new_v4().to_string(),
            collected_unix_ns,
            source_identity: None,
            data,
        }
    }

    /// Encode to the stored byte form.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("event serialization cannot fail")
    }
}

/// Minimal header used to validate a body before full decoding.
#[derive(Deserialize)]
struct Header {
    format_version: u32,
    event_id: String,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FormatError {
    #[error("event body is not a valid event object: {0}")]
    Malformed(String),
    #[error(
        "unsupported event format_version {0} (supported: {SUPPORTED_EVENT_FORMAT_VERSIONS:?})"
    )]
    UnsupportedVersion(u32),
}

/// Validate the format version and return the event_id of an encoded body.
pub fn check_body(body: &[u8]) -> Result<String, FormatError> {
    let h: Header =
        serde_json::from_slice(body).map_err(|e| FormatError::Malformed(e.to_string()))?;
    if !SUPPORTED_EVENT_FORMAT_VERSIONS.contains(&h.format_version) {
        return Err(FormatError::UnsupportedVersion(h.format_version));
    }
    if h.event_id.is_empty() || h.event_id.len() > 128 {
        return Err(FormatError::Malformed(
            "event_id must be 1..=128 bytes".into(),
        ));
    }
    Ok(h.event_id)
}

/// Fully decode an encoded body, checking the version first.
pub fn decode(body: &[u8]) -> Result<Event, FormatError> {
    check_body(body)?;
    serde_json::from_slice(body).map_err(|e| FormatError::Malformed(e.to_string()))
}

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(sha256(bytes))
}

pub fn now_unix_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Event {
        Event::new(
            EventData::ModbusPoll(ModbusPoll {
                endpoint: "127.0.0.1:502".into(),
                device_id: 1,
                function: "read_holding_registers".into(),
                start_address: 0,
                count: 2,
                poll_index: 0,
                nominal_interval_ms: 1000,
                started_unix_ns: 1,
                completed_unix_ns: 2,
                duration_ns: 1,
                new_connection: true,
                outcome: ModbusOutcome::Ok {
                    registers: vec![0, 65535],
                },
            }),
            2,
        )
    }

    #[test]
    fn roundtrip_and_version_check() {
        let e = sample();
        let b = e.encode();
        assert_eq!(decode(&b).unwrap(), e);
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["kind"], "modbus_poll");
        assert_eq!(v["status"], "ok");
        let mut v2 = v.clone();
        v2["format_version"] = 2.into();
        assert_eq!(
            check_body(&serde_json::to_vec(&v2).unwrap()),
            Err(FormatError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn failed_read_has_no_registers() {
        let mut e = sample();
        if let EventData::ModbusPoll(p) = &mut e.data {
            p.outcome = ModbusOutcome::ReadFailed {
                error_kind: "timeout".into(),
                message: "x".into(),
            };
        }
        let v: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        assert_eq!(v["status"], "read_failed");
        assert!(v.get("registers").is_none());
    }
}
