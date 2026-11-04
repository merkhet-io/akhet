//! Versioned event format.
//!
//! Every spooled record is encoded once at capture time. The encoded bytes are
//! what gets checksummed, stored and replayed; they are never re-encoded, so
//! the receiver sees exactly the bytes the collector committed.
//!
//! - Format version 1: one compact JSON object. MQTT payloads are carried as
//!   `payload_base64`.
//! - Format version 2 (MQTT messages only): the same JSON object without
//!   `payload_base64`, then one `\n`, then the raw payload bytes
//!   (`payload_len` of them). Compact JSON never contains a raw newline, so the
//!   first `\n` ends the header. This avoids the 4/3 base64 inflation.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// Event format version written by this build unless raw payload storage is
/// chosen.
pub const EVENT_FORMAT_VERSION: u32 = 1;
/// Format version of an MQTT event whose payload follows the JSON header as
/// raw bytes.
pub const RAW_PAYLOAD_FORMAT_VERSION: u32 = 2;
/// Event format versions this build can read.
pub const SUPPORTED_EVENT_FORMAT_VERSIONS: &[u32] = &[1, 2];

/// How the collector stores MQTT payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadStorage {
    /// Format version 1: base64 inside the JSON object (readable by every
    /// Akhet build and by any JSON consumer).
    #[default]
    Base64,
    /// Format version 2: raw bytes after the JSON header.
    Raw,
}

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
    /// First configured topic filter (in configuration order) that matches
    /// `topic`; empty when none does (e.g. a message queued by the broker for
    /// a filter that was removed from the configuration).
    pub subscription: String,
    /// Every configured topic filter that matches `topic`, in configuration
    /// order. Added within format version 1: absent in older records, where
    /// `subscription` was the single configured filter.
    #[serde(default)]
    pub subscriptions: Vec<String>,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    /// DUP flag as received. Indicates a redelivery attempt by the broker; it
    /// is not a reliable duplicate detector.
    pub dup: bool,
    /// Raw payload bytes, base64 (standard alphabet, padded). Always present
    /// in format version 1, absent in format version 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    /// Format version 2: the payload bytes stored after the header. Never
    /// part of the JSON object.
    #[serde(skip)]
    pub payload_raw: Option<Vec<u8>>,
    pub payload_len: usize,
    /// Set when a source identity was configured but could not be extracted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_identity_error: Option<String>,
}

impl MqttMessage {
    /// Attach the payload in the given storage form.
    pub fn set_payload(&mut self, payload: &[u8], storage: PayloadStorage) {
        self.payload_len = payload.len();
        match storage {
            PayloadStorage::Base64 => {
                self.payload_base64 =
                    Some(base64::engine::general_purpose::STANDARD.encode(payload));
                self.payload_raw = None;
            }
            PayloadStorage::Raw => {
                self.payload_base64 = None;
                self.payload_raw = Some(payload.to_vec());
            }
        }
    }

    /// The payload bytes, whichever way they were stored.
    pub fn payload(&self) -> Result<std::borrow::Cow<'_, [u8]>, String> {
        if let Some(raw) = &self.payload_raw {
            return Ok(std::borrow::Cow::Borrowed(raw));
        }
        let b64 = self
            .payload_base64
            .as_deref()
            .ok_or_else(|| "MQTT event carries no payload".to_string())?;
        base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map(std::borrow::Cow::Owned)
            .map_err(|e| format!("payload_base64 is not valid base64: {e}"))
    }
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

    /// Encode to the stored byte form. An MQTT message with a raw payload is
    /// written as format version 2 (header, `\n`, payload bytes).
    pub fn encode(&self) -> Vec<u8> {
        if let EventData::MqttMessage(MqttMessage {
            payload_raw: Some(raw),
            ..
        }) = &self.data
        {
            let mut header = self.clone();
            header.format_version = RAW_PAYLOAD_FORMAT_VERSION;
            let mut out = serde_json::to_vec(&header).expect("event serialization cannot fail");
            out.push(b'\n');
            out.extend_from_slice(raw);
            return out;
        }
        if self.format_version == RAW_PAYLOAD_FORMAT_VERSION {
            let mut v1 = self.clone();
            v1.format_version = EVENT_FORMAT_VERSION;
            return serde_json::to_vec(&v1).expect("event serialization cannot fail");
        }
        serde_json::to_vec(self).expect("event serialization cannot fail")
    }
}

/// Minimal header used to validate a body before full decoding.
#[derive(Deserialize)]
struct Header {
    format_version: u32,
    event_id: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    payload_len: Option<usize>,
    #[serde(default)]
    payload_base64: Option<serde::de::IgnoredAny>,
}

/// Split a stored body into its JSON header and, for format version 2, the
/// raw payload that follows the first `\n`.
pub fn split_body(body: &[u8]) -> (&[u8], Option<&[u8]>) {
    match body.iter().position(|b| *b == b'\n') {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    }
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
    check_body_version(body).map(|(id, _)| id)
}

/// Like [`check_body`], also returning the body's format version.
pub fn check_body_version(body: &[u8]) -> Result<(String, u32), FormatError> {
    let (header, tail) = split_body(body);
    let h: Header =
        serde_json::from_slice(header).map_err(|e| FormatError::Malformed(e.to_string()))?;
    if !SUPPORTED_EVENT_FORMAT_VERSIONS.contains(&h.format_version) {
        return Err(FormatError::UnsupportedVersion(h.format_version));
    }
    match (h.format_version, tail) {
        (RAW_PAYLOAD_FORMAT_VERSION, Some(raw)) => {
            if h.kind.as_deref() != Some("mqtt_message") {
                return Err(FormatError::Malformed(
                    "format_version 2 is only defined for mqtt_message".into(),
                ));
            }
            if h.payload_base64.is_some() {
                return Err(FormatError::Malformed(
                    "format_version 2 must not carry payload_base64".into(),
                ));
            }
            if h.payload_len != Some(raw.len()) {
                return Err(FormatError::Malformed(format!(
                    "payload_len {:?} does not match the {} payload bytes after the header",
                    h.payload_len,
                    raw.len()
                )));
            }
        }
        (RAW_PAYLOAD_FORMAT_VERSION, None) => {
            return Err(FormatError::Malformed(
                "format_version 2 needs a newline and the raw payload after the header".into(),
            ));
        }
        (_, Some(_)) => {
            return Err(FormatError::Malformed(format!(
                "format_version {} is a single JSON object without trailing data",
                h.format_version
            )));
        }
        (_, None) => {}
    }
    if h.event_id.is_empty() || h.event_id.len() > 128 {
        return Err(FormatError::Malformed(
            "event_id must be 1..=128 bytes".into(),
        ));
    }
    Ok((h.event_id, h.format_version))
}

/// Fully decode an encoded body, checking the version first.
pub fn decode(body: &[u8]) -> Result<Event, FormatError> {
    check_body(body)?;
    let (header, tail) = split_body(body);
    let mut ev: Event =
        serde_json::from_slice(header).map_err(|e| FormatError::Malformed(e.to_string()))?;
    if let EventData::MqttMessage(m) = &mut ev.data {
        match tail {
            Some(raw) => m.payload_raw = Some(raw.to_vec()),
            None if m.payload_base64.is_none() => {
                return Err(FormatError::Malformed(
                    "format_version 1 mqtt_message without payload_base64".into(),
                ));
            }
            None => {}
        }
    }
    Ok(ev)
}

/// The stored body as one JSON value for display: format version 2 bodies get
/// their payload re-attached as `payload_base64`, so `inspect` output has the
/// same shape for both versions. Not the stored bytes.
pub fn display_json(body: &[u8]) -> serde_json::Value {
    let (header, tail) = split_body(body);
    let mut v: serde_json::Value = serde_json::from_slice(header).unwrap_or_default();
    if let (Some(raw), Some(obj)) = (tail, v.as_object_mut()) {
        obj.insert(
            "payload_base64".into(),
            base64::engine::general_purpose::STANDARD.encode(raw).into(),
        );
    }
    v
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
        v2["format_version"] = 3.into();
        assert_eq!(
            check_body(&serde_json::to_vec(&v2).unwrap()),
            Err(FormatError::UnsupportedVersion(3))
        );
    }

    #[test]
    fn mqtt_record_without_subscriptions_still_decodes() {
        // Format-version-1 MQTT body written before `subscriptions` existed.
        let old = br#"{"format_version":1,"event_id":"e1","collected_unix_ns":1,"kind":"mqtt_message","broker":"h:1883","client_id":"c","subscription":"plant/#","topic":"plant/a","qos":1,"retain":false,"dup":false,"payload_base64":"","payload_len":0}"#;
        let e = decode(old).unwrap();
        let EventData::MqttMessage(m) = e.data else {
            panic!("not an MQTT event")
        };
        assert_eq!(m.subscription, "plant/#");
        assert!(m.subscriptions.is_empty());
    }

    fn mqtt(payload: &[u8], storage: PayloadStorage) -> Event {
        let mut m = MqttMessage {
            broker: "h:1883".into(),
            client_id: "c".into(),
            subscription: "plant/#".into(),
            subscriptions: vec!["plant/#".into()],
            topic: "plant/a".into(),
            qos: 1,
            retain: false,
            dup: false,
            payload_base64: None,
            payload_raw: None,
            payload_len: 0,
            source_identity_error: None,
        };
        m.set_payload(payload, storage);
        Event::new(EventData::MqttMessage(m), 1)
    }

    fn payload_of(e: &Event) -> Vec<u8> {
        let EventData::MqttMessage(m) = &e.data else {
            panic!("not an MQTT event")
        };
        m.payload().unwrap().into_owned()
    }

    #[test]
    fn raw_payload_roundtrips_bit_exact_as_version_2() {
        // Newlines, NUL, invalid UTF-8: the header ends at the first newline,
        // everything after it is payload.
        let mut payload: Vec<u8> = (0u8..=255).collect();
        payload.extend_from_slice(b"\n\n{\"x\":1}\n");
        let e = mqtt(&payload, PayloadStorage::Raw);
        let body = e.encode();
        let (header, tail) = split_body(&body);
        assert_eq!(tail, Some(&payload[..]));
        let h: serde_json::Value = serde_json::from_slice(header).unwrap();
        assert_eq!(h["format_version"], 2);
        assert_eq!(h["payload_len"], payload.len());
        assert!(h.get("payload_base64").is_none());
        assert_eq!(check_body_version(&body).unwrap().1, 2);
        let d = decode(&body).unwrap();
        assert_eq!(payload_of(&d), payload);
        // Re-encoding the decoded event reproduces the stored bytes.
        assert_eq!(d.encode(), body);
        // Display form carries the payload as base64, like version 1.
        assert_eq!(
            display_json(&body)["payload_base64"],
            base64::engine::general_purpose::STANDARD.encode(&payload)
        );
    }

    #[test]
    fn raw_and_base64_store_the_same_payload() {
        let payload = br#"{"t":[1,2,3],"v":[-32768,0,32767]}"#.repeat(200);
        let b64 = mqtt(&payload, PayloadStorage::Base64).encode();
        let raw = mqtt(&payload, PayloadStorage::Raw).encode();
        assert_eq!(check_body_version(&b64).unwrap().1, 1);
        assert_eq!(payload_of(&decode(&b64).unwrap()), payload);
        assert_eq!(payload_of(&decode(&raw).unwrap()), payload);
        // Header overhead is the same; only the payload shrinks from 4/3.
        assert!(
            raw.len() * 4 < b64.len() * 3 + 400,
            "{} vs {}",
            raw.len(),
            b64.len()
        );
        // Switching a decoded version 2 event back to base64 writes version 1.
        let mut back = decode(&raw).unwrap();
        if let EventData::MqttMessage(m) = &mut back.data {
            m.set_payload(&payload, PayloadStorage::Base64);
        }
        assert_eq!(check_body_version(&back.encode()).unwrap().1, 1);
        // Empty payloads are allowed in both versions.
        let empty = mqtt(b"", PayloadStorage::Raw).encode();
        assert!(empty.ends_with(b"\n"));
        assert_eq!(payload_of(&decode(&empty).unwrap()), b"");
    }

    #[test]
    fn malformed_version_2_bodies_are_rejected() {
        let good = mqtt(b"abc", PayloadStorage::Raw).encode();
        let (header, _) = split_body(&good);
        let malformed = |b: &[u8]| matches!(check_body(b), Err(FormatError::Malformed(_)));
        // Version 2 without the payload part.
        assert!(malformed(header));
        // Payload length differs from payload_len.
        let mut long = good.clone();
        long.push(b'x');
        assert!(malformed(&long));
        assert!(malformed(&good[..good.len() - 1]));
        // Version 1 with trailing data.
        let v1 = mqtt(b"abc", PayloadStorage::Base64).encode();
        let mut v1_tail = v1.clone();
        v1_tail.extend_from_slice(b"\nabc");
        assert!(malformed(&v1_tail));
        // Version 2 is only defined for MQTT messages.
        let mut modbus: serde_json::Value = serde_json::from_slice(&sample().encode()).unwrap();
        modbus["format_version"] = 2.into();
        let mut b = serde_json::to_vec(&modbus).unwrap();
        b.push(b'\n');
        assert!(malformed(&b));
        // Version 2 must not also carry payload_base64.
        let mut h: serde_json::Value = serde_json::from_slice(header).unwrap();
        h["payload_base64"] = "YWJj".into();
        let mut b = serde_json::to_vec(&h).unwrap();
        b.extend_from_slice(b"\nabc");
        assert!(malformed(&b));
        // A version 1 MQTT record must carry its payload.
        let mut v: serde_json::Value = serde_json::from_slice(&v1).unwrap();
        v.as_object_mut().unwrap().remove("payload_base64");
        assert!(matches!(
            decode(&serde_json::to_vec(&v).unwrap()),
            Err(FormatError::Malformed(_))
        ));
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
