//! MQTT 3.1.1 acquisition with acknowledgment after durable commit.
//!
//! Library: rumqttc 0.25 with `manual_acks=true`. Verified from its source
//! (`state.rs::handle_incoming_publish`): with manual acks, an incoming QoS 1
//! PUBLISH produces no PUBACK until `AsyncClient::try_ack`/`ack` enqueues one.
//! On connection loss `EventLoop::clean()` discards queued, unsent PUBACKs so
//! they are not sent on the next connection. The next `poll()` reconnects.
//!
//! Pipeline (all queues bounded except the ack-notice channel, whose length is
//! bounded by the number of unacknowledged QoS 1 messages, i.e. by the broker's
//! in-flight window and at most 65535 packet ids):
//!
//! ```text
//! poll task --(bounded queue)--> committer --(notices)--> ack task --> request channel --> poll task
//! ```
//!
//! - The poll task owns the rumqttc `EventLoop`. When the queue is full it
//!   stops reading the socket (TCP backpressure to the broker) and counts a
//!   saturation event.
//! - The committer (group commit) takes the message it waited for plus
//!   whatever else is already queued, up to `max_batch` messages (optionally
//!   waiting up to `max_linger` for more), appends them in ONE SQLite
//!   transaction on the blocking pool and only after that commit emits their
//!   ack notices, in arrival order. If the transaction fails, nothing from the
//!   batch is acknowledged. At low rates batches have one message, so latency
//!   is unchanged with the default `max_linger` of 0.
//! - The ack task enqueues a PUBACK only if the message arrived on the current
//!   connection (generation check under a mutex shared with the poll task, which
//!   bumps the generation and purges stale queued PUBACKs on disconnect). Packet
//!   ids are only meaningful within one connection/session.
//!
//! Subscriptions: all configured filters go into one SUBSCRIBE. With an
//! existing session only the difference to the filter set recorded in the
//! spool is (un)subscribed, see [`SubscriptionRecord`].
//!
//! Delivery: at-least-once into the spool, provided the broker keeps the
//! persistent session (clean_session=false) and its queued messages. A crash or
//! disconnect after commit but before the PUBACK reaches the broker causes
//! redelivery and a second local event with a new event_id, unless a source
//! identity is configured.

use super::{AsyncSpool, CollectError, Stop, log};
use crate::event::{self, Event, EventData, MqttMessage, SourceIdentity};
use crate::faults::point;
use crate::spool::{AppendOutcome, SourceClaim};
use base64::Engine as _;
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event as MqEvent, EventLoop, MqttOptions,
    Outgoing, Packet, Publish, QoS, StateError, SubscribeFilter, SubscribeReasonCode,
    TlsConfiguration, Transport,
};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Largest MQTT topic length (UTF-8 string with a 16-bit length prefix).
const MAX_TOPIC_BYTES: usize = 65_535;

#[derive(Debug, Clone)]
pub struct TlsSettings {
    /// PEM CA bundle. When absent, the platform trust store is used.
    pub ca_file: Option<PathBuf>,
    /// Optional PEM client certificate chain and private key (mutual TLS).
    pub client_cert: Option<(PathBuf, PathBuf)>,
}

/// Optional, explicitly configured source identity.
///
/// Meaning: "the publisher guarantees that within `namespace`, the pair
/// (topic, value at `json_pointer` in the JSON payload) identifies exactly one
/// logical message, and republishes of that message carry identical payload
/// bytes". With this, upstream redelivery of an already committed message is
/// recognized and acknowledged without storing a second event, and reuse of
/// the identity for different bytes is rejected as a conflict.
#[derive(Debug, Clone)]
pub struct SourceIdentityConfig {
    pub namespace: String,
    pub json_pointer: String,
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    /// Topic filters, subscribed with QoS 1 in one SUBSCRIBE. Order matters
    /// only for the `subscription` field of events (first matching filter).
    pub topics: Vec<String>,
    pub tls: Option<TlsSettings>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub max_payload_bytes: usize,
    pub queue_capacity: usize,
    /// Most messages committed in one spool transaction (group commit).
    /// 1 commits every message in its own transaction.
    pub max_batch: usize,
    /// After the first message of a batch, wait up to this long for more
    /// (until `max_batch`). Zero only takes what is already queued.
    pub max_linger: Duration,
    pub source_identity: Option<SourceIdentityConfig>,
    /// Stop (gracefully) after this many messages were committed or
    /// recognized as source duplicates.
    pub limit: Option<u64>,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    /// Maximum time to flush acknowledgments and disconnect on shutdown.
    pub shutdown_grace: Duration,
    /// Notified each time every configured filter is known to be subscribed
    /// (SUBACK with QoS 1 for each filter requested on this connection, or
    /// CONNACK with an existing session already holding them). Removals of
    /// filters no longer configured may still be in progress. Used by tests
    /// and embedders to avoid racing the subscription.
    pub ready: Option<Arc<tokio::sync::Notify>>,
}

impl MqttConfig {
    pub fn new(host: &str, port: u16, client_id: &str, topic: &str) -> Self {
        Self::with_topics(host, port, client_id, vec![topic.to_string()])
    }

    pub fn with_topics(host: &str, port: u16, client_id: &str, topics: Vec<String>) -> Self {
        MqttConfig {
            host: host.into(),
            port,
            client_id: client_id.into(),
            topics,
            tls: None,
            username: None,
            password: None,
            keep_alive: Duration::from_secs(30),
            max_payload_bytes: 256 * 1024,
            queue_capacity: 64,
            max_batch: 64,
            max_linger: Duration::ZERO,
            source_identity: None,
            limit: None,
            reconnect_min: Duration::from_millis(200),
            reconnect_max: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(10),
            ready: None,
        }
    }

    pub fn validate(&self, max_event_bytes: usize) -> Result<(), CollectError> {
        let c = |m: &str| Err(CollectError::Config(m.to_string()));
        if self.client_id.is_empty() || self.client_id.len() > 256 {
            return c("client id must be 1..=256 bytes (persistent sessions need a stable id)");
        }
        if self.topics.is_empty() {
            return c("at least one topic filter is required");
        }
        let mut seen = BTreeSet::new();
        for t in &self.topics {
            validate_filter(t)
                .map_err(|m| CollectError::Config(format!("topic filter {t:?}: {m}")))?;
            if !seen.insert(t.as_str()) {
                return Err(CollectError::Config(format!(
                    "duplicate topic filter {t:?}"
                )));
            }
        }
        if self.queue_capacity == 0 || self.queue_capacity > 65_535 {
            return c("queue capacity must be 1..=65535");
        }
        if self.max_batch == 0 || self.max_batch > 65_535 {
            return c("max batch must be 1..=65535");
        }
        if self.max_linger > Duration::from_secs(10) {
            return c("max linger must be at most 10 s");
        }
        if self.max_payload_bytes == 0 || self.max_payload_bytes > 64 * 1024 * 1024 {
            return c("max payload bytes must be 1..=67108864");
        }
        // base64 expansion plus topic, matching filters (`subscription` and
        // `subscriptions`) and JSON envelope must fit an event.
        let filter_bytes: usize = self.topics.iter().map(|t| t.len() + 3).sum();
        let worst =
            self.max_payload_bytes.div_ceil(3) * 4 + MAX_TOPIC_BYTES * 2 + filter_bytes * 4 + 4096;
        if worst > max_event_bytes {
            return Err(CollectError::Config(format!(
                "--max-payload-bytes {} can encode to {worst} bytes, above --max-event-bytes {max_event_bytes}",
                self.max_payload_bytes
            )));
        }
        if let Some(si) = &self.source_identity {
            if si.namespace.is_empty() || si.namespace.contains('\u{1f}') {
                return c("source identity namespace must be non-empty");
            }
            if !si.json_pointer.starts_with('/') {
                return c("source identity JSON pointer must start with '/'");
            }
        }
        if self.reconnect_min.is_zero() || self.reconnect_max < self.reconnect_min {
            return c("reconnect backoff must be positive and min <= max");
        }
        Ok(())
    }
}

/// Check an MQTT 3.1.1 topic filter (section 4.7): non-empty, at most 65535
/// bytes, no NUL, `#` only as the whole last level, `+` only as a whole level.
pub fn validate_filter(filter: &str) -> Result<(), &'static str> {
    if filter.is_empty() {
        return Err("must be non-empty");
    }
    if filter.len() > MAX_TOPIC_BYTES {
        return Err("longer than 65535 bytes");
    }
    if filter.contains('\0') {
        return Err("contains NUL");
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || i + 1 != levels.len()) {
            return Err("'#' must be the whole last level");
        }
        if level.contains('+') && *level != "+" {
            return Err("'+' must be a whole level");
        }
    }
    Ok(())
}

/// MQTT 3.1.1 topic matching (section 4.7): `+` matches exactly one level,
/// a trailing `#` matches the parent level and any number of child levels.
/// Filters starting with a wildcard do not match topics starting with `$`.
/// `filter` must be valid (see [`validate_filter`]).
pub fn filter_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Counters reported at shutdown.
#[derive(Debug, Default, serde::Serialize)]
pub struct MqttSummary {
    pub received: u64,
    pub committed: u64,
    pub source_duplicates: u64,
    /// Spool transactions used (group commit: one per batch).
    pub commit_transactions: u64,
    /// Largest number of messages committed in one transaction.
    pub max_batch_committed: u64,
    pub acks_enqueued: u64,
    pub acks_skipped_after_reconnect: u64,
    pub queue_saturation_events: u64,
    pub dropped_unacknowledged_on_shutdown: u64,
    pub connections: u64,
    pub connection_errors: u64,
    pub qos0_messages: u64,
    /// Messages whose topic matches none of the configured filters (e.g.
    /// queued by the broker for a filter that has since been removed).
    pub unmatched_messages: u64,
}

// Shared with the metrics endpoint when one is configured.
use crate::metrics::MqttCounters as Counters;

impl Counters {
    fn summary(&self) -> MqttSummary {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        MqttSummary {
            received: g(&self.received),
            committed: g(&self.committed),
            source_duplicates: g(&self.source_duplicates),
            commit_transactions: g(&self.transactions),
            max_batch_committed: g(&self.max_batch),
            acks_enqueued: g(&self.acks_enqueued),
            acks_skipped_after_reconnect: g(&self.acks_skipped),
            queue_saturation_events: g(&self.saturation),
            dropped_unacknowledged_on_shutdown: g(&self.dropped),
            connections: g(&self.connections),
            connection_errors: g(&self.connection_errors),
            qos0_messages: g(&self.qos0),
            unmatched_messages: g(&self.unmatched),
        }
    }
}

struct Received {
    publish: Publish,
    generation: u64,
    received_unix_ns: i64,
}

struct AckNotice {
    publish: Publish,
    generation: u64,
}

fn build_tls(t: &TlsSettings) -> Result<TlsConfiguration, CollectError> {
    let client = t
        .client_cert
        .as_ref()
        .map(|(c, k)| (c.as_path(), k.as_path()));
    let config =
        crate::tls::client_config(t.ca_file.as_deref(), client).map_err(CollectError::Config)?;
    Ok(TlsConfiguration::Rustls(Arc::new(config)))
}

fn options(cfg: &MqttConfig) -> Result<MqttOptions, CollectError> {
    let mut o = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    // Outgoing: the one SUBSCRIBE carrying every filter (2 + len + 1 each,
    // plus packet id and fixed header) must fit.
    let subscribe_bytes: usize = cfg.topics.iter().map(|t| t.len() + 3).sum::<usize>() + 16;
    o.set_clean_session(false)
        .set_manual_acks(true)
        .set_keep_alive(cfg.keep_alive)
        // Packet = topic (2 + up to 65535) + packet id (2) + payload.
        .set_max_packet_size(
            cfg.max_payload_bytes + MAX_TOPIC_BYTES + 4,
            subscribe_bytes.max(64 * 1024),
        );
    if let Some(user) = &cfg.username {
        o.set_credentials(user.clone(), cfg.password.clone().unwrap_or_default());
    }
    if let Some(t) = &cfg.tls {
        o.set_transport(Transport::tls_with_config(build_tls(t)?));
    }
    Ok(o)
}

/// Extract the configured source identity from a payload.
fn source_identity(
    si: &SourceIdentityConfig,
    topic: &str,
    payload: &[u8],
) -> Result<(SourceIdentity, SourceClaim), String> {
    let v: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("payload is not JSON: {e}"))?;
    let key = match v.pointer(&si.json_pointer) {
        Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(_) => {
            return Err(format!(
                "{} is not a non-empty string or number",
                si.json_pointer
            ));
        }
        None => return Err(format!("{} not present in payload", si.json_pointer)),
    };
    if key.len() > 512 {
        return Err("source identity longer than 512 bytes".into());
    }
    let mut content = Vec::with_capacity(topic.len() + 1 + payload.len());
    content.extend_from_slice(topic.as_bytes());
    content.push(0);
    content.extend_from_slice(payload);
    let claim = SourceClaim {
        key: format!("mqtt\u{1f}{}\u{1f}{}\u{1f}{}", si.namespace, topic, key),
        content_sha256: event::sha256(&content),
    };
    Ok((
        SourceIdentity {
            namespace: si.namespace.clone(),
            key,
        },
        claim,
    ))
}

fn to_event(cfg: &MqttConfig, r: &Received) -> (Event, Option<SourceClaim>) {
    let p = &r.publish;
    let subscriptions: Vec<String> = cfg
        .topics
        .iter()
        .filter(|f| filter_matches(f, &p.topic))
        .cloned()
        .collect();
    let mut msg = MqttMessage {
        broker: format!("{}:{}", cfg.host, cfg.port),
        client_id: cfg.client_id.clone(),
        subscription: subscriptions.first().cloned().unwrap_or_default(),
        subscriptions,
        topic: p.topic.clone(),
        qos: p.qos as u8,
        retain: p.retain,
        dup: p.dup,
        payload_base64: base64::engine::general_purpose::STANDARD.encode(&p.payload),
        payload_len: p.payload.len(),
        source_identity_error: None,
    };
    let mut identity = None;
    let mut claim = None;
    if let Some(si) = &cfg.source_identity {
        match source_identity(si, &p.topic, &p.payload) {
            Ok((i, c)) => {
                identity = Some(i);
                claim = Some(c);
            }
            Err(e) => msg.source_identity_error = Some(e),
        }
    }
    let mut ev = Event::new(EventData::MqttMessage(msg), r.received_unix_ns);
    ev.source_identity = identity;
    (ev, claim)
}

/// Stable error-kind label for a connection error.
fn connection_error_kind(e: &ConnectionError) -> &'static str {
    match e {
        ConnectionError::Io(_) => "io",
        ConnectionError::Tls(_) => "tls",
        ConnectionError::NetworkTimeout | ConnectionError::FlushTimeout => "timeout",
        ConnectionError::ConnectionRefused(_) => "refused",
        ConnectionError::MqttState(_) | ConnectionError::NotConnAck(_) => "protocol",
        _ => "other",
    }
}

/// What the spool records about the broker session's subscriptions, stored
/// in the spool `meta` table under [`subscription_key`].
///
/// Invariant (kept by writing before and after every change): when the
/// broker reports an existing session, it holds every filter in `confirmed`
/// and no filter outside `possible`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionRecord {
    /// Filters the broker confirmed with a QoS 1 SUBACK.
    pub confirmed: BTreeSet<String>,
    /// Filters the session may hold (a SUBSCRIBE may have reached the
    /// broker, or an UNSUBSCRIBE may not have). Superset of `confirmed`.
    pub possible: BTreeSet<String>,
}

/// `meta` key for the subscription record of one broker address and client id.
pub fn subscription_key(cfg: &MqttConfig) -> String {
    format!(
        "mqtt_subscriptions {}",
        serde_json::json!([format!("{}:{}", cfg.host, cfg.port), cfg.client_id])
    )
}

/// Subscription changes to make after CONNACK.
#[derive(Debug, PartialEq, Eq)]
struct SubscriptionPlan {
    /// Filters to SUBSCRIBE (configuration order), in one packet.
    subscribe: Vec<String>,
    /// Filters to UNSUBSCRIBE.
    unsubscribe: Vec<String>,
    /// Record to store before sending any request.
    before: SubscriptionRecord,
    /// Record once every request was acknowledged (stored directly when
    /// there is nothing to request).
    after: SubscriptionRecord,
}

fn plan_subscriptions(
    topics: &[String],
    session_present: bool,
    known: Option<&SubscriptionRecord>,
) -> SubscriptionPlan {
    let target: BTreeSet<String> = topics.iter().cloned().collect();
    let after = SubscriptionRecord {
        confirmed: target.clone(),
        possible: target.clone(),
    };
    match known.filter(|_| session_present) {
        // No session at the broker: it holds nothing. Unknown session state:
        // subscribe to everything (idempotent; re-sends retained messages)
        // and remove nothing, since we do not know what to remove.
        None => SubscriptionPlan {
            subscribe: topics.to_vec(),
            unsubscribe: Vec::new(),
            before: SubscriptionRecord {
                confirmed: BTreeSet::new(),
                possible: target,
            },
            after,
        },
        Some(k) => SubscriptionPlan {
            subscribe: topics
                .iter()
                .filter(|t| !k.confirmed.contains(*t))
                .cloned()
                .collect(),
            unsubscribe: k.possible.difference(&target).cloned().collect(),
            before: SubscriptionRecord {
                confirmed: k.confirmed.intersection(&target).cloned().collect(),
                possible: k.possible.union(&target).cloned().collect(),
            },
            after,
        },
    }
}

/// Per-run subscription bookkeeping, driven by the poll loop.
///
/// The configured set is active once every configured filter is confirmed
/// (SUBACK, or already confirmed in an existing session). Removals finish
/// when their UNSUBACKs arrive; capture does not wait for them, and until
/// then the record keeps the removed filters as `possible` so that the next
/// start retries the UNSUBSCRIBE.
struct Subscriptions {
    spool: AsyncSpool,
    key: String,
    /// Record as last read from or written to the spool.
    known: Option<SubscriptionRecord>,
    /// Filters of the outstanding SUBSCRIBE, in request order.
    pending_subscribe: Option<Vec<String>>,
    /// Filters being unsubscribed (one UNSUBSCRIBE each) and how many
    /// UNSUBACKs are still outstanding.
    pending_unsubscribe: Vec<String>,
    unsubacks_outstanding: usize,
}

impl Subscriptions {
    async fn load(cfg: &MqttConfig, spool: AsyncSpool) -> Result<Self, CollectError> {
        let key = subscription_key(cfg);
        let known = match spool.meta_get(&key).await? {
            None => None,
            Some(v) => match serde_json::from_str::<SubscriptionRecord>(&v) {
                Ok(r) => Some(r),
                Err(e) => {
                    log(format!(
                        "mqtt: ignoring unreadable subscription record in spool ({e})"
                    ));
                    None
                }
            },
        };
        Ok(Subscriptions {
            spool,
            key,
            known,
            pending_subscribe: None,
            pending_unsubscribe: Vec::new(),
            unsubacks_outstanding: 0,
        })
    }

    async fn store(&mut self, rec: SubscriptionRecord) -> Result<(), CollectError> {
        if self.known.as_ref() == Some(&rec) {
            return Ok(());
        }
        let v = serde_json::to_string(&rec).expect("record serialization cannot fail");
        self.spool.meta_set(&self.key, &v).await?;
        self.known = Some(rec);
        Ok(())
    }

    /// Start the subscription changes for a new connection. Returns true when
    /// every configured filter is already held (nothing to subscribe).
    async fn on_connack(
        &mut self,
        cfg: &MqttConfig,
        client: &AsyncClient,
        session_present: bool,
    ) -> Result<bool, CollectError> {
        self.pending_subscribe = None;
        self.pending_unsubscribe.clear();
        self.unsubacks_outstanding = 0;
        if session_present && self.known.is_none() {
            log(
                "mqtt: the spool has no record of this session's subscriptions; subscribing to all \
                 configured filters (the broker re-sends retained messages for them)",
            );
        }
        let plan = plan_subscriptions(&cfg.topics, session_present, self.known.as_ref());
        if plan.subscribe.is_empty() && plan.unsubscribe.is_empty() {
            self.store(plan.after).await?;
            return Ok(true);
        }
        // Durable before the broker can act on any request.
        self.store(plan.before).await?;
        if !plan.unsubscribe.is_empty() {
            log(format!(
                "mqtt: unsubscribing from filters no longer configured: {:?}",
                plan.unsubscribe
            ));
        }
        for t in &plan.unsubscribe {
            client
                .try_unsubscribe(t.clone())
                .map_err(|e| CollectError::Fatal(format!("unsubscribe request failed: {e}")))?;
        }
        self.unsubacks_outstanding = plan.unsubscribe.len();
        self.pending_unsubscribe = plan.unsubscribe;
        if plan.subscribe.is_empty() {
            return Ok(true);
        }
        let filters = plan
            .subscribe
            .iter()
            .map(|t| SubscribeFilter::new(t.clone(), QoS::AtLeastOnce));
        client
            .try_subscribe_many(filters)
            .map_err(|e| CollectError::Fatal(format!("subscribe request failed: {e}")))?;
        self.pending_subscribe = Some(plan.subscribe);
        Ok(false)
    }

    /// Check every SUBACK return code. Returns true when the configured set
    /// became active.
    async fn on_suback(
        &mut self,
        cfg: &MqttConfig,
        return_codes: &[SubscribeReasonCode],
    ) -> Result<bool, CollectError> {
        let Some(filters) = self.pending_subscribe.take() else {
            log("mqtt: ignoring unexpected SUBACK");
            return Ok(false);
        };
        if return_codes.len() != filters.len() {
            return Err(CollectError::Fatal(format!(
                "broker answered {} SUBACK return codes for {} topic filters",
                return_codes.len(),
                filters.len()
            )));
        }
        for (code, filter) in return_codes.iter().zip(&filters) {
            if *code != SubscribeReasonCode::Success(QoS::AtLeastOnce) {
                return Err(CollectError::Fatal(format!(
                    "broker granted {code:?} for {filter:?}; QoS 1 is required for at-least-once capture"
                )));
            }
        }
        log(format!("mqtt: subscribed to {filters:?} with QoS 1"));
        let mut rec = self.known.clone().unwrap_or_default();
        rec.confirmed = cfg.topics.iter().cloned().collect();
        rec.possible.extend(rec.confirmed.iter().cloned());
        self.store(rec).await?;
        Ok(true)
    }

    async fn on_unsuback(&mut self) -> Result<(), CollectError> {
        if self.unsubacks_outstanding == 0 {
            log("mqtt: ignoring unexpected UNSUBACK");
            return Ok(());
        }
        self.unsubacks_outstanding -= 1;
        if self.unsubacks_outstanding > 0 {
            return Ok(());
        }
        // Without relying on UNSUBACK order: only record the removal once
        // every UNSUBSCRIBE of this connection was acknowledged.
        let removed = std::mem::take(&mut self.pending_unsubscribe);
        log(format!("mqtt: unsubscribed from {removed:?}"));
        let mut rec = self.known.clone().unwrap_or_default();
        for f in &removed {
            rec.possible.remove(f);
            rec.confirmed.remove(f);
        }
        self.store(rec).await
    }
}

fn fatal_connection_error(e: &ConnectionError) -> Option<String> {
    match e {
        ConnectionError::ConnectionRefused(code) => Some(match code {
            ConnectReturnCode::BadUserNamePassword | ConnectReturnCode::NotAuthorized => {
                format!(
                    "broker refused connection: {code:?} (check AKHET_MQTT_USERNAME/AKHET_MQTT_PASSWORD)"
                )
            }
            other => format!("broker refused connection: {other:?}"),
        }),
        ConnectionError::MqttState(StateError::Deserialization(
            rumqttc::mqttbytes::Error::PayloadSizeLimitExceeded(n),
        )) => Some(format!(
            "incoming MQTT packet of {n} bytes exceeds the configured limit; it was not acknowledged and \
             would be redelivered forever, so collection stops (raise --max-payload-bytes or fix the publisher)"
        )),
        _ => None,
    }
}

/// Run the MQTT collector until `stop` is triggered, `limit` is reached or a
/// fatal error occurs.
pub async fn run(
    cfg: MqttConfig,
    spool: AsyncSpool,
    max_event_bytes: usize,
    stop: Stop,
) -> Result<MqttSummary, CollectError> {
    cfg.validate(max_event_bytes)?;
    let opts = options(&cfg)?;
    // Request channel: subscribe, PUBACKs, disconnect. PUBACKs are enqueued
    // with try_ack and retried, so a full channel never blocks the committer.
    let (client, eventloop) = AsyncClient::new(opts, cfg.queue_capacity.max(16) * 2);
    let metrics = spool.metrics().cloned();
    let counters = metrics.as_ref().map(|m| m.mqtt.clone()).unwrap_or_default();
    counters
        .queue_capacity
        .store(cfg.queue_capacity as u64, Ordering::Relaxed);
    let generation = Arc::new(Mutex::new(0u64));
    let fatal: Arc<Mutex<Option<CollectError>>> = Arc::new(Mutex::new(None));
    let stopping = Arc::new(AtomicBool::new(false));

    let (queue_tx, queue_rx) = mpsc::channel::<Received>(cfg.queue_capacity);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel::<AckNotice>();

    let subscriptions = Subscriptions::load(&cfg, spool.clone()).await?;
    let committer = tokio::spawn(committer(
        cfg.clone(),
        spool,
        queue_rx,
        ack_tx,
        counters.clone(),
        fatal.clone(),
        stop.clone(),
    ));
    let acker = tokio::spawn(acker(
        client.clone(),
        ack_rx,
        generation.clone(),
        counters.clone(),
    ));

    poll_loop(
        &cfg,
        eventloop,
        client,
        queue_tx,
        generation,
        counters.clone(),
        fatal.clone(),
        stop,
        stopping,
        committer,
        acker,
        metrics,
        subscriptions,
    )
    .await;

    let summary = counters.summary();
    if let Some(e) = fatal.lock().unwrap_or_else(|p| p.into_inner()).take() {
        log(format!("mqtt: stopped on error; summary {summary:?}"));
        return Err(e);
    }
    Ok(summary)
}

fn set_fatal(fatal: &Mutex<Option<CollectError>>, e: CollectError, stop: &Stop) {
    log(format!("mqtt: fatal: {e}"));
    let mut f = fatal.lock().unwrap_or_else(|p| p.into_inner());
    if f.is_none() {
        *f = Some(e);
    }
    stop.trigger();
}

/// Notify readiness once the configured set is active; stop on errors.
fn subscription_progress(
    cfg: &MqttConfig,
    counters: &Counters,
    active: Result<bool, CollectError>,
    fatal: &Mutex<Option<CollectError>>,
    stop: &Stop,
) {
    match active {
        Ok(true) => {
            log(format!("mqtt: subscription active for {:?}", cfg.topics));
            counters.subscribed.store(true, Ordering::Relaxed);
            if let Some(r) = &cfg.ready {
                r.notify_one();
            }
        }
        Ok(false) => {}
        Err(e) => set_fatal(fatal, e, stop),
    }
}

#[allow(clippy::too_many_arguments)]
async fn poll_loop(
    cfg: &MqttConfig,
    mut eventloop: EventLoop,
    client: AsyncClient,
    queue_tx: mpsc::Sender<Received>,
    generation: Arc<Mutex<u64>>,
    counters: Arc<Counters>,
    fatal: Arc<Mutex<Option<CollectError>>>,
    stop: Stop,
    stopping: Arc<AtomicBool>,
    committer: tokio::task::JoinHandle<()>,
    acker: tokio::task::JoinHandle<()>,
    metrics: Option<Arc<crate::metrics::Metrics>>,
    mut subscriptions: Subscriptions,
) {
    let mut queue_tx = Some(queue_tx);
    let mut backoff = cfg.reconnect_min;
    let mut drain_deadline: Option<tokio::time::Instant> = None;
    let mut committer = Some(committer);
    let mut acker = Some(acker);

    loop {
        // Transition into the shutdown phase exactly once.
        if drain_deadline.is_none() && stop.is_triggered() {
            stopping.store(true, Ordering::SeqCst);
            // Not ready while draining.
            counters.subscribed.store(false, Ordering::Relaxed);
            drain_deadline = Some(tokio::time::Instant::now() + cfg.shutdown_grace);
            // Closing the queue lets the committer finish its current
            // transaction and then drop (without acking) anything queued.
            queue_tx = None;
            let committer = committer.take();
            let acker = acker.take();
            let client = client.clone();
            tokio::spawn(async move {
                if let Some(c) = committer {
                    let _ = c.await;
                }
                if let Some(a) = acker {
                    let _ = a.await;
                }
                // All permitted PUBACKs are queued ahead of DISCONNECT.
                for _ in 0..500 {
                    match client.try_disconnect() {
                        Ok(()) => break,
                        Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                    }
                }
            });
        }

        let polled = if let Some(deadline) = drain_deadline {
            match tokio::time::timeout_at(deadline, eventloop.poll()).await {
                Ok(r) => r,
                Err(_) => {
                    log(
                        "mqtt: shutdown grace period expired; unsent acknowledgments will cause redelivery",
                    );
                    break;
                }
            }
        } else {
            // Cancelling poll() at most loses an unsent PUBACK (-> redelivery),
            // never sends one early.
            tokio::select! {
                r = eventloop.poll() => r,
                _ = stop.wait() => continue,
            }
        };

        match polled {
            Ok(MqEvent::Incoming(Packet::ConnAck(ack))) => {
                counters.connections.fetch_add(1, Ordering::Relaxed);
                counters.connected.store(true, Ordering::Relaxed);
                // Set once the configured filter set is active.
                counters.subscribed.store(false, Ordering::Relaxed);
                backoff = cfg.reconnect_min;
                log(format!(
                    "mqtt: connected to {}:{} as {:?} (session_present={})",
                    cfg.host, cfg.port, cfg.client_id, ack.session_present
                ));
                // Without a session the broker holds no subscriptions: subscribe
                // to every filter. With one, only apply the difference to what
                // the spool recorded, because re-subscribing would re-send
                // retained messages.
                let active = subscriptions
                    .on_connack(cfg, &client, ack.session_present)
                    .await;
                subscription_progress(cfg, &counters, active, &fatal, &stop);
            }
            Ok(MqEvent::Incoming(Packet::SubAck(suback))) => {
                let active = subscriptions.on_suback(cfg, &suback.return_codes).await;
                subscription_progress(cfg, &counters, active, &fatal, &stop);
            }
            Ok(MqEvent::Incoming(Packet::UnsubAck(_))) => {
                let done = subscriptions.on_unsuback().await.map(|()| false);
                subscription_progress(cfg, &counters, done, &fatal, &stop);
            }
            Ok(MqEvent::Incoming(Packet::Publish(publish))) => {
                counters.received.fetch_add(1, Ordering::Relaxed);
                let Some(tx) = queue_tx.as_ref() else {
                    // Shutting down: not committed, so never acknowledged.
                    counters.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                if publish.payload.len() > cfg.max_payload_bytes {
                    set_fatal(
                        &fatal,
                        CollectError::Fatal(format!(
                            "payload of {} bytes on {:?} exceeds --max-payload-bytes {}; not acknowledged",
                            publish.payload.len(),
                            publish.topic,
                            cfg.max_payload_bytes
                        )),
                        &stop,
                    );
                    continue;
                }
                let gen_now = *generation.lock().unwrap_or_else(|p| p.into_inner());
                let item = Received {
                    publish,
                    generation: gen_now,
                    received_unix_ns: event::now_unix_ns(),
                };
                // Counted before sending so the committer's decrement can
                // never precede it.
                counters.queue_depth.fetch_add(1, Ordering::Relaxed);
                match tx.try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(item)) => {
                        let n = counters.saturation.fetch_add(1, Ordering::Relaxed) + 1;
                        if n == 1 || n.is_power_of_two() {
                            log(format!(
                                "mqtt: queue full ({} messages); pausing socket reads (backpressure), saturation events: {n}",
                                cfg.queue_capacity
                            ));
                        }
                        tokio::select! {
                            r = tx.send(item) => if r.is_err() {
                                counters.dropped.fetch_add(1, Ordering::Relaxed);
                                counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                            },
                            _ = stop.wait() => {
                                counters.dropped.fetch_add(1, Ordering::Relaxed);
                                counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        counters.dropped.fetch_add(1, Ordering::Relaxed);
                        counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
            Ok(MqEvent::Outgoing(Outgoing::Disconnect)) => {
                if drain_deadline.is_some() {
                    // Flush happened inside poll(); the DISCONNECT is on the wire.
                    log("mqtt: disconnected cleanly");
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => {
                counters.connection_errors.fetch_add(1, Ordering::Relaxed);
                counters.connected.store(false, Ordering::Relaxed);
                counters.subscribed.store(false, Ordering::Relaxed);
                if let Some(m) = &metrics {
                    m.error("mqtt", connection_error_kind(&e));
                }
                {
                    // Invalidate acknowledgments for the lost connection and
                    // purge any PUBACK enqueued in the race window.
                    let mut g = generation.lock().unwrap_or_else(|p| p.into_inner());
                    *g += 1;
                    eventloop.clean();
                }
                if drain_deadline.is_some() {
                    log(format!("mqtt: connection ended during shutdown: {e}"));
                    break;
                }
                if let Some(msg) = fatal_connection_error(&e) {
                    set_fatal(&fatal, CollectError::Fatal(msg), &stop);
                    continue;
                }
                log(format!(
                    "mqtt: connection error: {e}; retrying in {} ms",
                    backoff.as_millis()
                ));
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = stop.wait() => {}
                }
                backoff = (backoff * 2).min(cfg.reconnect_max);
            }
        }
    }
    // Make sure helper tasks are finished (or abandoned) before returning.
    queue_tx.take();
    if let Some(c) = committer.take() {
        let _ = c.await;
    }
    if let Some(a) = acker.take() {
        a.abort();
    }
}

async fn committer(
    cfg: MqttConfig,
    spool: AsyncSpool,
    mut rx: mpsc::Receiver<Received>,
    ack_tx: mpsc::UnboundedSender<AckNotice>,
    counters: Arc<Counters>,
    fatal: Arc<Mutex<Option<CollectError>>>,
    stop: Stop,
) {
    let faults = spool.faults().clone();
    loop {
        let first = tokio::select! {
            biased;
            _ = stop.wait() => break,
            m = rx.recv() => match m { Some(m) => m, None => break },
        };
        // Group commit: never more than the remaining --limit, so a limit
        // still stops after exactly that many messages.
        let done = counters.committed.load(Ordering::Relaxed)
            + counters.source_duplicates.load(Ordering::Relaxed);
        let cap = match cfg.limit {
            Some(l) => (l.saturating_sub(done) as usize).clamp(1, cfg.max_batch),
            None => cfg.max_batch,
        };
        let mut batch = vec![first];
        while batch.len() < cap {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }
        if !cfg.max_linger.is_zero() && batch.len() < cap {
            let deadline = tokio::time::Instant::now() + cfg.max_linger;
            while batch.len() < cap {
                tokio::select! {
                    biased;
                    _ = stop.wait() => break,
                    m = tokio::time::timeout_at(deadline, rx.recv()) => match m {
                        Ok(Some(m)) => batch.push(m),
                        Ok(None) | Err(_) => break,
                    },
                }
            }
        }
        counters
            .queue_depth
            .fetch_sub(batch.len() as u64, Ordering::Relaxed);
        for item in &batch {
            if item.publish.qos == QoS::AtMostOnce {
                counters.qos0.fetch_add(1, Ordering::Relaxed);
            }
        }
        faults.hit(point::MQTT_BEFORE_COMMIT);
        let events: Vec<(Event, Option<SourceClaim>)> =
            batch.iter().map(|item| to_event(&cfg, item)).collect();
        for (ev, _) in &events {
            if let EventData::MqttMessage(m) = &ev.data
                && m.subscriptions.is_empty()
            {
                counters.unmatched.fetch_add(1, Ordering::Relaxed);
            }
        }
        let outcome = spool.append_batch(events).await;
        let results = match outcome {
            Ok(r) => r,
            Err(e) => {
                // Transaction rolled back: nothing from the batch is stored,
                // so nothing is acknowledged. Stop visibly.
                set_fatal(&fatal, CollectError::Spool(e), &stop);
                break;
            }
        };
        faults.hit(point::MQTT_AFTER_COMMIT);
        counters.transactions.fetch_add(1, Ordering::Relaxed);
        counters
            .max_batch
            .fetch_max(batch.len() as u64, Ordering::Relaxed);
        // Results are in arrival order and end at the first item failure;
        // the items before it are committed and may be acknowledged.
        let batch_len = batch.len();
        let mut item_error = None;
        let mut acked = 0;
        for (item, result) in batch.into_iter().zip(results) {
            match result {
                Ok(AppendOutcome::Inserted { .. }) => {
                    counters.committed.fetch_add(1, Ordering::Relaxed);
                }
                Ok(
                    AppendOutcome::DuplicateSource { seq } | AppendOutcome::DuplicateEvent { seq },
                ) => {
                    counters.source_duplicates.fetch_add(1, Ordering::Relaxed);
                    log(format!(
                        "mqtt: redelivery of source identity already stored at seq {seq}; acknowledging without a new record"
                    ));
                }
                Err(e) => {
                    item_error = Some(e);
                    break;
                }
            }
            let _ = ack_tx.send(AckNotice {
                publish: item.publish,
                generation: item.generation,
            });
            acked += 1;
        }
        if let Some(e) = item_error {
            // Not stored (e.g. conflicting source identity): never
            // acknowledged; the rest of the batch was not attempted.
            counters
                .dropped
                .fetch_add((batch_len - acked - 1) as u64, Ordering::Relaxed);
            set_fatal(&fatal, CollectError::Spool(e), &stop);
            break;
        }
        let done = counters.committed.load(Ordering::Relaxed)
            + counters.source_duplicates.load(Ordering::Relaxed);
        if cfg.limit.is_some_and(|l| done >= l) {
            log(format!("mqtt: limit of {done} messages reached; stopping"));
            stop.trigger();
        }
    }
    // Anything still queued was never committed: drop without acknowledgment.
    rx.close();
    while let Ok(_unacked) = rx.try_recv() {
        counters.dropped.fetch_add(1, Ordering::Relaxed);
        counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
    let dropped = counters.dropped.load(Ordering::Relaxed);
    if dropped > 0 {
        log(format!(
            "mqtt: {dropped} received but uncommitted messages were not acknowledged; the broker redelivers them if the session persists"
        ));
    }
}

async fn acker(
    client: AsyncClient,
    mut rx: mpsc::UnboundedReceiver<AckNotice>,
    generation: Arc<Mutex<u64>>,
    counters: Arc<Counters>,
) {
    while let Some(n) = rx.recv().await {
        loop {
            let full = {
                let g = generation.lock().unwrap_or_else(|p| p.into_inner());
                if *g != n.generation {
                    counters.acks_skipped.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                match client.try_ack(&n.publish) {
                    Ok(()) => {
                        counters.acks_enqueued.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                    Err(rumqttc::ClientError::TryRequest(_)) => true,
                    Err(_) => return, // event loop gone
                }
            };
            if !full {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_identity_extraction() {
        let si = SourceIdentityConfig {
            namespace: "plant-a".into(),
            json_pointer: "/msg/id".into(),
        };
        let (i, c) = source_identity(&si, "t/1", br#"{"msg":{"id":42},"v":1}"#).unwrap();
        assert_eq!(i.key, "42");
        let (_, c2) = source_identity(&si, "t/1", br#"{"msg":{"id":42},"v":2}"#).unwrap();
        assert_eq!(c.key, c2.key);
        assert_ne!(c.content_sha256, c2.content_sha256);
        assert!(source_identity(&si, "t/1", b"not json").is_err());
        assert!(source_identity(&si, "t/1", br#"{"msg":{}}"#).is_err());
    }

    #[test]
    fn config_validation() {
        let mut c = MqttConfig::new("h", 1, "id", "t/#");
        assert!(c.validate(1 << 20).is_ok());
        c.max_payload_bytes = 1 << 20;
        assert!(c.validate(1 << 20).is_err());
        c.max_payload_bytes = 1024;
        c.client_id.clear();
        assert!(c.validate(1 << 20).is_err());
        c.client_id = "id".into();
        c.topics = vec!["plant/line1/#".into(), "sensors/hub-1".into()];
        assert!(c.validate(1 << 20).is_ok());
        c.topics.push("plant/line1/#".into());
        let e = c.validate(1 << 20).unwrap_err().to_string();
        assert!(e.contains("duplicate"), "{e}");
        c.topics = vec![];
        assert!(c.validate(1 << 20).is_err());
        c.topics = vec!["a/#/b".into()];
        assert!(c.validate(1 << 20).is_err());
    }

    #[test]
    fn filter_validation() {
        for ok in [
            "#", "+", "a", "a/b", "a/+/c", "+/+", "a/#", "/", "/a", "$SYS/#",
        ] {
            assert!(validate_filter(ok).is_ok(), "{ok}");
        }
        for bad in ["", "a#", "a/#/b", "#/a", "a/b#", "a+", "a/+b", "a\0b"] {
            assert!(validate_filter(bad).is_err(), "{bad:?}");
        }
        assert!(validate_filter(&"a".repeat(65_536)).is_err());
    }

    #[test]
    fn wildcard_matching() {
        let yes = [
            ("plant/line1/spindle", "plant/line1/spindle"),
            ("plant/+/spindle", "plant/line1/spindle"),
            ("plant/#", "plant/line1/spindle"),
            ("plant/#", "plant"),
            ("#", "plant/line1"),
            ("+/+", "/finance"),
            ("+", ""),
            ("sport/+", "sport/"),
            ("$SYS/#", "$SYS/broker/load"),
            ("$SYS/+", "$SYS/x"),
        ];
        for (f, t) in yes {
            assert!(filter_matches(f, t), "{f} should match {t}");
        }
        let no = [
            ("plant/line1/spindle", "plant/line1"),
            ("plant/+", "plant/line1/spindle"),
            ("plant/+", "plant"),
            ("plant/#", "plants"),
            ("plant/line1", "Plant/line1"),
            ("sensors/hub-1", "plant/line1"),
            ("#", "$SYS/broker"),
            ("+/broker", "$SYS/broker"),
            ("/+", "a/b"),
        ];
        for (f, t) in no {
            assert!(!filter_matches(f, t), "{f} should not match {t}");
        }
    }

    fn set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn topics(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn subscription_plan() {
        let t = topics(&["b/#", "a/1"]);
        let full = SubscriptionRecord {
            confirmed: set(&["a/1", "b/#"]),
            possible: set(&["a/1", "b/#"]),
        };
        // No broker session: subscribe to all, regardless of the record.
        for known in [None, Some(&full)] {
            let p = plan_subscriptions(&t, false, known);
            assert_eq!(p.subscribe, t);
            assert!(p.unsubscribe.is_empty());
            assert_eq!(p.before.confirmed, set(&[]));
            assert_eq!(p.after, full);
        }
        // Session present, record matches: nothing to do.
        let p = plan_subscriptions(&t, true, Some(&full));
        assert!(p.subscribe.is_empty() && p.unsubscribe.is_empty());
        assert_eq!(p.after, full);
        // Session present, unknown record: subscribe to all, remove nothing.
        let p = plan_subscriptions(&t, true, None);
        assert_eq!(p.subscribe, t);
        assert!(p.unsubscribe.is_empty());
        // Changed set: add c/+, drop a/1.
        let t2 = topics(&["b/#", "c/+"]);
        let p = plan_subscriptions(&t2, true, Some(&full));
        assert_eq!(p.subscribe, topics(&["c/+"]));
        assert_eq!(p.unsubscribe, topics(&["a/1"]));
        assert_eq!(p.before.confirmed, set(&["b/#"]));
        assert_eq!(p.before.possible, set(&["a/1", "b/#", "c/+"]));
        // Interrupted change (only `before` was stored): re-applied, and a
        // removed filter is never assumed to be held.
        let p = plan_subscriptions(&t, true, Some(&p.before));
        assert_eq!(p.subscribe, topics(&["a/1"]));
        assert_eq!(p.unsubscribe, topics(&["c/+"]));
    }
}
