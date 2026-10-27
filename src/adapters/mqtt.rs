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
//! - The committer appends each message in its own SQLite transaction on the
//!   blocking pool and only then emits an ack notice.
//! - The ack task enqueues a PUBACK only if the message arrived on the current
//!   connection (generation check under a mutex shared with the poll task, which
//!   bumps the generation and purges stale queued PUBACKs on disconnect). Packet
//!   ids are only meaningful within one connection/session.
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
    Outgoing, Packet, Publish, QoS, StateError, SubscribeReasonCode, TlsConfiguration, Transport,
};
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
    pub topic: String,
    pub tls: Option<TlsSettings>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub max_payload_bytes: usize,
    pub queue_capacity: usize,
    pub source_identity: Option<SourceIdentityConfig>,
    /// Stop (gracefully) after this many messages were committed or
    /// recognized as source duplicates.
    pub limit: Option<u64>,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    /// Maximum time to flush acknowledgments and disconnect on shutdown.
    pub shutdown_grace: Duration,
    /// Notified each time the subscription is known to be active (SUBACK
    /// with QoS 1, or CONNACK with an existing session). Used by tests and
    /// embedders to avoid racing the subscription.
    pub ready: Option<Arc<tokio::sync::Notify>>,
}

impl MqttConfig {
    pub fn new(host: &str, port: u16, client_id: &str, topic: &str) -> Self {
        MqttConfig {
            host: host.into(),
            port,
            client_id: client_id.into(),
            topic: topic.into(),
            tls: None,
            username: None,
            password: None,
            keep_alive: Duration::from_secs(30),
            max_payload_bytes: 256 * 1024,
            queue_capacity: 64,
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
        if self.topic.is_empty() {
            return c("topic filter must be non-empty");
        }
        if self.queue_capacity == 0 || self.queue_capacity > 65_535 {
            return c("queue capacity must be 1..=65535");
        }
        if self.max_payload_bytes == 0 || self.max_payload_bytes > 64 * 1024 * 1024 {
            return c("max payload bytes must be 1..=67108864");
        }
        // base64 expansion plus topic and JSON envelope must fit an event.
        let worst = self.max_payload_bytes.div_ceil(3) * 4 + MAX_TOPIC_BYTES * 2 + 4096;
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

/// Counters reported at shutdown.
#[derive(Debug, Default, serde::Serialize)]
pub struct MqttSummary {
    pub received: u64,
    pub committed: u64,
    pub source_duplicates: u64,
    pub acks_enqueued: u64,
    pub acks_skipped_after_reconnect: u64,
    pub queue_saturation_events: u64,
    pub dropped_unacknowledged_on_shutdown: u64,
    pub connections: u64,
    pub connection_errors: u64,
    pub qos0_messages: u64,
}

#[derive(Default)]
struct Counters {
    received: AtomicU64,
    committed: AtomicU64,
    source_duplicates: AtomicU64,
    acks_enqueued: AtomicU64,
    acks_skipped: AtomicU64,
    saturation: AtomicU64,
    dropped: AtomicU64,
    connections: AtomicU64,
    connection_errors: AtomicU64,
    qos0: AtomicU64,
}

impl Counters {
    fn summary(&self) -> MqttSummary {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        MqttSummary {
            received: g(&self.received),
            committed: g(&self.committed),
            source_duplicates: g(&self.source_duplicates),
            acks_enqueued: g(&self.acks_enqueued),
            acks_skipped_after_reconnect: g(&self.acks_skipped),
            queue_saturation_events: g(&self.saturation),
            dropped_unacknowledged_on_shutdown: g(&self.dropped),
            connections: g(&self.connections),
            connection_errors: g(&self.connection_errors),
            qos0_messages: g(&self.qos0),
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
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    let cfg_err = |m: String| CollectError::Config(m);
    let mut roots = rustls::RootCertStore::empty();
    match &t.ca_file {
        Some(path) => {
            for cert in CertificateDer::pem_file_iter(path)
                .map_err(|e| cfg_err(format!("reading CA file {}: {e}", path.display())))?
            {
                let cert = cert.map_err(|e| cfg_err(format!("parsing CA file: {e}")))?;
                roots
                    .add(cert)
                    .map_err(|e| cfg_err(format!("adding CA certificate: {e}")))?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                let _ = roots.add(cert);
            }
        }
    }
    if roots.is_empty() {
        return Err(cfg_err(
            "no trusted CA certificates available for TLS".into(),
        ));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| cfg_err(format!("TLS setup: {e}")))?
        .with_root_certificates(roots);
    let config = match &t.client_cert {
        None => builder.with_no_client_auth(),
        Some((cert, key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .map_err(|e| cfg_err(format!("reading client cert: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| cfg_err(format!("parsing client cert: {e}")))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|e| cfg_err(format!("reading client key: {e}")))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| cfg_err(format!("client certificate: {e}")))?
        }
    };
    Ok(TlsConfiguration::Rustls(Arc::new(config)))
}

fn options(cfg: &MqttConfig) -> Result<MqttOptions, CollectError> {
    let mut o = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    o.set_clean_session(false)
        .set_manual_acks(true)
        .set_keep_alive(cfg.keep_alive)
        // Packet = topic (2 + up to 65535) + packet id (2) + payload.
        .set_max_packet_size(cfg.max_payload_bytes + MAX_TOPIC_BYTES + 4, 64 * 1024);
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
    let mut msg = MqttMessage {
        broker: format!("{}:{}", cfg.host, cfg.port),
        client_id: cfg.client_id.clone(),
        subscription: cfg.topic.clone(),
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
    let counters = Arc::new(Counters::default());
    let generation = Arc::new(Mutex::new(0u64));
    let fatal: Arc<Mutex<Option<CollectError>>> = Arc::new(Mutex::new(None));
    let stopping = Arc::new(AtomicBool::new(false));

    let (queue_tx, queue_rx) = mpsc::channel::<Received>(cfg.queue_capacity);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel::<AckNotice>();

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
                backoff = cfg.reconnect_min;
                log(format!(
                    "mqtt: connected to {}:{} as {:?} (session_present={})",
                    cfg.host, cfg.port, cfg.client_id, ack.session_present
                ));
                if ack.session_present {
                    if let Some(r) = &cfg.ready {
                        r.notify_one();
                    }
                } else {
                    // Broker has no session for us: (re)subscribe. With an
                    // existing session the subscription persists, and
                    // re-subscribing would re-send retained messages.
                    if let Err(e) = client.try_subscribe(cfg.topic.clone(), QoS::AtLeastOnce) {
                        set_fatal(
                            &fatal,
                            CollectError::Fatal(format!("subscribe request failed: {e}")),
                            &stop,
                        );
                    }
                }
            }
            Ok(MqEvent::Incoming(Packet::SubAck(suback))) => {
                for code in &suback.return_codes {
                    match code {
                        SubscribeReasonCode::Success(QoS::AtLeastOnce) => {
                            log(format!("mqtt: subscribed to {:?} with QoS 1", cfg.topic));
                            if let Some(r) = &cfg.ready {
                                r.notify_one();
                            }
                        }
                        other => set_fatal(
                            &fatal,
                            CollectError::Fatal(format!(
                                "broker granted {other:?} for {:?}; QoS 1 is required for at-least-once capture",
                                cfg.topic
                            )),
                            &stop,
                        ),
                    }
                }
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
                            },
                            _ = stop.wait() => {
                                counters.dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        counters.dropped.fetch_add(1, Ordering::Relaxed);
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
        let item = tokio::select! {
            biased;
            _ = stop.wait() => break,
            m = rx.recv() => match m { Some(m) => m, None => break },
        };
        if item.publish.qos == QoS::AtMostOnce {
            counters.qos0.fetch_add(1, Ordering::Relaxed);
        }
        faults.hit(point::MQTT_BEFORE_COMMIT);
        let (ev, claim) = to_event(&cfg, &item);
        let outcome = spool.append(ev, claim).await;
        match outcome {
            Ok(o) => {
                faults.hit(point::MQTT_AFTER_COMMIT);
                match o {
                    AppendOutcome::Inserted { .. } => {
                        counters.committed.fetch_add(1, Ordering::Relaxed);
                    }
                    AppendOutcome::DuplicateSource { seq }
                    | AppendOutcome::DuplicateEvent { seq } => {
                        counters.source_duplicates.fetch_add(1, Ordering::Relaxed);
                        log(format!(
                            "mqtt: redelivery of source identity already stored at seq {seq}; acknowledging without a new record"
                        ));
                    }
                }
                let _ = ack_tx.send(AckNotice {
                    publish: item.publish,
                    generation: item.generation,
                });
                let done = counters.committed.load(Ordering::Relaxed)
                    + counters.source_duplicates.load(Ordering::Relaxed);
                if cfg.limit.is_some_and(|l| done >= l) {
                    log(format!("mqtt: limit of {done} messages reached; stopping"));
                    stop.trigger();
                }
            }
            Err(e) => {
                // Not committed: never acknowledged. Stop visibly.
                set_fatal(&fatal, CollectError::Spool(e), &stop);
                break;
            }
        }
    }
    // Anything still queued was never committed: drop without acknowledgment.
    rx.close();
    while let Ok(_unacked) = rx.try_recv() {
        counters.dropped.fetch_add(1, Ordering::Relaxed);
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
    }
}
