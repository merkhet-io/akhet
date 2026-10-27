//! MQTT over TLS: a test CA and a TLS-terminating loopback proxy (tokio-rustls)
//! in front of the rumqttd broker. Exercises the collector's real TLS client
//! configuration (`--tls-ca-file`) and certificate validation failure.

mod support;

use akhet::adapters::mqtt::{self, MqttConfig, TlsSettings};
use akhet::adapters::{AsyncSpool, Stop};
use akhet::faults::Faults;
use akhet::spool::{DEFAULT_MAX_EVENT_BYTES, Spool};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use support::*;
use tokio_rustls::rustls;

struct Pki {
    ca_pem: String,
    server_cert_der: Vec<u8>,
    server_key_der: Vec<u8>,
}

fn pki() -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "akhet test CA");
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    Pki {
        ca_pem: ca_cert.pem(),
        server_cert_der: leaf.der().to_vec(),
        server_key_der: leaf_key.serialize_der(),
    }
}

/// TLS terminator on an ephemeral port forwarding plaintext to `upstream`.
async fn tls_proxy(p: &Pki, upstream: u16) -> u16 {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(p.server_cert_der.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(p.server_key_der.clone())),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let Ok(mut up) = tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await
                else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut up).await;
            });
        }
    });
    port
}

fn tls_cfg(port: u16, client: &str, ca: Option<PathBuf>) -> MqttConfig {
    let mut c = MqttConfig::new("localhost", port, client, "secure/#");
    c.tls = Some(TlsSettings {
        ca_file: ca,
        client_cert: None,
    });
    c.reconnect_min = Duration::from_millis(50);
    c.reconnect_max = Duration::from_millis(200);
    c
}

async fn run_collector(
    c: MqttConfig,
    db: &Path,
    stop: Stop,
) -> Result<mqtt::MqttSummary, akhet::adapters::CollectError> {
    let spool = AsyncSpool::new(Spool::open(db, MAX_DB).unwrap(), Faults::none());
    mqtt::run(c, spool, DEFAULT_MAX_EVENT_BYTES, stop).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn captures_over_tls_with_configured_ca() {
    let broker_port = free_port();
    broker::start_broker(broker_port);
    let p = pki();
    let tls_port = tls_proxy(&p, broker_port).await;
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, &p.ca_pem).unwrap();
    let db = dir.path().join("spool.sqlite");

    let mut c = tls_cfg(tls_port, "akhet-tls", Some(ca));
    c.limit = Some(1);
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, Stop::new()).await });
    publish(
        broker_port,
        "pub-tls",
        &[("secure/x", b"over-tls".to_vec(), true)],
    )
    .await;
    let s = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(s.committed, 1);
    let evs = events(&db);
    assert_eq!(evs[0]["topic"], "secure/x");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untrusted_server_certificate_is_refused() {
    let broker_port = free_port();
    broker::start_broker(broker_port);
    let p = pki();
    let tls_port = tls_proxy(&p, broker_port).await;
    let other = pki(); // a CA that did not sign the server certificate
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("other-ca.pem");
    std::fs::write(&ca, &other.ca_pem).unwrap();
    let db = dir.path().join("spool.sqlite");
    publish(
        broker_port,
        "pub-tls2",
        &[("secure/x", b"never".to_vec(), true)],
    )
    .await;

    let stop = Stop::new();
    let s2 = stop.clone();
    let c = tls_cfg(tls_port, "akhet-tls-bad", Some(ca));
    let db2 = db.clone();
    let h = tokio::spawn(async move { run_collector(c, &db2, s2).await });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop.trigger();
    let s = tokio::time::timeout(Duration::from_secs(20), h)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(s.connections, 0, "{s:?}");
    assert!(s.connection_errors >= 2, "retried with backoff: {s:?}");
    assert_eq!(count(&db), 0);
}
