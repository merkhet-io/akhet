//! Secure replay: TLS with a generated test CA, mutual TLS and bearer tokens
//! between the `akhet replay` binary and the example receiver (in-process, or
//! as an `akhet receive` process to cover its command-line options).

mod support;

use akhet::spool::Spool;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use std::path::{Path, PathBuf};
use std::process::Output;
use support::*;

const EXIT_CONFIG: i32 = 2;
const EXIT_AUTH: i32 = 8;
const TOKEN: &str = "s3cret-replay-token_0123456789";

struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

fn ca(name: &str) -> Ca {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    let cert = params.self_signed(&key).unwrap();
    Ca {
        issuer: Issuer::new(params, key),
        pem: cert.pem(),
    }
}

/// Writes `<name>.pem` and `<name>.key` signed by `ca`; returns their paths.
fn leaf(
    dir: &Path,
    name: &str,
    ca: &Ca,
    sans: &[&str],
    usage: ExtendedKeyUsagePurpose,
) -> (PathBuf, PathBuf) {
    let key = KeyPair::generate().unwrap();
    let mut params =
        CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
    params.distinguished_name.push(DnType::CommonName, name);
    params.extended_key_usages = vec![usage];
    let cert = params.signed_by(&key, &ca.issuer).unwrap();
    let (c, k) = (
        dir.join(format!("{name}.pem")),
        dir.join(format!("{name}.key")),
    );
    std::fs::write(&c, cert.pem()).unwrap();
    std::fs::write(&k, key.serialize_pem()).unwrap();
    (c, k)
}

fn write_ca(dir: &Path, name: &str, ca: &Ca) -> PathBuf {
    let p = dir.join(format!("{name}.pem"));
    std::fs::write(&p, &ca.pem).unwrap();
    p
}

struct Pki {
    ca: Ca,
    ca_file: PathBuf,
    server: (PathBuf, PathBuf),
}

/// A CA and a server certificate for 127.0.0.1 (or the given names).
fn pki(dir: &Path, sans: &[&str]) -> Pki {
    let ca = ca("akhet test CA");
    let ca_file = write_ca(dir, "ca", &ca);
    let server = leaf(
        dir,
        "server",
        &ca,
        sans,
        ExtendedKeyUsagePurpose::ServerAuth,
    );
    Pki {
        ca,
        ca_file,
        server,
    }
}

fn seed(db: &Path, n: u16) {
    use akhet::event::{Event, EventData, ModbusOutcome, ModbusPoll};
    let mut s = Spool::open(db, MAX_DB).unwrap();
    for i in 0..n {
        let e = Event::new(
            EventData::ModbusPoll(ModbusPoll {
                endpoint: "sim:502".into(),
                device_id: 1,
                function: "read_holding_registers".into(),
                start_address: 0,
                count: 1,
                poll_index: u64::from(i),
                nominal_interval_ms: 1000,
                started_unix_ns: 1,
                completed_unix_ns: 2,
                duration_ns: 1,
                new_connection: false,
                outcome: ModbusOutcome::Ok { registers: vec![i] },
            }),
            2,
        );
        s.append(&e, None).unwrap();
    }
}

fn checkpoint(db: &Path, sink: &str) -> i64 {
    Spool::open(db, MAX_DB)
        .unwrap()
        .sink(sink)
        .unwrap()
        .map(|s| s.checkpoint_seq)
        .unwrap_or(0)
}

/// `akhet replay` with up to 3 attempts; `token` goes to AKHET_REPLAY_TOKEN.
fn replay(db: &Path, url: &str, token: Option<&str>, extra: &[&str]) -> Output {
    let mut args = vec![
        "replay",
        "--url",
        url,
        "--sink",
        "s",
        "--max-attempts",
        "3",
        "--max-backoff-ms",
        "100",
    ];
    args.extend_from_slice(extra);
    let mut c = akhet(db, &args, None);
    if let Some(t) = token {
        c.env("AKHET_REPLAY_TOKEN", t);
    }
    let out = c.output().unwrap();
    let (so, se) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    eprintln!("$ akhet {} -> {:?}\n{so}{se}", args.join(" "), out.status);
    assert!(
        !so.contains(TOKEN) && !se.contains(TOKEN),
        "token must never be printed"
    );
    out
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn tls_receiver(dir: &Path, p: &Pki, token: Option<&str>) -> TestReceiver {
    let cfg = akhet::tls::server_config(&p.server.0, &p.server.1, None).unwrap();
    TestReceiver::start_with(&dir.join("rx.sqlite"), "https", |r| {
        let r = r.with_tls(cfg);
        match token {
            Some(t) => r.with_token(t),
            None => r,
        }
    })
}

#[test]
fn replay_over_tls_with_trusted_custom_ca_and_token() {
    let dir = tempfile::tempdir().unwrap();
    let p = pki(dir.path(), &["127.0.0.1"]);
    let db = dir.path().join("spool.sqlite");
    seed(&db, 4);
    let rx = tls_receiver(dir.path(), &p, Some(TOKEN));
    let out = replay(&db, &rx.url, Some(TOKEN), &["--ca-file", path(&p.ca_file)]);
    assert!(out.status.success(), "{}", stderr(&out));
    let s = stdout_json(&out);
    assert_eq!(s["delivered"], 4);
    assert_eq!(s["receiver_id"], rx.receiver_id.as_str());
    assert_eq!(bodies(&rx.db), bodies(&db));
    assert_eq!(checkpoint(&db, "s"), 4);
}

#[test]
fn batched_replay_over_tls_with_token() {
    let dir = tempfile::tempdir().unwrap();
    let p = pki(dir.path(), &["127.0.0.1"]);
    let db = dir.path().join("spool.sqlite");
    seed(&db, 25);
    let rx = tls_receiver(dir.path(), &p, Some(TOKEN));
    let out = replay(
        &db,
        &rx.url,
        Some(TOKEN),
        &["--ca-file", path(&p.ca_file), "--records-per-request", "10"],
    );
    assert!(out.status.success(), "{}", stderr(&out));
    let s = stdout_json(&out);
    assert_eq!(s["batch_protocol"], true, "{s}");
    assert_eq!(s["delivered"], 25);
    assert_eq!(s["requests"], 3, "batches of 10, 10 and 5");
    assert_eq!(bodies(&rx.db), bodies(&db));
    assert_eq!(checkpoint(&db, "s"), 25);
}

#[test]
fn untrusted_ca_fails_permanently_without_retries() {
    let dir = tempfile::tempdir().unwrap();
    let p = pki(dir.path(), &["127.0.0.1"]);
    let other = write_ca(dir.path(), "other-ca", &ca("unrelated CA"));
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    let rx = tls_receiver(dir.path(), &p, None);
    let t0 = std::time::Instant::now();
    let out = replay(&db, &rx.url, None, &["--ca-file", path(&other)]);
    assert_eq!(out.status.code(), Some(EXIT_AUTH), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains("certificate verification failed"), "{err}");
    assert!(err.contains("not retried"), "{err}");
    assert!(!err.contains("transient failure"), "{err}");
    assert!(t0.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(rx.count(), 0);
    assert_eq!(checkpoint(&db, "s"), 0);
}

#[test]
fn wrong_hostname_fails_permanently() {
    let dir = tempfile::tempdir().unwrap();
    // Certificate valid for another name only; we connect to 127.0.0.1.
    let p = pki(dir.path(), &["receiver.invalid"]);
    let db = dir.path().join("spool.sqlite");
    seed(&db, 1);
    let rx = tls_receiver(dir.path(), &p, None);
    let out = replay(&db, &rx.url, None, &["--ca-file", path(&p.ca_file)]);
    assert_eq!(out.status.code(), Some(EXIT_AUTH), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains("certificate verification failed"), "{err}");
    assert!(err.contains("not valid for name"), "{err}");
    assert!(!err.contains("transient failure"), "{err}");
    assert_eq!(rx.count(), 0);
}

#[test]
fn missing_or_wrong_token_gets_401_and_does_not_advance_the_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let p = pki(dir.path(), &["127.0.0.1"]);
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    let rx = tls_receiver(dir.path(), &p, Some(TOKEN));
    let ca = ["--ca-file", path(&p.ca_file)];
    assert!(replay(&db, &rx.url, Some(TOKEN), &ca).status.success());
    assert_eq!(checkpoint(&db, "s"), 2);
    seed(&db, 2);

    for token in [None, Some("wrong-token"), Some(&TOKEN[..TOKEN.len() - 1])] {
        let out = replay(&db, &rx.url, token, &ca);
        assert_eq!(out.status.code(), Some(EXIT_AUTH), "{}", stderr(&out));
        let err = stderr(&out);
        assert!(err.contains("HTTP 401"), "{err}");
        assert!(!err.contains("transient failure"), "{err}");
        assert_eq!(checkpoint(&db, "s"), 2, "checkpoint unchanged");
        assert_eq!(rx.count(), 2);
    }

    assert!(replay(&db, &rx.url, Some(TOKEN), &ca).status.success());
    assert_eq!(checkpoint(&db, "s"), 4);
    assert_eq!(rx.count(), 4);
}

#[test]
fn mutual_tls_with_and_without_client_certificate() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let p = pki(d, &["127.0.0.1"]);
    let client_ca = ca("akhet client CA");
    let client_ca_file = write_ca(d, "client-ca", &client_ca);
    let good = leaf(
        d,
        "client",
        &client_ca,
        &["edge-01"],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    // Signed by the server CA, which the receiver does not accept for clients.
    let foreign = leaf(
        d,
        "foreign-client",
        &p.ca,
        &["edge-02"],
        ExtendedKeyUsagePurpose::ClientAuth,
    );

    // The receiver as a separate process, configured on its command line.
    let db = d.join("spool.sqlite");
    let rx_db = d.join("rx.sqlite");
    seed(&db, 3);
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let mut child = akhet(
        &db,
        &[
            "receive",
            "--listen",
            &listen,
            "--db",
            path(&rx_db),
            "--tls-cert",
            path(&p.server.0),
            "--tls-key",
            path(&p.server.1),
            "--client-ca",
            path(&client_ca_file),
        ],
        None,
    )
    .env("AKHET_RECEIVER_TOKEN", TOKEN)
    .spawn()
    .unwrap();
    wait_port(port);
    let url = format!("https://{listen}");
    let ca = ["--ca-file", path(&p.ca_file)];

    let without = replay(&db, &url, Some(TOKEN), &ca);
    let with_foreign = replay(
        &db,
        &url,
        Some(TOKEN),
        &[
            ca[0],
            ca[1],
            "--client-cert",
            path(&foreign.0),
            "--client-key",
            path(&foreign.1),
        ],
    );
    let with_good = replay(
        &db,
        &url,
        Some(TOKEN),
        &[
            ca[0],
            ca[1],
            "--client-cert",
            path(&good.0),
            "--client-key",
            path(&good.1),
        ],
    );
    // A valid client certificate does not replace the token.
    let good_cert_no_token = replay(
        &db,
        &url,
        None,
        &[
            ca[0],
            ca[1],
            "--client-cert",
            path(&good.0),
            "--client-key",
            path(&good.1),
        ],
    );
    let _ = child.kill();
    let _ = child.wait();

    for out in [&without, &with_foreign] {
        assert_eq!(out.status.code(), Some(EXIT_AUTH), "{}", stderr(out));
        let err = stderr(out);
        assert!(err.contains("server rejected the TLS handshake"), "{err}");
        assert!(!err.contains("transient failure"), "{err}");
    }
    assert!(with_good.status.success(), "{}", stderr(&with_good));
    assert_eq!(stdout_json(&with_good)["delivered"], 3);
    assert_eq!(good_cert_no_token.status.code(), Some(EXIT_AUTH));
    assert!(stderr(&good_cert_no_token).contains("HTTP 401"));
    assert_eq!(count(&rx_db), 3);
    assert_eq!(checkpoint(&db, "s"), 3);
}

#[test]
fn token_is_never_sent_over_plain_http_without_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 2);
    // A listener that records whether anything connected at all.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let out = replay(&db, &url, Some(TOKEN), &[]);
    assert_eq!(out.status.code(), Some(EXIT_CONFIG), "{}", stderr(&out));
    assert!(stderr(&out).contains("refusing to send the bearer token over plain http"));
    assert!(listener.accept().is_err(), "no connection may be attempted");
    // TLS options with an http:// URL are a configuration error as well.
    let ca = dir.path().join("none.pem");
    let out = replay(&db, &url, None, &["--ca-file", path(&ca)]);
    assert_eq!(out.status.code(), Some(EXIT_CONFIG));

    // With the explicit opt-in (e.g. loopback only), it is sent and accepted.
    let rx = TestReceiver::start_with(&dir.path().join("rx.sqlite"), "http", |r| {
        r.with_token(TOKEN)
    });
    let out = replay(&db, &rx.url, Some(TOKEN), &["--allow-insecure-token"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(rx.count(), 2);
}

#[test]
fn invalid_token_characters_are_rejected_before_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spool.sqlite");
    seed(&db, 1);
    let out = replay(&db, "https://127.0.0.1:1", Some("bad token\r\nX: y"), &[]);
    assert_eq!(out.status.code(), Some(EXIT_CONFIG));
    assert!(stderr(&out).contains("AKHET_REPLAY_TOKEN"));
    assert!(!stderr(&out).contains("bad token"));
}

/// The receiver's minimal HTTP/1.1 layer, spoken to over a raw socket.
#[test]
fn receiver_http_layer_rejects_unsupported_requests_cleanly() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    let rx = TestReceiver::start_with(&dir.path().join("rx.sqlite"), "http", |r| {
        r.with_token(TOKEN)
    });
    let addr = rx.url.trim_start_matches("http://").to_string();
    let send = |raw: String| {
        let mut s = std::net::TcpStream::connect(&addr).unwrap();
        s.write_all(raw.as_bytes()).unwrap();
        let mut resp = String::new();
        s.read_to_string(&mut resp).unwrap();
        resp
    };
    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    let r = send("GET /v1/identity HTTP/1.1\r\nHost: x\r\n\r\n".into());
    assert!(r.starts_with("HTTP/1.1 401 "), "{r}");
    assert!(r.contains("WWW-Authenticate: Bearer"), "{r}");
    let r = send(format!("GET /v1/identity HTTP/1.1\r\n{auth}\r\n"));
    assert!(r.starts_with("HTTP/1.1 200 "), "{r}");
    assert!(r.contains(&rx.receiver_id), "{r}");
    let r = send(format!(
        "POST /v1/events HTTP/1.1\r\n{auth}Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
    ));
    assert!(r.starts_with("HTTP/1.1 501 "), "{r}");
    let r = send(format!(
        "POST /v1/events HTTP/1.1\r\n{auth}Content-Length: 999999999\r\n\r\n{}",
        "x".repeat(10_000)
    ));
    assert!(r.starts_with("HTTP/1.1 413 "), "{r}");
    let r = send(format!("GET /nope HTTP/1.1\r\n{auth}\r\n"));
    assert!(r.starts_with("HTTP/1.1 404 "), "{r}");
    assert_eq!(rx.count(), 0);
}
