//! Shared helpers for integration tests. Protocol peers are real loopback
//! servers (rumqttd broker, tokio-modbus server, akhet receiver), not mocks.
#![allow(dead_code)]

pub mod broker;
pub mod modbus_sim;

use akhet::faults::Faults;
use akhet::receiver::Receiver;
use akhet::spool::Spool;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub const MAX_DB: u64 = akhet::spool::DEFAULT_MAX_DB_BYTES;

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_akhet")
}

/// `akhet --spool <spool> <args...>` with optional fault spec.
pub fn akhet(spool: &Path, args: &[&str], fault: Option<&str>) -> Command {
    let mut c = Command::new(bin());
    c.arg("--spool").arg(spool).args(args);
    c.env_remove("AKHET_FAULT");
    c.env_remove("AKHET_SPOOL");
    if let Some(f) = fault {
        c.env("AKHET_FAULT", f);
    }
    c.stdin(Stdio::null());
    c
}

pub fn run(spool: &Path, args: &[&str], fault: Option<&str>) -> Output {
    let out = akhet(spool, args, fault).output().expect("spawn akhet");
    eprintln!(
        "$ akhet {} -> {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

pub fn stdout_json(out: &Output) -> serde_json::Value {
    let s = String::from_utf8_lossy(&out.stdout);
    let last = s.lines().last().unwrap_or("null");
    serde_json::from_str(last).unwrap_or(serde_json::Value::Null)
}

#[cfg(unix)]
pub fn killed_by_signal(out: &Output) -> bool {
    use std::os::unix::process::ExitStatusExt;
    out.status.signal().is_some()
}

/// In-process example receiver on an ephemeral loopback port.
pub struct TestReceiver {
    pub url: String,
    pub db: PathBuf,
    pub receiver_id: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TestReceiver {
    pub fn start(db: &Path) -> TestReceiver {
        let spool = Spool::open(db, MAX_DB).unwrap();
        let r = Receiver::bind("127.0.0.1:0", spool, Faults::none()).unwrap();
        let url = format!("http://{}", r.local_addr().unwrap());
        let receiver_id = r.receiver_id().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let thread = std::thread::spawn(move || r.serve(&s2));
        TestReceiver {
            url,
            db: db.to_path_buf(),
            receiver_id,
            stop,
            thread: Some(thread),
        }
    }

    pub fn count(&self) -> i64 {
        count(&self.db)
    }
}

impl Drop for TestReceiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub fn count(db: &Path) -> i64 {
    let s = Spool::open(db, MAX_DB).unwrap();
    s.status().unwrap().records
}

pub fn bodies(db: &Path) -> Vec<Vec<u8>> {
    let s = Spool::open(db, MAX_DB).unwrap();
    s.read_after(0, 100_000)
        .unwrap()
        .into_iter()
        .map(|r| r.body)
        .collect()
}

pub fn events(db: &Path) -> Vec<serde_json::Value> {
    bodies(db)
        .iter()
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect()
}

pub fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

pub fn wait_port(port: u16) {
    assert!(
        wait_until(Duration::from_secs(10), || std::net::TcpStream::connect((
            "127.0.0.1",
            port
        ))
        .is_ok()),
        "port {port} not ready"
    );
}

/// Publish QoS 1 messages with rumqttc and wait for all PUBACKs.
pub async fn publish(port: u16, client_id: &str, msgs: &[(&str, Vec<u8>, bool)]) {
    use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
    let mut o = MqttOptions::new(client_id, "127.0.0.1", port);
    o.set_max_packet_size(4 << 20, 4 << 20);
    let (client, mut el) = AsyncClient::new(o, 1000);
    for (topic, payload, retain) in msgs {
        client
            .publish(*topic, QoS::AtLeastOnce, *retain, payload.clone())
            .await
            .unwrap();
    }
    let mut acked = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while acked < msgs.len() {
        let ev = tokio::time::timeout_at(deadline, el.poll())
            .await
            .expect("publisher timed out")
            .expect("publisher connection");
        if let Event::Incoming(Packet::PubAck(_)) = ev {
            acked += 1;
        }
    }
    client.disconnect().await.unwrap();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), el.poll()).await {
            Ok(Ok(Event::Outgoing(Outgoing::Disconnect))) | Ok(Err(_)) | Err(_) => break,
            _ => {}
        }
    }
}

/// A TCP forwarder whose live connections can be severed on demand, used to
/// force real MQTT disconnects between collector and broker.
pub struct Proxy {
    pub port: u16,
    sever: tokio::sync::broadcast::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Proxy {
    pub async fn start(upstream: u16) -> Proxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sever, _) = tokio::sync::broadcast::channel::<()>(4);
        let tx = sever.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                let mut rx = tx.subscribe();
                tokio::spawn(async move {
                    let Ok(mut outbound) =
                        tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await
                    else {
                        return;
                    };
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        _ = rx.recv() => {}
                    }
                });
            }
        });
        Proxy { port, sever, task }
    }

    /// Drop all currently proxied connections.
    pub fn sever(&self) {
        let _ = self.sever.send(());
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
