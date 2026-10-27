//! Publish QoS 1 test messages and wait for the broker's PUBACKs.
//!
//!     cargo run --example mqtt_publish -- 127.0.0.1:1883 sensors/demo 5
//!
//! Payloads are JSON objects with a publisher-assigned `"id"` (usable with
//! `--source-id-json-pointer /id`). Development/test tooling only.

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:1883".into());
    let topic = args.next().unwrap_or_else(|| "sensors/demo".into());
    let n: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(5);
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let id = format!("akhet-example-publisher-{}", std::process::id());
    let mut opts = MqttOptions::new(id, host, port.parse().expect("port"));
    opts.set_keep_alive(Duration::from_secs(10));
    let (client, mut el) = AsyncClient::new(opts, 100);
    for i in 0..n {
        let payload = format!(
            r#"{{"id":"{}-{i}","value":{}}}"#,
            std::process::id(),
            i * 10
        );
        client
            .publish(topic.clone(), QoS::AtLeastOnce, false, payload)
            .await
            .unwrap();
    }
    let mut acked = 0;
    while acked < n {
        match tokio::time::timeout(Duration::from_secs(10), el.poll()).await {
            Ok(Ok(Event::Incoming(Packet::PubAck(_)))) => acked += 1,
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("mqtt: {e}"),
            Err(_) => panic!("timed out waiting for PUBACKs"),
        }
    }
    eprintln!("published {n} QoS 1 messages to {topic} on {addr}");
    client.disconnect().await.ok();
    let _ = tokio::time::timeout(Duration::from_secs(2), el.poll()).await;
}
