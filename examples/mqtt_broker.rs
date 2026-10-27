//! Embedded loopback MQTT 3.1.1 broker (rumqttd) for local runs.
//!
//!     cargo run --example mqtt_broker -- 1883
//!
//! Listens on 127.0.0.1 only, no authentication, in-memory state: broker
//! sessions and queued messages are lost when it exits. Development/test
//! tooling only; use a properly configured broker (e.g. Mosquitto with
//! persistence) for anything else.

#[path = "../tests/support/broker.rs"]
mod broker;

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);
    broker::start_broker(port);
    eprintln!("mqtt broker listening on 127.0.0.1:{port} (in-memory, development only)");
    loop {
        std::thread::park();
    }
}
