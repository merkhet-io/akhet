//! Embedded loopback MQTT broker (rumqttd), shared by the `mqtt_broker`
//! example and the integration tests. Development use only.
#![allow(dead_code)]

/// Start rumqttd on 127.0.0.1:`port` in a background thread (it runs until
/// the process exits) and wait until it accepts connections.
pub fn start_broker(port: u16) {
    let cfg: rumqttd::Config = serde_json::from_value(serde_json::json!({
        "id": 0,
        "router": {
            "max_connections": 1000,
            "max_outgoing_packet_count": 200,
            "max_segment_size": 10485760,
            "max_segment_count": 10
        },
        "v4": {
            "1": {
                "name": "v4-1",
                "listen": format!("127.0.0.1:{port}"),
                "next_connection_delay_ms": 1,
                "connections": {
                    "connection_timeout_ms": 60000,
                    "max_payload_size": 2097152,
                    "max_inflight_count": 100,
                    "dynamic_filters": true
                }
            }
        }
    }))
    .expect("broker config");
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(cfg);
        broker.start().expect("broker");
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(std::time::Instant::now() < deadline, "broker did not start");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
