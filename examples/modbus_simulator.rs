//! Loopback Modbus TCP simulator for local runs without hardware.
//!
//!     cargo run --example modbus_simulator -- 127.0.0.1:5020
//!
//! Holding registers 0..100 hold 100 + address. Out-of-range reads return the
//! IllegalDataAddress exception. Development/test tooling only.

#[path = "../tests/support/modbus_sim.rs"]
mod modbus_sim;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:5020".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("modbus simulator listening on {addr} (holding registers 0..100 = 100 + address)");
    modbus_sim::serve(listener, modbus_sim::Sim::default()).await
}
