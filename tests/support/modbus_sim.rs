//! Loopback Modbus TCP simulator (holding registers only), shared by the
//! `modbus_simulator` example and the integration tests.
//!
//! Registers 0..100: register i holds 100 + i. Reads beyond 100 return the
//! IllegalDataAddress exception. Every other function returns IllegalFunction.
#![allow(dead_code)]

use std::future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio_modbus::prelude::*;
use tokio_modbus::server::tcp::{Server, accept_tcp_connection};

pub const SIM_REGISTERS: u16 = 100;

#[derive(Clone, Default)]
pub struct Sim {
    pub reads: Arc<AtomicU64>,
}

impl tokio_modbus::server::Service for Sim {
    type Request = SlaveRequest<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = future::Ready<Result<Response, ExceptionCode>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let r = match req.request {
            Request::ReadHoldingRegisters(addr, cnt) => {
                self.reads.fetch_add(1, Ordering::Relaxed);
                let end = u32::from(addr) + u32::from(cnt);
                if end > u32::from(SIM_REGISTERS) {
                    Err(ExceptionCode::IllegalDataAddress)
                } else {
                    Ok(Response::ReadHoldingRegisters(
                        (addr..addr + cnt).map(|a| 100 + a).collect(),
                    ))
                }
            }
            _ => Err(ExceptionCode::IllegalFunction),
        };
        future::ready(r)
    }
}

/// Serve on an already bound listener until the task is dropped/aborted.
pub async fn serve(listener: TcpListener, sim: Sim) -> std::io::Result<()> {
    let server = Server::new(listener);
    let on_connected = move |stream, addr: SocketAddr| {
        let sim = sim.clone();
        async move { accept_tcp_connection(stream, addr, move |_| Ok(Some(sim.clone()))) }
    };
    server
        .serve(&on_connected, |err| eprintln!("modbus sim: {err}"))
        .await
}
