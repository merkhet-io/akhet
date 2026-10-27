//! Example HTTP receiver for `akhet replay` (protocol `akhet-replay-v1`).
//!
//! It stores received events in its own Akhet spool file (same format, so
//! `akhet --spool <receiver.sqlite> inspect` works). An event is committed with
//! the same synchronous SQLite transaction as the collector spool *before* a
//! success status is returned. Identical re-sends of an event_id return 200;
//! a different body for an existing event_id returns 409.
//!
//! This is an example endpoint, not a hardened service: single-threaded, no
//! authentication, no TLS. Bind it to loopback or a trusted network only.

use crate::event::{self, FormatError};
use crate::faults::{Faults, point};
use crate::spool::{AppendOutcome, Spool, SpoolError};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tiny_http::{Header, Method, Request, Response, Server};

pub struct Receiver {
    server: Server,
    spool: Spool,
    faults: Faults,
}

fn json_response(status: u16, v: serde_json::Value) -> Response<std::io::Cursor<Vec<u8>>> {
    Response::from_data(serde_json::to_vec(&v).unwrap_or_default())
        .with_status_code(status)
        .with_header(
            Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                .expect("static header"),
        )
}

fn header<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

impl Receiver {
    pub fn bind(listen: &str, spool: Spool, faults: Faults) -> Result<Receiver, String> {
        let server = Server::http(listen).map_err(|e| format!("bind {listen}: {e}"))?;
        Ok(Receiver {
            server,
            spool,
            faults,
        })
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.server.server_addr().to_ip()
    }

    pub fn receiver_id(&self) -> &str {
        self.spool.spool_id()
    }

    /// Serve until `stop` is set.
    pub fn serve(mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::SeqCst) {
            match self.server.recv_timeout(Duration::from_millis(100)) {
                Ok(Some(req)) => self.handle(req),
                Ok(None) => {}
                Err(e) => {
                    eprintln!("akhet: receiver: accept error: {e}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn handle(&mut self, mut req: Request) {
        let url = req.url().split('?').next().unwrap_or("").to_string();
        let resp = match (req.method(), url.as_str()) {
            (Method::Get, "/v1/identity") => json_response(
                200,
                serde_json::json!({"receiver_id": self.spool.spool_id(), "protocol": "akhet-replay-v1"}),
            ),
            (Method::Post, "/v1/events") => {
                let (status, body) = self.post_event(&mut req);
                json_response(status, body)
            }
            _ => json_response(404, serde_json::json!({"error": "not found"})),
        };
        let committed = resp.status_code().0 == 201;
        if committed {
            self.faults.hit(point::RECEIVER_AFTER_COMMIT);
        }
        if let Err(e) = req.respond(resp) {
            eprintln!("akhet: receiver: failed to send response: {e}");
        }
    }

    fn post_event(&mut self, req: &mut Request) -> (u16, serde_json::Value) {
        let err = |s: u16, m: String| (s, serde_json::json!({"error": m}));
        let max = self.spool.max_event_bytes();
        if req.body_length().is_some_and(|n| n > max) {
            return err(413, format!("body exceeds {max} bytes"));
        }
        let mut body = Vec::new();
        if let Err(e) = req.as_reader().take(max as u64 + 1).read_to_end(&mut body) {
            return err(400, format!("reading body: {e}"));
        }
        if body.len() > max {
            return err(413, format!("body exceeds {max} bytes"));
        }
        let event_id = match event::check_body(&body) {
            Ok(id) => id,
            Err(FormatError::UnsupportedVersion(v)) => {
                return err(422, format!("unsupported event format_version {v}"));
            }
            Err(e) => return err(400, e.to_string()),
        };
        if header(req, "Idempotency-Key") != Some(event_id.as_str()) {
            return err(
                400,
                "Idempotency-Key header must equal the body event_id".into(),
            );
        }
        if let Some(sha) = header(req, "Akhet-Body-SHA256")
            && !sha.eq_ignore_ascii_case(&event::sha256_hex(&body))
        {
            return err(
                400,
                "Akhet-Body-SHA256 does not match the received body".into(),
            );
        }
        match self.spool.append_body(&body, None) {
            Ok(AppendOutcome::Inserted { seq }) => {
                (201, serde_json::json!({"status": "stored", "seq": seq}))
            }
            Ok(o) => (
                200,
                serde_json::json!({"status": "duplicate", "seq": o.seq()}),
            ),
            Err(e @ SpoolError::Conflict { .. }) => err(409, e.to_string()),
            Err(e @ SpoolError::EventTooLarge { .. }) => err(413, e.to_string()),
            Err(e @ SpoolError::Format(_)) => err(422, e.to_string()),
            Err(e) if e.is_capacity() => err(507, e.to_string()),
            Err(e) => err(500, e.to_string()),
        }
    }
}
