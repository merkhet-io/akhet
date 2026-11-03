//! Example HTTP receiver for `akhet replay` (protocol `akhet-replay-v1`).
//!
//! It stores received events in its own Akhet spool file (same format, so
//! `akhet --spool <receiver.sqlite> inspect` works). An event is committed with
//! the same synchronous SQLite transaction as the collector spool *before* a
//! success status is returned. Identical re-sends of an event_id return 200;
//! a different body for an existing event_id returns 409.
//!
//! It also implements the optional batch extension (`batch-v1`, see
//! `replay.rs`): `POST /v1/events/batch` stores up to
//! [`BATCH_MAX_RECORDS`] events in ONE transaction and answers with a
//! per-record status array after that commit.
//!
//! This is an example endpoint, not a hardened service: single-threaded, no
//! authentication, no TLS. Bind it to loopback or a trusted network only.

use crate::event::{self, FormatError};
use crate::faults::{Faults, point};
use crate::replay::BATCH_FEATURE;
use crate::spool::{AppendOutcome, BatchItem, ItemErrors, Spool, SpoolError};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tiny_http::{Header, Method, Request, Response, Server};

/// Most records accepted in one batch request.
pub const BATCH_MAX_RECORDS: usize = 1000;
/// Largest accepted batch request body.
pub const BATCH_MAX_BYTES: usize = 32 << 20;
/// Longest record header line in a batch body (without the newline).
const BATCH_MAX_HEADER: usize = 256;

pub struct Receiver {
    server: Server,
    spool: Spool,
    faults: Faults,
    batch: bool,
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
            batch: true,
        })
    }

    /// Behave like a receiver without the `batch-v1` extension (tests of the
    /// client fallback).
    #[doc(hidden)]
    pub fn without_batch(mut self) -> Self {
        self.batch = false;
        self
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
        let mut committed_new = false;
        let resp = match (req.method(), url.as_str()) {
            (Method::Get, "/v1/identity") if self.batch => json_response(
                200,
                serde_json::json!({
                    "receiver_id": self.spool.spool_id(),
                    "protocol": "akhet-replay-v1",
                    "features": [BATCH_FEATURE],
                    "batch_max_records": BATCH_MAX_RECORDS,
                    "batch_max_bytes": BATCH_MAX_BYTES,
                }),
            ),
            (Method::Get, "/v1/identity") => json_response(
                200,
                serde_json::json!({"receiver_id": self.spool.spool_id(), "protocol": "akhet-replay-v1"}),
            ),
            (Method::Post, "/v1/events") => {
                let (status, body) = self.post_event(&mut req);
                json_response(status, body)
            }
            (Method::Post, "/v1/events/batch") if self.batch => {
                let (status, body, stored) = self.post_batch(&mut req);
                if stored {
                    committed_new = true;
                }
                json_response(status, body)
            }
            _ => json_response(404, serde_json::json!({"error": "not found"})),
        };
        let committed = committed_new || resp.status_code().0 == 201;
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
        if let Err((status, msg)) = validate(
            &body,
            max,
            header(req, "Idempotency-Key"),
            header(req, "Akhet-Body-SHA256"),
            ("Idempotency-Key header", "Akhet-Body-SHA256"),
        ) {
            return err(status, msg);
        }
        append_response(self.spool.append_body(&body, None))
    }

    /// Batch extension: parse the framed records, validate each one, store
    /// the valid ones in one transaction, then report per-record statuses.
    /// The bool is true when at least one new event was committed.
    fn post_batch(&mut self, req: &mut Request) -> (u16, serde_json::Value, bool) {
        let err = |s: u16, m: String| (s, serde_json::json!({"error": m}), false);
        if req.body_length().is_some_and(|n| n > BATCH_MAX_BYTES) {
            return err(413, format!("batch body exceeds {BATCH_MAX_BYTES} bytes"));
        }
        let mut body = Vec::new();
        if let Err(e) = req
            .as_reader()
            .take(BATCH_MAX_BYTES as u64 + 1)
            .read_to_end(&mut body)
        {
            return err(400, format!("reading body: {e}"));
        }
        if body.len() > BATCH_MAX_BYTES {
            return err(413, format!("batch body exceeds {BATCH_MAX_BYTES} bytes"));
        }
        let frames = match parse_batch(&body) {
            Ok(f) => f,
            Err((status, m)) => return err(status, m),
        };
        let max = self.spool.max_event_bytes();
        // Per-record pre-checks; only valid records go into the transaction.
        let checks: Vec<Result<(), (u16, String)>> = frames
            .iter()
            .map(|f| {
                validate(
                    &body[f.body.clone()],
                    max,
                    Some(f.event_id),
                    Some(f.sha256),
                    ("record event_id", "record SHA-256"),
                )
            })
            .collect();
        let items: Vec<BatchItem<'_>> = frames
            .iter()
            .zip(&checks)
            .filter(|(_, c)| c.is_ok())
            .map(|(f, _)| BatchItem {
                body: &body[f.body.clone()],
                source: None,
            })
            .collect();
        let mut stored = match self.spool.append_batch(&items, ItemErrors::Independent) {
            Ok(r) => r.into_iter(),
            // Whole transaction failed: nothing stored, retryable.
            Err(e) => {
                let (status, v) = append_response(Err(e));
                return (status, v, false);
            }
        };
        let mut any_new = false;
        let results: Vec<serde_json::Value> = frames
            .iter()
            .zip(checks)
            .map(|(f, check)| {
                let (status, mut v) = match check {
                    Err((status, m)) => (status, serde_json::json!({"error": m})),
                    Ok(()) => append_response(stored.next().expect("one result per valid item")),
                };
                any_new |= status == 201;
                v["event_id"] = f.event_id.into();
                v["status_code"] = status.into();
                v
            })
            .collect();
        (200, serde_json::json!({"results": results}), any_new)
    }
}

/// Checks shared by single and batch posts: size, format, claimed event_id
/// and (when given) the claimed SHA-256 of the body.
fn validate(
    body: &[u8],
    max: usize,
    claimed_id: Option<&str>,
    claimed_sha: Option<&str>,
    (id_source, sha_source): (&str, &str),
) -> Result<(), (u16, String)> {
    if body.len() > max {
        return Err((413, format!("body exceeds {max} bytes")));
    }
    let event_id = match event::check_body(body) {
        Ok(id) => id,
        Err(FormatError::UnsupportedVersion(v)) => {
            return Err((422, format!("unsupported event format_version {v}")));
        }
        Err(e) => return Err((400, e.to_string())),
    };
    if claimed_id != Some(event_id.as_str()) {
        return Err((400, format!("{id_source} must equal the body event_id")));
    }
    if let Some(sha) = claimed_sha
        && !sha.eq_ignore_ascii_case(&event::sha256_hex(body))
    {
        return Err((
            400,
            format!("{sha_source} does not match the received body"),
        ));
    }
    Ok(())
}

/// HTTP status and JSON body for one append result.
fn append_response(r: Result<AppendOutcome, SpoolError>) -> (u16, serde_json::Value) {
    let err = |s: u16, m: String| (s, serde_json::json!({"error": m}));
    match r {
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

struct Frame<'a> {
    event_id: &'a str,
    sha256: &'a str,
    body: std::ops::Range<usize>,
}

/// Parse a `batch-v1` body: records of `<event_id> <sha256-hex> <length>\n`
/// followed by `<length>` body bytes and `\n`. Framing errors reject the
/// whole request (400); too many records is 413.
fn parse_batch(body: &[u8]) -> Result<Vec<Frame<'_>>, (u16, String)> {
    let bad = |m: &str| Err((400, format!("malformed batch body: {m}")));
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        if frames.len() == BATCH_MAX_RECORDS {
            return Err((413, format!("more than {BATCH_MAX_RECORDS} records")));
        }
        let rest = &body[pos..];
        let Some(nl) = rest
            .iter()
            .take(BATCH_MAX_HEADER + 1)
            .position(|b| *b == b'\n')
        else {
            return bad("record header line missing or too long");
        };
        let Ok(line) = std::str::from_utf8(&rest[..nl]) else {
            return bad("record header is not UTF-8");
        };
        let parts: Vec<&str> = line.split(' ').collect();
        let [event_id, sha256, len] = parts.as_slice() else {
            return bad("record header must be `<event_id> <sha256> <length>`");
        };
        let Ok(len) = len.parse::<usize>() else {
            return bad("record length is not a number");
        };
        let start = pos + nl + 1;
        let end = match start.checked_add(len) {
            Some(end) if end < body.len() && body[end] == b'\n' => end,
            _ => return bad("record body truncated or not followed by a newline"),
        };
        frames.push(Frame {
            event_id,
            sha256,
            body: start..end,
        });
        pos = end + 1;
    }
    if frames.is_empty() {
        return bad("no records");
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_framing() {
        let body = b"a 00 3\n{x}\nb 11 0\n\n";
        let f = parse_batch(body).unwrap();
        assert_eq!(f.len(), 2);
        assert_eq!((f[0].event_id, f[0].sha256), ("a", "00"));
        assert_eq!(&body[f[0].body.clone()], b"{x}");
        assert!(f[1].body.is_empty());
        for bad in [
            &b""[..],
            b"a 00 3\n{x}",         // missing trailing newline
            b"a 00 4\n{x}\n",       // length past the end
            b"a 00\n{x}\n",         // missing length
            b"a 00 x\n{x}\n",       // non-numeric length
            b"a 00 3 extra\n{x}\n", // extra field
        ] {
            assert_eq!(parse_batch(bad).err().map(|e| e.0), Some(400), "{bad:?}");
        }
        let long = format!("{} 00 0\n\n", "x".repeat(300));
        assert!(parse_batch(long.as_bytes()).is_err());
        let many = "a 00 0\n\n".repeat(BATCH_MAX_RECORDS + 1);
        assert_eq!(parse_batch(many.as_bytes()).err().map(|e| e.0), Some(413));
    }
}
