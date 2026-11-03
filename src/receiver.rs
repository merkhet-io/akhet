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
//! Optional security: TLS (rustls), mutual TLS (client certificates must chain
//! to a configured CA) and a bearer token (`Authorization: Bearer <token>`,
//! compared in constant time; missing or wrong tokens get 401 on every path).
//!
//! The HTTP layer is a deliberately small HTTP/1.1 subset, so that plain TCP
//! and rustls streams share one code path and one rustls version: one request
//! per connection (`Connection: close`), `Content-Length` bodies only (no
//! chunked requests), bounded request head and body, 10 s socket timeouts.
//!
//! This is an example endpoint, not a hardened service: single-threaded (one
//! connection at a time, so a slow client delays others), no rate limiting,
//! no token rotation. Prefer loopback or a trusted network, and enable TLS and
//! a token whenever traffic crosses a network you do not control.

use crate::event::{self, FormatError};
use crate::faults::{Faults, point};
use crate::replay::BATCH_FEATURE;
use crate::spool::{AppendOutcome, BatchItem, ItemErrors, Spool, SpoolError};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Socket read/write timeout per connection (also bounds the TLS handshake).
const IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Request line plus headers.
const MAX_HEAD_BYTES: usize = 16 * 1024;
const MAX_HEADERS: usize = 64;
/// Most records accepted in one batch request.
pub const BATCH_MAX_RECORDS: usize = 1000;
/// Largest accepted batch request body.
pub const BATCH_MAX_BYTES: usize = 32 << 20;
/// Longest record header line in a batch body (without the newline).
const BATCH_MAX_HEADER: usize = 256;

pub struct Receiver {
    listener: TcpListener,
    spool: Spool,
    faults: Faults,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// SHA-256 of the expected bearer token.
    token_sha256: Option<[u8; 32]>,
    /// Advertise and serve the `batch-v1` extension.
    batch: bool,
    /// Set when the current batch request committed at least one new event.
    committed_new: bool,
}

struct Head {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A request that cannot be served: answer with this status, or (for I/O and
/// TLS errors) just close the connection.
enum Bad {
    Status(u16, String),
    Io(io::Error),
}

impl From<io::Error> for Bad {
    fn from(e: io::Error) -> Bad {
        Bad::Io(e)
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Content Too Large",
        422 => "Unprocessable Content",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        505 => "HTTP Version Not Supported",
        507 => "Insufficient Storage",
        _ => "Internal Server Error",
    }
}

fn write_response<W: Write>(w: &mut W, status: u16, body: &serde_json::Value) -> io::Result<()> {
    let body = serde_json::to_vec(body).unwrap_or_default();
    let mut head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        reason(status),
        body.len()
    );
    if status == 401 {
        head.push_str("WWW-Authenticate: Bearer realm=\"akhet\"\r\n");
    }
    head.push_str("\r\n");
    w.write_all(head.as_bytes())?;
    w.write_all(&body)?;
    w.flush()
}

fn read_head<R: BufRead>(r: &mut R) -> Result<Head, Bad> {
    let bad = |s: u16, m: &str| Bad::Status(s, m.to_string());
    let mut used = 0usize;
    let mut lines: Vec<String> = Vec::new();
    loop {
        let mut buf = Vec::new();
        let limit = (MAX_HEAD_BYTES - used + 1) as u64;
        let n = r.take(limit).read_until(b'\n', &mut buf)?;
        used += n;
        if used > MAX_HEAD_BYTES {
            return Err(bad(431, "request head too large"));
        }
        if n == 0 || !buf.ends_with(b"\n") {
            return Err(Bad::Io(io::ErrorKind::UnexpectedEof.into()));
        }
        buf.pop();
        if buf.ends_with(b"\r") {
            buf.pop();
        }
        if buf.is_empty() {
            if lines.is_empty() {
                continue; // tolerate a leading empty line (RFC 9112 2.2)
            }
            break;
        }
        if lines.len() > MAX_HEADERS {
            return Err(bad(431, "too many headers"));
        }
        lines.push(String::from_utf8(buf).map_err(|_| bad(400, "request head is not UTF-8"))?);
    }
    let mut parts = lines[0].split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(bad(400, "malformed request line"));
    };
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(bad(505, "only HTTP/1.0 and HTTP/1.1 are supported"));
    }
    let mut headers = Vec::with_capacity(lines.len() - 1);
    for line in &lines[1..] {
        let Some((name, value)) = line.split_once(':') else {
            return Err(bad(400, "malformed header line"));
        };
        if name.is_empty() || name.bytes().any(|b| b.is_ascii_whitespace()) {
            return Err(bad(400, "malformed header name"));
        }
        headers.push((name.to_string(), value.trim().to_string()));
    }
    Ok(Head {
        method: method.to_string(),
        path: target.split('?').next().unwrap_or("").to_string(),
        headers,
    })
}

/// Read a `Content-Length` body of at most `max` bytes.
fn read_body<S: BufRead + Write>(s: &mut S, head: &Head, max: usize) -> Result<Vec<u8>, Bad> {
    if head.header("Transfer-Encoding").is_some() {
        return Err(Bad::Status(
            501,
            "Transfer-Encoding is not supported; send Content-Length".into(),
        ));
    }
    let mut lengths = head
        .headers
        .iter()
        .filter(|(n, _)| n.eq_ignore_ascii_case("Content-Length"))
        .map(|(_, v)| v.parse::<u64>());
    let len = match (lengths.next(), lengths.next()) {
        (None, _) => 0,
        (Some(Ok(n)), None) => n,
        _ => return Err(Bad::Status(400, "invalid Content-Length".into())),
    };
    if len > max as u64 {
        return Err(Bad::Status(413, format!("body exceeds {max} bytes")));
    }
    if head
        .header("Expect")
        .is_some_and(|e| e.eq_ignore_ascii_case("100-continue"))
    {
        s.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        s.flush()?;
    }
    let mut body = Vec::with_capacity(len as usize);
    s.take(len).read_to_end(&mut body)?;
    if body.len() as u64 != len {
        return Err(Bad::Io(io::ErrorKind::UnexpectedEof.into()));
    }
    Ok(body)
}

/// Constant-time equality of two SHA-256 digests (hashing first hides the
/// token length; the comparison does not stop at the first difference).
fn digest_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(diff) == 0
}

fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

/// Close without discarding unread client data abruptly: an immediate close
/// with unread input makes the kernel send RST, which can destroy a response
/// (or TLS alert) the client has not read yet.
fn linger_close(mut tcp: TcpStream) {
    let _ = tcp.shutdown(Shutdown::Write);
    let _ = tcp.set_read_timeout(Some(Duration::from_millis(200)));
    let end = Instant::now() + Duration::from_secs(1);
    let mut buf = [0u8; 4096];
    while Instant::now() < end {
        match tcp.read(&mut buf) {
            Ok(n) if n > 0 => {}
            _ => break,
        }
    }
}

/// Buffered reads plus unbuffered writes over the same stream.
struct Conn<S: Read + Write> {
    inner: BufReader<S>,
}

impl<S: Read + Write> Read for Conn<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<S: Read + Write> BufRead for Conn<S> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, n: usize) {
        self.inner.consume(n)
    }
}

impl<S: Read + Write> Write for Conn<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.get_mut().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.get_mut().flush()
    }
}

impl Receiver {
    /// Bind a plain-HTTP receiver without authentication. Add TLS and a
    /// token with [`Receiver::with_tls`] and [`Receiver::with_token`].
    pub fn bind(listen: &str, spool: Spool, faults: Faults) -> Result<Receiver, String> {
        let listener = TcpListener::bind(listen).map_err(|e| format!("bind {listen}: {e}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("bind {listen}: {e}"))?;
        Ok(Receiver {
            listener,
            spool,
            faults,
            tls: None,
            token_sha256: None,
            batch: true,
            committed_new: false,
        })
    }

    /// Behave like a receiver without the `batch-v1` extension (tests of the
    /// client fallback).
    #[doc(hidden)]
    pub fn without_batch(mut self) -> Self {
        self.batch = false;
        self
    }

    /// Serve HTTPS with this configuration (see [`crate::tls::server_config`]).
    pub fn with_tls(mut self, cfg: Arc<rustls::ServerConfig>) -> Self {
        self.tls = Some(cfg);
        self
    }

    /// Require `Authorization: Bearer <token>` on every request.
    pub fn with_token(mut self, token: &str) -> Self {
        self.token_sha256 = Some(sha256(token.as_bytes()));
        self
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.local_addr().ok()
    }

    pub fn receiver_id(&self) -> &str {
        self.spool.spool_id()
    }

    /// Serve until `stop` is set.
    pub fn serve(mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::SeqCst) {
            match self.listener.accept() {
                Ok((tcp, peer)) => self.connection(tcp, peer),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(e) => {
                    eprintln!("akhet: receiver: accept error: {e}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn connection(&mut self, tcp: TcpStream, peer: SocketAddr) {
        // Accepted sockets may inherit the listener's non-blocking mode.
        let setup = tcp
            .set_nonblocking(false)
            .and_then(|_| tcp.set_read_timeout(Some(IO_TIMEOUT)))
            .and_then(|_| tcp.set_write_timeout(Some(IO_TIMEOUT)));
        if let Err(e) = setup {
            eprintln!("akhet: receiver: {peer}: socket setup: {e}");
            return;
        }
        let _ = tcp.set_nodelay(true);
        let tcp = match &self.tls {
            None => {
                let mut conn = Conn {
                    inner: BufReader::new(tcp),
                };
                if let Err(e) = self.exchange(&mut conn) {
                    eprintln!("akhet: receiver: {peer}: {e}");
                }
                conn.inner.into_inner()
            }
            Some(cfg) => {
                let session = match rustls::ServerConnection::new(cfg.clone()) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("akhet: receiver: TLS setup: {e}");
                        return;
                    }
                };
                let mut conn = Conn {
                    inner: BufReader::new(rustls::StreamOwned::new(session, tcp)),
                };
                match self.exchange(&mut conn) {
                    Ok(()) => {
                        let tls = conn.inner.get_mut();
                        tls.conn.send_close_notify();
                        let _ = tls.flush();
                    }
                    // Includes handshake failures (untrusted or missing
                    // client certificate); rustls has sent the alert.
                    Err(e) => eprintln!("akhet: receiver: {peer}: {e}"),
                }
                conn.inner.into_inner().sock
            }
        };
        linger_close(tcp);
    }

    /// One request and its response. Errors are I/O or TLS failures.
    fn exchange<S: BufRead + Write>(&mut self, s: &mut S) -> io::Result<()> {
        let (status, body) = match self.handle(s) {
            Ok(r) => r,
            Err(Bad::Status(status, m)) => (status, serde_json::json!({"error": m})),
            Err(Bad::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(Bad::Io(e)) => return Err(e),
        };
        if status == 201 || std::mem::take(&mut self.committed_new) {
            self.faults.hit(point::RECEIVER_AFTER_COMMIT);
        }
        write_response(s, status, &body)
    }

    fn authorized(&self, head: &Head) -> bool {
        let Some(expected) = &self.token_sha256 else {
            return true;
        };
        let presented = head
            .header("Authorization")
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
            .map(|(_, t)| t.trim())
            .unwrap_or("");
        // Always compare, so a missing token takes the same path as a wrong one.
        digest_eq(&sha256(presented.as_bytes()), expected) && !presented.is_empty()
    }

    fn handle<S: BufRead + Write>(&mut self, s: &mut S) -> Result<(u16, serde_json::Value), Bad> {
        let head = read_head(s)?;
        if !self.authorized(&head) {
            // The body (if any) is left unread; linger_close drains it.
            return Err(Bad::Status(401, "missing or invalid bearer token".into()));
        }
        match (head.method.as_str(), head.path.as_str()) {
            ("GET", "/v1/identity") if self.batch => Ok((
                200,
                serde_json::json!({
                    "receiver_id": self.spool.spool_id(),
                    "protocol": "akhet-replay-v1",
                    "features": [BATCH_FEATURE],
                    "batch_max_records": BATCH_MAX_RECORDS,
                    "batch_max_bytes": BATCH_MAX_BYTES,
                }),
            )),
            ("GET", "/v1/identity") => Ok((
                200,
                serde_json::json!({"receiver_id": self.spool.spool_id(), "protocol": "akhet-replay-v1"}),
            )),
            ("POST", "/v1/events") => {
                let body = read_body(s, &head, self.spool.max_event_bytes())?;
                Ok(self.post_event(&head, &body))
            }
            ("POST", "/v1/events/batch") if self.batch => {
                let body = read_body(s, &head, BATCH_MAX_BYTES)?;
                let (status, v, stored) = self.post_batch(&body);
                self.committed_new = stored;
                Ok((status, v))
            }
            _ => Err(Bad::Status(404, "not found".into())),
        }
    }

    fn post_event(&mut self, head: &Head, body: &[u8]) -> (u16, serde_json::Value) {
        if let Err((status, m)) = validate(
            body,
            self.spool.max_event_bytes(),
            head.header("Idempotency-Key"),
            head.header("Akhet-Body-SHA256"),
            ("Idempotency-Key header", "Akhet-Body-SHA256"),
        ) {
            return (status, serde_json::json!({"error": m}));
        }
        append_response(self.spool.append_body(body, None))
    }

    /// Batch extension: parse the framed records, validate each one, store
    /// the valid ones in one transaction, then report per-record statuses.
    /// The bool is true when at least one new event was committed.
    fn post_batch(&mut self, body: &[u8]) -> (u16, serde_json::Value, bool) {
        let err = |s: u16, m: String| (s, serde_json::json!({"error": m}), false);
        let frames = match parse_batch(body) {
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

    fn head(raw: &str) -> Result<Head, u16> {
        read_head(&mut io::Cursor::new(raw.as_bytes().to_vec())).map_err(|e| match e {
            Bad::Status(s, _) => s,
            Bad::Io(_) => 0,
        })
    }

    #[test]
    fn parses_request_head() {
        let h =
            head("\r\nPOST /v1/events?x=1 HTTP/1.1\r\nHost: a\r\nX-A:  b \r\n\r\nbody").unwrap();
        assert_eq!(h.method, "POST");
        assert_eq!(h.path, "/v1/events");
        assert_eq!(h.header("x-a"), Some("b"));
        assert_eq!(head("GET / HTTP/2\r\n\r\n").err(), Some(505));
        assert_eq!(head("GET /\r\n\r\n").err(), Some(400));
        assert_eq!(
            head("GET / HTTP/1.1\r\nBad Name: x\r\n\r\n").err(),
            Some(400)
        );
        assert_eq!(head("GET / HTTP/1.1\r\nHost: a\r\n").err(), Some(0));
        let big = format!(
            "GET / HTTP/1.1\r\nX: {}\r\n\r\n",
            "a".repeat(MAX_HEAD_BYTES)
        );
        assert_eq!(head(&big).err(), Some(431));
    }

    #[test]
    fn token_comparison() {
        let a = sha256(b"secret");
        assert!(digest_eq(&a, &sha256(b"secret")));
        assert!(!digest_eq(&a, &sha256(b"secreT")));
        assert!(!digest_eq(&a, &sha256(b"")));
    }

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
