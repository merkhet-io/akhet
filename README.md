# Akhet

**Durable signal collection and replay: a functional Rust example.**

A small Rust collector that acquires MQTT messages and Modbus TCP holding
registers, commits each observation to a local SQLite spool, and replays
committed records to an HTTP receiver independently of acquisition. Capture
works without any downstream service.

This is infrastructure, not a CNC application, monitoring suite or high-rate
DAQ.

## Build

Requires Rust 1.89 or newer (MSRV, set in `Cargo.toml`; CI tests it). All
dependencies are pinned in `Cargo.lock`. SQLite is compiled in (`bundled`).

```sh
cargo build --release --locked
alias akhet=./target/release/akhet   # or use `cargo run --release --` below
akhet --help
```

## Local example without hardware

Development-only peers ship as Cargo examples: an embedded MQTT broker
(rumqttd, in-memory), a Modbus TCP simulator (tokio-modbus) and a QoS 1
publisher. Use separate terminals.

```sh
# Terminal 1: loopback MQTT broker on 127.0.0.1:1883 (in-memory, no auth)
cargo run --example mqtt_broker -- 1883

# Terminal 2: Modbus TCP simulator on 127.0.0.1:5020
#             (holding register N holds 100 + N, registers 0..100)
cargo run --example modbus_simulator -- 127.0.0.1:5020

# Terminal 3: poll four holding registers three times
akhet --spool data/spool.sqlite modbus --host 127.0.0.1 --port 5020 \
  --device-id 1 --address 0 --count 4 --interval-ms 1000 --timeout-ms 1000 --limit 3

# Terminal 3: collect MQTT (stop with Ctrl-C, or use --limit N)
akhet --spool data/spool.sqlite mqtt --host 127.0.0.1 --port 1883 \
  --client-id akhet-example-01 --topic 'sensors/#'

# Terminal 4: publish three QoS 1 messages
cargo run --example mqtt_publish -- 127.0.0.1:1883 sensors/demo 3

# Inspect the spool
akhet --spool data/spool.sqlite status
akhet --spool data/spool.sqlite inspect --limit 5
akhet --spool data/spool.sqlite verify

# Terminal 5: example HTTP receiver (stores into its own spool file)
akhet receive --listen 127.0.0.1:8080 --db data/receiver.sqlite

# Replay (run as often as you like; it resumes from the sink checkpoint)
akhet --spool data/spool.sqlite replay --url http://127.0.0.1:8080 --sink local-demo
akhet --spool data/receiver.sqlite inspect
```

`--spool` can also be set with `AKHET_SPOOL`. Collectors and replay print a
JSON summary on stdout at exit; logs go to stderr. Ctrl-C/SIGTERM stops any
command gracefully.

## Architecture

```text
 MQTT broker ──> mqtt adapter ───┐                         ┌─> replay ──HTTP──> receiver
                                 ├─> spool (SQLite file) ──┤   (separate        (commits, then
 Modbus TCP  ──> modbus adapter ─┘   one txn per event     │    process)         returns 2xx)
                                                           └─> status / inspect / verify
```

Crate layout (`src/`):

| Module | Role |
|---|---|
| `event.rs` | Versioned event format (`format_version` 1), encoding, checksums |
| `spool.rs` | SQLite spool: schema versioning, append, checksummed reads, capacity, sink checkpoints |
| `adapters/mqtt.rs` | MQTT 3.1.1 (rumqttc) with acknowledgment after commit |
| `adapters/modbus.rs` | Modbus TCP holding-register polling (tokio-modbus) |
| `adapters/mod.rs` | Blocking-pool spool handle, stop signal |
| `replay.rs` | HTTP replay with checkpoint binding, sink lock, bounded retries |
| `receiver.rs` | Example HTTP receiver (`akhet receive`): minimal HTTP/1.1, optional TLS and auth |
| `tls.rs` | PEM loading and rustls configuration shared by MQTT, replay and receiver |
| `faults.rs` | Fault injection for crash tests (`AKHET_FAULT`) |
| `main.rs` | CLI |

Concurrency is modest: a single-threaded Tokio runtime; SQLite work runs on
the blocking pool (at most 4 threads) so `fsync` never stalls network I/O.
Replay and the receiver are synchronous. Queues are bounded (see MQTT).

### Event format

Each record is one JSON object, encoded once at capture. Those exact bytes are
checksummed (SHA-256), stored, replayed and stored by the receiver; they are
never re-encoded. Common fields: `format_version`, `event_id` (UUIDv4,
generated once at capture and unchanged by retries), `collected_unix_ns`
(collector UTC clock), `kind`, optional `source_identity`. Readers reject
unknown `format_version` values. The format is an example contract, not a
stable public interchange format.

MQTT (`kind: "mqtt_message"`): `broker`, `client_id`, `subscription`, `topic`,
`qos`, `retain`, `dup`, `payload_base64` (raw bytes, standard base64),
`payload_len`, optional `source_identity_error`.

Modbus (`kind: "modbus_poll"`): `endpoint`, `device_id`, `function`
(`read_holding_registers`), `start_address` (zero-based), `count`,
`poll_index`, `nominal_interval_ms`, `started_unix_ns`, `completed_unix_ns`,
`duration_ns`, `new_connection`, and either `status: "ok"` with `registers`
(raw uint16) or `status: "read_failed"` with `error_kind`
(`connect`|`timeout`|`transport`|`exception`|`response_length`|`resolve`) and
`message`. Failed reads never carry register values.

## Delivery semantics

### MQTT → spool: at-least-once, acknowledged after durable commit

- The collector subscribes with QoS 1, `clean_session=false` (persistent
  session) and rumqttc `manual_acks=true`. Verified in the rumqttc 0.25.1
  source: with manual acks an incoming QoS 1 PUBLISH produces no PUBACK until
  `AsyncClient::try_ack`/`ack` is called (`state.rs`,
  `handle_incoming_publish`), and on a connection error `EventLoop::clean()`
  drops queued, unsent PUBACKs.
- Each message is committed in its own SQLite transaction; only then is its
  PUBACK queued. A PUBACK is only sent on the connection the message arrived
  on: packet ids are connection/session-scoped, so after a reconnect,
  acknowledgments for older messages are skipped (`acks_skipped_after_reconnect`)
  and the broker redelivers those messages.
- Consequences: a crash, disconnect or shutdown after commit but before the
  PUBACK reaches the broker causes redelivery, stored as a **second event with
  a new `event_id`**. Locally generated event IDs do not deduplicate upstream
  redelivery. Packet IDs are not source identities and are not stored.
- Recovery depends on the broker: it must keep the persistent session and its
  queued QoS 1 messages across collector downtime, and persist them to disk if
  they must survive broker restarts (e.g. Mosquitto `persistence true`, an
  adequate `max_queued_messages`, no session expiry shorter than your outages).
  If the broker drops the session (`session_present=false`) or its queue
  overflows, messages are lost upstream and Akhet cannot detect it. The bundled
  example broker is in-memory. Observed: rumqttd redelivers with `dup=false`;
  treat `dup` as informational only.
- The subscription is (re)issued only when the broker reports no session.
  Changing `--topic` for an existing `--client-id` requires a new client id.
- If the broker grants less than QoS 1, collection stops with an error.
  Publications sent with QoS 0 are delivered at QoS 0, recorded with `qos: 0`
  and counted (`qos0_messages`); they have no redelivery guarantee. QoS 2 is
  not supported (the subscription requests QoS 1).
- Optional source identity, only when explicitly configured:
  `--source-id-namespace NAME --source-id-json-pointer /path`. Meaning: the
  publisher guarantees that, within `NAME`, the pair (topic, JSON value at the
  pointer) identifies exactly one logical message and that republishes carry
  identical bytes. Then a redelivery of a committed message is acknowledged
  without a new record (`source_duplicates`), and reuse of the identity for
  different bytes is a conflict: collection stops and the message is not
  acknowledged. Messages without the field are stored with
  `source_identity_error` and no identity.
- Bounds: `--max-payload-bytes` (default 256 KiB) also sets the client's
  maximum incoming packet size. An oversize message stops collection with an
  error rather than being dropped or redelivered forever. `--queue-capacity`
  (default 64) bounds messages between socket and spool; when it is full, the
  collector stops reading the socket (TCP backpressure) and counts
  `queue_saturation_events`. The client library additionally buffers up to 10
  decoded packets per read. The ack-notice channel is bounded only by the
  number of unacknowledged QoS 1 messages, i.e. by the broker's in-flight
  window (at most 65535 packet ids).
- Graceful shutdown (SIGINT/SIGTERM, `--limit`): the in-progress commit
  finishes, queued uncommitted messages are dropped **without** acknowledgment
  (`dropped_unacknowledged_on_shutdown`), PUBACKs for committed messages are
  flushed, then DISCONNECT is sent (10 s grace).
- TLS: `--tls` uses the platform trust store; `--tls-ca-file` a PEM CA bundle;
  `--tls-client-cert/--tls-client-key` enable mutual TLS (rustls with ring).
  Credentials come from `AKHET_MQTT_USERNAME` / `AKHET_MQTT_PASSWORD`, never
  from arguments, and are not spooled. Authentication refusals stop collection;
  network and TLS errors are retried with exponential backoff (200 ms to 30 s).
- MQTT 3.1.1 only. Use a stable, unique `--client-id` per collector.

### Modbus TCP

- Read-only function 0x03, TCP only; no RTU, no writes, no scaling, no
  signedness/word-order interpretation. Addresses are zero-based protocol
  addresses; `count` 1..125; `address + count <= 65536`.
- One event per poll. Polls are scheduled at `--interval-ms` with missed-tick
  delay (no catch-up bursts); connection attempts happen at most once per
  interval, so an unreachable device cannot cause a busy loop. Timeouts,
  transport errors and length mismatches close the connection and the next poll
  reconnects. Modbus exceptions keep the connection.
- Timestamps are collector clocks around the request, not sensor sampling
  instants. Polling is irregular (scheduling jitter, timeouts, reconnects) and
  must not be treated as a uniformly sampled waveform. Observations missed while
  the collector is down are absent.

### Spool → receiver: at-least-once with receiver deduplication

- Protocol `akhet-replay-v1`: `GET {url}/v1/identity` returns the receiver's
  stable `receiver_id`; `POST {url}/v1/events` carries the stored bytes with
  `Idempotency-Key: <event_id>` and `Akhet-Body-SHA256`.
- The receiver commits durably (same SQLite settings) before answering `201`;
  an identical re-send of an existing `event_id` answers `200`; a different
  body for an existing `event_id` answers `409`.
- The local checkpoint advances only after a `200`/`201`, one record at a time.
  A crash or lost response after the remote commit re-sends the same
  `event_id`, which the receiver deduplicates. This is **at-least-once delivery
  with receiver-side deduplication, not exactly-once**.
- A sink name is bound on first use to the destination URL and the receiver's
  `receiver_id`. Replaying that sink to another URL, or to a receiver that
  reports a different identity (e.g. its database was reset), is refused; use a
  new sink name (it starts from the beginning and the receiver deduplicates).
- One replayer per (spool, sink): an exclusive OS file lock
  (`<spool>.replay-<hash>.lock`, released automatically if the process dies).
- Transient failures (connection errors, timeouts, 408, 429, 5xx) are retried
  with exponential backoff (250 ms doubling, capped by `--max-backoff-ms`,
  default 30 s) up to `--max-attempts` (default 8), then replay exits with code
  6. Permanent failures (409, other 4xx) stop replay at that record with exit
  code 5. Authentication failures (401/403 from either endpoint, an untrusted
  or mismatched server certificate, a client certificate the server refuses)
  are not retried and exit with code 8; the checkpoint stays where it was.
  `--follow` keeps polling for new records.
- Replay runs independently of acquisition (separate process; the SQLite busy
  timeout coordinates access). Sequence numbers are assigned inside the write
  transaction, so a checkpoint never skips a later-committed lower sequence.

### Secure replay: TLS and bearer tokens

Replay and the example receiver can use TLS (rustls with ring, the same
rustls 0.23 as the MQTT client), mutual TLS and a bearer token. All three are
optional and independent; use TLS plus at least one of token or client
certificate whenever replay crosses a network you do not control.

```sh
# Receiver: TLS, a required token, and (optionally) required client certificates
export AKHET_RECEIVER_TOKEN="$(cat /path/to/token)"
akhet receive --listen 0.0.0.0:8443 --db data/receiver.sqlite \
  --tls-cert server.pem --tls-key server.key --client-ca clients-ca.pem

# Replay: https, a private CA, the same token, and a client certificate
export AKHET_REPLAY_TOKEN="$(cat /path/to/token)"
akhet --spool data/spool.sqlite replay --url https://receiver.example:8443 --sink central \
  --ca-file ca.pem --client-cert edge.pem --client-key edge.key
```

- `https://` URLs verify the server certificate and host name against the
  platform trust store (`rustls-native-certs`), or only against `--ca-file`
  (PEM bundle) when given. `--client-cert`/`--client-key` (PEM) present a
  client certificate. TLS options with an `http://` URL are a configuration
  error (exit 2).
- The token is read only from `AKHET_REPLAY_TOKEN` / `AKHET_RECEIVER_TOKEN`
  (never from arguments, so it does not show up in process listings), is never
  printed, and is sent as `Authorization: Bearer <token>` on every request.
  Allowed characters: `A-Z a-z 0-9 - . _ ~ + / =`. An empty variable means no
  token.
- Replay refuses to send a token over plain `http://` (exit 2, before any
  connection is made) unless `--allow-insecure-token` is given. The flag exists
  for loopback use and for a TLS-terminating proxy on the same host; anywhere
  else the token would travel in cleartext. Replay never follows redirects.
- The receiver requires the token on every path (including `/v1/identity`) and
  answers `401` otherwise. It compares SHA-256 digests of the presented and
  expected tokens in constant time. With `--client-ca`, the TLS handshake fails
  unless the client presents a certificate chaining to that CA; certificate
  checks happen at the TLS layer, so they do not produce an HTTP status.
- Classification: a certificate rejected by either side and `401`/`403` are
  permanent (exit 8, no retries, checkpoint unchanged). Retrying cannot fix a
  wrong CA, host name, certificate or token, and repeated attempts with a bad
  credential only fill logs. Other TLS and network errors stay transient.
- The receiver's HTTP layer is a small HTTP/1.1 subset written for this
  example (one request per connection, `Content-Length` bodies only, bounded
  head and body, 10 s socket timeouts) so that plain TCP and TLS share one code
  path and the binary carries a single rustls version.

What this protects, and what it does not:

- Protects: confidentiality and integrity of events and the token in transit;
  the replayer's assurance that it talks to the receiver named in the URL (or
  holding a certificate from your CA); the receiver's assurance that a client
  knows the token and, with `--client-ca`, holds a key certified by that CA.
- Does not protect: **the spool and receiver databases at rest are not
  encrypted**, and the token sits in the process environment; anyone who can
  read those files or that environment gets the data or the credential. A
  token is a shared secret with no expiry, scope or rotation support beyond
  restarting both sides. A client certificate's identity is not mapped to a
  sink, source or authorization decision: any certified client may write any
  event. There is no certificate revocation checking (no CRL/OCSP), no rate
  limiting, and the single-threaded receiver can be stalled by a slow client
  for up to its socket timeout. Akhet does not sign events end to end; the
  receiver trusts whatever an authenticated client sends.
- The receiver remains an example endpoint. For production, put the protocol
  behind a service you operate and review.

## Durability, integrity and limits

- Every append is `BEGIN IMMEDIATE … COMMIT` with `journal_mode=DELETE`,
  `synchronous=FULL` and `fullfsync=ON` (F_FULLFSYNC on macOS, ignored
  elsewhere). When an append returns, SQLite has asked the OS to flush the
  journal and the database. Durability across power loss additionally requires
  a local filesystem and a storage device that honor flushes (no lying volatile
  write cache, no network filesystem). **Only process crashes were tested**
  (`abort()` fault injection, comparable to `kill -9`); the OS page cache
  survives those, so they do not demonstrate power-failure durability. No
  power-cut testing has been done.
- Versioning: the file is marked with SQLite `application_id` `0x414B4854`
  and schema `user_version` 1; other files and other schema versions are
  rejected (including the old Python prototype's spool). Events carry
  `format_version`; unsupported versions are rejected on append and on read.
- Checksums: each stored body has a SHA-256; every read (inspect, replay,
  verify) recomputes it and fails explicitly (exit 3) on mismatch. `verify`
  also runs SQLite `quick_check`.
- Identity reuse: an `event_id` stored again with identical bytes is an
  idempotent no-op; with different bytes it is rejected. The same holds for
  configured source identities.
- Capacity: `--max-db-bytes` (default 1 GiB) caps the SQLite database pages
  (`max_page_count`). When it is reached, appends fail visibly (exit 4,
  "database-size limit reached"); committed records are untouched; nothing is
  overwritten or evicted. MQTT messages then stay unacknowledged at the broker;
  Modbus polling stops. **This is a database-size limit, not a disk-space
  guarantee**: the rollback journal, lock files and other processes need
  additional space. A full filesystem below the limit is reported separately
  ("filesystem full"). Pass the same `--max-db-bytes` to every command that
  writes a spool (the limit is applied per connection).
- Retention: records are kept; this increment never deletes anything. Plan
  disk space, or rotate spool files manually after replay.
- Security: no encryption at rest. Keep spool files in an access-controlled
  local directory. Without TLS and a token (see Secure replay) bind the
  receiver to loopback or a trusted network.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success (including graceful stop) |
| 1 | Other runtime error (e.g. MQTT auth refused, oversize message, QoS downgrade) |
| 2 | Invalid arguments or configuration |
| 3 | Integrity: corruption, unsupported schema/format, conflicting identity, sink binding mismatch |
| 4 | Spool database-size limit reached or filesystem full |
| 5 | Receiver rejected a record permanently |
| 6 | Receiver unavailable after bounded retries |
| 7 | Another replayer holds this sink |
| 8 | Authentication failed: HTTP 401/403, or a TLS certificate rejected by either side (not retried) |

## Verification

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

`cargo test` runs unit tests and loopback integration tests against real
protocol peers rather than mocks: an embedded rumqttd broker, a tokio-modbus TCP
server, the akhet HTTP receiver (in-process and as a separate process), a
TLS-terminating proxy with a generated test CA, and a TCP proxy that severs
connections. Crash tests run the `akhet` binary with `AKHET_FAULT=abort:<point>`
(see `src/faults.rs`). Covered: restart after durable append; crash before and
after MQTT commit; crash before send, after remote commit and after checkpoint
during replay; receiver crash after commit; repeated replay without duplicate
receiver records; conflicting event and source identity; corrupted payload;
unsupported schema and event format; capacity exhaustion; MQTT disconnects,
redelivery and queue saturation; acknowledgments not carried across reconnects;
graceful shutdown leaving uncommitted messages unacknowledged; Modbus timeout,
exception and reconnect with explicit failed observations; concurrent-replayer
exclusion and sink binding; MQTT TLS with a trusted and an untrusted CA;
replay over TLS with a trusted CA, an untrusted CA and a wrong host name;
mutual TLS with a valid, a foreign and no client certificate; missing and
wrong tokens (401, checkpoint unchanged); refusal to send a token over plain
HTTP without the opt-in; malformed and unsupported HTTP requests.

Against an external broker (CI runs this with Mosquitto 2):

```sh
AKHET_TEST_BROKER_PORT=1883 cargo test --locked --test mqtt_external -- --ignored
```

Not covered: power-failure durability, broker restarts with persistence, real
Modbus devices, long-duration soak tests, throughput or latency benchmarks.

## Scope boundaries

- No MorletDB integration. Mapping these generic events into MorletDB needs an
  explicit contract for payload schema, units, sampling rate, source clock,
  recording identity and gaps; irregular Modbus polls are not a uniform
  waveform.
- No ML, adaptive discarding, Parquet conversion, custom storage engine,
  retention/deletion, fleet management or performance claims.

## License

MIT; see [LICENSE](LICENSE). Dependencies keep their own licences, all
permissive (MIT, Apache-2.0, BSD, ISC, Zlib, Unicode-3.0, CDLA-Permissive-2.0
and similar); `cargo metadata` lists them. Release binaries statically include
SQLite (public domain) through rusqlite's `bundled` feature.
