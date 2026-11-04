<p align="center"><img src="assets/brand/readme-hero.svg" alt="Akhet — Commit first. Ack after. Durable edge collection for MQTT and Modbus TCP: a local SQLite spool and independent replay to HTTP receivers and MorletDB." width="100%"></p>

<p align="center">
<span title="Egyptian hieroglyph akhet, U+1320C">𓈌</span><br>
<i>ꜣḫt</i>, “horizon”<br>
<sub>Gardiner N27: the sun between two hills, the place where it rises and sets</sub>
</p>

<p align="center">
<b>Akhet is a durable signal collector for the edge, written in Rust.</b><br>
It takes MQTT messages and Modbus TCP registers at the machine, commits each one to a local<br>
SQLite spool, and replays them later to an HTTP receiver or into MorletDB.
</p>

<p align="center">
<a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-a8df73?labelColor=090c0d"></a>
<a href="https://github.com/merkhet-io/akhet/actions/workflows/ci.yml"><img alt="Rust CI" src="https://github.com/merkhet-io/akhet/actions/workflows/ci.yml/badge.svg"></a>
<img alt="Rust 1.89+" src="https://img.shields.io/badge/Rust-1.89+-a8df73?labelColor=090c0d&logo=rust&logoColor=e4e8e6">
<img alt="MQTT 3.1.1 | Modbus TCP" src="https://img.shields.io/badge/MQTT_3.1.1-Modbus_TCP-24302e?labelColor=090c0d">
<a href="https://github.com/merkhet-io/morletdb"><img alt="Sink: MorletDB" src="https://img.shields.io/badge/sink-MorletDB-24302e?labelColor=090c0d&logo=postgresql&logoColor=e4e8e6"></a>
</p>

<p align="center">
<a href="#build">Build</a> · <a href="#architecture">Architecture</a> · <a href="#delivery-semantics">Delivery</a> · <a href="#morletdb-sink">MorletDB sink</a> · <a href="#retention">Retention</a> · <a href="#durability-integrity-and-limits">Limits</a> · <a href="docs/benchmarks.md">Benchmarks</a>
</p>

---

## Why Akhet

**Commit first, acknowledge after.** An MQTT message is acknowledged to the
broker only once it is committed to the local spool. A crash, a disconnect or a
dead uplink leads to a redelivery, not to the loss of an acknowledged message.
Process crashes are tested; power cuts are not yet (see
[Durability](#durability-integrity-and-limits)).

- **Capture without a downstream.** Acquisition only writes to local disk.
  Replay is a separate process that resumes from a checkpoint per sink.
- **Byte-exact and checked.** Each record is checksummed at capture and
  replayed with the exact stored bytes; the receiver deduplicates by
  `event_id`.
- **From messages to recordings.** Optional rules decode JSON payloads into
  per-channel sample streams on a fixed rate, measure timing against that
  grid, and write them into MorletDB with the controller context alongside.

This is infrastructure, not a CNC application, monitoring suite or high-rate
DAQ. The name is the Egyptian word for the horizon: the edge where things are
first seen, before they reach the archive.

## Build

Requires Rust 1.89 or newer (MSRV, set in `Cargo.toml`; CI tests it). All
dependencies are pinned in `Cargo.lock`. SQLite is compiled in (`bundled`).

```sh
cargo build --release --locked
alias akhet=./target/release/akhet   # or use `cargo run --release --` below
akhet --help
```

## Deployment

Tagged releases (`v*`) publish static Linux binaries for x86_64 and aarch64
with `SHA256SUMS` and GitHub build-provenance attestations, plus a multi-arch
container image (`ghcr.io/merkhet-io/akhet`, distroless, non-root, `/data`
volume). `deploy/` has hardened systemd units and a single-replica Kubernetes
StatefulSet. [docs/deployment.md](docs/deployment.md) covers installation,
verification, the one-collector-per-spool rule and the upgrade path for an
existing spool (today a build refuses a spool with a different schema version
rather than migrating it).

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

# Terminal 3: collect MQTT (stop with Ctrl-C, or use --limit N);
#             repeat --topic for several filters
akhet --spool data/spool.sqlite mqtt --host 127.0.0.1 --port 1883 \
  --client-id akhet-example-01 --topic 'sensors/#' --topic 'plant/line1/+'

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

# Delete records every bound sink has acknowledged (see Retention)
akhet --spool data/spool.sqlite prune --min-age 0s --dry-run
akhet --spool data/spool.sqlite prune --min-age 0s
```

`--spool` can also be set with `AKHET_SPOOL`. Collectors and replay print a
JSON summary on stdout at exit; logs go to stderr. Ctrl-C/SIGTERM stops any
command gracefully.

### Settings file

Options can also come from a TOML file (`--config-file` or
`AKHET_CONFIG_FILE`). Keys are the long option names (`client-id` or
`client_id`). Global options go at the top; the others go in a table per
command, so one file can serve the collector and its replay:

```toml
spool = "/var/lib/akhet/spool.sqlite"
max-db-bytes = 1073741824

[mqtt]
host = "broker.local"
topic = ["plant/+/sensors", "plant/+/state"]   # repeated options are lists
client-id = "akhet-edge-01"
tls = true                                     # switches are true or false

[replay]
url = "https://collector.example"
sink = "central"
follow = true
```

```sh
akhet --config-file akhet.toml mqtt
akhet --config-file akhet.toml replay --limit 100   # the command line still wins
```

- **Precedence:** command line, then the option's environment variable (such
  as `AKHET_SPOOL`), then the file, then the default. A list on the command
  line replaces the file's list.
- **Checked:** values go through the same parsing as on the command line. An
  unknown key or table, a wrong shape (a list for a single value, a string
  for a switch) or an invalid value stops with exit 2.
- **No secrets:** passwords, tokens and the MorletDB connection string are
  environment variables only; the file has no keys for them.
- Tables for other commands are ignored. Relative paths are relative to the
  working directory, as on the command line.
  [`deploy/systemd/akhet.toml`](deploy/systemd/akhet.toml) is the example
  used by the systemd units.

## Architecture

```text
 MQTT broker ──> mqtt adapter ───┐                         ┌─> replay ──HTTP──> receiver
                                 ├─> spool (SQLite file) ──┤   (separate        (commits, then
 Modbus TCP  ──> modbus adapter ─┘   one txn per event     │    process)         returns 2xx)
                                     or MQTT batch
                                                           └─> status / inspect / verify
```

Crate layout (`src/`):

| Module | Role |
|---|---|
| `event.rs` | Versioned event format (`format_version` 1 and 2), encoding, checksums |
| `spool.rs` | SQLite spool: schema versioning, append, checksummed reads, capacity, sink checkpoints, pruning |
| `adapters/mqtt.rs` | MQTT 3.1.1 (rumqttc) with group commit and acknowledgment after commit |
| `adapters/modbus.rs` | Modbus TCP holding-register polling (tokio-modbus) |
| `adapters/mod.rs` | Blocking-pool spool handle, stop signal |
| `replay.rs` | HTTP replay with checkpoint binding, sink lock, bounded retries, optional batch requests, TLS and token auth |
| `receiver.rs` | Example HTTP receiver (`akhet receive`): minimal HTTP/1.1, optional TLS and auth, batch extension |
| `tls.rs` | PEM loading and rustls configuration shared by MQTT, replay, receiver and the MorletDB connection |
| `pgtls.rs` | MorletDB connection TLS: libpq `sslmode`/`sslrootcert`/`sslcert`/`sslkey`, secure default for remote hosts |
| `faults.rs` | Fault injection for crash tests (`AKHET_FAULT`) |
| `metrics.rs` | Optional Prometheus metrics, liveness and readiness endpoint |
| `main.rs` | CLI |

Concurrency is modest: a single-threaded Tokio runtime; SQLite work runs on
the blocking pool (at most 4 threads) so `fsync` never stalls network I/O.
Replay and the receiver are synchronous. Queues are bounded (see MQTT).

### Event format

Each record is encoded once at capture. Those exact bytes are checksummed
(SHA-256), stored, replayed and stored by the receiver; they are never
re-encoded. Format version 1 (the default) is one JSON object; format version 2
(MQTT only, see [Payload storage](#payload-storage)) is a JSON header line
followed by the raw payload. Common fields: `format_version`, `event_id` (UUIDv4,
generated once at capture and unchanged by retries), `collected_unix_ns`
(collector UTC clock), `kind`, optional `source_identity`. Readers reject
unknown `format_version` values. The format is an example contract, not a
stable public interchange format.

MQTT (`kind: "mqtt_message"`): `broker`, `client_id`, `subscription`,
`subscriptions`, `topic`, `qos`, `retain`, `dup`, `payload_base64` (raw bytes,
standard base64), `payload_len`, optional `source_identity_error`.
`subscriptions` lists every configured `--topic` filter that matches `topic`
(MQTT 3.1.1 wildcard rules for `+` and `#`), in configuration order;
`subscription` is the first of them, or `""` when none matches (see below).
`subscriptions` was added within `format_version` 1: records written before it
lack the field, and in them `subscription` is the single configured filter.
Readers that ignore unknown fields are unaffected.

#### Payload storage

`mqtt --payload-storage base64|raw` (default `base64`) chooses how MQTT
payloads are stored:

- `base64`: format version 1, `payload_base64` inside the JSON object. Base64
  adds a third to the payload; any JSON consumer can read the record.
- `raw`: format version 2. The body is the same JSON object **without**
  `payload_base64`, then one newline (`\n`), then exactly `payload_len` raw
  payload bytes. Compact JSON never contains a raw newline, so the first
  newline ends the header; the payload may contain anything. Readers check
  that the byte count matches `payload_len` and that version 2 is used only
  for `mqtt_message`. On the 8 × 4000-sample JSON benchmark payload the spool
  shrinks from 8.26 to 6.21 bytes per sample (−25%, within 1% of the payload
  itself), and both spools return the published payloads bit for bit
  ([benchmarks](docs/benchmarks.md#spool-size-per-sample)).

Both versions can share a spool, and every reader in this build (`inspect`,
`decode`, `replay`, the receiver, the MorletDB sink) accepts both. `inspect`
prints version 2 records with the payload re-attached as `payload_base64`, so
its output has one shape; the stored bytes are unchanged. Compatibility:
builds before version 2 refuse these records (unsupported `format_version`),
so a receiver must run a build that reads version 2, or replay stops with exit
5 (`422`). Replay sends single version 2 bodies as
`Content-Type: application/vnd.akhet.event.v2`.

Modbus (`kind: "modbus_poll"`): `endpoint`, `device_id`, `function`
(`read_holding_registers`), `start_address` (zero-based), `count`,
`poll_index`, `nominal_interval_ms`, `started_unix_ns`, `completed_unix_ns`,
`duration_ns`, `new_connection`, and either `status: "ok"` with `registers`
(raw uint16) or `status: "read_failed"` with `error_kind`
(`connect`|`timeout`|`transport`|`exception`|`response_length`|`resolve`) and
`message`. Failed reads never carry register values.

### Payload decoding (read-only)

`akhet decode --config <file>` maps spooled MQTT payloads to per-channel
sample batches. It reads the spool and never modifies it: the raw payload bytes
stay stored exactly as received, and a corrected decoder config can be re-run
over the same records.

```sh
akhet --spool data/s.sqlite decode --config examples/decoder.json            # counts and ranges
akhet --spool data/s.sqlite decode --config examples/decoder.json --samples  # full arrays
akhet --spool data/s.sqlite decode --config examples/decoder.json --summary-only
```

Supported shape: one message holds one or more *signal groups*; each group has
one timestamp array and one value array per signal, all of equal length. The
config (JSON, `format_version: 1`, see `examples/decoder.json`) has one rule
per topic filter; the first matching rule applies:

| Field | Meaning |
|---|---|
| `topic` | MQTT filter (`+`, `#`) |
| `source` | Source template: `{topic}`, `{topic.N}` (zero-based level), `{/json/pointer}` into the message |
| `groups` | Optional JSON pointer to the array of groups; absent = the message is one group |
| `group` | Group name template; additionally `{group:/pointer}` into the group |
| `timestamps`, `timestamp_unit` | Pointer to the integer timestamp array; `ns`, `us`, `ms` or `s` since the Unix epoch |
| `signals` | `{"map": "/pointer"}` (object of name → array) or `{"list": [{"name", "pointer"}]}` |
| `units`, `default_unit` | Unit per signal name; a signal without a unit is an issue, never unlabelled |
| `metadata` | Static JSON object attached to every channel |

A channel is identified by `(source, group, signal)`. Validation per group:
integer timestamps, strictly increasing within the group, equal array lengths,
finite numeric values, a configured unit, and no channel twice in one message.
Every failure is an explicit issue with its kind (`no_matching_rule`,
`not_json`, `bad_structure`, `bad_template`, `bad_timestamp`,
`non_monotonic_timestamps`, `length_mismatch`, `non_finite_value`, `no_unit`,
`too_large`, `duplicate_channel`), rule, topic, group index, signal and sample
index. A group with an issue yields no batches, so a group is never partly
decoded; other groups of the same message still decode. Values are parsed as
IEEE 754 float64 from the JSON text and are bit-exact for numbers that
round-trip through float64.

Decoding does not interpret timing: it reports the publisher's timestamps
as given. Converting timestamps to a uniform sample grid is a separate step.

### Sample grid placement (read-only)

`akhet grid --config <file> [--chunks [--samples]]` places decoded samples of
each channel on a uniform grid (sample rate, anchor timestamp, integer sample
offsets), the form a uniformly sampled store needs. Dropping per-sample
timestamps is only honest when they lie on a regular grid, so placement is
strict:

- A sample within the tolerance of the next grid position is placed there.
- A sample within tolerance of a *later* grid position ends the recording
  (`gap`, with the number of missing grid positions); one off the grid ends it
  as `off_grid` or, when the re-estimated rate differs, `rate_change`. The next
  recording starts at that sample and records the discontinuity (reason,
  previous recording, its last timestamp, gap length). Missing values are
  never invented.
- A timestamp at or before the latest one is never reordered: an exact repeat
  (same timestamp and value bits, within the last `dedup_window_samples`) is
  skipped as a duplicate, which absorbs redelivered batches; a different value
  is a `conflict` issue; anything else is an `out_of_order` issue.
- Samples are never interpolated, resampled or moved by more than the
  tolerance. The reconstructed timestamp of sample `k` is
  `anchor_unix_ns + round(k * 1e9 / rate_hz)`.

Per rule, `timing` sets `rate_hz` (absent: estimated per recording from the
median spacing of its first `estimate_samples` samples, rounded to
`rate_resolution_hz`), `tolerance_ns` or `tolerance_fraction` of a period
(default 0.1), `dedup_window_samples` (8192) and `max_chunk_samples` (32768).

Recording keys are deterministic: `<source>/<group>/<signal>@<anchor_unix_ns>`
(parts percent-encoded). Each chunk carries timing evidence: the original
timestamps of its first and last sample and the maximum and RMS deviation from
the grid. The summary reports per channel: samples in and placed, duplicates,
conflicts, out-of-order and unplaced samples, recordings, splits by reason,
missing samples in gaps, maximum and RMS reconstruction error, and
`samples_beyond_tolerance` (zero by construction, reported as a check).

The grid state of a channel is serializable so a replay sink can persist it
atomically with its checkpoint. Clock drift of the source is not corrected; it
shows up as growing deviation and, beyond the tolerance, as splits.
### Context timelines (read-only)

Controller signals such as program number, tool, part count or feed override
change irregularly and are not waveforms. List them in a rule's
`context_signals`; their values may be numbers, strings (up to 1024 bytes) or
booleans and need no unit (`bad_context_value` otherwise).

`akhet context --config <file>` turns their observations into change events
with last-value-held semantics, printed as JSON lines with a summary per
channel:

- Repeated observations of the value in effect produce no event.
- A newer observation with a different value is a change event with the
  publisher timestamp (`clock: "source"`), the collector timestamp of the
  message (`collected_unix_ns`) and the previous value.
- A late or out-of-order observation never rewrites the timeline: if it agrees
  with the value in effect at its time it is counted as `late_consistent`
  (this absorbs redelivered messages); otherwise it is a `late_conflict` issue,
  or `late_unknown` when it predates the retained history (4096 changes per
  channel).

The timeline is a deterministic function of the spooled records in order, so
it can be rebuilt exactly from the spool. Values compare as JSON values (`1`
and `1.0` differ).

### MorletDB sink

`akhet morlet` replays the spool into PostgreSQL with the `pg_morlet`
extension. Per record: `decode` → `grid` per waveform channel → recordings and
chunks in a MorletDB stream collection; `context_signals` → interval rows in
the collection's context timeline.

```sh
export AKHET_MORLET_URL='postgresql://user:password@db.example:5432/morlet?sslrootcert=/etc/akhet/db-ca.pem'   # never on argv
akhet --spool data/s.sqlite morlet --sink central --collection signals \
  --config decoder.json --create-collection --follow
```

- **Recordings:** one per grid recording, created with
  `morlet_ensure_recording(rate, channel = "<source>/<group>/<signal>", unit,
  metadata)`. Metadata holds the rule's static metadata plus an `akhet`
  object: source, group, signal, anchor timestamp, whether the rate was
  estimated, the tolerance and the discontinuity that started it. Chunks go
  through `morlet_append_chunk` at the grid's sample offsets; values are sent
  as binary float64 and read back bit-exactly.
- **Acquisitions:** a source's recordings are grouped into bounded MorletDB
  acquisitions. Each acquisition is a run of the source from one boundary to
  the next. Every channel is cut at the boundary, so each acquisition holds
  exactly one recording per channel, and channels keep a stable role
  `<group>/<signal>` across acquisitions.
  - **Reading:** `morlet_read_acquisition(collection, id, t_start_ns,
    t_end_ns)` reads all channels of one run on one time axis (nanoseconds
    from the origin). `morlet_compare_acquisition` and history comparisons
    can match channels by role across runs.
  - **Boundaries (a new acquisition starts):**
    - at the source's first recording;
    - at a discontinuity of any channel (gap, off-grid sample, rate change);
    - at a change of a context key listed in `--acquisition-context`
      (repeatable or comma-separated, e.g. `program`);
    - at multiples of `--acquisition-period-s` since the Unix epoch
      (default 86400: every UTC midnight; `0` disables).
  - **Cutting:** at a boundary, every waveform channel of the source is cut
    on its own grid. The recording ends before the first sample at or after
    the boundary, and a new recording starts there with the same rate:
    nothing is lost, repeated or re-estimated. The recording metadata holds
    `cut_at_unix_ns`, and the grid report counts `splits_cut`.
    - A channel whose samples are already past the boundary (a context
      message that arrived late) is cut at its next sample. Its offset in
      the new acquisition is then slightly positive, and still exact.
    - A channel seen for the first time after boundaries were declared (data
      arriving late) is cut at the last 16 boundaries of its source and
      registered in the matching acquisitions.
    - If a late channel would need a role that is already taken in a past
      acquisition, its recording is not registered there, and an issue of
      kind `role_taken` is written.
  - **Id and origin:** id `<source>@<origin unix ns>`, machine = the source
    name. The origin is the boundary truncated to the microsecond
    (`timestamptz` resolution). Each channel's `offset_ns` is its first
    sample's time minus the origin (exact), with clock `daq` and offset
    uncertainty = the grid tolerance.
  - **Metadata:** each acquisition records why it was opened
    (`akhet.opened_by`: `start`, `discontinuity`, `context` with key and
    values, `period`, or `role_reused`).
  - **Creation:** an acquisition is created with its first channel, so a
    boundary that no samples follow creates nothing. Registration is in the
    same transaction as the samples; MorletDB accepts identical retries.
  - **Enabling:** `--create-collection` enables acquisitions on the
    collection (also on an existing one). Without that flag, a collection
    without acquisition tables gets a warning and no registration or cuts.
    `--no-acquisitions` turns both off.
  - **Context:** still written per recording (below), not with
    `morlet_add_acquisition_context`.
- **Chunks:** within one transaction, the contiguous samples of a recording
  are merged into one chunk of at most the channel's `max_chunk_samples`
  (default and MorletDB maximum 32,768). They are written before the
  transaction's context rows and its commit.
  - With small messages, chunk size therefore follows `--records-per-tx`,
    not the message size: for example, 100 records of 10 samples give
    1,000-sample chunks.
  - A gap, a new recording or the size limit starts a new chunk.
  - Committed records are never sent again, so different chunk boundaries
    between runs cannot conflict.
- **Chunk summaries:** on by default (pg_morlet 0.3.4-alpha.1 or later).
  MorletDB then stores count, min, max and sums with every chunk, so
  `morlet_interval_stats` and `morlet_interval_overview` over hours or days
  read summaries instead of decoding the samples. The results are bitwise the
  same; without summaries an interval is limited to 16,777,216 samples.
  - `--create-collection` enables them (`morlet_enable_chunk_summaries`), also
    on an existing collection. Chunks stored before then have no summary;
    Akhet prints a note, and `morlet_build_chunk_summaries(collection,
    recording_id)` fills them in.
  - Without that flag, a collection without summaries gets a warning and is
    left unchanged. An older pg_morlet without the functions also gets a
    warning. `--no-chunk-summaries` turns all of this off.
- **Feature extraction:** on by default, so recordings are searchable by
  features (`morlet_search_interval`, `morlet_search_streams`). With
  `--extractor auto` (the default) each new recording gets pg_morlet's
  extractor for its rate: `morlet-energy-v1` for 128–200,000 Hz,
  `morlet-energy-lowrate-v1` for 1–128 Hz, and `none` (raw samples only)
  outside both. Window 512, stride 256 and `zscore` normalization unless set.
  - `--extractor none` turns it off: raw samples only, no segment rows; such
    recordings are skipped by feature search.
  - Any other value is passed to MorletDB as the extractor version.
  - The choice is fixed per recording when it is created. Changing the flag
    affects new recordings only, so a running collection can mix raw-only and
    searchable recordings.
  - Extraction runs synchronously in the append, in the sink's transaction,
    and so costs ingest time.
  - Similarity search by envelopes (`morlet_search_similar`) is separate: it
    needs `morlet_enable_envelopes` and `morlet_build_envelopes`, which the
    sink does not run.
- **Timing evidence:** per chunk in `akhet_morlet_evidence`:
  - first and last original timestamp;
  - max and RMS deviation over the merged samples;
  - the last spool seq that contributed samples.
  Evidence rows tile each recording exactly.
- **Explicit issues:** decode, timing and context issues go to
  `akhet_morlet_issues` (sink, spool seq, kind and details).
- **Context:** for each context key of a source, the value in effect becomes
  an interval `[start_sample, end_sample)` on every recording of the same
  source it overlaps (key = signal name). Times map to the first sample at or
  after the change.
  - **When rows appear:** an interval is written once its end is known: at
    the next change of that key, or clipped at the end of a recording.
  - **Lag:** the value currently in effect is therefore missing from the
    collection's context timeline (`<collection>_ctx`) and from context
    segments and comparisons. On an active recording it is absent for as
    long as it stays in effect: the lag has no upper bound. A program that
    runs for a whole shift appears only when the next program starts, or
    when the recording ends (gap or discontinuity).
  - **Why no open rows:** pg_morlet 0.3.3 has no open-ended interval that can
    be extended. Revising the row on every transaction would add a revision
    per key and recording each time, and mark segments stale. Closing and
    reopening at chunk boundaries would split one program run into many
    segments, and compare-to-history works per segment.
  - **The value in effect now:** `akhet_morlet_context_current`, one row per
    sink, source and key. It holds the value, `since_unix_ns` (source clock),
    `collected_unix_ns` and the spool seq. It is updated in the same
    transaction as the samples whenever a value changes. To find "now" in a
    recording, map `since_unix_ns` to samples with the recording's anchor
    and rate, or use the acquisition's time axis.
- **Exactness:** derived state (grid state per channel, context timelines,
  written context offsets) lives in `akhet_morlet_sinks`. Each grid's
  duplicate-detection window lives in `akhet_morlet_recent`, as binary blocks
  of placed samples. Each transaction adds the new samples and drops blocks
  that left the window, so `-0.0` and every other bit pattern survive a
  restart. Both are updated **in the same transaction** as the samples, context rows and the sink's spool
  checkpoint. On any error or crash, the transaction rolls back and the sink
  resumes from the committed state. The local checkpoint is advanced after the
  commit and moved forward from the database if a crash came in between; a
  database checkpoint *behind* the local one (restored database) is refused.
  Crash tests at the existing replay fault points show no lost or duplicated
  samples.
- **Binding:** the database gets a random `database_id` (table
  `akhet_morlet_identity`), used like a receiver identity; a sink name is
  bound to one spool, one collection and one database. Re-using it elsewhere
  is refused (exit 3).
- **Errors:** connection-class failures are retried with bounded backoff
  (exit 6 when exhausted). Anything MorletDB rejects (conflicting samples at
  an existing offset, overlapping context, a rate or chunk it refuses) is
  permanent (exit 5), with the database message.
- **TLS:** libpq parameters in `AKHET_MORLET_URL` (URL query or
  `key=value`), handled by Akhet with rustls (the same rustls 0.23 and ring as
  MQTT and replay):
  - `sslmode`: `verify-full` (chain and host name), `verify-ca` (chain only),
    `require` (encrypted, server not authenticated; a warning is printed) or
    `disable`. `allow` and `prefer` are refused because they can fall back to
    plain text.
  - **Default:** without `sslmode`, a loopback host (`localhost`, 127.0.0.0/8,
    `::1`) or a Unix socket connects without TLS, and **any other host uses
    `verify-full`** (a notice is printed). Set `sslmode=disable` explicitly for
    a remote server without TLS.
  - `sslrootcert`: PEM file of trusted CAs; absent or `system` means the
    platform trust store (libpq uses `~/.postgresql/root.crt` instead). As in
    libpq, `require` with an explicit `sslrootcert` verifies the chain.
  - `sslcert` and `sslkey`: client certificate (both or neither).
  - An untrusted or misnamed server certificate, a refused client
    certificate or a server without TLS is permanent: exit 8, not retried.
    The summary reports the effective `sslmode`; the URL and password are
    never printed.
- **Privileges:** the role needs to create the `akhet_morlet_*` tables in its
  schema (once) and use the collection; `--create-collection` additionally
  creates the stream collection and its context tables.

Limits: one sink transaction holds `--records-per-tx`
records. The state row (about 1 KB per channel) is rewritten on every
transaction; the duplicate-detection windows (`dedup_window_samples` per
channel) are stored incrementally in `akhet_morlet_recent`. Two sinks must
not write the same recordings. `--window-size`, `--stride`,
`--normalization` and `--sample-quantum` are passed to MorletDB for new
recordings.

## Delivery semantics

### MQTT → spool: at-least-once, acknowledged after durable commit

- The collector subscribes with QoS 1, `clean_session=false` (persistent
  session) and rumqttc `manual_acks=true`. Verified in the rumqttc 0.25.1
  source: with manual acks an incoming QoS 1 PUBLISH produces no PUBACK until
  `AsyncClient::try_ack`/`ack` is called (`state.rs`,
  `handle_incoming_publish`), and on a connection error `EventLoop::clean()`
  drops queued, unsent PUBACKs.
- Group commit: the committer takes the next message plus whatever is
  already queued, up to `--max-batch` messages (default 64), and commits them
  in ONE SQLite transaction; only after that commit are their PUBACKs queued,
  in arrival order. If the transaction fails, no message of the batch is
  acknowledged. At low rates a batch is a single message, so latency does not
  change; `--max-linger-ms` (default 0) optionally waits for more messages
  before committing. `--max-batch 1` restores one transaction per message.
  Each message keeps its own event, `event_id` and dedup checks inside the
  batch. If one message fails its checks (a conflicting source identity), the
  messages before it are committed and acknowledged, it and the rest of the
  batch are not, and collection stops. A PUBACK is only sent on the connection the message arrived
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
- Tested with Mosquitto 2 and `persistence true` (`ci/mosquitto-persist.conf`,
  `autosave_interval 1`): queued messages, and messages Akhet committed but did
  not acknowledge, survive a clean broker restart and a `SIGKILL` after an
  autosave; a running collector reconnects across the restart without loss.
  The same tests fail against `persistence false`. After a broker crash,
  messages newer than its last autosave are lost upstream; Mosquitto's
  default `autosave_interval` is 1800 s, so set it to what you can afford to
  lose.
- Several filters: `--topic` is repeatable. Filters are validated (non-empty,
  MQTT 3.1.1 wildcard syntax, no duplicates) and subscribed with QoS 1 in one
  SUBSCRIBE; every SUBACK return code is checked. Overlapping filters (e.g.
  `plant/#` and `plant/line1/+`) are allowed; the broker may then deliver a
  message once or once per matching filter, and each delivered copy is stored.
  All filters share one persistent session, one queue and one spool.
- Subscriptions of an existing session: re-subscribing would make the broker
  re-send retained messages, so with `session_present=true` the collector only
  applies the difference to the filter set it last confirmed for this broker
  address and client id. That set is recorded in the spool's `meta` table
  (not an event, never replayed), written before each change and after the
  broker acknowledges it. Added filters are subscribed (retained messages for
  them arrive once); removed filters are unsubscribed. With
  `session_present=false` the broker holds no subscriptions and all filters
  are subscribed. Consequences:
  - Messages the broker queued for a removed filter before it processed the
    UNSUBSCRIBE are still delivered; they are stored with `subscription: ""`,
    `subscriptions: []` and counted in `unmatched_messages`.
  - A removal is recorded once the broker sends UNSUBACK; until then the next
    start sends the UNSUBSCRIBE again. Capture of the configured filters does
    not wait for it. Observed: the bundled example broker (rumqttd 0.20) never
    answers UNSUBSCRIBE on a resumed session and keeps delivering the removed
    filter; Mosquitto 2 removes it.
  - The record lives in the spool. If the spool has none for a session the
    broker still holds (new or replaced spool file, or a collector built
    before this change), all configured filters are subscribed (retained
    messages arrive again) and filters subscribed earlier that are no longer
    configured cannot be identified, so they are not removed. Do not share a
    `--client-id` between spools or tools; to start from a clean slate, use a
    new client id.
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
  (one batch) finishes, queued uncommitted messages are dropped **without** acknowledgment
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
- The local checkpoint advances only after a `200`/`201`, one record at a time
  (or, with batches, over the successful prefix of a batch; see below).
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
- Optional batch extension `batch-v1` (opt-in with `--records-per-request N`,
  default 1). The receiver advertises it in the identity response
  (`"features": ["batch-v1"]`, plus `batch_max_records`/`batch_max_bytes`);
  otherwise replay falls back to single POSTs. `POST {url}/v1/events/batch`
  carries up to N stored bodies byte-exact, each framed as
  `<event_id> <sha256-hex> <length>\n<body>\n`, and at most
  `--max-request-bytes` (default 8 MiB) per request. The receiver checks each
  record like a single POST, commits the accepted ones in one transaction,
  then answers `200` with one `status_code` per record (`201`, `200`, `409`,
  …, same meaning as for single POSTs). The checkpoint advances over the
  contiguous prefix of `201`/`200` only; the first other code is handled
  like the same code on a single POST. A whole-request `408`/`429`/`5xx`
  is retried as a unit. A crash or lost response re-sends up to a whole
  batch, which the receiver deduplicates per record; records after a rejected
  one may already be stored and show up as duplicates later. Details are in
  the `src/replay.rs` module docs. Batch requests carry the same bearer token and
  TLS settings as single POSTs.
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

- Every append (one event, or one MQTT group-commit batch) is
  `BEGIN IMMEDIATE … COMMIT` with `journal_mode=DELETE`,
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
  `format_version` (1, or 2 for raw MQTT payloads; the spool schema is the
  same for both); unsupported versions are rejected on append and on read.
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
- Capacity warnings: appends (collectors, example receiver) log a warning to
  stderr when page usage first crosses 80 % and 95 % of the limit (freelist
  pages count as free; a level re-arms once usage falls 5 points below it). `status` reports
  `used_pages`, `used_percent`, `headroom_pages`/`headroom_bytes` and
  `freelist_count`.
- Retention: nothing is deleted unless you run `prune` (below). Without it the
  spool fills up and collection stops at the limit.

## Retention

`akhet prune` deletes records that are safe to drop and prints a JSON report
(`records`, `first_seq`/`last_seq` deleted, `safe_seq`, `limited_by`,
`batches`, `page_count`, `freelist_count`, `headroom_pages`).

- **Only acknowledged records.** A record is eligible only if
  `seq <= min(checkpoint_seq)` over **all** sinks bound in the spool, i.e.
  every sink has received a 2xx for it. A sink that lags, or that was used once
  and abandoned, holds back deletion for everyone (`status` shows each sink's
  `pending_records` and the resulting `prune_safe_seq`). If no sink is bound,
  nothing is ever deleted.
- **Minimum age.** `--min-age` (default `24h`; units `s`, `m`, `h`, `d`) keeps
  records for at least that long after they were appended, by the collector's
  wall clock (`appended_unix_ns`). Pruning deletes a contiguous prefix: it
  stops at the first record that is too young or above the minimum checkpoint,
  so a backwards clock step delays deletion rather than causing it.
- **Bounded transactions.** Records are deleted in batches of `--batch-size`
  (default 1000), one `BEGIN IMMEDIATE … COMMIT` each, re-reading the sink
  checkpoints inside every transaction. Collectors wait for at most one batch
  (SQLite busy timeout 10 s). A crash rolls back only the batch in progress;
  committed batches stay deleted. `--dry-run` reports without deleting.
- **Space reuse, not shrinking.** Deleted pages go to SQLite's freelist and are
  reused by later appends before the file grows, so once pruning keeps up,
  `page_count` stays bounded and the `--max-db-bytes` limit keeps working (the
  limit counts all pages, free or not; freelist pages count as headroom). The
  file on disk is never shrunk: Akhet runs no `VACUUM`, because a full VACUUM
  rewrites the whole database, needs up to twice its size in free disk space
  and blocks collectors for its duration, and `auto_vacuum` cannot be enabled
  on an existing spool without one. If you must shrink a file, stop all
  collectors and replayers first and run `VACUUM` with the `sqlite3` shell.
- **Journal space.** A delete batch writes the original pages to the rollback
  journal, outside the database-size limit. Pruning a spool on a completely
  full filesystem can fail; lower `--batch-size` to reduce journal size.
- **Periodic pruning.** `replay --follow --prune-interval 10m
  [--prune-min-age 24h]` prunes at most once per interval from the replayer,
  using the same rules (minimum over all sinks, not just the replaying one).
  It is off by default. Several replayers may prune the same spool
  concurrently; each batch is serialized by SQLite.
- **Consequences of deleting.** A sink bound *after* records were pruned starts
  from the oldest remaining record; replay logs a warning naming the first seq
  it will deliver. Identity deduplication (`event_id`, configured MQTT source
  identities) only covers records still in the spool: a broker redelivery of an
  already pruned message is stored again as a new record. Keep `--min-age`
  longer than any plausible redelivery delay. `seq` stays monotonic after
  deletes (`AUTOINCREMENT`); it is never reused, so checkpoints remain valid.
- **Not implemented: segment rotation.** Records are not rotated into sealed
  segment files. Freelist reuse with bounded delete batches already keeps the
  file bounded without VACUUM; rotation would mainly allow returning space to
  the filesystem and archiving segments, and is left as follow-up work.
- Security: no encryption at rest. Keep spool files in an access-controlled
  local directory. Without TLS and a token (see Secure replay) bind the
  receiver to loopback or a trusted network.

## Observability

`mqtt`, `modbus` and `replay` accept `--metrics-listen ADDR` (off by default)
to serve three endpoints over plain HTTP:

| Path | Meaning |
|---|---|
| `GET /metrics` | Prometheus text exposition format (0.0.4) |
| `GET /healthz` | Liveness: `200 ok` while the process and the endpoint thread run. It does not show that collection is making progress; use the counters for that. |
| `GET /readyz` | Readiness: `200 ready`, or `503` with the failing conditions in the body |

```sh
akhet --spool data/spool.sqlite mqtt --host 127.0.0.1 --port 1883 \
  --client-id akhet-example-01 --topic 'sensors/#' --metrics-listen 127.0.0.1:9464
curl -s 127.0.0.1:9464/metrics
```

The endpoint has no authentication and no TLS. Bind it to loopback
(`127.0.0.1:PORT`) or a trusted network. A bind failure exits with code 2.

Readiness, per command:

- `mqtt`: connected (CONNACK received, no connection error since), the
  configured filter set active on the current connection (every filter
  granted QoS 1 by a SUBACK, or already held by an existing session according
  to the spool's subscription record), and the spool not full. Not ready
  while shutting down.
- `modbus`: the most recent poll succeeded and the spool is not full. Not
  ready before the first poll completes. Failed polls are still recorded.
- `replay`: the receiver identity was fetched and the sink binding verified,
  and the most recent HTTP attempt (identity or event) did not fail. A retry
  in progress makes replay not ready until an attempt succeeds.
- "Spool full" means an append failed with the database-size limit or a full
  filesystem, or the last spool sample shows fewer than 64 free pages under
  `--max-db-bytes` (the margin the spool uses to classify `SQLITE_FULL`).

How values are collected: collectors and replay update atomic counters
(a relaxed add per event, no locks); the endpoint runs on its own thread and
only reads them, so a slow scraper cannot block collection or `fsync`. Spool
size and pending records need SQLite queries, and in `journal_mode=DELETE` any
reader holds a shared lock that makes a concurrent commit wait. A separate
sampler thread therefore reads them through its own read-only connection every
`--metrics-spool-interval-ms` (default 5000), not per scrape, and scrapes serve
the last sample. Each sample is two `PRAGMA`s, one index lookup and one range
count per sink, which is proportional to that sink's pending records.
`akhet_spool_sample_timestamp_seconds` shows its age; a failed sample is
counted as `akhet_errors_total{component="metrics",kind="spool_sample"}` and
the previous sample is kept.

Metrics reference (all prefixed `akhet_`; counters reset when the process
starts, like the exit-time JSON summaries, which are unchanged):

| Metric | Type | Commands | Meaning |
|---|---|---|---|
| `akhet_info{mode,version}` | gauge | all | Always 1; labels identify the command and build |
| `akhet_ready` | gauge | all | 1 when `/readyz` reports ready |
| `akhet_errors_total{component,kind}` | counter | all | Errors by kind, see below |
| `akhet_spool_max_db_bytes` | gauge | all | `--max-db-bytes` |
| `akhet_spool_db_bytes_used` | gauge | all | SQLite pages in use × page size (sampled) |
| `akhet_spool_headroom_bytes` | gauge | all | Bytes left under `--max-db-bytes` (sampled). Not free disk space |
| `akhet_spool_last_seq` | gauge | all | Highest committed sequence number (sampled) |
| `akhet_spool_pending_records{sink}` | gauge | all | Records above each sink's checkpoint (sampled) |
| `akhet_spool_sample_timestamp_seconds` | gauge | all | Unix time of the last successful spool sample |
| `akhet_spool_commit_duration_seconds` | histogram | mqtt, modbus | Duration of each collector spool transaction (one fsync; with MQTT group commit one observation per batch); buckets 0.5 ms to 10 s |
| `akhet_mqtt_messages_received_total` | counter | mqtt | PUBLISH packets received |
| `akhet_mqtt_messages_committed_total` | counter | mqtt | Messages committed as new records |
| `akhet_mqtt_source_duplicates_total` | counter | mqtt | Redeliveries recognized by source identity, acknowledged without a new record |
| `akhet_mqtt_acks_enqueued_total` | counter | mqtt | PUBACKs enqueued after commit |
| `akhet_mqtt_acks_skipped_after_reconnect_total` | counter | mqtt | PUBACKs withheld because the message arrived on an earlier connection |
| `akhet_mqtt_queue_saturation_events_total` | counter | mqtt | Times the socket-to-spool queue was full and socket reads paused |
| `akhet_mqtt_dropped_unacknowledged_total` | counter | mqtt | Received messages dropped uncommitted and unacknowledged at shutdown |
| `akhet_mqtt_qos0_messages_total` | counter | mqtt | Messages delivered at QoS 0 |
| `akhet_mqtt_commit_transactions_total` | counter | mqtt | Spool transactions used by the collector (group commit: one per batch) |
| `akhet_mqtt_max_batch_committed` | gauge | mqtt | Largest number of messages committed in one transaction so far |
| `akhet_mqtt_unmatched_messages_total` | counter | mqtt | Messages whose topic matched none of the configured filters |
| `akhet_mqtt_connections_total` | counter | mqtt | Successful connections (CONNACK) |
| `akhet_mqtt_connection_errors_total` | counter | mqtt | Connection errors |
| `akhet_mqtt_queue_depth` | gauge | mqtt | Messages queued between socket and spool |
| `akhet_mqtt_queue_capacity` | gauge | mqtt | `--queue-capacity` |
| `akhet_mqtt_connected` | gauge | mqtt | 1 while connected |
| `akhet_mqtt_subscribed` | gauge | mqtt | 1 while the configured filter set is active with QoS 1 |
| `akhet_modbus_polls_total{status}` | counter | modbus | Completed polls, `status` `ok` or `read_failed` |
| `akhet_modbus_connections_total` | counter | modbus | TCP connections established |
| `akhet_modbus_last_poll_ok` | gauge | modbus | 1 if the most recent poll succeeded |
| `akhet_replay_delivered_total` | counter | replay | Records acknowledged (201/200) and checkpointed |
| `akhet_replay_receiver_duplicates_total` | counter | replay | Deliveries the receiver reported as already stored (200) |
| `akhet_replay_retries_total` | counter | replay | Retries after transient failures |
| `akhet_replay_checkpoint_seq` | gauge | replay | Sink checkpoint sequence number |
| `akhet_replay_lag_records` | gauge | replay | Records above this sink's checkpoint (sampled; absent until the sink exists) |
| `akhet_replay_receiver_bound` | gauge | replay | 1 once the receiver identity is bound |
| `akhet_replay_last_attempt_failed` | gauge | replay | 1 if the most recent HTTP attempt failed |

Error kinds (`akhet_errors_total`): `component="mqtt"` with `kind` `io`, `tls`,
`timeout`, `refused`, `protocol`, `other` (connection errors);
`component="modbus"` with the `error_kind` of `read_failed` events
(`connect`, `timeout`, `transport`, `exception`, `response_length`,
`resolve`); `component="spool"` with `capacity`, `disk_full`, `conflict`,
`corrupt`, `event_too_large`, `format`, `other` (failed collector appends);
`component="replay"` with `transient` (each failed attempt), `unavailable`
(retries exhausted), `rejected` (permanent HTTP failure), `protocol`;
`component="metrics"` with `spool_sample`. A series appears once its first
error occurs.

Not exported: samples (decoded values) collected. Events store raw payloads
and registers; counting samples needs payload decoding (issue #2). Sampled
gauges can be up to one sampling interval old. Replay lag is in records, not
time.

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

`akhet morlet` uses the same codes: 3 for sink binding mismatches, 5 for writes
MorletDB rejects, 6 when the database stays unreachable, 2 for configuration.

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
graceful shutdown leaving uncommitted messages unacknowledged; several topic
filters with redelivery on two unrelated topics after a reconnect and after a
crash, and adding/removing filters on an existing session; Modbus timeout,
exception and reconnect with explicit failed observations; concurrent-replayer
exclusion and sink binding; MQTT TLS with a trusted and an untrusted CA;
replay over TLS with a trusted CA, an untrusted CA and a wrong host name;
mutual TLS with a valid, a foreign and no client certificate; missing and
wrong tokens (401, checkpoint unchanged); refusal to send a token over plain
HTTP without the opt-in; malformed and unsupported HTTP requests; payload
decoding of multi-group batches (bit-exact values) and explicit issues for
malformed payloads; grid placement with jitter, gaps, duplicates, conflicts,
out-of-order samples, rate estimation and rate changes; context timelines with repeated values, redelivered and
late contradicting observations, rebuilt identically from the spool; pruning
limited by the slowest sink, by the minimum age and by having no sink; reads,
`verify`, `status` and replay after early records are gone; freelist reuse
under the size limit after a full spool is pruned; crashes inside a prune
transaction and between batches (`prune.before_commit`,
`prune.after_commit`) followed by `verify`; metrics and readiness scraped from
running collectors and replay (counters move, readiness flips as broker,
device and receiver appear or disappear).

A short bounded-size soak test appends, replays to the example receiver and
prunes in cycles, writing several times the size limit through a 128 KiB spool
and asserting that `page_count` stops growing. Run it longer with:

```sh
AKHET_SOAK_CYCLES=500 cargo test --locked --test retention soak -- --nocapture
```

Against an external broker (CI runs this with Mosquitto 2):

```sh
AKHET_TEST_BROKER_PORT=1883 cargo test --locked --test mqtt_external -- --ignored
```

Broker restarts, against a Mosquitto with persistence that the tests restart
and kill (CI runs this too; the module docs of `tests/mqtt_broker_restart.rs`
have the `docker run` line):

```sh
AKHET_TEST_PERSISTENT_BROKER_PORT=1884 AKHET_TEST_BROKER_CONTAINER=akhet-mosquitto-persist \
  cargo test --locked --test mqtt_broker_restart -- --ignored --test-threads=1
```

Group commit and batched replay have their own crash tests: a kill inside a
batch transaction (`abort:collector.batch_partial`, nothing stored, everything
redelivered), a kill after a batch commit before its acknowledgments (whole
batch redelivered), and batched replay killed after the remote commit or after
the checkpoint, plus a conflict in the middle of a batch.

Against MorletDB (CI runs this with the published pg_morlet image,
`0.3.5-alpha.1`).
`ci/morletdb-test.sh` starts a disposable container (loopback, test
credentials, TLS with a throwaway CA, container name `morlet`):

```sh
ci/morletdb-test.sh /tmp/akhet-pgcerts
AKHET_TEST_MORLET_URL=postgresql://morlet:morlet-test@127.0.0.1:54339/morlet \
AKHET_TEST_MORLET_CERTS=/tmp/akhet-pgcerts \
  cargo test --locked --test morlet -- --ignored --test-threads=1
```

The TLS test checks `verify-full` (succeeds with the CA; fails with a wrong
host name, another CA or no CA), `verify-ca` and the plain loopback default.

Not covered: power-failure durability, real Modbus devices, multi-day soak
tests.

### Benchmark

```sh
cargo run --release --locked --example throughput            # about 5 minutes
cargo run --release --locked --example throughput -- --runs 1 --quick
```

It measures MQTT ingest (messages/s, payload MB/s, commit latency p50/p99,
with and without group commit) and replay records/s (single vs batch) on
loopback, with a payload shaped like 8 channels × 4000 samples per 100 ms as
JSON. Results, settings and hardware are in
[docs/benchmarks.md](docs/benchmarks.md). They come from one development
machine (macOS, `F_FULLFSYNC`); numbers for x86_64 and arm64 Linux edge
hardware are still missing.

`examples/morlet_sink.rs` measures the MorletDB sink (samples/s, chunks/s,
time per transaction, sink state size) for a configurable payload shape
against `AKHET_MORLET_URL`; see the MorletDB section of the same document. `examples/morlet_acquisition.rs` times
`morlet_read_acquisition` on a source with many gaps.

## Scope boundaries

- The MorletDB sink handles MQTT payloads decoded by rules. Irregular Modbus
  polls are not a uniform waveform and are not written to MorletDB.
- Payloads are stored as received (base64 or raw). Lossless sample coding at
  the edge, e.g. with the MorletDB codec before replay, is not implemented.
- No ML, adaptive discarding, Parquet conversion, custom storage engine,
  segment rotation, fleet management or performance claims beyond the
  measurements in docs/benchmarks.md. Deletion exists only as the explicit,
  acknowledgment-gated `prune` described above.

## License

MIT; see [LICENSE](LICENSE). Dependencies keep their own licences, all
permissive (MIT, Apache-2.0, BSD, ISC, Zlib, Unicode-3.0, CDLA-Permissive-2.0
and similar); `cargo metadata` lists them. Release binaries statically include
SQLite (public domain) through rusqlite's `bundled` feature.
