# Benchmarks

Throughput of MQTT ingest (with and without group commit) and of replay
(single POSTs vs the `batch-v1` extension). These numbers come from **one
development machine**, not from edge hardware; read the limits below before
using them for sizing.

## How to reproduce

```sh
cargo run --release --locked --example throughput                 # 3 runs, about 5 minutes
cargo run --release --locked --example throughput -- --runs 1 --quick
cargo run --release --locked --example throughput -- --dir /path/on/target/disk
```

The example (`examples/throughput.rs`) runs everything in one process on
loopback:

- the embedded rumqttd broker (in-memory, as in the tests);
- a QoS 1 publisher (rumqttc, up to 100 messages in flight);
- the MQTT collector exactly as the CLI runs it (current-thread Tokio runtime,
  at most 4 blocking threads, default `--queue-capacity 64`,
  `--max-payload-bytes` raised to 512 KiB), writing to a fresh spool in a
  temporary directory;
- the example HTTP receiver (`akhet receive`) with its own fresh spool, fed by
  `replay::run`.

Spool settings are the defaults: `journal_mode=DELETE`, `synchronous=FULL`,
`fullfsync=ON` (F_FULLFSYNC on macOS).

Payloads:

- **daq**: 8 channels × 4000 samples per 100 ms as JSON (40 kHz per
  channel, 320 000 samples/s in total), integer samples in -32768..=32767,
  about 200 KB per message. The source rate is 10 messages/s, about
  2 MB/s of payload.
- **small**: a JSON object of about 40 bytes, to show the per-transaction
  cost on its own.

Measurements:

- **Saturated ingest**: the publisher sends N messages as fast as the broker
  accepts them, so the collector is the bottleneck. Throughput is N divided by
  the time from the first publish to the last spool commit. The publisher
  finished within 0.02 s in every run, so the broker buffered the backlog.
- **Commit latency**: wall time of one spool transaction (inserts, COMMIT and
  the fsyncs SQLite issues), p50/p99 over all transactions of all runs. With
  group commit one transaction holds a whole batch.
- **Spool MB/s**: final spool file size divided by the elapsed time (logical
  database growth; MQTT payloads are stored base64-encoded inside JSON, so this
  is about 4/3 of the payload rate). Device-level writes (rollback journal,
  metadata) were not measured.
- `max_batch 1` is one transaction per message, i.e. the behaviour before
  group commit (same code path with a batch size of one).
- **Replay**: the spool of the first saturated group-commit run (300 daq or
  2000 small events) replayed to a fresh receiver. `records/request 1` is the
  single-POST protocol; `100` uses `batch-v1` with the default
  `--max-request-bytes` of 8 MiB, so daq requests carry about 29 records.

## Results

Machine: Apple M5 Max (18 cores), 128 GiB RAM, internal Apple SSD
(APFS, "Apple Fabric" protocol), macOS 27.0.1 (Darwin 27.0.0, arm64). Rust
1.99 release build. Spools in the default temp directory on the internal
SSD. Run on 2026-11-03. This is a macOS desktop-class machine with
F_FULLFSYNC, **not edge hardware**.

### MQTT ingest, saturated publisher (median of 3 runs)

| payload | max_batch | messages | msg/s | payload MB/s | samples/s | spool MB/s | transactions | mean batch | commit p50 ms | commit p99 ms |
|---|---|---|---|---|---|---|---|---|---|---|
| daq 8x4000 JSON | 1 | 300 | 58 | 11.42 | 1 850 996 | 15.29 | 300 | 1.0 | 11.9 | 57.7 |
| daq 8x4000 JSON | 64 | 300 | 874 | 172.55 | 27 964 817 | 231.03 | 7 | 42.9 | 41.7 | 55.6 |
| small JSON | 1 | 2000 | 87 | 0.00 | - | 0.05 | 2000 | 1.0 | 11.1 | 17.0 |
| small JSON | 64 | 2000 | 3708 | 0.15 | - | 2.18 | 41 | 48.8 | 13.0 | 18.1 |

### MQTT ingest at the source rate (daq, 10 msg/s, 100 messages, one run)

| max_batch | msg/s | transactions | commit p50 ms | commit p99 ms | commit max ms |
|---|---|---|---|---|---|
| 1 | 10.1 | 100 | 13.4 | 15.9 | 18.8 |
| 64 | 10.1 | 100 | 13.4 | 19.7 | 28.3 |

### Replay to the example receiver (median of 3 runs)

| payload | records/request | records | records/s | MB/s | requests |
|---|---|---|---|---|---|
| daq 8x4000 JSON | 1 | 300 | 39 | 10.32 | 300 |
| daq 8x4000 JSON | 100 | 300 | 388 | 102.30 | 12 |
| small JSON | 1 | 2000 | 44 | 0.02 | 2000 |
| small JSON | 100 | 2000 | 4441 | 1.76 | 20 |

Whole benchmark process (broker, publisher, collector, replay and receiver
together), from `/usr/bin/time -l`: 270 s wall, 5.9 s user, 21.7 s system
CPU, 1.09 GB maximum resident set size. The memory is dominated by the
in-memory broker, which keeps every published message of the run; it says
nothing about the collector's own footprint, which was not measured
separately.

## Reading the numbers

- On this machine one durable SQLite commit costs about 11–13 ms, almost
  independent of size for small transactions. That cost, not CPU, bounds
  one-transaction-per-message ingest at roughly 60–90 messages/s.
- Group commit amortises it: with saturated input, batches averaged 43–49
  messages and ingest rose about 15× (daq) and about 40× (small). A daq
  transaction of about 43 messages (about 11 MB of spool data) took 42 ms.
- At the source rate of the daq shape (10 messages/s) both settings keep up
  and every batch has one message, so commit latency is the same; group
  commit only changes behaviour under a backlog. The daq source needs
  10 messages/s and about 2 MB/s; on this machine `max_batch 1` sustains about
  58 messages/s, so the headroom without group commit is roughly 6×, and
  about 87× with it.
- Replay with single POSTs pays two durable commits per record (receiver
  insert and local checkpoint). Batches pay two per request: about 10×
  (daq, request size bound by 8 MiB) and about 100× (small) here. Single-POST
  small-record replay varied between runs (33–56 s for 2000 records).
- Tail latencies (p99) include occasional slow flushes of the host system;
  three runs per configuration are not enough to characterise them.

## Missing, and therefore not claimed

- **No x86_64 or arm64 Linux edge numbers.** Edge devices with eMMC/SD or
  industrial SSDs, ext4/xfs and `fsync` instead of `F_FULLFSYNC` can differ
  by an order of magnitude in either direction. The issue's acceptance
  criterion (published benchmark on edge hardware) is therefore only partly
  met.
- No device-level disk write rate, no per-process CPU or memory for the
  collector alone, no long-duration (soak) run, no measurement with a
  persistent external broker (e.g. Mosquitto with persistence) or over a real
  network.
- Loopback HTTP and MQTT hide network latency; over a WAN, batched replay
  should gain more, single POSTs less.
