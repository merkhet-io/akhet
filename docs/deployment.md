# Deploying Akhet at the edge

This page covers release binaries, the container image, the example systemd
units and Kubernetes manifest, and how to upgrade a host that already has a
spool file. Read the README's delivery and durability sections first; nothing
here changes those guarantees.

## Two constraints that apply everywhere

- **One collector per spool file.** Each `akhet mqtt` or `akhet modbus`
  process gets its own spool file. Replay (`akhet replay`) runs as a separate
  process against the same file, and that is supported: SQLite locking and the
  busy timeout coordinate the two. Two collectors writing to one file, or two
  hosts sharing one file, are not supported.
- **Local filesystem only.** Keep the spool on a local disk (ext4, xfs, a
  cloud block volume). SQLite on network filesystems (NFS, SMB/CIFS, most
  `ReadWriteMany` Kubernetes storage) is not supported. Their locking and
  flush behaviour break the durability assumptions in the README.

## Install from a release

Releases are built by `.github/workflows/release.yml` when a `v*` tag is
pushed. Each release has:

| File | Contents |
|---|---|
| `akhet-<version>-x86_64-unknown-linux-musl.tar.gz` | static binary, `LICENSE`, `README.md`, `docs/`, `deploy/` |
| `akhet-<version>-aarch64-unknown-linux-musl.tar.gz` | the same for 64-bit ARM (arm64) |
| `SHA256SUMS` | SHA-256 of both tarballs |

The binaries are statically linked against musl, so they run on any Linux
distribution of that architecture (glibc or musl based) with no shared
libraries to install. TLS uses rustls with the system trust store
(`/etc/ssl/certs`), so install your distribution's `ca-certificates` package
if you use `--tls` without `--tls-ca-file`.

Download and verify. The checksum shows the file arrived intact; the
attestation shows it was built by this repository's release workflow from a
specific commit (signed through Sigstore, checked by the GitHub CLI):

```sh
VERSION=0.1.0-alpha.1
ARCH=aarch64            # or x86_64
gh release download "v$VERSION" -R merkhet-io/akhet \
  -p "akhet-$VERSION-$ARCH-unknown-linux-musl.tar.gz" -p SHA256SUMS

sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify "akhet-$VERSION-$ARCH-unknown-linux-musl.tar.gz" \
  -R merkhet-io/akhet

tar -xzf "akhet-$VERSION-$ARCH-unknown-linux-musl.tar.gz"
sudo install -m 0755 "akhet-$VERSION-$ARCH-unknown-linux-musl/akhet" /usr/local/bin/akhet
akhet --version
```

Without the GitHub CLI you can still check `SHA256SUMS`, but that file comes
from the same release page, so it only protects against corruption, not
against a tampered release.

## Container image

```sh
docker pull ghcr.io/merkhet-io/akhet:0.1.0-alpha.1
gh attestation verify oci://ghcr.io/merkhet-io/akhet:0.1.0-alpha.1 -R merkhet-io/akhet
```

The image is multi-arch (`linux/amd64`, `linux/arm64`) and built from the
`Dockerfile` in this repository:

- a static musl binary built by the same Dockerfile stage as the release
  tarballs, on `gcr.io/distroless/static` (CA certificates, tzdata, no
  shell);
- runs as the unprivileged user `65532:65532`;
- `/data` is a volume, and `AKHET_SPOOL` defaults to
  `/data/akhet-spool.sqlite`;
- `ENTRYPOINT ["akhet"]`, so arguments are akhet subcommands. Akhet handles
  SIGTERM itself, so `docker stop` stops it gracefully (exit 0).

```sh
docker volume create akhet-spool

# Collector. Credentials come from the environment, never from arguments.
docker run -d --name akhet-mqtt --restart on-failure \
  -v akhet-spool:/data \
  --env-file ./mqtt-credentials.env \
  ghcr.io/merkhet-io/akhet:0.1.0-alpha.1 \
  mqtt --host broker.example.internal --topic 'sensors/#' --client-id akhet-edge-01

# Replay, as a second container on the same volume.
docker run -d --name akhet-replay --restart on-failure \
  -v akhet-spool:/data \
  ghcr.io/merkhet-io/akhet:0.1.0-alpha.1 \
  replay --url http://receiver.example.internal:8080 --sink central --follow

docker run --rm -v akhet-spool:/data ghcr.io/merkhet-io/akhet:0.1.0-alpha.1 status
```

Use a named volume or a bind mount of a local directory owned by `65532`.
`--restart on-failure` restarts on every non-zero exit code, including
integrity errors (exit 3); the systemd units below are stricter.

To build the image yourself: `docker build -t akhet:dev .`. To extract only
the static binary for the host's architecture:
`docker build --target binary --output type=local,dest=dist .`

## systemd

`deploy/systemd/` contains:

| File | Install as | Purpose |
|---|---|---|
| `akhet.sysusers` | `/etc/sysusers.d/akhet.conf` | system user `akhet` |
| `akhet.env` | `/etc/akhet/akhet.env` (0644) | non-secret settings for both units |
| `mqtt-credentials.env` | `/etc/akhet/mqtt-credentials.env` (0600, root) | `AKHET_MQTT_USERNAME` / `AKHET_MQTT_PASSWORD` |
| `akhet-mqtt.service` | `/etc/systemd/system/` | MQTT collector |
| `akhet-replay.service` | `/etc/systemd/system/` | `replay --follow` to the receiver |

```sh
cd deploy/systemd
sudo install -m 0644 akhet.sysusers /etc/sysusers.d/akhet.conf
sudo systemd-sysusers
sudo install -d -m 0755 /etc/akhet
sudo install -m 0644 akhet.env /etc/akhet/akhet.env
sudo install -m 0600 -o root -g root mqtt-credentials.env /etc/akhet/mqtt-credentials.env
sudoedit /etc/akhet/akhet.env /etc/akhet/mqtt-credentials.env
sudo install -m 0644 akhet-mqtt.service akhet-replay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now akhet-mqtt.service akhet-replay.service
journalctl -u akhet-mqtt -u akhet-replay -f
```

What the units do:

- `StateDirectory=akhet` creates `/var/lib/akhet` (mode 0700, owner `akhet`);
  the spool and the replay lock files live there.
- systemd reads `EnvironmentFile=` as root before dropping privileges, so the
  credentials file stays `root:root 0600`. Only the collector unit loads it.
- Restart policy follows the README exit codes. The collector restarts on
  exit 1 but not on 2 (configuration), 3 (integrity) or 4 (capacity). Replay
  also restarts on 6 (receiver unavailable after bounded retries) but not on
  2, 3, 4, 5 (record rejected) or 7 (sink held by another replayer). A unit
  that stops for one of those reasons stays `failed` until someone looks.
  `StartLimitBurst` caps restarts at 5 in 10 minutes.
- Hardening: no capabilities, `ProtectSystem=strict`, private `/tmp` and
  devices, a `@system-service` syscall filter and IPv4/IPv6/Unix sockets
  only. `systemd-analyze security` rates both units 1.2 ("OK").

For Modbus, copy `akhet-mqtt.service` to e.g. `akhet-modbus.service`, give it
its own spool (`Environment=AKHET_SPOOL=/var/lib/akhet/modbus.sqlite`) and an
`akhet modbus ...` command line, and add a matching replay unit with that
spool. Do not point two collectors at one spool.

## Kubernetes

`deploy/kubernetes/akhet.yaml` is a single-file example: a ConfigMap, a
headless Service and a StatefulSet with `replicas: 1`, a `volumeClaimTemplate`
for the spool, and two containers sharing that volume (collector and replay).
MQTT credentials come from a Secret you create yourself:

```sh
kubectl create secret generic akhet-mqtt \
  --from-literal=AKHET_MQTT_USERNAME=edge \
  --from-literal=AKHET_MQTT_PASSWORD='...'
kubectl apply -f deploy/kubernetes/akhet.yaml
```

- A StatefulSet stops the old Pod before starting its replacement, so updates
  never run two collectors on one volume. Do not raise `replicas`.
- Use a storage class backed by a block device (cloud disk, `local-path`),
  not NFS. `ReadWriteOncePod` is a stricter access mode where the driver
  supports it.
- Containers run as non-root with a read-only root filesystem, no
  capabilities and the `RuntimeDefault` seccomp profile.
- Kubernetes restarts containers whatever their exit code, so an integrity
  failure (exit 3) ends in `CrashLoopBackOff` instead of a clean stop. The
  spool is not modified by the retries, but alert on restarts and read
  `kubectl logs akhet-0 -c collector --previous`.
- There are no liveness or readiness probes: akhet has no health endpoint.
  Collector progress is visible in the logs and in `akhet status`
  (`kubectl exec akhet-0 -c replay -- akhet status`).

## Upgrading a host with an existing spool

The spool file carries its schema version in SQLite `PRAGMA user_version`
(currently 1, see `src/spool.rs`). **Today a build opens only the schema
version it was built for.** If the versions differ, every command (`mqtt`,
`modbus`, `replay`, `status`, `inspect`, `verify`) refuses the file with
`unsupported spool schema version N (this build supports M)` and exit code 3.
It does not modify the file. There is no automatic migration. If a future
release changes the schema, it will ship an explicit migration step and say
so in its release notes; until then, check the notes before upgrading.

Records already replayed stay at the receiver. Records not yet replayed stay
in the spool until a build that can read it replays them. Because MQTT
messages are acknowledged only after commit, messages that arrive while the
collector is stopped wait at the broker (if it keeps the persistent session;
see the README).

1. **Stop the collector and replay.**
   `sudo systemctl stop akhet-mqtt akhet-replay`
   (or `kubectl scale statefulset akhet --replicas=0`, or `docker stop`).
2. **Back up the spool.** With everything stopped, a plain copy is
   consistent: `sudo cp -p /var/lib/akhet/spool.sqlite /var/lib/akhet/spool.sqlite.bak-0.1.0-alpha.1`.
   If a process might still be running, use SQLite's online backup instead of
   `cp`: `sqlite3 /var/lib/akhet/spool.sqlite ".backup '/var/lib/akhet/spool.sqlite.bak-0.1.0-alpha.1'"`.
   If a `spool.sqlite-journal` file is still there after the stop (left by
   a crash mid-transaction), it belongs to the database: never delete it by
   hand. Either copy it along with the database, or run step 3 first, which
   opens the spool, rolls the interrupted transaction back and removes it.
3. **Verify with the current binary.**
   `sudo -u akhet akhet --spool /var/lib/akhet/spool.sqlite verify`
   must print `"status":"ok"`. Do not upgrade a spool that fails
   verification; keep the backup and investigate first.
4. **Fetch the new binary** without installing it yet: download and verify
   the release as above and keep it as `./akhet-new`. Test it against a
   **copy** of the backup:
   ```sh
   sudo cp /var/lib/akhet/spool.sqlite.bak-0.1.0-alpha.1 /root/akhet-upgrade-test.sqlite
   sudo ./akhet-new --spool /root/akhet-upgrade-test.sqlite verify
   sudo ./akhet-new --spool /root/akhet-upgrade-test.sqlite status
   ```
   Exit 0 means the new build reads this schema. Exit 3 with "unsupported
   spool schema version" means it does not: follow that release's migration
   notes, or stay on the old version. Either way the backup is untouched.
5. **Swap the binary and restart.**
   ```sh
   sudo cp -p /usr/local/bin/akhet /usr/local/bin/akhet-0.1.0-alpha.1   # for rollback
   sudo install -m 0755 akhet-new /usr/local/bin/akhet
   sudo systemctl start akhet-mqtt akhet-replay
   sudo -u akhet akhet --spool /var/lib/akhet/spool.sqlite status
   ```
   Check in `status` and `journalctl -u akhet-mqtt -u akhet-replay` that the
   record count grows and the replay checkpoint advances. For containers,
   change the image tag and start again.

### Rollback

Stop both units, put the old binary (or image tag) back and start again.
Without a schema change the old binary reads the spool as it is, including
records the new version wrote. If a migration did run, the old binary refuses
the migrated file with exit 3; then restore the backup from step 2, accepting
that records collected after the upgrade are not in it. Replay to the same
sink after a restore re-sends records past the restored checkpoint; the
receiver deduplicates by `event_id`.

## What has been tested

Locally, on an arm64 host: the image builds, `--version`, `status` and
`verify` on a fresh volume, a Modbus collector, a `replay --follow` container
and a receiver container sharing volumes, `docker stop` exits 0. The
x86_64 static binary was built under emulation and ran in a Debian container.
The systemd units ran under systemd in a Debian 13 container against a
password-protected Mosquitto 2 broker, and a spool with `user_version` 2 made
both units stop with exit 3 and no restart. The Kubernetes manifest ran on
k3s with `local-path` storage, survived a rolling restart without losing the
spool, and delivered every record to a receiver. The release workflow has
only been linted (actionlint); it has not run, so release assets, the
multi-arch image and attestations are untested until the first tag.
