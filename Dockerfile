# syntax=docker/dockerfile:1
#
# Akhet container image: a statically linked musl binary on distroless/static.
#
# Why this layout:
# - rustls (ring) and rusqlite's bundled SQLite mean there is no OpenSSL or
#   system SQLite to link. On Alpine the default Rust target is *-linux-musl,
#   which links statically, so the runtime image needs no libc at all.
# - gcr.io/distroless/static ships only CA certificates (used by --tls with the
#   platform trust store), tzdata, /etc/passwd with a `nonroot` user (65532)
#   and no shell or package manager.
# - The same `binary` stage is exported by the release workflow as the release
#   tarball binary, so the image and the tarballs come from one recipe.
#
# Base images are pinned by version tag here. Pin them by digest
# (`image:tag@sha256:...`) when cutting a release so rebuilds are reproducible;
# tags can move. distroless publishes no versioned tags, only moving ones.
#
# Build:          docker build -t akhet:dev .
# Export binary:  docker build --target binary --output type=local,dest=dist .

ARG RUST_IMAGE=rust:1.99.0-alpine3.22
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian13:nonroot

FROM ${RUST_IMAGE} AS build
# musl-dev: C runtime headers for ring and the bundled SQLite (cc crate).
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin akhet \
 && install -D -m 0755 target/release/akhet /out/akhet \
 && /out/akhet --version \
 && mkdir -p /out/data

# Export stage for `--target binary --output type=local,...` (release tarballs).
FROM scratch AS binary
COPY --from=build /out/akhet /akhet

FROM ${RUNTIME_IMAGE} AS runtime
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="akhet" \
      org.opencontainers.image.description="Durable MQTT and Modbus TCP collection with a local SQLite spool and HTTP replay (example infrastructure)" \
      org.opencontainers.image.source="https://github.com/merkhet-io/akhet" \
      org.opencontainers.image.url="https://github.com/merkhet-io/akhet" \
      org.opencontainers.image.documentation="https://github.com/merkhet-io/akhet/blob/main/docs/deployment.md" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build /out/akhet /usr/local/bin/akhet
COPY LICENSE /usr/share/doc/akhet/LICENSE
# Owned by the unprivileged user so a fresh named volume inherits it.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
WORKDIR /data
ENV AKHET_SPOOL=/data/akhet-spool.sqlite
VOLUME ["/data"]
# akhet handles SIGTERM/SIGINT itself (graceful stop), so it can run as PID 1.
ENTRYPOINT ["akhet"]
CMD ["--help"]
