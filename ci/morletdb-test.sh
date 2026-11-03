#!/usr/bin/env bash
# Disposable MorletDB for the `morlet` tests: loopback only, test credentials,
# TLS enabled with a throwaway CA (plain connections still work).
#   ci/morletdb-test.sh <cert-dir> [port] [image]
# The server certificate names only `localhost`, so verify-full against
# 127.0.0.1 is a host name mismatch (used by the TLS test).
set -euo pipefail
dir=$1
port=${2:-54339}
image=${3:-ghcr.io/merkhet-io/morletdb:0.3.3-alpha.1}
mkdir -p "$dir"
cd "$dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=akhet test CA" \
  -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost\n' > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
  -extfile san.ext -out server.crt 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=other CA" \
  -keyout other-ca.key -out other-ca.pem 2>/dev/null
chmod 644 server.key
docker rm -f morlet >/dev/null 2>&1 || true
# PostgreSQL requires the key to be owned by its user with mode 0600.
docker run -d --name morlet -p "127.0.0.1:$port:5432" \
  -e POSTGRES_USER=morlet -e POSTGRES_PASSWORD=morlet-test -e POSTGRES_DB=morlet \
  -v "$PWD:/certs:ro" --entrypoint bash "$image" -c '
    install -o postgres -g postgres -m 0600 /certs/server.key /var/lib/postgresql/server.key
    install -o postgres -g postgres -m 0644 /certs/server.crt /var/lib/postgresql/server.crt
    exec docker-entrypoint.sh postgres -c ssl=on \
      -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key' >/dev/null
for _ in $(seq 1 60); do docker exec morlet pg_isready -U morlet -d morlet >/dev/null 2>&1 && break; sleep 1; done
sleep 2
docker exec morlet psql -U morlet -d morlet -v ON_ERROR_STOP=1 -q -c 'CREATE EXTENSION IF NOT EXISTS pg_morlet'
