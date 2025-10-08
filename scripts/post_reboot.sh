#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cp -n .env.sample .env || true
docker compose up -d

echo "Waiting for API..."
for i in {1..60}; do
  if curl -fsS "http://localhost:${API_PORT:-8090}/healthz" >/dev/null; then
    echo "API OK"; break
  fi
  sleep 1
done

# optional DB ping
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT:-5439}/ts" -c "SELECT now();" || true

# Safe idempotent folder ingest for Muse (if any files prepared)
./scripts/ingest_folder.sh ./data/muse_raw acme_demo || true
