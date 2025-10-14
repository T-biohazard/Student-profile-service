#!/usr/bin/env bash
set -euo pipefail

# Quick E2E for SEED-IV features: compose up, create schema, normalize, ingest, verify.

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
RAW="${RAW_DIR:-${ROOT}/data/SEED-IV/eeg_feature_csv_data}"
TENANT="${TENANT_ID:-seed4_demo}"
OUT="${OUT_DIR:-${ROOT}/tmp/seed4_norm}"
DEVICE="${DEVICE:-seed4-features}"
API="${API_URL:-http://localhost:8090}"
KEY="${API_KEY:-changeme}"

cd "${ROOT}"

echo "[stack] docker compose up"
docker compose up -d

echo "[db] ensure schema"
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT:-5439}/ts" -f sql/schema.sql >/dev/null

echo "[normalize] SEED-IV"
./scripts/normalize_seed4.sh "${RAW}" "${TENANT}" "${OUT}" "${DEVICE}"

echo "[ingest] folder"
./scripts/ingest_seed4.sh "${OUT}" "${API}" "${KEY}"

echo "[verify] counts"
docker exec -it eeg-timescale psql -U ts -d ts -c \
"SELECT tenant_id, COUNT(*) AS rows FROM public.eeg_samples GROUP BY 1 ORDER BY rows DESC LIMIT 10;"

docker exec -it eeg-timescale psql -U ts -d ts -c \
"SELECT session_id, COUNT(*) rows, MIN(ts) start, MAX(ts) stop
 FROM public.eeg_samples WHERE tenant_id='${TENANT}'
 GROUP BY 1 ORDER BY rows DESC LIMIT 10;"
