#!/usr/bin/env bash
set -euo pipefail

# Ingest all *.normalized.csv from a folder.
# Usage:
#   ./scripts/ingest_seed4.sh <NORM_DIR> [API_URL] [API_KEY]
#
# Example:
#   ./scripts/ingest_seed4.sh "$PWD/tmp/seed4_norm" "http://localhost:8090" "changeme"

NORM_DIR="${1:-}"
API="${2:-http://localhost:8090}"
KEY="${3:-changeme}"

if [[ -z "${NORM_DIR}" ]]; then
  echo "Usage: $0 <NORM_DIR> [API_URL] [API_KEY]" >&2
  exit 1
fi

mapfile -t files < <(ls -1 "${NORM_DIR}"/*.normalized.csv 2>/dev/null || true)
if (( ${#files[@]} == 0 )); then
  echo "No *.normalized.csv files found in ${NORM_DIR}" >&2
  exit 0
fi

echo "[ingest] API: ${API}"
for ONE in "${files[@]}"; do
  T=$(awk -F, 'NR==2{print $1}' "$ONE")
  U=$(awk -F, 'NR==2{print $2}' "$ONE")
  S=$(awk -F, 'NR==2{print $3}' "$ONE")
  D=$(awk -F, 'NR==2{print $4}' "$ONE")

  echo " -> $(basename "$ONE")   tenant=${T} user=${U} session=${S} device=${D}"
  curl -fsS -X POST -F "file=@${ONE}" \
    "${API}/ingest/batch?api_key=${KEY}&tenant_id=${T}&user_id=${U}&session_id=${S}&device=${D}&mode=upsert" \
    || echo "   [warn] failed: ${ONE}"
done
