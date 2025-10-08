#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR="${1:-./tmp/wesad_normalized}"
TENANT="${2:-wesad_demo}"
API="${API:-http://localhost:${API_PORT:-8090}}"
KEY="${API_KEY:-changeme}"
DEVICE="wesad-e4"

shopt -s nullglob
for f in "$OUT_DIR"/*.normalized.csv; do
  BN="$(basename "$f")"
  SESSION="${BN%.normalized.csv}"
  USER_ID="${SESSION%%_*}"
  echo "POST -> $SESSION"
  curl -fsS -X POST -F "file=@$f"     "$API/ingest/batch?api_key=$KEY&tenant_id=$TENANT&user_id=$USER_ID&session_id=$SESSION&device=$DEVICE&mode=upsert"     || { echo "ERROR posting $f"; exit 1; }
  echo
done
echo "Done."
