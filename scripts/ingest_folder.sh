#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="${1:-./data/muse_raw}"
TENANT="${2:-acme_demo}"
OUT_DIR="${3:-./tmp/normalized}"
API="${API:-http://localhost:${API_PORT:-8090}}"
KEY="${API_KEY:-changeme}"

mkdir -p "$OUT_DIR"

# Parse
python3 tools/muse2_parser.py --raw_dir "$RAW_DIR" --out_dir "$OUT_DIR" --tenant_id "$TENANT"

# Post each file
shopt -s nullglob
for f in "$OUT_DIR"/*.normalized.csv; do
  BN="$(basename "$f")"
  SESSION="${BN%.normalized.csv}"
  USER_ID="${SESSION%%_*}"
  echo "POST -> $SESSION"
  curl -fsS -X POST -F "file=@$f"     "$API/ingest/batch?api_key=$KEY&tenant_id=$TENANT&user_id=$USER_ID&session_id=$SESSION&device=muse2&mode=upsert"     || { echo "ERROR posting $f"; exit 1; }
  echo
done
echo "Done."
