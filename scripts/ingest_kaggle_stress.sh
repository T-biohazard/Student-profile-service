#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.."; pwd)"

API="${API:-http://localhost:${API_PORT:-8090}}"
KEY="${API_KEY:-changeme}"

NORM_DIR="${1:-$ROOT/tmp/kaggle_stress_norm}"
TENANT="${2:-kaggle_stress_demo}"
DEVICE="${3:-kaggle-stress}"

shopt -s nullglob
for f in "$NORM_DIR"/*.normalized.csv; do
  base="$(basename "$f" .normalized.csv)"
  # our Kaggle normalizer uses session_id "<user>_kaggle_stress_<stem>"
  # we can recover user/session cheaply from filename:
  session_id="$base"
  user_id="${base%%_*}"              # part before first underscore
  echo "POST -> $base"
  curl -fsS -X POST -F "file=@${f}" \
    "$API/ingest/batch?api_key=$KEY&tenant_id=$TENANT&user_id=$user_id&session_id=$session_id&device=$DEVICE&mode=upsert" \
    || { echo "ERROR posting $f"; exit 1; }
done
echo "Done."
