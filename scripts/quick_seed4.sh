#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/quick_seed4.sh [RAW_DIR] [TENANT_ID] [OUT_DIR] [DEVICE]
#
# If RAW_DIR is omitted, the script will try to auto-detect a folder containing CSVs.

RAW_DIR="${1:-}"
TENANT="${2:-seed4_demo}"
OUT_DIR="${3:-$PWD/tmp/seed4_norm}"
DEVICE="${4:-seed4-features}"

auto_detect_raw_dir() {
  # Search common places for CSVs (fast, shallow first; then deeper)
  for hint in \
    "$PWD/data/SEED-IV" \
    "$PWD/data/SEED_IV" \
    "$PWD/data/seed-iv" \
    "$PWD/data" \
    "$PWD"; do
    if find "$hint" -maxdepth 3 -type f -name "*.csv" | head -n1 | grep -q .; then
      echo "$hint"
      return 0
    fi
  done
  return 1
}

if [[ -z "$RAW_DIR" ]]; then
  echo "[quick] RAW_DIR not provided — trying auto-detect..."
  if RAW_DIR="$(auto_detect_raw_dir)"; then
    echo "[quick] RAW_DIR detected: $RAW_DIR"
  else
    echo "[quick] Could not auto-detect RAW_DIR with CSVs."
    echo "Usage: $0 /absolute/path/to/seed_iv_csv_folder [TENANT_ID] [OUT_DIR] [DEVICE]" >&2
    exit 1
  fi
fi

# Ensure scripts are executable
chmod +x scripts/normalize_seed4.sh || true

echo "== Normalize SEED-IV =="
./scripts/normalize_seed4.sh "$RAW_DIR" "$TENANT" "$OUT_DIR" "$DEVICE"

echo "== Sanity check =="
COUNT=$(ls -1 "$OUT_DIR"/*.normalized.csv 2>/dev/null | wc -l | tr -d ' ')
echo "[quick] normalized files: $COUNT"
if [[ "$COUNT" -gt 0 ]]; then
  FIRST="$(ls -1 "$OUT_DIR"/*.normalized.csv | head -n1)"
  echo "[quick] preview: $FIRST"
  head -n 6 "$FIRST"
else
  echo "[quick] No normalized files found in $OUT_DIR — check your RAW_DIR path."
fi
