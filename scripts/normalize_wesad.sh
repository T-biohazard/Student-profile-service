#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; export PYTHONPATH="$(cd "$SCRIPT_DIR/.."; pwd):$(cd "$SCRIPT_DIR/../tools"; pwd):${PYTHONPATH}"
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_DIR="${1:-./data/wesad}"
TENANT="${2:-wesad_demo}"
OUT_DIR="${3:-./tmp/wesad_normalized}"

mkdir -p "$OUT_DIR"
python3 -m tools.normalize --dataset wesad --raw_dir "$RAW_DIR" --out_dir "$OUT_DIR" --tenant_id "$TENANT"
echo "WESAD normalized -> $OUT_DIR"
