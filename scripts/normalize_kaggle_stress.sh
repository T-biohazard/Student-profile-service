#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.."; pwd)"
export PYTHONPATH="$ROOT:$ROOT/tools:${PYTHONPATH:-}"

RAW="${1:-$ROOT/data/kaggle_stress}"
TENANT="${2:-kaggle_stress_demo}"
OUT="${3:-$ROOT/tmp/kaggle_stress_norm}"

mkdir -p "$OUT"
python3 -m tools.normalize --dataset kaggle_stress \
  --raw_dir "$RAW" --out_dir "$OUT" --tenant_id "$TENANT" --device kaggle-stress

echo "Kaggle stress normalized -> $OUT"
