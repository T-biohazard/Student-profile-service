#!/usr/bin/env bash
set -euo pipefail

# Normalize SEED-IV feature CSVs into canonical ingest CSV.
# Usage:
#   ./scripts/normalize_seed4.sh <RAW_DIR> <TENANT_ID> <OUT_DIR> [DEVICE]
#
# Example:
#   ./scripts/normalize_seed4.sh \
#     "$PWD/data/SEED-IV/eeg_feature_csv_data" \
#     seed4_demo \
#     "$PWD/tmp/seed4_norm" \
#     seed4-features

RAW_DIR="${1:-}"
TENANT="${2:-}"
OUT_DIR="${3:-}"
DEVICE="${4:-seed4-features}"

if [[ -z "${RAW_DIR}" || -z "${TENANT}" || -z "${OUT_DIR}" ]]; then
  echo "Usage: $0 <RAW_DIR> <TENANT_ID> <OUT_DIR> [DEVICE]" >&2
  exit 1
fi

# Make sure module imports work whether run from repo root or elsewhere
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${ROOT_DIR}:${ROOT_DIR}/tools"

mkdir -p "${OUT_DIR}"

echo "[seed4] normalizing from: ${RAW_DIR} -> ${OUT_DIR}  (tenant=${TENANT}, device=${DEVICE})"
python3 -m tools.normalize \
  --dataset seed4_features \
  --raw_dir "${RAW_DIR}" \
  --out_dir "${OUT_DIR}" \
  --tenant_id "${TENANT}" \
  --device "${DEVICE}"

echo "[seed4] wrote $(ls -1 "${OUT_DIR}"/*.normalized.csv 2>/dev/null | wc -l | xargs) files."
ls -1 "${OUT_DIR}"/*.normalized.csv 2>/dev/null | head -n 10 || true
