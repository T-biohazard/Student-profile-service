# tools/normalize.py
from __future__ import annotations
import argparse, sys
from pathlib import Path

# Local parsers
from tools.parsers.seed_iv import SEEDIVParser
from tools.parsers.wesad import WESADParser  # kept for parity with other scripts
from tools.parsers.kaggle_stress import KaggleStressParser  # present in repo

PARSERS = {
    "seed4_features": SEEDIVParser(),
    "wesad": WESADParser(),
    "kaggle_stress": KaggleStressParser(),
}

def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize raw datasets into canonical ingest CSV format.")
    ap.add_argument("--dataset", required=True, choices=PARSERS.keys())
    ap.add_argument("--raw_dir", required=True, help="Path to raw dataset root (folder with CSV files).")
    ap.add_argument("--out_dir", required=True, help="Folder to write normalized CSVs.")
    ap.add_argument("--tenant_id", required=True, help="Tenant/org code to embed in rows.")
    ap.add_argument("--device", default="", help="Override device string to embed in rows.")
    args = ap.parse_args()

    parser = PARSERS[args.dataset]
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    parser.normalize(args.raw_dir, args.out_dir, args.tenant_id, args.device or getattr(parser, "name", ""))

if __name__ == "__main__":
    main()
