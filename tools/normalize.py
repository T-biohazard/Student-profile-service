import argparse, sys
from .parsers.kaggle_stress import KaggleStressParser
from pathlib import Path

# local parsers
from .parsers.wesad import WESADParser
# legacy muse parser in tools root (kept as-is)
import subprocess, os
from .parsers.kaggle_stress import KaggleStressParser  # <-- new

# tools/normalize.py  (top section)

import argparse, sys, subprocess, os
from pathlib import Path

from .parsers.wesad import WESADParser
from .parsers.kaggle_stress import KaggleStressParser

DATASETS = {
    "wesad": WESADParser(),
    "kaggle_stress": KaggleStressParser(),
}

def main():
    ap = argparse.ArgumentParser(description="Normalize datasets to canonical ingest CSV")
    ap.add_argument("--dataset", required=True, choices=list(DATASETS.keys()) + ["muse2"])
    ap.add_argument("--raw_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--tenant_id", required=True)
    ap.add_argument("--device", default="")
    args = ap.parse_args()

    if args.dataset == "muse2":
        # call legacy script to avoid duplication
        cmd = [
            "python3", "tools/muse2_parser.py",
            "--raw_dir", args.raw_dir,
            "--out_dir", args.out_dir,
            "--tenant_id", args.tenant_id,
            "--device", args.device or "muse2"
        ]
        print("Running:", " ".join(cmd))
        subprocess.check_call(cmd)
        return

    parser = DATASETS[args.dataset]
    parser.normalize(args.raw_dir, args.out_dir, args.tenant_id, args.device or parser.name)

if __name__ == "__main__":
    main()
