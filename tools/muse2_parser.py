#!/usr/bin/env python3
import argparse, csv, json, re
from pathlib import Path
from datetime import datetime

CANONICAL_HEADER = [
    "tenant_id","user_id","session_id","device","channel",
    "ts","value","sr_hz","seq_no","meta"
]

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())

def detect_timestamp_field(fieldnames):
    if not fieldnames: return None
    for pref in ["Timestamp","TimeStamp","timestamp","Time","time"]:
        if pref in fieldnames:
            return pref
    for f in fieldnames:
        n = norm(f)
        if any(key in n for key in ["timestamp","time","datetime","utc","date"]):
            return f
    return None

def detect_channel_fields(fieldnames):
    m = {}
    for f in fieldnames or []:
        n = norm(f)
        for code in ["af7","af8","tp9","tp10","aux","rightaux","leftaux"]:
            if code in n:
                pretty = (
                    "AF7" if "af7" in code else
                    "AF8" if "af8" in code else
                    "TP9" if "tp9" in code else
                    "TP10" if "tp10" in code else
                    "Right AUX" if "rightaux" in code else
                    "Left AUX" if "leftaux" in code else
                    "AUX"
                )
                m.setdefault(pretty, f)
    return m

def stem_as_ids(stem: str):
    parts = stem.split("_")
    user_id = parts[0] if parts else "museUser"
    session_id = stem
    return user_id, session_id

def parse_ts(tstr: str) -> str:
    t = tstr.strip()
    try:
        _ = datetime.fromisoformat(t.replace("Z","+00:00"))
        return t
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S.%f", "%m/%d/%Y %H:%M:%S.%f"):
        try:
            dt = datetime.strptime(t, fmt)
            return dt.isoformat() + "Z"
        except Exception:
            continue
    return t

def normalize_file(in_path: Path, out_dir: Path, tenant_id: str, device: str = "muse2") -> Path:
    stem = in_path.stem
    user_id, session_id = stem_as_ids(stem)
    out_path = out_dir / f"{stem}.normalized.csv"

    with in_path.open("r", newline="", encoding="utf-8", errors="ignore") as f_in, out_path.open("w", newline="") as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(CANONICAL_HEADER)

        if not reader.fieldnames:
            return out_path

        ts_field = detect_timestamp_field(reader.fieldnames)
        chan_map = detect_channel_fields(reader.fieldnames)

        if not ts_field or not chan_map:
            return out_path

        seq_no = 0
        for row in reader:
            raw_ts = row.get(ts_field)
            if not raw_ts:
                continue
            tstr = parse_ts(raw_ts)

            for pretty, source_col in chan_map.items():
                val = row.get(source_col, "")
                if val in (None, "", "NaN"):
                    continue
                seq_no += 1
                meta = {"kind": "raw", "source_col": source_col}
                writer.writerow([
                    tenant_id, user_id, session_id, device, pretty, tstr, val, "", seq_no, json.dumps(meta)
                ])

    return out_path

def main():
    ap = argparse.ArgumentParser(description="Normalize Muse2 raw CSVs to canonical ingest format")
    ap.add_argument("--raw_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--tenant_id", required=True)
    ap.add_argument("--device", default="muse2")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for p in sorted(raw_dir.glob("*.csv")):
        out_p = normalize_file(p, out_dir, tenant_id=args.tenant_id, device=args.device)
        print(f"normalized: {p.name} -> {out_p.name}")

if __name__ == "__main__":
    main()
