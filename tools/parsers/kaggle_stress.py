# # tools/parsers/kaggle_stress.py
# import csv, json, re
# from pathlib import Path
# from typing import Dict, Optional, Tuple, Iterable
# from .base import BaseParser, CANONICAL_HEADER

# DEFAULT_DEVICE = "kaggle-stress"

# # Fallback sample rates used only when a CSV has no explicit time column
# DEFAULT_SR: Dict[str, float] = {
#     "EDA": 4.0, "GSR": 4.0,
#     "BVP": 64.0, "PPG": 64.0,
#     "TEMP": 4.0, "HR": 1.0, "RESP": 32.0,
#     "ACC_X": 32.0, "ACC_Y": 32.0, "ACC_Z": 32.0,
# }

# # Column aliasing (normalize keys before lookup)
# ALIASES_RAW = {
#     "eda":"EDA", "gsr":"EDA",
#     "bvp":"BVP", "ppg":"BVP",
#     "temp":"TEMP", "temperature":"TEMP",
#     "hr":"HR", "heart_rate":"HR",
#     "resp":"RESP", "respiration":"RESP", "rr":"RESP",
#     "ax":"ACC_X", "accx":"ACC_X", "acc_x":"ACC_X",
#     "ay":"ACC_Y", "accy":"ACC_Y", "acc_y":"ACC_Y",
#     "az":"ACC_Z", "accz":"ACC_Z", "acc_z":"ACC_Z",
# }
# TIME_HINTS = ("time","timestamp","ts","epoch","datetime","date")
# LABEL_HINTS_NORM = {"label","class","target","stress","y","ytrue","y_true"}

# def _norm(s: str) -> str:
#     return re.sub(r"[^a-z0-9]+", "", s.lower()) if s else ""

# ALIASES: Dict[str, str] = { _norm(k): v for k, v in ALIASES_RAW.items() }

# def _is_time_col(name: str) -> bool:
#     n = _norm(name)
#     return any(h in n for h in TIME_HINTS)

# def _canonical_col(name: str) -> Optional[str]:
#     n = _norm(name)
#     if n in ALIASES:
#         return ALIASES[n]
#     up = name.strip().upper()
#     if up in DEFAULT_SR:
#         return up
#     return None

# def _stream_rows(path: Path) -> Tuple[Iterable[list], list]:
#     f = path.open("r", encoding="utf-8", errors="ignore", newline="")
#     rdr = csv.reader(f)
#     header = next(rdr, None) or []
#     def gen():
#         try:
#             for r in rdr:
#                 if r:
#                     yield [c.strip() for c in r]
#         finally:
#             f.close()
#     return gen(), [h.strip() for h in header]

# def _float_or_none(x: str) -> Optional[float]:
#     try: return float(x)
#     except: return None

# def _parse_time_cell(cell: str) -> Tuple[Optional[float], Optional[str]]:
#     """
#     Returns (t_numeric_seconds, t_string_iso_or_none).
#     Accepts seconds or milliseconds; passes ISO/any string timestamps through as-is.
#     """
#     if not cell:
#         return None, None
#     tnum = _float_or_none(cell)
#     if tnum is not None:
#         # Heuristic: treat very large values as milliseconds epoch and convert to seconds
#         if tnum > 1e10:   # e.g., 1690999999000
#             tnum = tnum / 1000.0
#         return tnum, None
#     # Not numeric: assume it's a usable timestamp string (e.g., 2023-08-02T12:34:56Z)
#     return None, cell

# class KaggleStressParser(BaseParser):
#     name = "kaggle_stress"

#     def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = DEFAULT_DEVICE) -> None:
#         root = Path(raw_root); out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
#         csvs = sorted(root.rglob("*.csv"))
#         if not csvs:
#             print(f"[kaggle_stress] No CSVs under {root}")
#             return

#         for p in csvs:
#             # One CSV -> one session
#             user_id, session_id = self._ids_from_path(p)
#             out_path = out / f"{session_id}.normalized.csv"

#             rows_iter, header = _stream_rows(p)
#             if not header:
#                 print(f"[kaggle_stress] Skip (no header): {p}")
#                 continue

#             # identify time column and an optional label/target column
#             time_idx: Optional[int] = None
#             label_idx: Optional[int] = None
#             for i, h in enumerate(header):
#                 if time_idx is None and _is_time_col(h):
#                     time_idx = i
#                 if label_idx is None and _norm(h) in LABEL_HINTS_NORM:
#                     label_idx = i

#             # map channel columns -> canonical names
#             chan_cols: Dict[str, int] = {}
#             for i, h in enumerate(header):
#                 if i == time_idx or i == label_idx:
#                     continue
#                 canon = _canonical_col(h)
#                 if canon:
#                     chan_cols.setdefault(canon, i)  # first match wins

#             numeric_only = not bool(chan_cols)

#             seq = 0
#             with out_path.open("w", newline="", encoding="utf-8") as f_out:
#                 w = csv.writer(f_out)
#                 w.writerow(CANONICAL_HEADER)

#                 if not numeric_only:
#                     # Pre-compute dt if synthesizing time
#                     per_chan_dt: Dict[str, Optional[float]] = {
#                         ch: (1.0 / DEFAULT_SR[ch]) if ch in DEFAULT_SR else None
#                         for ch in chan_cols.keys()
#                     }

#                     for i, r in enumerate(rows_iter, start=1):
#                         # Parse time (numeric seconds or passthrough string)
#                         t_num: Optional[float] = None
#                         t_str: Optional[str] = None
#                         if time_idx is not None and time_idx < len(r):
#                             t_num, t_str = _parse_time_cell(r[time_idx])

#                         # optional label
#                         row_label = None
#                         if label_idx is not None and label_idx < len(r):
#                             row_label = r[label_idx].strip()

#                         for ch, col in chan_cols.items():
#                             if col >= len(r): 
#                                 continue
#                             v = r[col].strip()
#                             if v == "" or v.lower() == "nan":
#                                 continue

#                             seq += 1
#                             # if t_num is not None:
#                             #     ts = self._fmt_epoch_seconds(t_num); sr_val = ""
#                             # elif t_str is not None:
#                             #     ts = t_str; sr_val = ""
#                             # else:
#                             #     dt = per_chan_dt.get(ch)
#                             #     t_synth = (i - 1) * (dt if dt else 0.0)
#                             #     ts = self._fmt_epoch_seconds(t_synth)
#                             #     sr_val = (1.0 / dt) if dt else ""

#                             # meta = {"source": "KaggleStress", "file": str(p.name)}
#                             # if row_label is not None:
#                             #     meta["label"] = row_label

#                             # w.writerow([
#                             #     tenant_id, user_id, session_id, device, ch,
#                             #     ts, v, sr_val, seq, json.dumps(meta)
#                             # ])
#                             # (time-series branch)
#                    # now:
#                         if t_num is not None:
#                             ts = self._fmt_epoch_seconds(t_num); sr_val = 0
#                         elif t_str is not None:
#                             ts = t_str; sr_val = 0
#                         else:
#                             dt = per_chan_dt.get(ch)
#                             t_synth = (i - 1) * (dt if dt else 0.0)
#                             ts = self._fmt_epoch_seconds(t_synth)
#                             sr_val = (1.0 / dt) if dt else 0

#                         # (survey fallback branch)
#                         # was:
# # w.writerow([tenant_id, user_id, session_id, device, ch, ts, f"{v}", "", seq, json.dumps(meta)])

# # now:
#                         w.writerow([tenant_id, user_id, session_id, device, ch, ts, f"{v}", 0, seq, json.dumps(meta)])

#                 else:
#                     # Tabular/survey fallback: output per-numeric-column summary rows
#                     # Gather numeric columns (skip time/label)
#                     num_cols = []
#                     for i, h in enumerate(header):
#                         if i == time_idx or i == label_idx:
#                             continue
#                         # Peek for any numeric value in this column
#                         saw_num = False
#                         for rr in rows_iter:
#                             if i < len(rr) and _float_or_none(rr[i]) is not None:
#                                 saw_num = True
#                                 break
#                         # rewind generator is hard; re-open stream for next checks
#                         if saw_num:
#                             num_cols.append((h, i))
#                         rows_iter, _ = _stream_rows(p)  # re-open to continue iteration safely

#                     ts = self._fmt_epoch_seconds(0.0)  # neutral timestamp
#                     for h, i in num_cols:
#                         # compute simple average as a single representative value
#                         vals = []
#                         for rr in rows_iter:
#                             if i < len(rr):
#                                 fv = _float_or_none(rr[i])
#                                 if fv is not None:
#                                     vals.append(fv)
#                         if not vals:
#                             rows_iter, _ = _stream_rows(p)
#                             continue

#                         v = sum(vals) / len(vals)
#                         ch = _canonical_col(h) or h.strip().upper()
#                         seq += 1
#                         meta = {"source": "KaggleStress", "file": str(p.name), "kind": "survey"}
#                         w.writerow([tenant_id, user_id, session_id, device, ch, ts, f"{v}", "", seq, json.dumps(meta)])
#                         rows_iter, _ = _stream_rows(p)

#             print(f"normalized: {p.name} -> {out_path.name}")

#     # ---- helpers ----
#     def _ids_from_path(self, p: Path) -> Tuple[str, str]:
#         parent = p.parent.name
#         user_id = parent if re.match(r"^[A-Za-z0-9_\-]+$", parent) else "user"
#         stem = p.stem
#         session_id = f"{user_id}_kaggle_stress_{stem}"
#         return user_id, session_id

#     def _fmt_epoch_seconds(self, t: float) -> str:
#         # Represent seconds since epoch as HH:MM:SS.sss on epoch baseline (like WESAD helper)
#         mins = int(t // 60)
#         secs = t - mins * 60
#         return f"1970-01-01 00:{mins:02d}:{secs:06.3f}Z"




# tools/parsers/kaggle_stress.py
import csv, json, re
from pathlib import Path
from typing import Dict, Optional, Tuple, Iterable, List
from .base import BaseParser, CANONICAL_HEADER

DEFAULT_DEVICE = "kaggle-stress"

# Fallback sample rates only used when a CSV has no explicit time column (we synthesize time)
DEFAULT_SR: Dict[str, float] = {
    "EDA": 4.0, "GSR": 4.0,
    "BVP": 64.0, "PPG": 64.0,
    "TEMP": 4.0, "HR": 1.0, "RESP": 32.0,
    "ACC_X": 32.0, "ACC_Y": 32.0, "ACC_Z": 32.0,
}

# Column aliasing (normalize keys before lookup)
ALIASES_RAW = {
    "eda":"EDA", "gsr":"EDA",
    "bvp":"BVP", "ppg":"BVP",
    "temp":"TEMP", "temperature":"TEMP",
    "hr":"HR", "heart_rate":"HR",
    "resp":"RESP", "respiration":"RESP", "rr":"RESP",
    "ax":"ACC_X", "accx":"ACC_X", "acc_x":"ACC_X",
    "ay":"ACC_Y", "accy":"ACC_Y", "acc_y":"ACC_Y",
    "az":"ACC_Z", "accz":"ACC_Z", "acc_z":"ACC_Z",
}
TIME_HINTS = ("time","timestamp","ts","epoch","datetime","date")
LABEL_HINTS_NORM = {"label","class","target","stress","y","ytrue","y_true"}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower()) if s else ""

ALIASES: Dict[str, str] = { _norm(k): v for k, v in ALIASES_RAW.items() }

def _is_time_col(name: str) -> bool:
    n = _norm(name)
    return any(h in n for h in TIME_HINTS)

def _canonical_col(name: str) -> Optional[str]:
    n = _norm(name)
    if n in ALIASES:
        return ALIASES[n]
    up = name.strip().upper()
    if up in DEFAULT_SR:
        return up
    return None

def _stream_rows(path: Path) -> Tuple[Iterable[List[str]], List[str]]:
    f = path.open("r", encoding="utf-8", errors="ignore", newline="")
    rdr = csv.reader(f)
    header = next(rdr, None) or []
    def gen():
        try:
            for r in rdr:
                if r:
                    yield [c.strip() for c in r]
        finally:
            f.close()
    return gen(), [h.strip() for h in header]

def _read_all_rows(path: Path) -> Tuple[List[List[str]], List[str]]:
    # Convenience for survey mode to avoid generator rewinds
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr, None) or []
        rows = [[c.strip() for c in r] for r in rdr if r]
    return rows, [h.strip() for h in header]

def _float_or_none(x: str) -> Optional[float]:
    try:
        return float(x)
    except:
        return None

def _parse_time_cell(cell: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Returns (t_numeric_seconds, t_string_iso_or_none).
    Accepts seconds or milliseconds; passes ISO/any string timestamps through as-is.
    """
    if not cell:
        return None, None
    tnum = _float_or_none(cell)
    if tnum is not None:
        # Heuristic: treat very large values as milliseconds epoch and convert to seconds
        if tnum > 1e10:   # e.g., 1690999999000
            tnum = tnum / 1000.0
        return tnum, None
    # Not numeric: assume it's a usable timestamp string (e.g., 2023-08-02T12:34:56Z)
    return None, cell

class KaggleStressParser(BaseParser):
    name = "kaggle_stress"

    def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = DEFAULT_DEVICE) -> None:
        root = Path(raw_root); out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
        csvs = sorted(root.rglob("*.csv"))
        if not csvs:
            print(f"[kaggle_stress] No CSVs under {root}")
            return

        for p in csvs:
            # One CSV -> one session
            user_id, session_id = self._ids_from_path(p)
            out_path = out / f"{session_id}.normalized.csv"

            # Stream for time-series path; we’ll also materialize when needed for survey path
            rows_iter, header = _stream_rows(p)
            if not header:
                print(f"[kaggle_stress] Skip (no header): {p}")
                continue

            # identify time column and an optional label/target column
            time_idx: Optional[int] = None
            label_idx: Optional[int] = None
            for i, h in enumerate(header):
                if time_idx is None and _is_time_col(h):
                    time_idx = i
                if label_idx is None and _norm(h) in LABEL_HINTS_NORM:
                    label_idx = i

            # map channel columns -> canonical names
            chan_cols: Dict[str, int] = {}
            for i, h in enumerate(header):
                if i == time_idx or i == label_idx:
                    continue
                canon = _canonical_col(h)
                if canon:
                    chan_cols.setdefault(canon, i)  # first match wins

            numeric_only = not bool(chan_cols)

            seq = 0
            with out_path.open("w", newline="", encoding="utf-8") as f_out:
                w = csv.writer(f_out)
                w.writerow(CANONICAL_HEADER)

                if not numeric_only:
                    # Pre-compute dt if synthesizing time
                    per_chan_dt: Dict[str, Optional[float]] = {
                        ch: (1.0 / DEFAULT_SR[ch]) if ch in DEFAULT_SR else None
                        for ch in chan_cols.keys()
                    }

                    for i, r in enumerate(rows_iter, start=1):
                        # Parse time (numeric seconds or passthrough string)
                        t_num: Optional[float] = None
                        t_str: Optional[str] = None
                        if time_idx is not None and time_idx < len(r):
                            t_num, t_str = _parse_time_cell(r[time_idx])

                        # optional label
                        row_label = None
                        if label_idx is not None and label_idx < len(r):
                            row_label = r[label_idx].strip()

                        for ch, col in chan_cols.items():
                            if col >= len(r):
                                continue
                            v = r[col].strip()
                            if v == "" or v.lower() == "nan":
                                continue

                            # Decide timestamp + sr_hz (always numeric)
                            if t_num is not None:
                                ts = self._fmt_epoch_seconds(t_num)
                                sr_val = 0
                            elif t_str is not None:
                                ts = t_str
                                sr_val = 0
                            else:
                                dt = per_chan_dt.get(ch)
                                t_synth = (i - 1) * (dt if dt else 0.0)
                                ts = self._fmt_epoch_seconds(t_synth)
                                sr_val = DEFAULT_SR.get(ch, 0)

                            seq += 1
                            meta = {"source": "KaggleStress", "file": str(p.name)}
                            if row_label is not None:
                                meta["label"] = row_label

                            w.writerow([
                                tenant_id, user_id, session_id, device, ch,
                                ts, v, sr_val, seq, json.dumps(meta)
                            ])

                else:
                    # Tabular/survey fallback: output per-numeric-column summary rows
                    rows_all, header_all = _read_all_rows(p)

                    # gather numeric columns (skip time/label)
                    num_cols: List[Tuple[str, int]] = []
                    for idx, h in enumerate(header_all):
                        if idx == time_idx or idx == label_idx:
                            continue
                        # if any value in the column is numeric, treat it as numeric
                        for rr in rows_all:
                            if idx < len(rr) and _float_or_none(rr[idx]) is not None:
                                num_cols.append((h, idx))
                                break

                    # neutral timestamp
                    ts = self._fmt_epoch_seconds(0.0)

                    for h, idx in num_cols:
                        vals: List[float] = []
                        for rr in rows_all:
                            if idx < len(rr):
                                fv = _float_or_none(rr[idx])
                                if fv is not None:
                                    vals.append(fv)
                        if not vals:
                            continue

                        v = sum(vals) / len(vals)
                        ch = _canonical_col(h) or h.strip().upper()
                        seq += 1
                        meta = {"source": "KaggleStress", "file": str(p.name), "kind": "survey"}
                        # sr_hz must be numeric -> use 0 for survey aggregates
                        w.writerow([tenant_id, user_id, session_id, device, ch, ts, f"{v}", 0, seq, json.dumps(meta)])

            print(f"normalized: {p.name} -> {out_path.name}")

    # ---- helpers ----
    def _ids_from_path(self, p: Path) -> Tuple[str, str]:
        parent = p.parent.name
        user_id = parent if re.match(r"^[A-Za-z0-9_\-]+$", parent) else "user"
        stem = p.stem
        session_id = f"{user_id}_kaggle_stress_{stem}"
        return user_id, session_id

    def _fmt_epoch_seconds(self, t: float) -> str:
        # Represent seconds since epoch as HH:MM:SS.sss on epoch baseline (like WESAD helper)
        mins = int(t // 60)
        secs = t - mins * 60
        return f"1970-01-01 00:{mins:02d}:{secs:06.3f}Z"
