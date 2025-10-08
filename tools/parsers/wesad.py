import csv, json, re
from pathlib import Path
from typing import List, Tuple
from .base import BaseParser, CANONICAL_HEADER

LABEL_MAP = {0:"transient",1:"baseline",2:"stress",3:"amusement",4:"meditation",5:"ignored",6:"ignored",7:"ignored"}
DEFAULT_SR = {"ACC":32.0,"BVP":64.0,"EDA":4.0,"TEMP":4.0,"HR":1.0}

class WESADParser(BaseParser):
    name = "wesad"

    def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = "wesad-e4") -> None:
        root = Path(raw_root); out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
        for subj_dir in sorted([p for p in root.iterdir() if p.is_dir() and re.match(r"^S\d+", p.name)]):
            user_id = subj_dir.name
            e4_dirs = [d for d in subj_dir.iterdir() if d.is_dir() and re.search(r"e4[_\-]?data", d.name, re.I)]
            if not e4_dirs: e4_dirs = [subj_dir]
            for e4_dir in e4_dirs:
                files = {f.stem.upper(): f for f in e4_dir.glob("*.csv")}
                tags = files.get("TAGS")
                label_points = self._read_tags(tags) if tags and tags.exists() else []
                session_id = f"{user_id}_wesad_e4"
                out_path = out / f"{session_id}.normalized.csv"
                with out_path.open("w", newline="", encoding="utf-8") as f_out:
                    w = csv.writer(f_out); w.writerow(CANONICAL_HEADER); seq = 0
                    if "ACC" in files:
                        seq = self._emit_fixed(w, tenant_id, user_id, session_id, device, "ACC", files["ACC"], DEFAULT_SR["ACC"], seq, acc_axes=True, labels=label_points)
                    for ch in ["BVP","EDA","TEMP","HR"]:
                        if ch in files:
                            seq = self._emit_fixed(w, tenant_id, user_id, session_id, device, ch, files[ch], DEFAULT_SR[ch], seq, acc_axes=False, labels=label_points)
                    if "IBI" in files:
                        seq = self._emit_ibi(w, tenant_id, user_id, session_id, device, "IBI", files["IBI"], seq, labels=label_points)
                print(f"normalized: {user_id} -> {out_path.name}")

    def _detect_header(self, path: Path):
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            first = f.readline().strip()
        return [h.strip() for h in first.split(",")] if first else []

    def _read_csv_rows(self, path: Path):
        rows = []
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            rdr = csv.reader(f); next(rdr, None)
            for r in rdr:
                if r: rows.append([c.strip() for c in r])
        return rows

    def _read_tags(self, path: Path) -> List[Tuple[float,int]]:
        try:
            hdr = self._detect_header(path); rows = self._read_csv_rows(path); out = []
            for r in rows:
                if len(r)>=2:
                    t = float(r[0]); lab = int(float(r[1])); out.append((t, lab))
                elif len(r)==1:
                    lab = int(float(r[0])); out.append((len(out), lab))
            out.sort(key=lambda x: x[0]); return out
        except Exception:
            return []

    def _label_for_time(self, t: float, label_points: List[Tuple[float,int]]):
        if not label_points: return 0, LABEL_MAP[0]
        lo, hi = 0, len(label_points)-1; best = label_points[0]
        while lo <= hi:
            mid = (lo+hi)//2
            if label_points[mid][0] <= t: best = label_points[mid]; lo = mid+1
            else: hi = mid-1
        lab_id = best[1]; return lab_id, LABEL_MAP.get(lab_id, "unknown")

    def _fmt_ts(self, t: float) -> str:
        return f"1970-01-01 00:{int(t//60):02d}:{(t%60):06.3f}Z"

    def _emit_fixed(self, w, tenant, user, session, device, ch_name, path, sr, seq, acc_axes: bool, labels):
        rows = self._read_csv_rows(path); hdr = self._detect_header(path)
        time_first = bool(hdr and hdr[0].lower().startswith(("time","ts","timestamp","epoch")))
        dt = 1.0/float(sr) if sr else 0.0
        if acc_axes:
            for i, r in enumerate(rows, start=1):
                if time_first and len(r)>=4:
                    try: t = float(r[0])
                    except: t = (i-1)*dt
                    vals = r[1:4]
                else:
                    t = (i-1)*dt; vals = r[0:3]
                for c_name, v in zip(["ACC_X","ACC_Y","ACC_Z"], vals):
                    if v=="" or v.lower()=="nan": continue
                    seq+=1; lab_id,lab = self._label_for_time(t, labels)
                    w.writerow([tenant,user,session,device,c_name,self._fmt_ts(t),v,sr,seq,json.dumps({"subject":user,"source":"WESAD","label_id":lab_id,"label_name":lab})])
        else:
            for i, r in enumerate(rows, start=1):
                if time_first and len(r)>=2:
                    try: t = float(r[0])
                    except: t = (i-1)*dt
                    v = r[1]
                else:
                    t = (i-1)*dt; v = r[0] if r else ""
                if v=="" or v.lower()=="nan": continue
                seq+=1; lab_id,lab = self._label_for_time(t, labels)
                w.writerow([tenant,user,session,device,ch_name,self._fmt_ts(t),v,sr,seq,json.dumps({"subject":user,"source":"WESAD","label_id":lab_id,"label_name":lab})])
        return seq

    def _emit_ibi(self, w, tenant, user, session, device, ch_name, path, seq, labels):
        rows = self._read_csv_rows(path); hdr = self._detect_header(path)
        time_first = bool(hdr and hdr[0].lower().startswith(("time","ts","timestamp","epoch")))
        for i, r in enumerate(rows, start=1):
            if time_first and len(r)>=2:
                try: t = float(r[0])
                except: t = float(i-1)
                v = r[1]
            else:
                t = float(i-1); v = r[0] if r else ""
            if v=="" or v.lower()=="nan": continue
            seq+=1; lab_id,lab = self._label_for_time(t, labels)
            w.writerow([tenant,user,session,device,ch_name,self._fmt_ts(t),v,"",seq,json.dumps({"subject":user,"source":"WESAD","label_id":lab_id,"label_name":lab})])
        return seq
