# tools/parsers/seed_iv.py
from __future__ import annotations
import csv, json, re
from pathlib import Path
from typing import Dict, Tuple, Iterable, List
from .base import BaseParser, CANONICAL_HEADER

DEFAULT_DEVICE = "seed4-features"

ID_HINTS = {
    "subject","participant","sub","sid","user","user_id",
    "trial","session","run","video","stimulus","clip","window","window_id","segment"
}
LABEL_HINTS = {
    "label","emotion","class","target","y","y_true","ytrue","y_pred","emotion_label"
}
IGNORE_COLS = {"index","Unnamed: 0","Unnamed: 0.1","Unnamed: 1"}

class SEEDIVParser(BaseParser):
    """Parser for SEED-IV CSV mirrors (feature summaries per window)."""

    name = "seed4_features"

    def _partition_columns(self, header: List[str]) -> Tuple[List[str], List[str], List[str]]:
        ids, labels, feats = [], [], []
        for h in header or []:
            if h is None:
                continue
            h_clean = str(h).strip()
            if not h_clean or h_clean in IGNORE_COLS:
                continue
            h_lower = h_clean.lower()
            if h_lower in ID_HINTS:
                ids.append(h_clean)
            elif h_lower in LABEL_HINTS:
                labels.append(h_clean)
            else:
                feats.append(h_clean)
        return ids, labels, feats

    def _ids_from_filename(self, p: Path) -> Tuple[str, str]:
        """Heuristically derive user_id/session_id from filename like 10_20151014.csv."""
        stem = p.stem
        # common patterns
        m = re.search(r'(?P<user>\d+)[-_](?P<date>(?:20)?\d{6,8})', stem)
        if m:
            user = m.group('user')
            session = m.group('date')
            return user, session
        # fallback: first numeric token as user, rest as session
        toks = re.split(r'[-_]', stem)
        user = next((t for t in toks if t.isdigit()), "") or ""
        session = stem if user == "" else stem.replace(user, "", 1).strip("-_")
        return user, session

    def _canon_channel(self, feat: str) -> str:
        f = feat.strip()
        if re.fullmatch(r'\d+', f):
            return f"feat_{int(f):03d}"
        return f

    def _discover_meta(self, row: Dict[str,str], id_cols: List[str], label_cols: List[str]) -> Tuple[str,str,Dict[str,str]]:
        user, session = "", ""
        meta: Dict[str,str] = {}

        # Prefer subject/user for user_id
        for k in id_cols:
            v = row.get(k, "")
            if v and not user and re.search(r"(sub|sid|subject|user)", k, re.I):
                user = str(v)

        # Prefer session/trial/run/window for session_id
        for k in id_cols:
            v = row.get(k, "")
            if v and not session and re.search(r"(session|trial|run|window|segment)", k, re.I):
                session = str(v)

        # Pack remaining ids and labels in meta
        for k in id_cols:
            v = row.get(k, "")
            if v != "":
                meta[k] = str(v)
        for k in label_cols:
            v = row.get(k, "")
            if v != "":
                meta[k] = str(v)
        if "label" not in meta:
            for k in label_cols:
                vv = row.get(k, "")
                if vv != "":
                    meta["label"] = str(vv)
                    break
        return user, session, meta

    def _iter_csv_files(self, raw_root: Path) -> Iterable[Path]:
        for p in raw_root.rglob("*.csv"):
            # skip previously normalized outputs if they accidentally sit in raw
            if p.name.lower().endswith(".normalized.csv"):
                continue
            yield p

    def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = "") -> None:
        raw_root_p = Path(raw_root)
        out_dir_p = Path(out_dir)
        out_dir_p.mkdir(parents=True, exist_ok=True)
        device = device or DEFAULT_DEVICE

        for src in self._iter_csv_files(raw_root_p):
            with src.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                header = reader.fieldnames or []
                id_cols, label_cols, feat_cols = self._partition_columns(header)
                if not feat_cols:
                    continue

                out_csv = out_dir_p / (src.stem + ".normalized.csv")
                with out_csv.open("w", newline="", encoding="utf-8") as g:
                    w = csv.writer(g)
                    w.writerow(CANONICAL_HEADER)

                    for i, row in enumerate(reader):
                        user, session, meta = self._discover_meta(row, id_cols, label_cols)
                        if not user or not session:
                            fu, fs = self._ids_from_filename(src)
                            user = user or fu
                            session = session or fs

                        meta.setdefault("source_file", src.name)
                        seq_no = i
                        ts = self._fmt_epoch_seconds(0.0)
                        sr = self._sr_val(0.0)

                        for feat in feat_cols:
                            val = self._parse_float(row.get(feat, ""), 0.0)
                            channel = self._canon_channel(feat)
                            w.writerow([
                                tenant_id, user, session, device,
                                channel, ts, f"{val:.8f}", f"{sr:.1f}", seq_no,
                                self._safe_meta(meta)
                            ])

                # sidecar: one per source (not per last out_csv); avoid ".normalized.normalized"
                sidecar = out_dir_p / (src.stem + ".normalized.meta.json")
                with sidecar.open("w", encoding="utf-8") as jf:
                    json.dump({
                        "tenant_id": tenant_id,
                        "device": device,
                        "source_csv": src.name,
                        "inferred_id_columns": id_cols,
                        "inferred_label_columns": label_cols,
                        "feature_columns_count": len(feat_cols)
                    }, jf, indent=2, ensure_ascii=False)
