# tools/parsers/base.py
from __future__ import annotations
import json, math
from typing import Any, Dict, Iterable, List, Tuple

# Canonical ingest header (all parsers must write exactly this)
CANONICAL_HEADER: List[str] = [
    "tenant_id","user_id","session_id","device",
    "channel","ts","value","sr_hz","seq_no","meta"
]

class BaseParser:
    """Common helpers for all dataset normalizers.
    Subclasses must implement .normalize(...).
    """

    # --- time helpers (epoch-seconds -> ISO on epoch day) ---
    def _fmt_epoch_seconds(self, t: float) -> str:
        # We don't need real timestamps for feature rows. Use epoch with ms=0.. for determinism.
        if t is None or (isinstance(t, float) and math.isnan(t)):
            t = 0.0
        # Represent as integer milliseconds as a string to keep loaders simple.
        # Many backends accept ISO8601 strings; we keep a compact, unambiguous numeric string instead.
        return str(int(round(float(t) * 1000.0)))

    # --- number parsing (robust) ---
    def _parse_float(self, x: Any, default: float = 0.0) -> float:
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return default
            return float(x)
        except Exception:
            try:
                s = str(x).strip()
                if s == "" or s.lower() in {"nan", "none"}:
                    return default
                return float(s)
            except Exception:
                return default

    # --- meta JSON in a stable, compact representation ---
    def _safe_meta(self, kv: Dict[str, Any]) -> str:
        return json.dumps(kv, separators=(",", ":"), ensure_ascii=False)

    # --- sr_hz: ALWAYS numeric for DB (0 means “feature/no rate”) ---
    def _sr_val(self, sr: Any) -> float:
        try:
            if sr is None or sr == "":
                return 0.0
            return float(sr)
        except Exception:
            return 0.0

    # --- required interface for subclasses ---
    def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = "") -> None:
        raise NotImplementedError("Subclasses must implement .normalize(...)")
