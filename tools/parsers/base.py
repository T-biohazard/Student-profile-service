from typing import Iterable, Dict, Any

CANONICAL_HEADER = [
    "tenant_id","user_id","session_id","device","channel",
    "ts","value","sr_hz","seq_no","meta"
]

class BaseParser:
    name: str = "base"
    def normalize(self, raw_root: str, out_dir: str, tenant_id: str, device: str = "") -> None:
        raise NotImplementedError
