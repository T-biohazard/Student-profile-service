from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
import csv, io, gzip, os
from .db import get_conn
from typing import List

API_KEY = os.getenv("API_KEY", "changeme")

CANONICAL_HEADER = [
    "tenant_id","user_id","session_id","device","channel",
    "ts","value","sr_hz","seq_no","meta"
]

app = FastAPI(title="EEG/WESAD Ingest API")

@app.get("/healthz")
def healthz():
    return {"ok": True}

def _read_csv(file_bytes: bytes) -> List[List[str]]:
    # Handle optional gzip
    try:
        if file_bytes[:2] == b'\x1f\x8b':
            with gzip.GzipFile(fileobj=io.BytesIO(file_bytes)) as gz:
                text = gz.read().decode("utf-8", "ignore")
        else:
            text = file_bytes.decode("utf-8", "ignore")
    except Exception:
        text = file_bytes.decode("utf-8", "ignore")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    return rows

@app.post("/ingest/batch")
async def ingest_batch(
    file: UploadFile = File(...),
    tenant_id: str = Query(...),
    user_id: str = Query(...),
    session_id: str = Query(...),
    device: str = Query("muse2"),
    api_key: str = Query("changeme"),
    mode: str = Query("upsert")
):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="bad api_key")

    content = await file.read()
    rows = _read_csv(content)
    if not rows:
        return {"status":"ok","inserted":0,"seen":0}

    header = rows[0]
    if header != CANONICAL_HEADER:
        raise HTTPException(status_code=422, detail=f"bad header, expected {CANONICAL_HEADER}, got {header}")

    data_rows = rows[1:]
    seen = len(data_rows)

    with get_conn() as conn:
        cur = conn.cursor()
        if mode == "replace":
            cur.execute(
                # Only purge existing rows for this session when mode=replace
                # (for upsert/idempotent loads this must NOT run)
                ("DELETE FROM public.eeg_samples WHERE tenant_id=%s AND user_id=%s AND session_id=%s" if mode=="replace" else None),
                (tenant_id, user_id, session_id)
            )
            conn.commit()
        inserted = 0
        for r in data_rows:
            # Enforce tenant/user/session/device from params (ignore per-row to prevent mismatch)
            r2 = [tenant_id, user_id, session_id, device] + r[4:]
            try:
                cur.execute(
                    """                    INSERT INTO public.eeg_samples
                    (tenant_id,user_id,session_id,device,channel,ts,value,sr_hz,seq_no,meta)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """, r2
                )
                if cur.rowcount == 1:
                    inserted += 1
            except Exception as e:
                # Skip malformed row
                if inserted % 10000 == 0:
                    print(f"[ingest] row error: {e}", flush=True)
                continue
        conn.commit()
    return JSONResponse({"status":"ok","inserted":inserted,"seen":seen})
