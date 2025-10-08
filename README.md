# EEG & WESAD Ingest Stack

**Objective:** Raw EEG/WESAD CSVs → Parser (normalize to canonical CSV) → Ingest API (FastAPI) → TimescaleDB.

## Quick Start
```bash
cp -n .env.sample .env
chmod +x scripts/*.sh
./scripts/quick_run.sh
```

Now open Swagger at `http://localhost:8090/docs` and try `POST /ingest/batch` with a normalized CSV.

## Folder ingest (Muse)
```bash
./scripts/ingest_folder.sh ./data/muse_raw acme_demo
```

## WESAD (Empatica E4) normalize & ingest
Place the dataset under:
```
./data/wesad/S11/S11_E4_Data/{ACC.csv,BVP.csv,EDA.csv,HR.csv,IBI.csv,TEMP.csv,tags.csv,info.txt}
```
Normalize and ingest:
```bash
./scripts/normalize_wesad.sh ./data/wesad wesad_demo
./scripts/ingest_wesad.sh ./tmp/wesad_normalized wesad_demo
```

## Verify in DB
```bash
PGPASSWORD=ts psql "postgres://ts:ts@localhost:5439/ts" -c "SELECT count(*) FROM public.eeg_samples;"
```

## Canonical CSV Header
```
tenant_id,user_id,session_id,device,channel,ts,value,sr_hz,seq_no,meta
```

Reposts are idempotent thanks to PK (tenant,user,session,channel,ts).
