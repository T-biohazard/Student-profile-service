# #!/usr/bin/env bash
# set -euo pipefail
# cd "$(dirname "$0")/.."

# cp -n .env.sample .env || true
# docker compose build
# docker compose up -d

# # initialize schema
# echo ">> Waiting for DB..."
# for i in {1..60}; do
#   if PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT:-5439}/ts" -c "SELECT 1" >/dev/null 2>&1; then
#     break
#   fi
#   sleep 1
# done
# PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT:-5439}/ts" -f sql/schema.sql

# # wait for API
# echo ">> Waiting for API..."
# for i in {1..60}; do
#   if curl -fsS "http://localhost:${API_PORT:-8090}/healthz" >/dev/null; then
#     echo "API is up"; break
#   fi
#   sleep 1
# done

# # tiny seed to verify idempotency
# echo "status, verify idempotency"
# cat > tmp/seed.normalized1.csv <<CSV
# tenant_id,user_id,session_id,device,channel,ts,value,sr_hz,seq_no,meta
# acme_demo1,museMonitor1,testSession_2025-06-02--09-47-17,muse2,AF7,2024-06-03 09:47:17.286Z,1,,4,"{\"kind\":\"raw\"}"
# acme_demo1,museMonitor1,testSession_2025-06-02--09-47-17,muse2,AF7,2024-06-03 09:47:17.286Z,1,,4,"{\"kind\":\"raw\"}"
# acme_demo1,museMonitor1,testSession_2025-06-02--09-47-17,muse2,AF8,2024-06-03 09:47:17.286Z,2,,4,"{\"kind\":\"raw\"}"
# CSV

# curl -fsS -X POST -F "file=@tmp/seed.normalized.csv"   "http://localhost:${API_PORT:-8090}/ingest/batch?api_key=${API_KEY:-changeme}&tenant_id=acme_demo&user_id=museMonitor&session_id=testSession_2024-06-02--09-47-17&device=muse2"

# curl -fsS -X POST -F "file=@tmp/seed.normalized.csv"   "http://localhost:${API_PORT:-8090}/ingest/batch?api_key=${API_KEY:-changeme}&tenant_id=acme_demo&user_id=museMonitor&session_id=testSession_2024-06-02--09-47-17&device=muse2"

# echo "--- done quick_run"



#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# ----- 1) Env -----
if [[ ! -f .env ]]; then
  cat > .env <<'ENV'
API_KEY=changeme
API_PORT=8090
HOST_PGPORT=5439
TIMESCALE_URL=postgres://ts:ts@timescale:5432/ts
ENV
  echo "Created .env with defaults."
fi
set -a; source .env; set +a

# ----- 2) Build & up -----
docker compose build
docker compose up -d

# ----- 3) DB ready & schema -----
echo ">> Waiting for DB..."
for i in {1..60}; do
  if PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT}/ts" -c "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT}/ts" -f sql/schema.sql

# ----- 4) API ready -----
echo ">> Waiting for API..."
for i in {1..60}; do
  if curl -fsS "http://localhost:${API_PORT}/healthz" >/dev/null; then
    echo "API is up"; break
  fi
  sleep 1
done

# ----- 5) Seed file (name matches curl below) -----
mkdir -p tmp
cat > tmp/seed.normalized.csv <<'CSV'
tenant_id,user_id,session_id,device,channel,ts,value,sr_hz,seq_no,meta
acme_demo,museMonitor,testSession_2024-06-02--09-47-17,muse2,AF7,2024-06-03T09:47:17.286Z,1,,1,"{}"
acme_demo,museMonitor,testSession_2024-06-02--09-47-17,muse2,AF8,2024-06-03T09:47:17.286Z,2,,2,"{}"
CSV


# (optional) wipe that session so first post definitely inserts >0
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT}/ts" -c \
"DELETE FROM public.eeg_samples
 WHERE tenant_id='acme_demo' AND user_id='museMonitor'
   AND session_id='testSession_2024-06-02--09-47-17';" >/dev/null

# ----- 6) Post twice (idempotency demo) -----
echo "status, verify idempotency"
curl -fsS -X POST -F "file=@tmp/seed.normalized.csv" \
  "http://localhost:${API_PORT}/ingest/batch?api_key=${API_KEY}&tenant_id=acme_demo&user_id=museMonitor&session_id=testSession_2024-06-02--09-47-17&device=muse2" && echo
curl -fsS -X POST -F "file=@tmp/seed.normalized.csv" \
  "http://localhost:${API_PORT}/ingest/batch?api_key=${API_KEY}&tenant_id=acme_demo&user_id=museMonitor&session_id=testSession_2024-06-02--09-47-17&device=muse2" && echo

# ----- 7) Show counts -----
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT}/ts" -c "SELECT COUNT(*) AS total FROM public.eeg_samples;"
PGPASSWORD=ts psql "postgres://ts:ts@localhost:${HOST_PGPORT}/ts" -c \
"SELECT tenant_id, COUNT(*) AS rows
 FROM public.eeg_samples GROUP BY 1 ORDER BY 2 DESC;"
echo "--- done quick_run"
