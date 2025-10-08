#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:${API_PORT:-8090}}"
KEY="${API_KEY:-changeme}"

cat > tmp/sample_ingest_demo.normalized.csv <<CSV
tenant_id,user_id,session_id,device,channel,ts,value,sr_hz,seq_no,meta
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AF7,2024-06-02 09:47:17.286Z,101,,1,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AF8,2024-06-02 09:47:17.286Z,102,,2,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,TP9,2024-06-02 09:47:17.286Z,103,,3,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,TP10,2024-06-02 09:47:17.286Z,104,,4,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AUX,2024-06-02 09:47:17.286Z,105,,5,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AF7,2024-06-02 09:47:18.286Z,106,,6,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AF8,2024-06-02 09:47:18.286Z,107,,7,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,TP9,2024-06-02 09:47:18.286Z,108,,8,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,TP10,2024-06-02 09:47:18.286Z,109,,9,"{\"kind\": \"raw\"}"
acme_demo,museMonitor,museMonitor_2024-06-02--09-47-17_demo,muse2,AUX,2024-06-02 09:47:18.286Z,110,,10,"{\"kind\": \"raw\"}"
CSV

curl -fsS -X POST -F "file=@tmp/sample_ingest_demo.normalized.csv"   "$API/ingest/batch?api_key=$KEY&tenant_id=acme_demo&user_id=museMonitor&session_id=museMonitor_2024-06-02--09-47-17_demo&device=muse2"
