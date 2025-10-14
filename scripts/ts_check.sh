#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env if needed) ---
DB_USER="${DB_USER:-ts}"
DB_PASS="${DB_PASS:-ts}"
DB_NAME="${DB_NAME:-ts}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-}"   # if empty, auto-detect from Docker
# ------------------------------------------

# Auto-detect port if not provided (looks for a running container exposing 5432)
if [[ -z "$DB_PORT" ]]; then
  # Try docker compose service first
  DB_PORT="$(docker port "$(docker ps --format '{{.Names}}' | head -n1)" 2>/dev/null | awk -F: '/5432\/tcp/ {print $2; exit}' || true)"
  # Fallback: find any container with 5432 published
  [[ -z "$DB_PORT" ]] && DB_PORT="$(docker ps --format '{{.ID}}' | xargs -I{} docker port {} 2>/dev/null | awk -F: '/5432\/tcp/ {print $2; exit}' || true)"
  # Final fallback
  [[ -z "$DB_PORT" ]] && DB_PORT=5432
fi

export PGPASSWORD="$DB_PASS"
PSQL="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME --no-align --pset pager=off"

echo "== Connection =="
echo "postgres://$DB_USER:****@$DB_HOST:$DB_PORT/$DB_NAME"

$PSQL <<'SQL'
\pset border 2
SELECT version() AS pg_version;
SELECT extname, extversion FROM pg_extension WHERE extname LIKE 'timescaledb%';

-- All hypertables (Timescale):
SELECT hypertable_schema, hypertable_name, compression_enabled
FROM timescaledb_information.hypertables
ORDER BY 1,2;

-- Find tables that look like our canonical format:
WITH cols AS (
  SELECT table_schema, table_name,
         COUNT(*) FILTER (WHERE column_name IN
           ('tenant_id','user_id','session_id','device','channel','ts','value','sr_hz','seq_no','meta')) AS hits
  FROM information_schema.columns
  WHERE table_schema NOT IN ('pg_catalog','information_schema')
  GROUP BY 1,2
)
SELECT table_schema, table_name, hits
FROM cols
WHERE hits >= 8
ORDER BY hits DESC, table_schema, table_name;
SQL

echo
echo "== Auto-pick a candidate table and show count + 5 sample rows =="
$PSQL <<'SQL'
\set ECHO all
\pset border 2
WITH cols AS (
  SELECT table_schema, table_name,
         COUNT(*) FILTER (WHERE column_name IN
           ('tenant_id','user_id','session_id','device','channel','ts','value','sr_hz','seq_no','meta')) AS hits
  FROM information_schema.columns
  WHERE table_schema NOT IN ('pg_catalog','information_schema')
  GROUP BY 1,2
),
pick AS (
  SELECT table_schema, table_name
  FROM cols
  WHERE hits >= 8
  ORDER BY hits DESC, table_schema, table_name
  LIMIT 1
)
-- Row count:
SELECT format('SELECT COUNT(*) AS row_count FROM %I.%I;', table_schema, table_name)
FROM pick
\gexec

-- Peek 5 rows:
SELECT format($f$
  SELECT tenant_id, user_id, session_id, device, channel, ts, value, sr_hz, seq_no, meta
  FROM %I.%I
  ORDER BY random()
  LIMIT 5;
$f$, table_schema, table_name)
FROM pick
\gexec
SQL
