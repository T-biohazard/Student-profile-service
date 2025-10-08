-- EEG samples canonical store
CREATE TABLE IF NOT EXISTS public.eeg_samples (
  tenant_id  TEXT NOT NULL,
  user_id    TEXT NOT NULL,
  session_id TEXT NOT NULL,
  device     TEXT NOT NULL,
  channel    TEXT NOT NULL,
  ts         TIMESTAMPTZ NOT NULL,
  value      DOUBLE PRECISION NOT NULL,
  sr_hz      DOUBLE PRECISION,
  seq_no     BIGINT,
  meta       JSONB,
  PRIMARY KEY (tenant_id, user_id, session_id, channel, ts)
);

-- Timescale hypertable
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='eeg_samples_ts_idx') THEN
    PERFORM 1;
  END IF;
END$$;

-- Create extension and hypertable if not done
CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('public.eeg_samples', 'ts', if_not_exists => TRUE);

-- Helpful index for session scans
CREATE INDEX IF NOT EXISTS eeg_session_ts_desc_idx
  ON public.eeg_samples (tenant_id, user_id, session_id, ts DESC);
