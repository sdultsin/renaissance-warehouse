-- Foundation DDL. Applied at version 0 by scripts/setup_db.py or first orchestrator run.
-- This creates the audit tables every other ingest writes to.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.sync_run (
  run_id              VARCHAR PRIMARY KEY,
  started_at          TIMESTAMPTZ NOT NULL,
  ended_at            TIMESTAMPTZ,
  status              VARCHAR NOT NULL,        -- running | success | partial | failed
  phase_count         INTEGER NOT NULL DEFAULT 0,
  phase_failed_count  INTEGER NOT NULL DEFAULT 0,
  notes               VARCHAR                  -- JSON string
);

CREATE TABLE IF NOT EXISTS core.sync_run_phase (
  run_id        VARCHAR NOT NULL,
  phase_name    VARCHAR NOT NULL,
  ingest_name   VARCHAR NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  ended_at      TIMESTAMPTZ,
  status        VARCHAR NOT NULL,              -- success | failed | skipped
  rows_in       BIGINT,
  rows_out      BIGINT,
  error         VARCHAR,
  notes         VARCHAR,                       -- JSON string
  PRIMARY KEY (run_id, phase_name, ingest_name)
);

-- schema_version is created/managed in core/db.py:apply_ddl_file().
