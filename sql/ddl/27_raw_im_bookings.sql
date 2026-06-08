-- Workstream F: raw_im_bookings — a ONE-TIME FROZEN snapshot of the bookings portal table.
-- Applied at schema version 27 by scripts/setup_db.py / orchestrator DDL applier.
--
-- Source: an upstream Supabase project (credentials/IDs via env), table `im_bookings`,
-- the table behind the bookings portal dashboard. Loaded by
-- scripts/backfill_im_bookings.py via PostgREST paging.
--
-- ⚠ THIS TABLE IS FROZEN. It is NOT part of the nightly orchestrator sync. It is a single
--   captured snapshot for analysis/attribution. Do not register a phase for it. If a fresh
--   snapshot is ever needed, re-run the backfill script with a new --snapshot-date; rows from
--   the prior snapshot are preserved (append-only by _snapshot_date), matching raw_* semantics.
--
-- Columns: every column im_bookings exposes, preserved verbatim (PostgREST `select=*`), as
-- VARCHAR except the clearly-numeric source columns. Plus warehouse audit columns:
--   _snapshot_date  DATE     — the frozen-as-of date (constant, passed to the script).
--   _source         VARCHAR  — source tag for the snapshot.
--   _loaded_at      TIMESTAMPTZ — ingestion wall-clock (matches raw_* convention).

CREATE TABLE IF NOT EXISTS raw_im_bookings (
  id               BIGINT,      -- im_bookings PK (verbatim)
  type             VARCHAR,     -- 'Booking' / 'Bookings' / 'Submitted Form' (dirty — verbatim)
  date             VARCHAR,     -- booking date as stored (ISO-ish string; kept as text)
  offer            VARCHAR,
  partner          VARCHAR,     -- funding partner (long form as stored in the portal)
  advisor          VARCHAR,
  owner_name       VARCHAR,
  company          VARCHAR,
  first_name       VARCHAR,
  last_name        VARCHAR,
  email            VARCHAR,
  phone            VARCHAR,
  job_title        VARCHAR,
  num_employees    VARCHAR,     -- stored free-form (ranges/strings) — keep as text
  annual_revenue   VARCHAR,     -- stored free-form — keep as text
  workspace        VARCHAR,     -- Instantly workspace display name (often NULL/stale)
  our_email        VARCHAR,     -- the sending inbox that booked it
  campaign         VARCHAR,
  status           VARCHAR,     -- free-text "Meeting booked on ..." / "Submitted Form" etc.
  inbox_manager    VARCHAR,
  campaign_manager VARCHAR,
  interested_in    VARCHAR,
  -- warehouse audit / freeze metadata
  _snapshot_date   DATE        NOT NULL,
  _source          VARCHAR     NOT NULL,
  _loaded_at       TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (id, _snapshot_date)
);
