-- Workstream F: raw_im_bookings — a ONE-TIME FROZEN snapshot of Darcy's im_bookings table.
-- Applied at schema version 27 by scripts/setup_db.py / orchestrator DDL applier.
--
-- Source: Supabase project pxrdmjjaxtqycuxhxmgi.supabase.co, table `im_bookings`
-- (~36,806 rows as of 2026-05-31), the table behind the Renaissance-Portal IM dashboard.
-- Loaded by scripts/backfill_im_bookings.py via PostgREST paging.
--
-- ⚠ THIS TABLE IS FROZEN. It is NOT part of the nightly orchestrator sync. It is a single
--   captured snapshot for analysis/attribution. Do not register a phase for it. If a fresh
--   snapshot is ever needed, re-run the backfill script with a new --snapshot-date; rows from
--   the prior snapshot are preserved (append-only by _snapshot_date), matching raw_* semantics.
--
-- Columns: every column im_bookings exposes, preserved verbatim (PostgREST `select=*`), as
-- VARCHAR except the clearly-numeric source columns. Plus warehouse audit columns:
--   _snapshot_date  DATE     — the frozen-as-of date (constant, passed to the script).
--   _source         VARCHAR  — 'darcy_portal_im_bookings'.
--   _loaded_at      TIMESTAMPTZ — ingestion wall-clock (matches raw_* convention).
--
-- im_bookings real column set (verified 2026-05-31, 22 cols):
--   id, type, date, offer, partner, advisor, owner_name, company, first_name, last_name,
--   email, phone, job_title, num_employees, annual_revenue, workspace, our_email, campaign,
--   status, inbox_manager, campaign_manager, interested_in
-- (brief listed 18; the live table also carries id, campaign_manager, interested_in.)

CREATE TABLE IF NOT EXISTS raw_im_bookings (
  id               BIGINT,      -- im_bookings PK (verbatim)
  type             VARCHAR,     -- 'Booking' / 'Bookings' / 'Submitted Form' (dirty — verbatim)
  date             VARCHAR,     -- booking date as stored (ISO-ish string; kept as text)
  offer            VARCHAR,
  partner          VARCHAR,     -- LONG form: GreenBridge Capital, Big Think Capital, GoQualifi, Llama, DCX, Infusion, Clarify, Capfront
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
