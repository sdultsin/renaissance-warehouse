-- @gate: add
-- core.batch_warmup_schedule — inbox-provisioning warmup -> go-live schedule.
-- Applied at schema version 1015 by scripts/setup_db.py / the warehouse DDL applier.
--
-- WHAT: one row per provisioning COHORT = (inbox vendor x Instantly workspace x
-- warmup-start date), plus pending-upload / upload-error buckets. Per cohort it carries
-- the vendor (MilkBox / MailIn), the workspace, the warmup-start date, the warmup
-- length, the inbox count, and a status. The derived GO-LIVE (cold-ready) date =
-- warmup_start + warmup_length. Per Sam + David (2026-06-25) each account flips Instantly
-- Warmup -> Active exactly 14 days after its OWN warmup-start date (NOT a single shared
-- flip date), so the schedule is day-by-day.
--
-- WHY: warmup-start dates and the resulting day-by-day go-live schedule lived only in
-- Instantly + a hand-kept Google "Batches" sheet + Slack, and were not queryable in the
-- warehouse. core.account_registry already holds these inboxes at row grain (vendor /
-- workspace / batch / cohort) but has no warmup-start date; this cohort dimension adds the
-- warmup -> go-live SCHEDULE so "how many inboxes go Active on day D, in which workspace"
-- is answerable for launch coordination (sending-capacity ramp).
--
-- DATA LOAD: this DDL is the TABLE + VIEW definition ONLY — it does NOT read any file, so
-- it always applies cleanly under both the nightly and the moderator apply-now (whose cwd
-- is /opt/moderator/bin, not the repo root). The rows are loaded box-side by
-- scripts/load_batch_warmup_schedule.py from the EXTERNAL, gitignored seed
-- seed_data/batch_warmup_schedule.csv (so inbox-supplier names + infra scale stay out of
-- the PUBLIC repo — same boundary as core.account_registry / core.funding_partner, and per
-- the 2026-06-24 box-owner note on DDL 92 that read_csv-in-DDL seeds are unsafe under apply).
--
-- REFRESH: point-in-time snapshot; statuses change as warmup completes and the 1,145
-- MilkBox upload error is resolved. To refresh: update the CSV on the box and re-run the
-- loader (idempotent INSERT OR REPLACE on cohort_id) — non-destructive, no DDL change.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.batch_warmup_schedule (
  cohort_id           VARCHAR PRIMARY KEY,  -- stable surrogate slug, e.g. milkbox_f1_20260610
  provider            VARCHAR NOT NULL,     -- inbox vendor: 'MilkBox' | 'MailIn'
  batch_key           VARCHAR,              -- provisioning batch(es): 'B123-B125' | 'B126' ...
  workspace           VARCHAR,              -- Instantly workspace ('Funding 1' ...); NULL if unassigned
  warmup_start_date   DATE,                 -- NULL = not started (pending upload / upload error)
  warmup_length_days  INTEGER NOT NULL DEFAULT 14,
  n_accounts          INTEGER,              -- inbox count in this cohort; NULL if unknown
  status              VARCHAR NOT NULL,     -- 'warming' | 'active' | 'upload_error' | 'pending_upload'
  source              VARCHAR,
  as_of_date          DATE,                 -- snapshot date of this fact
  notes               VARCHAR,
  _curated_at         TIMESTAMPTZ           -- set by the loader
);

-- Per-cohort go-live schedule (derived). go_live_date = warmup_start + warmup_length.
CREATE OR REPLACE VIEW core.v_warmup_golive_schedule AS
SELECT
  cohort_id, provider, batch_key, workspace,
  warmup_start_date, warmup_length_days,
  CASE WHEN warmup_start_date IS NOT NULL
       THEN CAST(warmup_start_date + to_days(warmup_length_days) AS DATE)
  END AS go_live_date,
  n_accounts, status,
  (warmup_start_date IS NOT NULL
   AND CAST(warmup_start_date + to_days(warmup_length_days) AS DATE) <= current_date) AS warmup_complete,
  source, as_of_date, notes
FROM core.batch_warmup_schedule;

-- Upcoming go-live RAMP: NEW inboxes scheduled to flip Warmup -> Active per
-- (go_live_date, provider, workspace) = "how much fresh sending capacity comes online on
-- day D". Scope is exactly status='warming' (the cohorts whose flip is still PENDING):
--   * 'warming' cohorts are warming NOW, so each has a warmup_start_date -> go_live_date is
--     always defined (never a silent drop), and the flip is in the future / just due.
--   * 'active' cohorts have ALREADY come online — they are not "new capacity on day D" and
--     are deliberately not re-counted here (their flip is history; see the schedule view).
--   * 'upload_error' / 'pending_upload' are not live and never contribute.
-- A warming cohort with unknown size (n_accounts NULL) contributes 0. Email volume =
-- inboxes x per-account daily send limit — join the sending-capacity views to attach a
-- limit; NOT assumed here. For the FULL per-cohort picture incl. active/error/pending and
-- each cohort's (historical or future) flip date + status, use core.v_warmup_golive_schedule.
CREATE OR REPLACE VIEW core.v_warmup_golive_daily AS
SELECT
  go_live_date,
  provider,
  workspace,
  SUM(COALESCE(n_accounts, 0)) AS inboxes_going_active
FROM core.v_warmup_golive_schedule
WHERE go_live_date IS NOT NULL
  AND status = 'warming'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
