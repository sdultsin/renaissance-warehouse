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
-- warehouse. This dimension answers "how many inboxes go Active on day D, in which
-- workspace" for launch coordination (sending-capacity ramp).
--
-- SOURCE: David's 2026-06-25 Slack update — MilkBox B123-B125 top-up across Funding 1/2/4
-- (warmup-start 2026-06-10 .. 2026-06-20; 14-day warmup; ~59,799 inboxes; 1,145 flagged as
-- an upload error under vendor re-check), and MailIn B126-B128 (not yet uploaded; 2-week
-- warmup assumed; workspace TBD).
--
-- Loaded from an EXTERNAL, gitignored seed file (seed_data/batch_warmup_schedule.csv) so
-- inbox-supplier names + infra scale are not committed to the PUBLIC repo — same policy
-- as core.funding_partner. The DDL itself carries NO data.
--
-- REFRESH: this is a point-in-time snapshot; statuses change as warmup completes and the
-- 1,145 MilkBox upload error is resolved. The load body below is insert-once (it runs
-- exactly once under version tracking, on the freshly-created empty table). To refresh,
-- update the CSV on the droplet and ship a NEW version DDL that UPSERTs
-- (ON CONFLICT (cohort_id) DO UPDATE SET ...) — non-destructive. The table is keyed on a
-- stable surrogate cohort_id so re-loads / upserts are clean.
--
-- Idempotent + non-destructive: CREATE IF NOT EXISTS + a guarded insert-once. Missing seed
-- file -> table left empty (guarded glob), warehouse still builds.

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
  _curated_at         TIMESTAMPTZ NOT NULL
);

-- Guarded insert-once seed from the external, gitignored CSV (empty strings -> NULL).
-- Non-destructive: ON CONFLICT DO NOTHING (no row removal) — same pattern as core.funding_partner.
INSERT INTO core.batch_warmup_schedule
  (cohort_id, provider, batch_key, workspace, warmup_start_date, warmup_length_days,
   n_accounts, status, source, as_of_date, notes, _curated_at)
SELECT
  cohort_id, provider, batch_key, workspace,
  TRY_CAST(warmup_start_date AS DATE),
  COALESCE(TRY_CAST(warmup_length_days AS INTEGER), 14),
  TRY_CAST(n_accounts AS INTEGER),
  status, source, TRY_CAST(as_of_date AS DATE), notes, now()
FROM read_csv_auto('seed_data/batch_warmup_schedule.csv', header=true, nullstr='', all_varchar=true)
WHERE (SELECT count(*) FROM glob('seed_data/batch_warmup_schedule.csv')) > 0
ON CONFLICT (cohort_id) DO NOTHING;

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

-- Daily go-live rollup: inboxes becoming Active per (go_live_date, provider, workspace).
-- (Email volume = inboxes x per-account daily send limit; join to the sending-capacity
-- views to attach a limit — deliberately NOT assumed here.)
CREATE OR REPLACE VIEW core.v_warmup_golive_daily AS
SELECT
  go_live_date,
  provider,
  workspace,
  SUM(COALESCE(n_accounts, 0)) AS inboxes_going_active
FROM core.v_warmup_golive_schedule
WHERE go_live_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
