-- @gate: add
-- Depends on 1015
-- ============================================================================
-- 1144_instantly_account_settings.sql — per-account SETTINGS + warmup-start daily
-- snapshot (task #28 step 3) + deprecation of the manual warmup-schedule CSV.
--
-- WHY: three gaps in one:
--   1. WARMUP-START TRUTH. `timestamp_warmup_start` is 100% populated on Instantly's
--      GET /accounts (600/600 verified 2026-07-19, active+paused, Google+MS). Until now
--      the only queryable warmup-start source was `core.batch_warmup_schedule` — a
--      14-row, hand-kept CSV snapshot frozen at as_of 2026-06-25 (cohort grain, stale
--      the day after every new provisioning batch). This sweep supersedes it at ACCOUNT
--      grain, refreshed nightly; the CSV table is marked DEPRECATED below (kept for
--      history — its 'active' cohort rows predate this table's history horizon).
--   2. NO WAREHOUSE HOME for `enable_slow_ramp` / `sending_gap` (the census parquet
--      omits both) — the config axis needed to interpret expected-volume ramps.
--   3. DROPLET-DEATH DURABILITY. `core.account_census` (the other per-date settings
--      source) is fed by a BOX-ONLY hourly poller (poll_live_accounts.py, not in this
--      repo) — same liability class as the retired account_truth CSVs. This entity
--      (entities/instantly_account_settings.py) is the repo-versioned sweep.
--
-- GRAIN: one row per (workspace_slug, account_email, snapshot_date). Values are lifted
-- verbatim from /accounts (no whole-workspace 413 risk on this endpoint: `emails=` is a
-- no-op there; pagination via next_starting_after). ~1.5M rows/night at current fleet
-- size — narrow rows; a retention/compaction policy can trim later if needed (same
-- accepted trade-off as raw_instantly_account_tag, PR #277).
--
-- LOAD: entities/instantly_account_settings.py, phase 'replies_late' (PASS B — a
-- full-fleet /accounts walk must never sit in front of PASS A's ~03:30 ET fleet-health
-- promote; nothing in the nightly rebuild reads this table, so PASS B is dependency-safe).
-- Upsert ON CONFLICT (workspace_slug, account_email, snapshot_date).
--
-- Reversible: DROP VIEW core.v_account_warmup_golive;
--             DROP VIEW core.v_sending_account_settings_current;
--             DROP TABLE main.raw_instantly_account_settings;
--             (COMMENTs: re-COMMENT to previous text; no data touched.)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS main.raw_instantly_account_settings (
  workspace_slug          VARCHAR NOT NULL,
  account_email           VARCHAR NOT NULL,
  snapshot_date           DATE    NOT NULL,
  domain                  VARCHAR,           -- split_part(account_email,'@',2)
  daily_limit             INTEGER,           -- configured max cold sends/day
  status                  INTEGER,           -- 1 active | 2 paused | -1 connection_error | -2 soft_bounce | -3 sending_error
  provider_code           INTEGER,           -- 1 imap | 2 google | 3/4 outlook
  warmup_status           INTEGER,           -- 1 active | 0 paused | -1 banned
  enable_slow_ramp        BOOLEAN,           -- Instantly slow-ramp toggle
  sending_gap             INTEGER,           -- minutes between sends
  timestamp_warmup_start  TIMESTAMPTZ,       -- WARMUP-START SOURCE OF TRUTH (100% populated)
  timestamp_created       TIMESTAMPTZ,       -- account creation in Instantly
  stat_warmup_score       INTEGER,           -- Instantly warmup health score (0-100)
  api_synced_at           TIMESTAMPTZ,
  _loaded_at              TIMESTAMPTZ,
  _run_id                 VARCHAR,
  PRIMARY KEY (workspace_slug, account_email, snapshot_date)
);

-- ----------------------------------------------------------------------------
-- Current-state settings per account = the LATEST snapshot per (workspace, email),
-- plus the DERIVED cold-send-start candidate.
--
-- cold_send_start_candidate is DERIVED, not an Instantly field. It is the earliest of:
--   * first_settings_daily_limit_date — first snapshot_date this sweep saw the account
--     with daily_limit > 0. History only ACCRUES from this table's deploy date forward,
--     so for accounts predating the sweep this component is right-censored (too late).
--   * first_actual_send_date — first date with actual_sends > 0 in
--     core.sending_account_daily (the actuals history, which reaches back before this
--     sweep existed).
-- For the REAL first-cold-send moment measured from the email log itself, use
-- core.account_first_cold_send (DDL 1082) — that remains the send-truth surface; this
-- candidate is the CONFIG-side signal ("when was the account first allowed/observed to
-- cold-send"), useful where the email log is incomplete.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_account_settings_current AS
WITH latest AS (
  SELECT *
  FROM main.raw_instantly_account_settings
  QUALIFY row_number() OVER (
    PARTITION BY workspace_slug, account_email
    ORDER BY snapshot_date DESC) = 1
),
first_limit AS (
  SELECT workspace_slug, account_email,
         MIN(snapshot_date) FILTER (WHERE daily_limit > 0) AS first_settings_daily_limit_date
  FROM main.raw_instantly_account_settings
  GROUP BY 1, 2
),
first_send AS (
  -- core.sending_account_daily keys accounts as account_id (= sending-account email),
  -- PK (date, account_id) — one row per email per date, workspace-agnostic.
  --
  -- DELIBERATELY joined on EMAIL ONLY (not workspace_slug) — do not "fix" this to a
  -- two-key join: workspace_slug labels are NOT canonical identities. The same physical
  -- workspace appears under multiple INSTANTLY_KEY_* slug labels (verified 2026-07-19,
  -- PR #277: koi-and-destroy == funding-4, sends match exactly), and actuals rows may
  -- carry the sibling label — a slug-matched join would silently NULL the actuals
  -- component for those accounts. Inboxes are provisioned into one physical workspace
  -- and not reused across them, so email is the stable account identity fleet-wide;
  -- the email-only join is the correct attribution across label aliases.
  SELECT account_id AS account_email,
         MIN(date) FILTER (WHERE actual_sends > 0) AS first_actual_send_date
  FROM core.sending_account_daily
  GROUP BY 1
)
SELECT
  l.workspace_slug,
  l.account_email,
  l.domain,
  l.snapshot_date                       AS settings_as_of,
  l.daily_limit,
  l.status,
  CASE l.status WHEN 1 THEN 'active' WHEN 2 THEN 'paused'
       WHEN -1 THEN 'connection_error' WHEN -2 THEN 'soft_bounce'
       WHEN -3 THEN 'sending_error' END AS status_label,
  l.provider_code,
  l.warmup_status,
  CASE l.warmup_status WHEN 1 THEN 'active' WHEN 0 THEN 'paused'
       WHEN -1 THEN 'banned' END        AS warmup_status_label,
  l.enable_slow_ramp,
  l.sending_gap,
  l.timestamp_warmup_start,
  -- Pinned to UTC: a bare CAST(TIMESTAMPTZ AS DATE) is session-timezone-dependent
  -- (an off-box reader in UTC-5 would shift midnight starts a day early).
  CAST(l.timestamp_warmup_start AT TIME ZONE 'UTC' AS DATE) AS warmup_start_date,
  l.timestamp_created,
  l.stat_warmup_score,
  fl.first_settings_daily_limit_date,   -- DERIVED (sweep history; accrues from deploy forward)
  fs.first_actual_send_date,            -- DERIVED (core.sending_account_daily actuals)
  -- DERIVED cold-send-start candidate = earliest non-NULL of the two components
  -- (NULL-safe least: LEAST(COALESCE(a,b), COALESCE(b,a))).
  LEAST(
    COALESCE(fl.first_settings_daily_limit_date, fs.first_actual_send_date),
    COALESCE(fs.first_actual_send_date, fl.first_settings_daily_limit_date)
  )                                     AS cold_send_start_candidate
FROM latest l
LEFT JOIN first_limit fl
       ON fl.workspace_slug = l.workspace_slug AND fl.account_email = l.account_email
LEFT JOIN first_send fs
       ON fs.account_email = l.account_email;

-- Query-time visibility of the deploy-date right-censoring (the DDL prose alone is not
-- visible to a query-time reader):
COMMENT ON COLUMN core.v_sending_account_settings_current.first_settings_daily_limit_date IS
  'DERIVED from this sweep''s history, which only ACCRUES from the table''s deploy date (2026-07-19) forward — RIGHT-CENSORED (biased late) for accounts that predate the sweep. Not a true first-cold-send.';
COMMENT ON COLUMN core.v_sending_account_settings_current.cold_send_start_candidate IS
  'DERIVED config-side candidate = earliest of first_settings_daily_limit_date (right-censored at the sweep deploy date 2026-07-19) and first_actual_send_date (core.sending_account_daily actuals). For the send-truth first cold send measured from the email log use core.account_first_cold_send (DDL 1082).';

-- ----------------------------------------------------------------------------
-- Account-grain warmup -> go-live schedule from the sweep — the REPLACEMENT for the
-- deprecated cohort-grain core.v_warmup_golive_schedule / v_warmup_golive_daily.
-- go_live_date = warmup_start + 14 days (each account flips Warmup -> Active exactly
-- 14 days after its OWN warmup-start — per Sam + David 2026-06-25, same constant the
-- deprecated cohort schedule used). Aggregate over go_live_date/workspace_slug for the
-- "capacity coming online on day D" question the old daily view answered.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_account_warmup_golive AS
SELECT
  workspace_slug,
  account_email,
  domain,
  warmup_start_date,
  CAST(warmup_start_date + INTERVAL 14 DAY AS DATE)  AS go_live_date,
  (warmup_start_date IS NOT NULL
   AND CAST(warmup_start_date + INTERVAL 14 DAY AS DATE) <= current_date) AS warmup_complete,
  warmup_status,
  warmup_status_label,
  status,
  status_label,
  daily_limit,
  settings_as_of
FROM core.v_sending_account_settings_current;

COMMENT ON COLUMN core.v_account_warmup_golive.warmup_complete IS
  'NOMINAL-SCHEDULE flag: date arithmetic only (warmup_start + 14d <= today), same semantics as the deprecated cohort view. Does NOT reflect live warmup_status — a paused/banned/restarted account can read TRUE while warmup_status_label says paused/banned. For "live and sending" gate on warmup_status/status too.';

-- ----------------------------------------------------------------------------
-- DEPRECATION of the manual warmup-schedule CSV surface (task #28 step 3).
-- Consumers audited 2026-07-19: core.batch_warmup_schedule is read ONLY by its own two
-- views below + the box loader script (scripts/load_batch_warmup_schedule.py); the sole
-- other repo reference (sql/ddl/1016_milkbox_sending_capacity.sql) is a prose comment.
-- No live view is repointed because none reads it. Table + views are KEPT (frozen
-- history, as_of 2026-06-25) — do not load new CSVs into them.
-- ----------------------------------------------------------------------------
COMMENT ON TABLE core.batch_warmup_schedule IS
  'DEPRECATED 2026-07-19 (task #28 step 3): stale 14-row manual CSV snapshot (as_of 2026-06-25). Warmup-start source of truth is now the nightly /accounts sweep -> main.raw_instantly_account_settings (timestamp_warmup_start, 100% populated); current state in core.v_sending_account_settings_current; go-live schedule in core.v_account_warmup_golive. Kept as frozen history — do not reload.';

COMMENT ON VIEW core.v_warmup_golive_schedule IS
  'DEPRECATED 2026-07-19 (task #28 step 3): built on the frozen manual CSV core.batch_warmup_schedule. Use core.v_account_warmup_golive (account grain, from the nightly Instantly /accounts sweep).';

COMMENT ON VIEW core.v_warmup_golive_daily IS
  'DEPRECATED 2026-07-19 (task #28 step 3): built on the frozen manual CSV core.batch_warmup_schedule. Aggregate core.v_account_warmup_golive by (go_live_date, workspace_slug) instead.';
