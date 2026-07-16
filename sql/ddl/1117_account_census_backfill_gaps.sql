-- 1117_account_census_backfill_gaps.sql  [2026-07-16]
-- @gate: add
--
-- PURPOSE: close the known GAPS in core.account_census so the per-day "what was live, and in what
-- state" history can be reconstructed for every day we hold a trustworthy poll.
--
-- BACKGROUND (verified 2026-07-16 against the raw polls in /root/core/live_accounts):
--   The hourly /accounts poller has written a parquet every hour since 2026-06-17 (720 files, no
--   gaps). core.account_census promoted only ONE per day and only from 2026-06-21, and 2026-06-30
--   was never promoted at all. The raw polls for the missing days are intact, so the gap is
--   recoverable WITHOUT re-querying Instantly.
--
-- WHAT THIS DOES *NOT* TOUCH (deliberate):
--   * 2026-07-01 / 2026-07-02 are NOT rewritten. Those days LOOK blended (rows whose snapshot_at is
--     2026-06-29 carry census_date 2026-07-01) but that is the entity's documented LAST-GOOD
--     CARRY-FORWARD behaviour, not corruption: the poller could not fetch renaissance-4 / renaissance-5
--     on those days, so their last-good rows were carried forward to keep the fleet from silently
--     shrinking. snapshot_at is the honest provenance marker. Rewriting those days from the raw
--     parquet would DESTROY real information and shrink the recorded fleet to a degraded 113,708-row
--     poll. Left exactly as-is; view (3) below makes the carry-forward explicit instead.
--   * 2026-06-17 is NOT backfilled. The poller was still being shaken out that day (workspace count
--     swung 2 -> 14 -> 11 within hours; row counts 825,604 -> 368,906 against a true live fleet of
--     ~314,069). No 2026-06-17 poll is a trustworthy census, and a wrong day is worse than a gap.
--
-- Fully additive + reversible: DROP the backup table + the view; DELETE the 4 backfilled census_dates.

-- ---------------------------------------------------------------------------------------------
-- (1) BACKUP FIRST — nothing in this file may cost us data we already hold.
--     Full copy of the table as it stands before any write below.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.account_census_bak_20260716 AS
SELECT * FROM core.account_census;

-- ---------------------------------------------------------------------------------------------
-- (2) BACKFILL the 4 recoverable missing days from their last COMPLETE poll of that day.
--     "Complete" = covers every workspace the poller saw that day (verified per-file 2026-07-16).
--     Chosen files (all 23:xx UTC = end-of-day state, matching what the hourly tick now leaves
--     behind as the surviving row for a day):
--       2026-06-18  accounts_live_20260618T230854Z.parquet  312,986 rows / 8 ws
--       2026-06-19  accounts_live_20260619T230852Z.parquet  312,986 rows / 8 ws
--       2026-06-20  accounts_live_20260620T230851Z.parquet  314,885 rows / 8 ws
--       2026-06-30  accounts_live_20260630T230945Z.parquet  436,630 rows / 8 ws
--     Idempotent: each day is deleted before insert, so a re-apply is a no-op-equivalent rewrite.
--
--     18-20 June parquets predate the poller change that added workspace_uuid/organization+warmup_limit,
--     so workspace_uuid is resolved via core.workspace.slug (verified 2026-07-16: 100% of rows on
--     every one of those days resolve to a real workspace_id; zero unresolved) and warmup_limit is
--     NULL (genuinely not captured that day — recorded as unknown, never invented).
-- ---------------------------------------------------------------------------------------------
DELETE FROM core.account_census
 WHERE census_date IN (DATE '2026-06-18', DATE '2026-06-19', DATE '2026-06-20', DATE '2026-06-30');

-- 18-20 June: legacy parquet schema (no workspace_uuid, no warmup_limit) -> resolve uuid from slug.
INSERT INTO core.account_census BY NAME (
    WITH src AS (
        SELECT * FROM read_parquet([
            '/root/core/live_accounts/accounts_live_20260618T230854Z.parquet',
            '/root/core/live_accounts/accounts_live_20260619T230852Z.parquet',
            '/root/core/live_accounts/accounts_live_20260620T230851Z.parquet'
        ], filename=true, union_by_name=true)
    )
    SELECT
        CAST(p.snapshot_at AS DATE)                          AS census_date,
        w.workspace_id                                       AS workspace_uuid,
        lower(p.email)                                       AS email,
        split_part(lower(p.email), '@', 2)                   AS domain,
        p.workspace_slug                                     AS workspace_slug,
        CAST(p.provider_code AS INTEGER)                     AS provider_code,
        CAST(p.daily_limit AS DOUBLE)                        AS daily_limit,
        CAST(p.warmup_status AS INTEGER)                     AS warmup_status,
        CASE CAST(p.warmup_status AS INTEGER) WHEN 1 THEN 'active'
             WHEN 0 THEN 'paused' WHEN -1 THEN 'banned' END  AS warmup_status_label,
        CAST(NULL AS INTEGER)                                AS warmup_limit,
        CAST(p.stat_warmup_score AS INTEGER)                 AS stat_warmup_score,
        CAST(p.status AS INTEGER)                            AS status,
        CASE CAST(p.status AS INTEGER) WHEN 1 THEN 'active' WHEN 2 THEN 'paused'
             WHEN -1 THEN 'connection_error' WHEN -2 THEN 'soft_bounce'
             WHEN -3 THEN 'sending_error' END                AS status_label,
        CAST(p.setup_pending AS BOOLEAN)                     AS setup_pending,
        CAST(p.timestamp_created AS TIMESTAMPTZ)             AS timestamp_created,
        CAST(p.timestamp_warmup_start AS TIMESTAMPTZ)        AS timestamp_warmup_start,
        CAST(p.timestamp_updated AS TIMESTAMPTZ)             AS timestamp_updated,
        CAST(p.snapshot_at AS TIMESTAMPTZ)                   AS snapshot_at,
        'instantly_api'                                      AS source,
        regexp_extract(p.filename, '(accounts_live_[^/]+\.parquet)$', 1) AS _snapshot_file,
        now()                                                AS _loaded_at,
        'ddl1117_backfill_gaps'                              AS _run_id
    FROM src p
    JOIN core.workspace w ON w.slug = p.workspace_slug
    WHERE p.email IS NOT NULL AND w.workspace_id IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY CAST(p.snapshot_at AS DATE), w.workspace_id, lower(p.email)
        ORDER BY p.snapshot_at DESC) = 1
);

-- 30 June: modern parquet schema (has workspace_uuid + warmup_limit) -> promote as the entity would.
INSERT INTO core.account_census BY NAME (
    SELECT
        CAST(p.snapshot_at AS DATE)                          AS census_date,
        p.workspace_uuid                                     AS workspace_uuid,
        lower(p.email)                                       AS email,
        split_part(lower(p.email), '@', 2)                   AS domain,
        p.workspace_slug                                     AS workspace_slug,
        CAST(p.provider_code AS INTEGER)                     AS provider_code,
        CAST(p.daily_limit AS DOUBLE)                        AS daily_limit,
        CAST(p.warmup_status AS INTEGER)                     AS warmup_status,
        CASE CAST(p.warmup_status AS INTEGER) WHEN 1 THEN 'active'
             WHEN 0 THEN 'paused' WHEN -1 THEN 'banned' END  AS warmup_status_label,
        CAST(p.warmup_limit AS INTEGER)                      AS warmup_limit,
        CAST(p.stat_warmup_score AS INTEGER)                 AS stat_warmup_score,
        CAST(p.status AS INTEGER)                            AS status,
        CASE CAST(p.status AS INTEGER) WHEN 1 THEN 'active' WHEN 2 THEN 'paused'
             WHEN -1 THEN 'connection_error' WHEN -2 THEN 'soft_bounce'
             WHEN -3 THEN 'sending_error' END                AS status_label,
        CAST(p.setup_pending AS BOOLEAN)                     AS setup_pending,
        CAST(p.timestamp_created AS TIMESTAMPTZ)             AS timestamp_created,
        CAST(p.timestamp_warmup_start AS TIMESTAMPTZ)        AS timestamp_warmup_start,
        CAST(p.timestamp_updated AS TIMESTAMPTZ)             AS timestamp_updated,
        CAST(p.snapshot_at AS TIMESTAMPTZ)                   AS snapshot_at,
        'instantly_api'                                      AS source,
        'accounts_live_20260630T230945Z.parquet'             AS _snapshot_file,
        now()                                                AS _loaded_at,
        'ddl1117_backfill_gaps'                              AS _run_id
    FROM read_parquet('/root/core/live_accounts/accounts_live_20260630T230945Z.parquet') p
    WHERE p.email IS NOT NULL AND p.workspace_uuid IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY CAST(p.snapshot_at AS DATE), p.workspace_uuid, lower(p.email)
        ORDER BY p.snapshot_at DESC) = 1
);

-- ---------------------------------------------------------------------------------------------
-- (3) MAKE CARRY-FORWARD EXPLICIT. The census mixes OBSERVED rows (we polled that inbox that day)
--     with CARRIED-FORWARD rows (the poller could not reach that workspace, so its last-good state
--     was reused). Today the only tell is snapshot_at <> census_date, which is subtle enough to be
--     misread as corruption. This view names it, so point-in-time answers can say how they know.
--     Read THIS view for history questions; the base table stays the raw record.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_account_census_provenance AS
SELECT
    c.*,
    (CAST(c.snapshot_at AS DATE) <> c.census_date)                  AS is_carried_forward,
    date_diff('day', CAST(c.snapshot_at AS DATE), c.census_date)    AS carried_forward_days,
    CASE WHEN CAST(c.snapshot_at AS DATE) = c.census_date
         THEN 'observed' ELSE 'carried_forward' END                 AS provenance
FROM core.account_census c;
