-- 1115_account_tags_daily_history.sql  [2026-07-16] lifecycle stage history
-- @gate: add
--
-- PURPOSE
-- core.account_tags is DELETE+INSERT per run (entities/account_tags.py:182-187) — it holds ONLY
-- today's tags. Every night the previous day's tag state is destroyed. Because the lifecycle STAGE
-- (Warmup / Rampup / Active / Rehab) is carried ENTIRELY by tags, the warehouse today cannot answer
-- "which stage was this inbox in on <past date>?" or "when did it start ramping up?".
-- Verified 2026-07-16: NO tag table anywhere carries a date dimension; core.sending_account_tag's
-- first_seen_at is NOT a stage ledger (212,041 rows share just 14 distinct first_seen_at values,
-- a single last_seen_at, and only 3 legacy labels). Instantly itself cannot report tag-apply dates
-- (knowledge/instantly-api.md), so OBSERVING daily is the only way to build this history.
--
-- This is the "pipeline-owned stage-entry ledger" that memory/reference_warmup_active_lifecycle.md
-- names as the missing prerequisite for the Rehab stage.
--
-- Complements (does NOT duplicate) core.account_census, which already snapshots per-inbox STATUS +
-- timestamp_created + timestamp_warmup_start daily (25 days, 2026-06-21..). Census carries no tags;
-- this carries tags. Together they give the full lifecycle.
--
-- ADDITIVE + FULLY REVERSIBLE: one new table + two new views. No existing table altered, no data
-- mutated, no consumer repointed. Revert = DROP the 2 views + 1 table.
-- Storage: ~400k rows/day, tags_arr compresses well; volume has 687GB free.

CREATE SCHEMA IF NOT EXISTS core;

-- ============================================================================================
-- (1) The daily tag snapshot. One row per (snapshot_date, workspace_uuid, email).
--     Appended once per night AFTER the account_tags ingest completes.
-- ============================================================================================
CREATE TABLE IF NOT EXISTS core.account_tags_daily (
    snapshot_date   DATE        NOT NULL,
    email           VARCHAR     NOT NULL,
    workspace_uuid  VARCHAR,
    workspace_slug  VARCHAR,
    tags_arr        VARCHAR[],
    n_tags          INTEGER,
    -- stage: derived from the generic status tag at snapshot time. NULL = inbox carries no
    -- status tag yet (untagged / pre-pipeline). Kept as a stored column so stage-entry queries
    -- do not have to re-derive it over millions of rows.
    stage           VARCHAR,
    _loaded_at      TIMESTAMP WITH TIME ZONE,
    _run_id         VARCHAR
);

-- ============================================================================================
-- (2) Stage-entry ledger: the FIRST date we observed each inbox in each stage.
--     This is the direct answer to "when did this inbox start ramping up / go active?".
--     Grain: one row per (workspace_uuid, email, stage).
--     CAVEAT surfaced in the column itself: first_observed_date is bounded below by the first
--     snapshot we ever took, so is_left_censored flags inboxes that were ALREADY in that stage
--     on day 1 (their true entry date predates our history and is unknowable).
-- ============================================================================================
CREATE OR REPLACE VIEW core.v_account_stage_entry AS
WITH first_day AS (SELECT MIN(snapshot_date) AS d0 FROM core.account_tags_daily)
SELECT
    d.email,
    d.workspace_uuid,
    ANY_VALUE(d.workspace_slug)                     AS workspace_slug,
    d.stage,
    MIN(d.snapshot_date)                            AS first_observed_date,
    MAX(d.snapshot_date)                            AS last_observed_date,
    COUNT(*)                                        AS days_observed_in_stage,
    (MIN(d.snapshot_date) = (SELECT d0 FROM first_day)) AS is_left_censored
FROM core.account_tags_daily d
WHERE d.stage IS NOT NULL
GROUP BY d.email, d.workspace_uuid, d.stage;

-- ============================================================================================
-- (3) Daily stage rollup — fleet-level "how many were in each stage on date X".
-- ============================================================================================
CREATE OR REPLACE VIEW core.v_stage_daily AS
SELECT
    snapshot_date,
    workspace_slug,
    stage,
    COUNT(*) AS inboxes
FROM core.account_tags_daily
WHERE stage IS NOT NULL
GROUP BY snapshot_date, workspace_slug, stage;
