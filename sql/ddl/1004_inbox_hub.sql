-- @gate: add
-- Depends on 1003
-- ============================================================================
-- 114_inbox_hub.sql  —  Unified Inbox Hub: RG tags + nightly live_state
-- ----------------------------------------------------------------------------
-- WHAT THIS DOES (David, 2026-06-23):
--   (1) ADD two RG-tag columns to the existing master table
--       core.sending_account_batch (the 2.55M final-data mirror). These two
--       columns ("RG# Tag (Tag1)" / "RG#-# (Tag2)") exist in the source
--       FINAL DATA CSV but were dropped by the original parquet loader. We add
--       them to the EXISTING table (no new table — keep the DB uncluttered).
--       VALUES are populated separately (see 115) from the source file; this
--       file only creates the column slots.
--   (2) CREATE core.v_inbox_hub — ONE view, stores no data — that is the single
--       inbox source-of-truth:
--         * spine = every inbox across BOTH the 2.55M CSV master AND the
--           Instantly census (core.sending_account) = the true ~2.85M universe
--           (FULL OUTER JOIN — folds in the ~297k that are in Instantly but
--           never made it into the CSV).
--         * tags  = provider/batch (already present) + the two RG-tag columns.
--         * live_state = live / paused / broken / retired, DERIVED at read-time
--           from the NIGHTLY-refreshed census, so it is always current and never
--           a frozen value. retired = left Instantly (retired_at set) OR never in
--           the census (CSV-only historical).
--
--   ADDITIVE + IDEMPOTENT. Creates a NEW view name + adds nullable columns.
--   Does NOT drop, rename, or rewrite any existing column/table/view.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- (1) RG-tag column slots on the existing master table (additive, re-apply safe).
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS rg_tag_1 VARCHAR;
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS rg_tag_2 VARCHAR;

-- (2) The unified Inbox Hub view.
CREATE OR REPLACE VIEW core.v_inbox_hub AS
WITH ranked AS (
    -- An email can appear under multiple batch generations. Pick ONE row per
    -- email deterministically: the CURRENT batch first, then most-recent load.
    SELECT
        lower(trim(account_email)) AS email,
        domain, raw_workspace, provider_tag, batch_key, rg_tag_1, rg_tag_2,
        row_number() OVER (
            PARTITION BY lower(trim(account_email))
            ORDER BY is_current_batch DESC NULLS LAST, _loaded_at DESC NULLS LAST
        ) AS rn
    FROM core.sending_account_batch
),
batch AS (
    SELECT email, domain, raw_workspace AS workspace_csv,
           provider_tag, batch_key, rg_tag_1, rg_tag_2
    FROM ranked WHERE rn = 1
),
census AS (
    SELECT
        lower(trim(email))                AS email,
        any_value(workspace_slug)         AS workspace_slug,
        any_value(status)                 AS status,
        max(retired_at)                   AS retired_at
    FROM core.sending_account
    GROUP BY 1
)
SELECT
    COALESCE(b.email, c.email)                         AS email,
    b.domain                                           AS domain,
    COALESCE(c.workspace_slug, b.workspace_csv)        AS workspace,
    b.provider_tag                                     AS provider_tag,
    b.batch_key                                        AS batch_key,
    b.rg_tag_1                                         AS rg_tag_1,
    b.rg_tag_2                                         AS rg_tag_2,
    CASE
        WHEN c.email IS NULL                              THEN 'retired'   -- CSV-only, not in Instantly census
        WHEN c.retired_at IS NOT NULL                     THEN 'retired'   -- left Instantly
        WHEN c.status IN ('connection_error','sending_error') THEN 'broken'
        WHEN c.status IN ('paused','conn_paused')         THEN 'paused'
        WHEN c.status IN ('active','conn_active')         THEN 'live'
        ELSE 'live'                                                        -- in-Instantly, other status
    END                                                AS live_state
FROM batch b
FULL OUTER JOIN census c ON b.email = c.email;
