-- @gate: add
-- Depends on 1002, 1072
-- ============================================================================
-- 1073_capacity_cancelled_split.sql — cancelled-vs-recoverable capacity truth
--                                     (TKT-1 Task C) [2026-07-03]
-- ----------------------------------------------------------------------------
-- WHY (handoffs/2026-07-01-TICKET-sending-truth-disconnects-batch-attribution.md §4-C):
--   The June reseller cancellation wave (~25k inboxes) was never removed from
--   Instantly, so cancelled-for-good inboxes sit in connection_error and the
--   old recoverable_daily_limit counted them as capacity that "returns once
--   the accounts reconnect" — they never will. Measured on serving snapshot
--   warehouse_20260703_043558_874.duckdb with the 2026-07-03 rg_dim test
--   export: 29,838 of 228,924 tagged-census accounts sit on cancelled RG tags;
--   226,811/day of the 300,216/day "recoverable" capacity is actually
--   cancelled-for-good (real recoverable = 73,405/day).
--
-- FIX: redefine core.v_sending_capacity_by_tag (and its _total rollup, both
--   from DDL 1002) to split cancelled out:
--     * cancelled_accounts / cancelled_daily_limit — NEW: accounts (any
--       connection status) whose RG tags resolve to is_cancelled in
--       core.rg_tag_dim (DDL 1072), and their configured daily_limit.
--     * recoverable_daily_limit — REDEFINED: connection_error capacity
--       EXCLUDING cancelled inboxes (the number that can actually come back).
--     * every pre-existing column keeps its name, position and semantics
--       (accounts / active_accounts / connection_error_accounts /
--       active_daily_limit / census_date unchanged).
--
-- Verified read-only on serving snapshot warehouse_20260703_043558_874.duckdb:
--   * BEFORE (current view): 27 rows, 228,924 accounts, active_daily_limit
--     3,014,008, recoverable_daily_limit 300,216.
--   * Equivalence check: the NEW view body dry-SELECTed with rg_tag_dim
--     stubbed EMPTY reproduces the current view EXACTLY — 27/27 rows matched
--     via FULL JOIN on (workspace_slug, tag_label, provider_code), 0
--     recoverable_daily_limit mismatches, 0 accounts mismatches, 0 cancelled.
--     So before build_infra_batch_v2.sql first populates core.rg_tag_dim this
--     DDL is a pure no-op on every existing consumer.
--   * AFTER (full rehearsal on a throwaway copy of the same snapshot with
--     rg_tag_dim populated by build_infra_batch_v2.sql): 27 rows, 228,924
--     accounts, active_daily_limit 3,014,008 (both unchanged);
--     cancelled_accounts 29,838 · cancelled_daily_limit 458,199 ·
--     recoverable_daily_limit 300,216 -> 73,405.
--   View-only DDL: no data rows touched.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- core.v_sending_capacity_by_tag — per (workspace, tag, provider) capacity off
-- LIVE census config (unchanged), now cancellation-aware. cancelled_inbox is
-- DISTINCT per (email, workspace_slug) so the LEFT JOIN can never fan out.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_capacity_by_tag AS
WITH rg_edges AS (
    SELECT lower(email) AS email, workspace_slug, unnest(tags_arr) AS tag
    FROM core.account_tags
),
cancelled_inbox AS (
    SELECT DISTINCT e.email, e.workspace_slug
    FROM rg_edges e
    JOIN core.rg_tag_dim d ON d.rg_tag = e.tag
    WHERE d.is_cancelled
)
SELECT
    t.workspace_slug,
    c.workspace_uuid,
    c.workspace_current_name,
    t.tag_label,
    c.provider_code,
    CASE c.provider_code WHEN 1 THEN 'OTD/custom' WHEN 2 THEN 'Google'
         WHEN 3 THEN 'Microsoft' ELSE 'other' END                       AS provider,
    count(*)                                                             AS accounts,
    count(*) FILTER (WHERE c.status_label = 'active')                    AS active_accounts,
    count(*) FILTER (WHERE c.status_label = 'connection_error')          AS connection_error_accounts,
    COALESCE(sum(c.daily_limit) FILTER (WHERE c.status_label = 'active'), 0)
                                                                        AS active_daily_limit,
    -- REDEFINED (2026-07-03): connection_error capacity EXCLUDING inboxes whose
    -- RG tags are cancelled-for-good — only genuinely reconnectable capacity.
    COALESCE(sum(c.daily_limit) FILTER (WHERE c.status_label = 'connection_error'
                                          AND x.email IS NULL), 0)
                                                                        AS recoverable_daily_limit,
    max(c.census_date)                                                   AS census_date,
    -- NEW columns appended AFTER all pre-existing ones so positional consumers
    -- (SELECT *) see every old column at its old position.
    count(*) FILTER (WHERE x.email IS NOT NULL)                          AS cancelled_accounts,
    COALESCE(sum(c.daily_limit) FILTER (WHERE x.email IS NOT NULL), 0)   AS cancelled_daily_limit
FROM core.sending_account_tag t
-- Join on email AND workspace_slug (unchanged from DDL 1002): exact, never fans out.
JOIN core.v_account_census_latest c
  ON c.email = t.email AND c.workspace_slug = t.workspace_slug
LEFT JOIN cancelled_inbox x
  ON x.email = lower(t.email) AND x.workspace_slug = t.workspace_slug
GROUP BY 1, 2, 3, 4, 5, 6;

-- ----------------------------------------------------------------------------
-- Per-(workspace, tag) rollup across providers — same columns as before plus
-- the cancelled split (kept consistent with the per-provider view above).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_capacity_by_tag_total AS
SELECT
    workspace_slug,
    workspace_uuid,
    workspace_current_name,
    tag_label,
    sum(accounts)                  AS accounts,
    sum(active_accounts)           AS active_accounts,
    sum(connection_error_accounts) AS connection_error_accounts,
    sum(active_daily_limit)        AS active_daily_limit,
    sum(recoverable_daily_limit)   AS recoverable_daily_limit,
    max(census_date)               AS census_date,
    sum(cancelled_accounts)        AS cancelled_accounts,
    sum(cancelled_daily_limit)     AS cancelled_daily_limit
FROM core.v_sending_capacity_by_tag
GROUP BY 1, 2, 3, 4
ORDER BY active_daily_limit DESC;
