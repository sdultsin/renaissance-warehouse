-- @gate: add
-- Depends on 111
-- ============================================================================
-- 1002_account_tag_sync.sql  —  POPULATE core.sending_account_tag + capacity view
-- ----------------------------------------------------------------------------
-- DEPLOY SLOT: version reserved by the Schema-Moderator (next-version). The
--   moderator re-checks MAX(version)+1 at apply; apply_ddl_file PK-dedupes on
--   version so a taken number SILENTLY NO-OPS.
--
-- WHY (HANDOFF-WAREHOUSE-B, Gap 1, measured 2026-06-23):
--   core.sending_account_tag (the account->tag edge table; scaffold in DDL 57,
--   provider_code col + v_otd_tag_membership view in DDL 111) is EMPTY (0 rows).
--   The DDL 111 comment says the POPULATE is "done by the GENERATOR (the extended
--   poll_live_accounts.py raw-API tag sync; see gen_account_tag_sync.py)" — but
--   that generator was NEVER built. So the literal infra tags Sam manages
--   ("Reseller Active", "Outreach Today Active") exist NOWHERE at account grain in
--   the warehouse, and /cm-work Step 1-2 capacity falls back to infra_type (ESP)
--   off raw_account_truth_daily_actuals — which is itself broken (HANDOFF-A: the
--   upstream CSV buckets the entire 48k-account Google fleet into 'Missing Current
--   Inventory' at daily_limit=0/expected_sends=0).
--
-- WHAT THIS UNIT DOES (the GENERATOR is entities/account_tag.py, NEW in this PR):
--   (1) raw_instantly_account_tag — the run-scoped raw landing table the new
--       nightly entity INSERTs into (one row per account x tag x run), mirroring
--       the raw_instantly_campaign_sending_tag pattern (DDL 03). The entity then
--       resolves it into the existing core.sending_account_tag (upsert + prune).
--   (2) core.v_sending_capacity_by_tag — the DURABLE per-tag capacity lens the
--       interim /cm-work mitigation can move BACK to the warehouse. It joins the
--       tag membership to core.v_account_census_latest (LIVE config truth: real
--       provider_code + daily_limit + status, fresh as of last night's census),
--       NOT the broken daily-actuals CSV. So "Reseller Active" capacity reads
--       3,307 active x 15 = ~49,605/day for F2 Google — the real number.
--
-- This DDL is ADDITIVE and IDEMPOTENT. It writes NO data rows (the entity does).
-- It does NOT touch core.sending_account_tag / census / any other surface.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ----------------------------------------------------------------------------
-- (1) Raw landing table for the nightly account->tag sync.
--     One row per (account email, tag) observed in a run. Run-scoped: the entity
--     DELETEs by _run_id before re-inserting, so a re-run is idempotent. Mirrors
--     raw_instantly_campaign_sending_tag (DDL 03). provider_code/status/daily_limit
--     are captured from the /accounts payload when present (best-effort; the
--     canonical provider_code is re-derived from census at resolution time).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_instantly_account_tag (
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR     NOT NULL,
    workspace_uuid  VARCHAR,
    workspace_slug  VARCHAR,
    email           VARCHAR     NOT NULL,
    tag_id          VARCHAR     NOT NULL,
    tag_label       VARCHAR     NOT NULL,
    provider_code   INTEGER,                     -- from /accounts payload (best-effort)
    status          INTEGER,                     -- connection-axis status code
    daily_limit     INTEGER,                     -- configured cap on the account
    PRIMARY KEY (email, tag_id, _run_id)
);

-- ----------------------------------------------------------------------------
-- (2) core.v_sending_capacity_by_tag — per (workspace, tag) sending capacity, off
--     LIVE census config (the correct source) joined to the tag membership.
--
--     Universe = accounts present in BOTH core.sending_account_tag (the edge) AND
--     the latest census (liveness/config truth). An account tagged but absent from
--     census is dead/invisible and contributes no capacity (excluded by the join).
--
--     active_daily_limit = SUM(daily_limit) over status_label='active' = the real
--     cold sending capacity/day for that tag. recoverable_daily_limit = the same
--     over connection_error (capacity that returns once the accounts reconnect).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_capacity_by_tag AS
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
    COALESCE(sum(c.daily_limit) FILTER (WHERE c.status_label = 'connection_error'), 0)
                                                                        AS recoverable_daily_limit,
    max(c.census_date)                                                   AS census_date
FROM core.sending_account_tag t
-- Join on email AND workspace_slug: v_account_census_latest is unique per email today
-- (verified 2026-06-23: 316,088 rows == distinct emails), but the entity sets
-- sending_account_tag.workspace_slug FROM census, so matching both keys is exact and
-- can never fan out / double-count if an email ever recurs across workspaces.
JOIN core.v_account_census_latest c
  ON c.email = t.email AND c.workspace_slug = t.workspace_slug
GROUP BY 1, 2, 3, 4, 5, 6;

-- ----------------------------------------------------------------------------
-- (2b) Per-(workspace, tag) rollup across providers — the headline /cm-work line:
--      "how much cold capacity does this infra tag carry, right now".
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
    max(census_date)               AS census_date
FROM core.v_sending_capacity_by_tag
GROUP BY 1, 2, 3, 4
ORDER BY active_daily_limit DESC;
