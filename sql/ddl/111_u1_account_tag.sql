-- @gate: add
-- Depends on 104
-- ============================================================================
-- 111_u1_account_tag.sql  —  UNIT U1: Account <-> infra-TAG membership
-- ----------------------------------------------------------------------------
-- DEPLOY SLOT: version 111 (intended). Live MAX(core.schema_version)=104 this
--   session; the Schema-Moderator RE-CHECKS MAX(version)+1 at apply and bumps the
--   whole remaining 105..112 block by any delta if the nightly moved the floor.
--   apply_ddl_file PK-dedupes on version -> if 111 is ever already present this
--   migration SILENTLY NO-OPS. The moderator's free-slot check is mandatory.
--   (Verified 2026-06-21: SELECT count(*) FROM core.schema_version WHERE version=111 = 0.)
--
-- WHAT THIS UNIT DOES (per RECONCILED-DEPLOY-PLAN §2 U1 + HANDOFF Strategy.1):
--   The table core.sending_account_tag ALREADY EXISTS, EMPTY (0 rows; scaffold
--   laid in 57_recovery_reassert.sql with cols email, workspace_slug, tag_id,
--   tag_label, first_seen_at, last_seen_at; PK (email, tag_label)).
--   This unit is mostly a POPULATE + VIEW + provider_code-column job, NOT a
--   fresh-table job. The POPULATE is done by the GENERATOR (the extended
--   poll_live_accounts.py raw-API tag sync; see gen_account_tag_sync.py here) —
--   the DDL below only:
--     (1) ADD COLUMN provider_code (HANDOFF asks for it; not in the scaffold), and
--     (2) CREATE the membership view core.v_otd_tag_membership
--         (per OTD account: Active-only / Warmup-only / Both / Neither).
--
-- This DDL is ADDITIVE and IDEMPOTENT. It writes NO data rows (the generator does).
-- It does NOT touch core.sending_account / census / any other surface.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ----------------------------------------------------------------------------
-- (1) provider_code column on the existing (empty) tag table.
--     DuckDB has no "ADD COLUMN IF NOT EXISTS"; use the IF NOT EXISTS form
--     available since v0.9 — re-apply-safe (no-op if the column already exists).
--     Carries the census provider_code (1=OTD/other, 2=Google, 3=Microsoft) so
--     the OTD filter (provider_code=1) is intrinsic to the tag row, not a re-join.
-- ----------------------------------------------------------------------------
ALTER TABLE core.sending_account_tag ADD COLUMN IF NOT EXISTS provider_code INTEGER;

-- ----------------------------------------------------------------------------
-- (2) core.v_otd_tag_membership — the headline membership lens.
--   Universe = the latest census, OTD only (provider_code=1) — census is the
--   authoritative account surface (NOT core.sending_account, which is the stale
--   1.36M-row inflated table). Liveness/identity = presence in latest census.
--   Per OTD account, LEFT JOIN the two infra-tag memberships and bucket:
--       Active-only  : in "Outreach Today Active",  not in Warmup
--       Warmup-only  : in "Outreach Today Warmup",  not in Active
--       Both         : in BOTH (a CONFLICT — an account should be in exactly one)
--       Neither      : in NEITHER (idle / invisible to CMs — the ~22k idle pool)
--
--   Tag labels are byte-exact "Outreach Today Active" / "Outreach Today Warmup"
--   (HANDOFF). NB these are DISTINCT from the vendor tag_label 'Outreach Today'
--   consumed by the unrelated v_tag_coverage_gaps view (DDL 31) — no collision,
--   different string. Matching is on the email key (census email == tag email,
--   both lower-cased at write time by the poller and the census loader).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_otd_tag_membership AS
WITH otd AS (
    SELECT
        c.email,
        c.workspace_slug,
        c.workspace_uuid,
        c.workspace_current_name,
        c.provider_code,
        c.daily_limit,
        c.status_label,
        c.warmup_status_label
    FROM core.v_account_census_latest c
    WHERE c.provider_code = 1            -- OTD / custom-SMTP only
),
active_tag AS (
    SELECT DISTINCT email
    FROM core.sending_account_tag
    WHERE tag_label = 'Outreach Today Active'
),
warmup_tag AS (
    SELECT DISTINCT email
    FROM core.sending_account_tag
    WHERE tag_label = 'Outreach Today Warmup'
)
SELECT
    o.email,
    o.workspace_slug,
    o.workspace_uuid,
    o.workspace_current_name,
    o.provider_code,
    o.daily_limit,
    o.status_label,
    o.warmup_status_label,
    (a.email IS NOT NULL)                                   AS in_active_tag,
    (w.email IS NOT NULL)                                   AS in_warmup_tag,
    CASE
        WHEN a.email IS NOT NULL AND w.email IS NOT NULL THEN 'Both'
        WHEN a.email IS NOT NULL                          THEN 'Active-only'
        WHEN w.email IS NOT NULL                          THEN 'Warmup-only'
        ELSE                                                   'Neither'
    END                                                     AS tag_membership
FROM otd o
LEFT JOIN active_tag a ON a.email = o.email
LEFT JOIN warmup_tag w ON w.email = o.email;

-- ----------------------------------------------------------------------------
-- (2b) Per-workspace rollup — THE DoD query, materialised as a view so the
--   "OTD accounts in neither infra tag, per workspace" answer is a single
--   no-API SELECT. Surfaces BOTH problems: the Neither gap AND the Both conflict.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_otd_tag_membership_by_workspace AS
SELECT
    workspace_slug,
    workspace_current_name,
    COUNT(*)                                              AS otd_accounts,
    COUNT(*) FILTER (WHERE tag_membership = 'Active-only') AS active_only,
    COUNT(*) FILTER (WHERE tag_membership = 'Warmup-only') AS warmup_only,
    COUNT(*) FILTER (WHERE tag_membership = 'Both')        AS both_conflict,
    COUNT(*) FILTER (WHERE tag_membership = 'Neither')     AS neither_idle
FROM core.v_otd_tag_membership
GROUP BY workspace_slug, workspace_current_name
ORDER BY neither_idle DESC;
