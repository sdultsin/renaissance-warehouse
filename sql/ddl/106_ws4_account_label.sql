-- @gate: add
-- Depends on 105
-- 106_ws4_account_label.sql  [2026-06-21 otd-sending-capacity / WS4 — renumbered from 105]
-- core.account_label: the per-(census_date,email) Infra × Vendor × Lifecycle label table.
--
-- VERSION (RECONCILED DEPLOY PLAN §3): this is DDL version **106** (live MAX(core.schema_version)=104,
--   VERIFIED this session: 103=workspace_name_normalization (WS1), 104=account_census (WS2)). Slots 105
--   (WS3 merged registry rebuild) and 106 (this) are BOTH FREE — verified count(*) FILTER(version IN 105,106)=0.
--   The NN_ filename prefix IS the schema version. The schema-moderator re-checks SELECT max(version)+1 at
--   apply and bumps the whole remaining block by any delta if the nightly moved the floor — no hardcoded
--   number is trusted past that re-check. apply_ddl_file PK-dedupes on version, so a taken/stale number would
--   SILENTLY no-op the whole migration; 106 is confirmed free so it will not.
--
-- HARD DEP: WS3 (v105) must apply FIRST. WS3 backfills core.sending_account.first_cold_send_at (today
--   0/all-NULL, VERIFIED) and rebuilds core.sending_account census-derived. WS4 does NOT depend on the
--   first_cold_send_at column to derive its label (it reads the cold producer directly — see below), but the
--   plan's RISK §5.5 requires first_cold_send_at to be populated before the Active/Warmup split is trusted;
--   WS4's cold_start IS the value first_cold_send_at should hold (same producer), so WS4 doubles as the
--   backfill source-of-truth and the post-deploy assert verifies non-NULL coverage for cold-ever accounts.
--
-- CONTRACT bindings:
--   C2: binds to EXISTING column names. This DDL does NOT touch core.sending_account, so it cannot violate
--       the frozen column contract. LIFECYCLE/cold proof is derived from core.sending_account_daily.actual_sends
--       (VERIFIED live: account_id is 100% email-like AND 100% lower-case across 36,831,351 rows, so
--       lower(account_id)=account_id=lower(email) join is exact — no case-sensitivity miss). cold_start here
--       IS the value core.sending_account.first_cold_send_at should hold (MIN(date) with actual_sends>0).
--   D1: lifecycle is BINARY — Active (EVER sent cold, looked back as far as cold history allows) | Warmup.
--       No third state. lifecycle_confidence ∈ {confident, uncertain}. The uncertain set is the primary
--       deliverable (uncertain-accounts CSV).
--   D5: vendor canonical via core.sending_account_vendor.vendor_category. VERIFIED live & populated
--       (774,925 emails, 5 categories: MailIn 534,603 / Outreach Today 179,629 / Reseller 39,998 /
--       Cheap Inboxes 20,096 / Unmapped 599). vendor populates NOW; '(pending)' only for census emails
--       absent from that table. NOTE: the live taxonomy is {MailIn, Outreach Today, Reseller, Cheap Inboxes,
--       Unmapped} — the generator passes vendor_category through verbatim, so no Panel/Milkbox remap here.
--
-- Source-of-truth keys (ALL VERIFIED live this session, snapshot warehouse_20260621_063139_227.duckdb):
--   - core.account_census(census_date, email, provider_code, status, warmup_status, warmup_status_label,
--     stat_warmup_score, timestamp_created, timestamp_warmup_start, daily_limit, workspace_slug) — WS2 live,
--     latest census_date=2026-06-21, 314,887 rows, 1 date. ALL referenced columns EXIST (verified).
--   - core.sending_account_daily(date, account_id, actual_sends) — COLD-ONLY ingest. ALL columns EXIST.
--   - core.sending_account_vendor(account_email, vendor_category) — D5 vendor. Columns EXIST + populated.
--   - core.account_mx_resolution(domain->infra) — WS3 SOFT side-table for the provider_code=1 waterfall.
--     ** VERIFIED ABSENT live AND not present anywhere in the droplet repo. ** The entity (account_label.py)
--     conditionally LEFT JOINs it ONLY if it exists; absent -> pc=1 falls back to OTD (the verified 174k/174k
--     outcome). This DDL creates no dependency on it. If WS3/v105 does NOT create account_mx_resolution, the
--     OTD fallback is the documented, correct behavior — NOT an error.
--
-- Fully additive + reversible: DROP TABLE core.account_label + the two views undoes this entirely.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.account_label (
    census_date            DATE        NOT NULL,
    email                  VARCHAR     NOT NULL,             -- lower-cased account key (stored lower(email))
    workspace_slug         VARCHAR,
    -- INFRA (account-class): Google | Outlook | OTD
    infra                  VARCHAR     NOT NULL,
    infra_source           VARCHAR,                          -- provider_code | mx_resolution | otd_fallback
    -- VENDOR (D5): live vendor_category passthrough | (pending)
    vendor                 VARCHAR     NOT NULL DEFAULT '(pending)',
    vendor_source          VARCHAR,                          -- sending_account_vendor | (pending)
    -- LIFECYCLE (D1, BINARY): Active | Warmup
    lifecycle              VARCHAR     NOT NULL,
    lifecycle_confidence   VARCHAR     NOT NULL,             -- confident | uncertain
    lifecycle_basis        VARCHAR,                          -- e.g. cold_send_history | <reason_uncertain>
    cold_start             DATE,                             -- MIN(date) with actual_sends>0 (= first_cold_send_at SoT)
    last_cold_send_date    DATE,
    total_cold_sends_ever  BIGINT      NOT NULL DEFAULT 0,
    cold_send_days         INTEGER     NOT NULL DEFAULT 0,
    -- census taxonomy carried for resolution context
    provider_code          INTEGER,
    warmup_status          INTEGER,
    warmup_score           DOUBLE,                           -- census stat_warmup_score is INTEGER; widened to DOUBLE here
    daily_limit            DOUBLE,
    timestamp_created      TIMESTAMP WITH TIME ZONE,
    timestamp_warmup_start TIMESTAMP WITH TIME ZONE,
    reason_uncertain       VARCHAR,                          -- NULL for confident rows; the CSV's why-uncertain
    created_before_cold_window BOOLEAN NOT NULL DEFAULT FALSE,
    _resolved_at           TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (census_date, email)
);

-- Latest-snapshot serving view the dashboards/portal READ (never re-derive infra/vendor/lifecycle).
CREATE OR REPLACE VIEW core.v_account_label_current AS
SELECT *
FROM core.account_label
WHERE census_date = (SELECT max(census_date) FROM core.account_label);

-- {Infra} {Lifecycle} rollup tag (e.g. "Google Active", "Outlook Warmup", "OTD Active").
CREATE OR REPLACE VIEW core.v_account_label_rollup AS
SELECT
    census_date,
    infra,
    lifecycle,
    lifecycle_confidence,
    infra || ' ' || lifecycle AS infra_lifecycle_tag,
    count(*)                                              AS accounts,
    count(*) FILTER (WHERE lifecycle_confidence='confident') AS confident_accounts,
    count(*) FILTER (WHERE lifecycle_confidence='uncertain') AS uncertain_accounts
FROM core.account_label
GROUP BY 1,2,3,4,5;
