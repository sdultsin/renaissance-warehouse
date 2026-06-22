-- 112_u3_missing_fields.sql  [2026-06-21 otd-sending-capacity / U3]
-- @gate: add
--
-- DDL version 112 (RECONCILED-DEPLOY-PLAN §3 row 8: U3, the LAST unit, additive, LOW risk, deferrable).
--   Live MAX(core.schema_version)=104 (VERIFIED this session on snapshot warehouse_20260621_063139_227;
--   103=workspace_name_normalization, 104=account_census). The reconciled batch renumbers WS3..U3 to
--   105..112; U3 = 112. v112 is FREE (no row in core.schema_version — VERIFIED). The NN_ prefix IS the
--   schema version; the Schema-Moderator RE-RUNS `SELECT max(version)+1 FROM core.schema_version`
--   immediately before apply and bumps the WHOLE remaining block by any delta if the nightly moved the
--   floor — so if 105..111 land first, this becomes whatever MAX+1 is. apply_ddl_file PK-dedupes on
--   version: a STALE/taken number SILENTLY NO-OPS the entire migration, so the free-slot check is
--   mandatory and was performed.
--
-- DEPENDS ON: 104 (core.account_census, live) and — for the daily-status append + provisioning batch —
--   nothing else hard. SOFT-uses core.sending_account (105 rebuild) for error_string population, but does
--   NOT hard-require it: the error_string column is added to BOTH surfaces and populated by the U3 enrich
--   job, which degrades gracefully if 105 hasn't landed. No collision with U1's core.sending_account_tag.
--
-- PURPOSE (RECONCILED-DEPLOY-PLAN §2 U3 + HANDOFF §Strategy.3): add the deliverability/infra fields the
-- census does NOT carry, all ADDITIVE on top of census + sending_account, blocking nothing:
--   (a) per-account error_string  (from get_account / test_account_vitals)
--   (b) domain_registered_at + registrar + DNS state (SPF/DKIM/DMARC/MX/NS) via nightly RDAP + dig
--   (c) OTD provisioning_batch_id  (reconstructed from created_at::date — VERIFIED 38 batches, 2026-02-17..06-17)
--   (d) daily account-STATUS history (core.account_status_daily APPEND) — census is a same-day snapshot only
--
-- FULLY REVERSIBLE: DROP the 3 new tables + 4 new views + the 2 added columns. No data mutated, no
-- consumer repointed (additive). Safe to defer to a follow-up gate (it is enrichment; downstream tiles
-- do not read it yet).
--
-- ============================================================================================
-- (a) per-account error_string  (verified absent today: core.account_census has no error col; sending_account
--     has has_errors BOOLEAN but no string). Additive column on both the census-latest enrichment table and
--     sending_account. We do NOT alter core.account_census (immutable per-date census) — instead a side
--     enrichment table keyed by (email) that the latest-census view LEFT JOINs, mirroring the relabel pattern.
-- ============================================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- Side enrichment, one row per account email (latest-known). Refreshed by the U3 nightly enrich job.
-- Keyed by lower(email) — the stable account identity (Instantly exposes no account id; PK matches census).
CREATE TABLE IF NOT EXISTS core.account_error (
    email            VARCHAR NOT NULL,                 -- lower(email), == census identity
    workspace_uuid   VARCHAR,                          -- provenance (account payload `organization`)
    has_errors       BOOLEAN,                          -- mirror of census/sending_account flag
    error_string     VARCHAR,                          -- raw get_account/test_account_vitals message (NULL = healthy)
    error_code       VARCHAR,                          -- parsed code if present
    checked_at       TIMESTAMP WITH TIME ZONE,         -- when the vitals call ran
    _loaded_at       TIMESTAMP WITH TIME ZONE,
    _run_id          VARCHAR,
    PRIMARY KEY (email)
);

-- Add error_string to core.sending_account (105 rebuild target) IF that table exists. Guarded so U3 can
-- apply BEFORE or AFTER 105 without failing: ADD COLUMN IF NOT EXISTS is a no-op when already present and
-- the table is guaranteed to exist (it is live today, pre-105, at 1.36M rows; 105 rebuilds it in place).
ALTER TABLE core.sending_account ADD COLUMN IF NOT EXISTS error_string VARCHAR;

-- ============================================================================================
-- (b) domain registration + DNS state. One row per domain (NOT per account — many accounts share a domain).
--     Populated by the nightly RDAP (registrar/created) + dig (SPF/DKIM/DMARC/MX/NS) job. SLOW external job:
--     intentionally decoupled — the table is additive and starts empty; the enrich job backfills over runs.
-- ============================================================================================

CREATE TABLE IF NOT EXISTS core.account_domain_dns (
    domain               VARCHAR NOT NULL,             -- split_part(lower(email),'@',2)
    domain_registered_at TIMESTAMP WITH TIME ZONE,     -- RDAP registration/created date
    registrar            VARCHAR,                       -- RDAP registrar name
    rdap_checked_at      TIMESTAMP WITH TIME ZONE,
    has_spf              BOOLEAN,                       -- dig TXT: v=spf1 present
    has_dkim             BOOLEAN,                       -- dig TXT on common selectors: DKIM present
    has_dmarc            BOOLEAN,                       -- dig TXT _dmarc: v=DMARC1 present
    has_mx               BOOLEAN,                       -- dig MX: >=1 record
    mx_provider          VARCHAR,                       -- coarse provider class from MX host (google/microsoft/other)
    ns_records           VARCHAR,                       -- dig NS: comma-joined nameservers (DNS state)
    spf_record           VARCHAR,                       -- raw SPF TXT (for drift/diff)
    dmarc_record         VARCHAR,                       -- raw DMARC TXT
    dns_checked_at       TIMESTAMP WITH TIME ZONE,
    _loaded_at           TIMESTAMP WITH TIME ZONE,
    _run_id              VARCHAR,
    PRIMARY KEY (domain)
);

-- ============================================================================================
-- (c) provisioning_batch_id — DERIVED, no source table needed. A batch = accounts provisioned the same day
--     (created_at::date). VERIFIED 38 distinct OTD created-dates 2026-02-17..2026-06-17. Pure view over the
--     latest census; zero ingest. provider_code=1 (OTD) is the headline use, but the view covers all providers.
-- ============================================================================================

CREATE OR REPLACE VIEW core.v_account_provisioning_batch AS
SELECT
    c.email,
    c.workspace_uuid,
    c.workspace_slug,
    c.provider_code,
    CAST(c.timestamp_created AS DATE)                                AS provisioning_batch_id,  -- created_at::date
    c.timestamp_created,
    count(*) OVER (PARTITION BY CAST(c.timestamp_created AS DATE))   AS batch_size              -- accounts in this batch (all providers)
FROM core.account_census c
WHERE c.census_date = (SELECT max(census_date) FROM core.account_census);

-- Per-batch rollup (one row per created-date), OTD-focused but provider-split available.
CREATE OR REPLACE VIEW core.v_provisioning_batch_summary AS
SELECT
    CAST(timestamp_created AS DATE)                       AS provisioning_batch_id,
    count(*)                                              AS accounts,
    count(*) FILTER (WHERE provider_code = 1)             AS otd_accounts,
    count(DISTINCT workspace_uuid)                        AS workspaces,
    count(DISTINCT domain)                                AS domains
FROM core.account_census
WHERE census_date = (SELECT max(census_date) FROM core.account_census)
GROUP BY 1
ORDER BY 1;

-- ============================================================================================
-- (d) daily account-STATUS history (APPEND). Census is a same-day snapshot keyed by census_date — but it is
--     promoted ONE date at a time (DELETE+INSERT for that census_date), so it is NOT a durable day-over-day
--     status ledger you can diff. account_status_daily APPENDS one immutable row per (status_date, account)
--     so status transitions (active->paused->error, warmup on->off->banned) survive even after the census
--     for that date is re-promoted. Derived from the latest census each nightly run; idempotent per date.
-- ============================================================================================

CREATE TABLE IF NOT EXISTS core.account_status_daily (
    status_date          DATE    NOT NULL,             -- the census_date this status was observed
    workspace_uuid       VARCHAR NOT NULL,
    email                VARCHAR NOT NULL,             -- lower(email)
    workspace_slug       VARCHAR,
    provider_code        INTEGER,
    status               INTEGER,                       -- raw census status code
    status_label         VARCHAR,                       -- connection axis label (active/paused/connection_error/...)
    warmup_status        INTEGER,
    warmup_status_label  VARCHAR,                       -- warmup axis label (active/paused/banned)
    daily_limit          DOUBLE,                        -- true cap that day (census)
    stat_warmup_score    INTEGER,
    error_string         VARCHAR,                       -- snapshot of the error at that date (LEFT JOIN account_error)
    _loaded_at           TIMESTAMP WITH TIME ZONE,
    _run_id              VARCHAR,
    PRIMARY KEY (status_date, workspace_uuid, email)    -- one row per account per day; re-run replaces that day only
);

-- Convenience: status transitions (today's label vs the prior recorded day's label, per account).
CREATE OR REPLACE VIEW core.v_account_status_transitions AS
SELECT
    status_date,
    workspace_uuid,
    email,
    provider_code,
    status_label,
    warmup_status_label,
    LAG(status_label)        OVER w AS prev_status_label,
    LAG(warmup_status_label) OVER w AS prev_warmup_status_label,
    (status_label IS DISTINCT FROM LAG(status_label) OVER w)               AS status_changed,
    (warmup_status_label IS DISTINCT FROM LAG(warmup_status_label) OVER w) AS warmup_changed
FROM core.account_status_daily
WINDOW w AS (PARTITION BY workspace_uuid, email ORDER BY status_date);

-- ============================================================================================
-- Unified enrichment view: latest census + error + domain DNS + provisioning batch, one row per account.
-- The single object a deliverability/infra dashboard reads. Additive — does not repoint any existing consumer.
-- ============================================================================================

CREATE OR REPLACE VIEW core.v_account_deliverability_enriched AS
SELECT
    c.census_date,
    c.workspace_uuid,
    c.email,
    c.domain,
    c.workspace_slug,
    c.provider_code,
    c.daily_limit,
    c.status,
    c.status_label,
    c.warmup_status,
    c.warmup_status_label,
    CAST(c.timestamp_created AS DATE)        AS provisioning_batch_id,
    c.timestamp_created,
    e.has_errors,
    e.error_string,
    e.error_code,
    e.checked_at                             AS error_checked_at,
    d.domain_registered_at,
    d.registrar,
    d.has_spf, d.has_dkim, d.has_dmarc, d.has_mx, d.mx_provider, d.ns_records,
    d.dns_checked_at
FROM core.account_census c
LEFT JOIN core.account_error      e ON e.email  = c.email
LEFT JOIN core.account_domain_dns d ON d.domain = c.domain
WHERE c.census_date = (SELECT max(census_date) FROM core.account_census);

-- NOTE: the Schema-Moderator records the (version, sql_file, applied_at) row in core.schema_version on a
-- successful apply (the 104 DDL file likewise carries NO self-insert). Do NOT hand-insert the version here;
-- a hand-insert would race the moderator's MAX+1 re-check and could write the wrong number.
