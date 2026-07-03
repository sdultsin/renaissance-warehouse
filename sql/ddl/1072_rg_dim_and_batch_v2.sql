-- @gate: add
-- Depends on 61, 1026
-- ============================================================================
-- 1072_rg_dim_and_batch_v2.sql — RG-tag dimension + batch-feed v2 columns +
--                                the David-question view (TKT-1 B+C) [2026-07-03]
-- ----------------------------------------------------------------------------
-- WHY (handoffs/2026-07-01-TICKET-sending-truth-disconnects-batch-attribution.md §3-4):
--   The June reseller cancellation wave (~25k inboxes, Darcy 06-24) was never
--   removed from Instantly, so cancelled-for-good inboxes poison every capacity
--   number and nobody can answer "which errored R1 inboxes are Avion vs Shekhar
--   vs cancelled?" in one query. The old batch feed mirrored 38 per-workspace
--   "Email Accounts" sheets, 33 of which are NOT shared with the exporter
--   identity (403) — the weekly refresh has failed --require-all since install.
--   The inbox->RG edge already lives in the warehouse (core.account_tags,
--   nightly since #126); only RG->(workspace/type/cancelled/renewal/partner)
--   metadata was missing, and it lives in 3 sheets we CAN read (verified
--   READ-ONLY from the box 2026-07-03: Inbox Hub Funding 2,863 rows + Cancelled
--   2,363 rows; Cancelling-Accounts 7 partner tabs; Batches registry 181 labels).
--
-- FIX (this DDL is the additive schema half; data flows via
--   scripts/export_infra_batch_v2.py -> scripts/build_infra_batch_v2.sql,
--   driven weekly by scripts/refresh_infra_batch.sh):
--   (1) core.rg_tag_dim — one row per RG (or named reseller) tag: the durable
--       RG -> workspace/type/cancelled/renewal/partner dimension (DDL-1071
--       registry-contract companion; full-replace load each refresh).
--   (2) core.sending_account_batch + is_cancelled / partner / rg_type /
--       attribution_source — cancellation truth and partner attribution as
--       first-class columns on the membership table; attribution_source says
--       whether a row is live-derived ('account_tags_live') or preserved from
--       the 06-12 sheet snapshot ('sheet_snapshot_20260612').
--   (3) core.v_inbox_attribution — ONE row per (inbox email x workspace)
--       answering the David question directly: live status + daily_limit
--       (census) + RG tags + partner + is_cancelled + batch.
--
-- Verified read-only on serving snapshot warehouse_20260703_043558_874.duckdb:
--   * core.account_tags: 1,058,907 rows; RG-pattern edges 1,796,541 over
--     894,775 distinct emails and 4,040 distinct RG tags.
--   * rg_dim.parquet test export (box, /tmp): 5,650 tags — 2,358 cancelled,
--     2,288 with partner, 4,838 with workspace; joins to 856,781/894,775
--     (95.8%) of live RG-tagged emails; 421,698 historical emails land on
--     cancelled tags.
--   * v_inbox_attribution dry SELECT (rg_tag_dim stubbed empty): 422,098 rows
--     == 422,098 distinct (email x workspace) == census rows (grain exact);
--     409,094 (96.9%) with RG tags; 162,033 (38.4%) with a batch mapping via
--     the CURRENT core.sending_account_batch (the "INFRA-BATCH STALE" number
--     the v2 rebuild lifts). NOTE: those numbers were measured on the original
--     RG-regex-filtered tag_attr body; the view now joins rg_tag_dim over ALL
--     tags (1073 semantics — named tags like 'Ace Reyes' contribute
--     is_cancelled/partner/rg_type/renewal). The census grain and the rg_tags
--     display column (still RG-regex-filtered) are unchanged by that widening;
--     only the attribution columns can gain values on named-tag-only inboxes.
--   * Full rehearsal on a throwaway copy of the same snapshot (this DDL +
--     build_infra_batch_v2.sql run TWICE — idempotent, identical counts):
--     sending_account_batch 2,852,201 rows (live 894,775 / legacy 1,957,426;
--     1,187,571 cancelled; 2,275,056 with partner); v_inbox_attribution then
--     shows 407,276/422,098 (96.5%) of census inboxes batch-mapped (unmapped
--     3.5%, acceptance <5%) and 47,563 cancelled; the David question returns
--     R1 connection_error split Avion-cancelled 2,260 / Shekhar 1,510 /
--     Avion 877 / cancelled-no-partner 400 / OTD 319 / … (sums to R1's 5,454).
--   This DDL itself writes no data rows (CREATE IF NOT EXISTS / ADD COLUMN IF
--   NOT EXISTS / CREATE OR REPLACE VIEW only).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ----------------------------------------------------------------------------
-- (1) core.rg_tag_dim — RG tag -> workspace / type / cancelled / renewal /
--     partner. PK = the literal tag string as it appears in Instantly account
--     tags AND in the Inbox Hub (single 'RG267' or named 'Ace Reyes' forms;
--     range tags like 'RG1000-1009' appear when the Hub tracks a block).
--     Loaded full-replace from rg_dim.parquet by build_infra_batch_v2.sql.
--     is_cancelled = membership in Inbox Hub "Cancelled" tab (Status-checked);
--     partner/renewal come from the Cancelling-Accounts per-partner tabs.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.rg_tag_dim (
    rg_tag         VARCHAR PRIMARY KEY,
    workspace_name VARCHAR,            -- Inbox Hub Workspace (display string, e.g. 'Funding 2')
    rg_type        VARCHAR,            -- Inbox Hub Type: Outreach Today / Reseller / MailIn / Tucows / MilkBox / Cheap Inboxes / Panel / Microsoft Panel
    is_cancelled   BOOLEAN,            -- in Inbox Hub 'Cancelled' tab (cancelled-for-good, NOT reconnectable)
    renewal        VARCHAR,            -- renewal day-of-month raw string ('27th'), from Cancelling-Accounts
    partner        VARCHAR,            -- Cancelling-Accounts tab: Shekhar / Avion / Outreach Today / MailIn / Inboxing / Cheap Inboxes / Panel
    _loaded_at     TIMESTAMPTZ,
    _run_id        VARCHAR
);

-- ----------------------------------------------------------------------------
-- (2) batch-feed v2 columns on the membership table (additive; backfilled by
--     build_infra_batch_v2.sql on its first run, NULL until then).
-- ----------------------------------------------------------------------------
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS is_cancelled BOOLEAN;
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS partner VARCHAR;
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS rg_type VARCHAR;
ALTER TABLE core.sending_account_batch ADD COLUMN IF NOT EXISTS attribution_source VARCHAR;

-- ----------------------------------------------------------------------------
-- (3) core.v_inbox_attribution — the David-question view: ONE row per live
--     (inbox email x workspace) from the latest census, with connection status,
--     configured daily_limit, the inbox's RG tags, partner, cancellation truth
--     and batch attribution. "Which errored R1 inboxes are Avion vs Shekhar vs
--     cancelled?" =
--       SELECT partner, is_cancelled, count(*) FROM core.v_inbox_attribution
--       WHERE workspace_slug='renaissance-1' AND status_label='connection_error'
--       GROUP BY 1,2;
--     Grain: exactly the census grain (email unique per workspace; verified
--     422,098 == distinct(email,workspace_slug) on warehouse_20260703_043558_874).
--     tag_attr is grouped per (email, workspace) and the batch join takes the
--     rn=1 current-generation row, so neither join can fan out.
--     is_cancelled / partner / rg_type / renewal derive from joining
--     core.rg_tag_dim over ALL of the inbox's tags — matching DDL 1073's
--     v_sending_capacity_by_tag semantics, so NAMED dim tags ('Ace Reyes'
--     form, part of the rg_tag_dim contract above) contribute cancellation/
--     partner truth here exactly as they do in the capacity split. The RG
--     regex is kept ONLY for the rg_tags display column.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_inbox_attribution AS
WITH tag_edges AS (
    SELECT lower(email) AS email, workspace_slug, unnest(tags_arr) AS tag
    FROM core.account_tags
),
tag_attr AS (
    SELECT r.email,
           r.workspace_slug,
           string_agg(r.tag, ',' ORDER BY r.tag)
               FILTER (WHERE regexp_matches(r.tag, '^RG[0-9]'))  AS rg_tags,
           bool_or(COALESCE(d.is_cancelled, false))       AS is_cancelled,
           max(d.partner)                                  AS partner,
           max(d.rg_type)                                  AS rg_type,
           max(d.renewal)                                  AS renewal
    FROM tag_edges r
    LEFT JOIN core.rg_tag_dim d ON d.rg_tag = r.tag
    GROUP BY 1, 2
),
batch AS (
    SELECT account_email, batch_key, batch_family, attribution_source,
           row_number() OVER (
               PARTITION BY account_email
               ORDER BY is_current_batch DESC NULLS LAST, batch_key DESC NULLS LAST
           ) AS rn
    FROM core.sending_account_batch
)
SELECT
    c.email,
    c.workspace_slug,
    c.workspace_current_name,                 -- canonical display name (never raw_pipeline_campaigns.workspace_name)
    c.status_label,                           -- active / connection_error / ...
    c.daily_limit,
    t.rg_tags,
    t.partner,
    COALESCE(t.is_cancelled, false) AS is_cancelled,
    t.rg_type,
    t.renewal,
    b.batch_key,
    b.batch_family,
    b.attribution_source,
    c.census_date
FROM core.v_account_census_latest c
LEFT JOIN tag_attr t ON t.email = lower(c.email) AND t.workspace_slug = c.workspace_slug
LEFT JOIN batch    b ON b.account_email = lower(c.email) AND b.rn = 1;
