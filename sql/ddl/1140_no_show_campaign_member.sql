-- @gate: add
-- Depends on: none (standalone additive table + summary view; core schema only)
-- ============================================================================
-- 1140_no_show_campaign_member.sql — WAREHOUSE = source of truth for ALL no-show
-- data (Sam standing principle 2026-07-18: Google Sheets are visualizations only;
-- MotherDuck/warehouse holds everything). Backfilled by entities/no_show_campaign_member.py
-- from a git-ignored seed (seed_data/no-show-backfill/*.jsonl).
--
-- Grain: (origin, campaign_id, source_tab, lead_email). Holds BOTH:
--   origin='instantly_campaign' — actual campaign MEMBERSHIP, one row per
--       (campaign_id, lead_email), downloaded from the 15 no-show / lifecycle
--       Warm-Leads campaigns via the proven POST /leads/list {campaign} pattern.
--       source_tab='' for these rows. bucket in (active_noshow, legacy_noshow, lifecycle).
--   origin='partner_file'       — the partner no-show EXPORT rows (ramir_no_show_resources,
--       the RG Master No-show sheet's native partner tabs). source_tab = the sheet
--       stream (btc_noshow, gbc_appout, gbc_stale, gbc_closedlost, ...). campaign_id =
--       the mapped active/lifecycle campaign. rg_status = the sheet's RG_STATUS.
--
-- partner attribution is derived from the campaign name / stream at load time.
-- load_source distinguishes the two feeds; loaded_at = load timestamp.
--
-- Additive only — no ALTER/DROP of any existing object. Reversible: DROP VIEW +
-- DROP TABLE.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.no_show_campaign_member (
    origin         VARCHAR NOT NULL,   -- 'instantly_campaign' | 'partner_file'
    campaign_id    VARCHAR NOT NULL,   -- Instantly campaign id (mapped campaign for partner_file rows)
    source_tab     VARCHAR NOT NULL,   -- sheet stream for partner_file rows; '' for campaign-membership rows
    lead_email     VARCHAR NOT NULL,   -- lower(trim()) — the join / reconciliation key
    campaign_name  VARCHAR,
    partner        VARCHAR,            -- GBC | BTC | GQ | Llama | CapFront | CapitalInfusion | DCX
    bucket         VARCHAR,            -- active_noshow | legacy_noshow | lifecycle
    first_name     VARCHAR,
    last_name      VARCHAR,
    company_name   VARCHAR,
    phone          VARCHAR,
    lead_status    INTEGER,            -- Instantly lead status code (campaign-membership rows)
    rg_status      VARCHAR,            -- RG_STATUS from the master sheet (partner_file rows)
    lead_id        VARCHAR,            -- Instantly lead id (campaign-membership rows)
    ts_created     TIMESTAMPTZ,        -- Instantly timestamp_created (campaign-membership rows)
    ts_updated     TIMESTAMPTZ,        -- Instantly timestamp_updated (campaign-membership rows)
    payload        JSON,               -- native custom vars / partner-file row detail
    load_source    VARCHAR NOT NULL,   -- instantly_campaign_download_20260718 | ramir_no_show_resources_20260718
    loaded_at      TIMESTAMPTZ,
    PRIMARY KEY (origin, campaign_id, source_tab, lead_email)
);

-- First insight surface: membership vs partner-file counts per partner / bucket / campaign.
CREATE OR REPLACE VIEW core.v_no_show_member_summary AS
SELECT partner,
       bucket,
       campaign_name,
       origin,
       count(*)                        AS n_rows,
       count(DISTINCT lead_email)      AS unique_emails
FROM core.no_show_campaign_member
GROUP BY partner, bucket, campaign_name, origin
ORDER BY partner, bucket, campaign_name, origin;
