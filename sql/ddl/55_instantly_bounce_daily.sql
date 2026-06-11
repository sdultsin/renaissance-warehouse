-- Version 55 (2026-06-11) — per-campaign-per-day bounce counts.
--
-- Source = the Instantly campaign-summary analytics endpoint:
--   GET /api/v2/campaigns/analytics?id=<cid>&start_date=<day>&end_date=<day>
--     -> bounced_count
-- The daily analytics endpoint (Track H, DDL 40) carries NO bounce field, so bounces
-- are fetched separately by scripts/build_campaign_daily.py (one call per campaign-day,
-- only for days with sent > 0) into this durable store. core.campaign_daily is a full
-- DELETE+rebuild every run — this table is what lets bounces survive that rebuild; the
-- builder UPDATE-joins it into campaign_daily.bounces after each rebuild.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.instantly_bounce_daily (
    campaign_id    VARCHAR NOT NULL,
    date           DATE    NOT NULL,
    bounced        BIGINT  NOT NULL,
    workspace_slug VARCHAR,
    _fetched_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (campaign_id, date)
);

CREATE INDEX IF NOT EXISTS ix_bounce_daily_date ON core.instantly_bounce_daily (date);
