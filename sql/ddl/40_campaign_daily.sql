-- Track H (2026-06-08) — per-campaign day-by-day metrics + variant grain.
-- Version 40.
--
-- Source = the Instantly analytics API (UI-faithful, NOT pipeline):
--   GET /api/v2/campaigns/analytics/daily?campaign_id=  -> per-day fact
--   GET /api/v2/campaigns/analytics/steps?campaign_id=  -> per-variant cumulative
-- The daily endpoint returns, per date: sent, unique_replies, unique_replies_automatic,
-- unique_opportunities (+ raw/non-unique). human/auto split is therefore native per-day:
--   replies_human <- unique_replies            (disjoint from auto)
--   replies_auto  <- unique_replies_automatic
-- meetings_booked joins from core.meeting (posted_at::date). bounces are NOT in the daily
-- endpoint (campaign-grain only) -> left 0/NULL for now (documented gap).
--
-- LIFECYCLE-AWARE: built by scripts/build_campaign_daily.py. Paused/completed/deleted
-- campaigns get only their real historical send days (the endpoint stops at last activity);
-- ACTIVE campaigns are densified min(date)..today so a no-send day records sent=0 (not a
-- missing row). Idempotent full rebuild per run.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.campaign_daily (
    campaign_id       VARCHAR NOT NULL,
    date              DATE    NOT NULL,
    workspace_slug    VARCHAR,
    campaign_status   VARCHAR,          -- active | paused | completed | ... (lifecycle)
    sent              BIGINT,
    opportunities     BIGINT,           -- unique_opportunities (UI interest-status)
    meetings_booked   BIGINT,
    replies_human     BIGINT,           -- unique_replies
    replies_auto      BIGINT,           -- unique_replies_automatic
    bounces           BIGINT,
    sent_cum          BIGINT,
    opportunities_cum BIGINT,
    replies_human_cum BIGINT,
    replies_auto_cum  BIGINT,
    _loaded_at        TIMESTAMPTZ NOT NULL,
    _run_id           VARCHAR,
    PRIMARY KEY (campaign_id, date)
);

CREATE INDEX IF NOT EXISTS ix_campaign_daily_date ON core.campaign_daily (date);
CREATE INDEX IF NOT EXISTS ix_campaign_daily_ws   ON core.campaign_daily (workspace_slug);

-- Per-variant cumulative grain (from the steps endpoint). Reconciles: sum(sent) over a
-- campaign's variants == that campaign's total sent.
CREATE TABLE IF NOT EXISTS core.campaign_variant (
    campaign_id    VARCHAR NOT NULL,
    step           VARCHAR,
    variant        VARCHAR,
    sent           BIGINT,
    replies_human  BIGINT,
    replies_auto   BIGINT,
    _loaded_at     TIMESTAMPTZ NOT NULL,
    _run_id        VARCHAR,
    PRIMARY KEY (campaign_id, step, variant)
);
