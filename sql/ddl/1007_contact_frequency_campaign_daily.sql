-- Campaign → lead contact log mirror + convenience view. Version 1007.
--
-- @gate: add
-- Depends on: entities/pipeline_mirror.py (adds the contact_frequency_campaign_daily SPEC)
--
-- WHY: the warehouse previously mirrored only OUTCOME events
-- (raw_pipeline_lead_events: bounced / interested / not_interested / …) plus
-- rolled-up per-campaign and per-lead COUNTS. It never carried the granular
-- per-campaign CONTACTED-LEAD ROSTER, so "what are all the leads this campaign
-- contacted?" was unanswerable from the warehouse (only Instantly had it).
--
-- That roster already exists upstream: pipeline-supabase
-- public.contact_frequency_campaign_daily (~49M rows) — the backbone of the
-- contact-frequency suppression system (the lead-side rollup
-- mirror.lead_contact_summary keeps per-lead counts but discards WHICH
-- campaigns). This mirrors the campaign-grain log so any campaign's full
-- contacted-lead list is one query, and the email joins back to the lead
-- inventory for full info.
--
-- GRAIN: one row per campaign_id × lead_email × send_date (source grain).
-- COVERAGE: send_date from 2026-05-08 onward (when this log began). Lifetime
-- per-lead counts go back further (contact_frequency_totals); the itemized
-- per-campaign roster starts 2026-05-08.
-- MIRROR MODE: upsert on _key = md5(campaign_id|lead_email|send_date),
-- incremental by updated_at watermark (see entities/pipeline_mirror.py SPECS).

CREATE TABLE IF NOT EXISTS raw_pipeline_contact_frequency_campaign_daily (
  _key            VARCHAR NOT NULL,
  workspace_id    VARCHAR,
  campaign_id     VARCHAR,
  campaign_name   VARCHAR,
  lead_email      VARCHAR,          -- citext upstream -> VARCHAR
  lead_domain     VARCHAR,
  send_date       DATE,
  sent_count      INTEGER,
  first_sent_at   TIMESTAMPTZ,
  last_sent_at    TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ,      -- watermark column
  _loaded_at      TIMESTAMPTZ NOT NULL,
  _run_id         VARCHAR NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_cfcd_key
  ON raw_pipeline_contact_frequency_campaign_daily (_key);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_cfcd_campaign
  ON raw_pipeline_contact_frequency_campaign_daily (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_cfcd_email
  ON raw_pipeline_contact_frequency_campaign_daily (lower(lead_email));

-- Convenience surface: one row per (campaign, lead) — the direct answer to
-- "all leads this campaign contacted", with first/last contact + total sends.
-- Join lower(lead_email) back to the lead inventory for full lead info.
CREATE OR REPLACE VIEW core.v_campaign_contacted_leads AS
SELECT
  campaign_id,
  any_value(campaign_name)   AS campaign_name,
  any_value(workspace_id)    AS workspace_id,
  lower(lead_email)          AS lead_email,
  min(first_sent_at)         AS first_contacted_at,
  max(last_sent_at)          AS last_contacted_at,
  sum(sent_count)            AS sends,
  count(DISTINCT send_date)  AS days_contacted
FROM raw_pipeline_contact_frequency_campaign_daily
GROUP BY campaign_id, lower(lead_email);
