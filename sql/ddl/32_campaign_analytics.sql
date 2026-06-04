-- Campaign-grain analytics mirror + canonical metrics view. Version 32.
--
-- WHY THIS EXISTS (2026-06-02):
--   The daily-grain fact table `raw_pipeline_campaign_daily_metrics` cannot be
--   summed to reproduce a campaign's true reply / opportunity counts:
--     * SENT/OPENS/CLICKS are real per-day events -> additive (sum is correct).
--     * unique_replies / unique_opportunities are per-DAY-distinct counts -> a
--       lead that replies on two days is "unique" on both, so SUM() double-counts.
--   Verified for "Instantly - Short" 2026-06-02:
--       UI / Instantly analytics : sent 43,315 | unique replies 457 | opps 49
--       SUM(daily unique_replies): 500   (+9%)   <- wrong
--       SUM(daily unique_opps)   : 98    (+100%) <- wrong
--   The ONLY source that matches the Instantly UI is the campaign-grain analytics
--   endpoint GET /api/v2/campaigns/analytics. This table mirrors it.
--
-- GRAIN: one row per campaign (latest snapshot). The entity UPSERTs on
--   campaign_id, so there is exactly one row per campaign and aggregates are
--   safe with no _run_id filter. `total_opportunities` here == the UI number.
--
-- COVERAGE CAVEAT: the analytics endpoint only returns LIVE campaigns. Campaigns
--   deleted from Instantly (kept in pipeline-supabase for billing history) will
--   not appear here; the canonical view falls back to additive pipeline sums for
--   `sent` and to reply_data distinct-counts for replies, and leaves
--   `opportunities` NULL for them (no faithful source exists).

-- =====================================================================
-- RAW (campaign-grain, one row per campaign, upserted)
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_instantly_campaign_analytics (
  campaign_id                    VARCHAR PRIMARY KEY,
  workspace_id                   VARCHAR NOT NULL,
  campaign_name                  VARCHAR,
  campaign_status                INTEGER,
  campaign_is_evergreen          BOOLEAN,
  leads_count                    BIGINT,
  contacted_count                BIGINT,
  emails_sent_count              BIGINT,     -- == UI "sent"
  new_leads_contacted_count      BIGINT,
  open_count                     BIGINT,
  open_count_unique              BIGINT,
  reply_count                    BIGINT,     -- raw replies (multiple per lead)
  reply_count_unique             BIGINT,     -- == UI "replied" (distinct repliers)
  reply_count_automatic          BIGINT,
  reply_count_automatic_unique   BIGINT,
  link_click_count               BIGINT,
  link_click_count_unique        BIGINT,
  bounced_count                  BIGINT,
  unsubscribed_count             BIGINT,
  completed_count                BIGINT,
  total_opportunities            BIGINT,     -- == UI "opportunities" (interest-status count)
  total_opportunity_value        BIGINT,
  api_response_raw               VARCHAR,
  _loaded_at                     TIMESTAMPTZ NOT NULL,
  _run_id                        VARCHAR NOT NULL
);

-- The canonical metrics VIEW (v_campaign_metrics) is defined in
-- sql/ddl/33_campaign_metrics_view.sql — it is based on raw_pipeline_campaigns
-- (the campaign superset that retains deleted-campaign history and carries the
-- pipeline-derived cm_name/infra_type we prefer), so it lives in its own file
-- that can be re-applied independently as the metric rules evolve.
