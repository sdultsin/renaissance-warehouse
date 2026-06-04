-- Canonical campaign metrics view. Version 33.
--
-- THE single place every consumer (dashboards, chatbot, opportunity entity,
-- analyses) should read campaign performance from. Encodes the metric-grain
-- rules established 2026-06-02 (see sql/ddl/32_campaign_analytics.sql header):
--
--   * SENT           -> campaign-grain analytics (emails_sent_count); additive
--                       daily sum as fallback for deleted campaigns.
--   * UNIQUE REPLIES -> campaign-grain analytics (reply_count_unique); lead-level
--                       COUNT(DISTINCT lead_email) fallback. NEVER SUM(daily
--                       unique_replies) — that double-counts across days.
--   * OPPORTUNITIES  -> campaign-grain analytics (total_opportunities) ONLY. This
--                       is the Instantly UI interest-status count. NULL for
--                       deleted campaigns (no faithful daily/lead-level proxy).
--
-- BASE = raw_pipeline_campaigns (latest _run_id): the campaign superset. It
-- retains campaigns deleted from Instantly (kept for billing history) which the
-- live analytics endpoint 404s on, AND carries the pipeline-derived
-- cm_name / product / infra_type / segment we prefer over warehouse regex
-- (feedback_prefer_pipeline_derived_over_regex). workspace_id here is the SLUG
-- (e.g. 'renaissance-1'), so filtering by workspace works.

CREATE OR REPLACE VIEW v_campaign_metrics AS
WITH
-- NOTE: the raw_pipeline_* tables are deduped to one row per natural _key since
-- spec 15 (sync modes), so NO _run_id filter is needed or wanted here — filtering
-- to the latest run would drop frozen/older rows the windowed mirror didn't re-pull.
pc AS (  -- campaign dimension (one row per campaign_id via upsert)
  SELECT campaign_id, workspace_id, workspace_name, name, cm_name, product,
         infra_type, segment, status, leads_count
  FROM raw_pipeline_campaigns
),
pipe_sent AS (  -- additive fact (one row per campaign_id+date)
  SELECT campaign_id, sum(sent) AS sent_additive
  FROM raw_pipeline_campaign_daily_metrics
  GROUP BY campaign_id
),
pipe_replies AS (  -- additive-safe distinct repliers from the lead-level mirror
  SELECT campaign_id, count(DISTINCT lower(lead_email)) AS unique_repliers
  FROM raw_pipeline_reply_data
  GROUP BY campaign_id
)
SELECT
  pc.campaign_id,
  pc.workspace_id,
  pc.workspace_name,
  pc.name                                                    AS campaign_name,
  pc.cm_name,
  pc.product                                                 AS offer,
  pc.infra_type,
  pc.segment,
  pc.status,
  regexp_matches(lower(pc.name), '\b(isaac|mca|cheap leads)\b') AS is_mca,
  -- SENT: analytics first, additive pipeline sum fallback.
  COALESCE(a.emails_sent_count, ps.sent_additive)            AS sent,
  -- UNIQUE REPLIES: analytics first, lead-level distinct fallback.
  COALESCE(a.reply_count_unique, pr.unique_repliers)         AS unique_replies,
  a.reply_count_automatic_unique                             AS auto_replies,
  -- OPPORTUNITIES: analytics ONLY (UI interest-status count). NULL for deleted.
  a.total_opportunities                                      AS opportunities,
  a.total_opportunity_value                                  AS opportunity_value,
  a.bounced_count                                            AS bounced,
  a.completed_count                                          AS completed,
  CASE WHEN COALESCE(a.emails_sent_count, ps.sent_additive) > 0
       THEN round(COALESCE(a.reply_count_unique, pr.unique_repliers)::DOUBLE
                  / COALESCE(a.emails_sent_count, ps.sent_additive), 5) END   AS reply_rate,
  CASE WHEN a.emails_sent_count > 0 AND a.total_opportunities IS NOT NULL
       THEN round(a.total_opportunities::DOUBLE / a.emails_sent_count, 5) END AS opp_rate,
  -- positive-reply rate = opps / unique replies (the copy/offer conversion).
  CASE WHEN a.reply_count_unique > 0 AND a.total_opportunities IS NOT NULL
       THEN round(a.total_opportunities::DOUBLE / a.reply_count_unique, 4) END AS positive_reply_rate,
  -- emails per opportunity (headline efficiency).
  CASE WHEN a.total_opportunities > 0
       THEN round(a.emails_sent_count::DOUBLE / a.total_opportunities, 0) END  AS email_per_opp,
  CASE WHEN a.campaign_id IS NOT NULL THEN 'instantly_analytics'
       ELSE 'pipeline_fallback' END                          AS metric_source,
  a._loaded_at                                               AS analytics_loaded_at
FROM pc
LEFT JOIN raw_instantly_campaign_analytics a ON a.campaign_id = pc.campaign_id
LEFT JOIN pipe_sent    ps ON ps.campaign_id = pc.campaign_id
LEFT JOIN pipe_replies pr ON pr.campaign_id = pc.campaign_id;
