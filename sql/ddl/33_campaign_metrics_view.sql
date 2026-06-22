-- Canonical campaign metrics view. Version 33.
--
-- THE single place every consumer (dashboards, chatbot, opportunity entity,
-- analyses) should read campaign performance from. Encodes the metric-grain
-- rules established 2026-06-02 (see sql/ddl/32_campaign_analytics.sql header):
--
--   * SENT           -> campaign-grain analytics (emails_sent_count); additive
--                       daily sum, then __ALL__ rollup, as fallbacks.
--   * UNIQUE REPLIES -> campaign-grain analytics (reply_count_unique); lead-level
--                       COUNT(DISTINCT lead_email) fallback CLAMPED to sent. NEVER
--                       SUM(daily unique_replies) — that double-counts across days.
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
--
-- v113 [2026-06-21]: HOTFIX. The WS7 rewrite (v109) that added the `offer`
-- COALESCE accidentally dropped two long-standing integrity protections:
--   (1) the cd_cum (__ALL__ rollup) THIRD sent fallback, and
--   (2) the LEAST(unique_repliers, sent) clamp on the lead-level reply fallback.
-- Their loss let 35 campaigns show unique_replies > sent (publish-gate canary
-- C4_email_cum_replies_le_sent) and 5 show reply_rate > 1 (C6_reply_rate_in_unit),
-- which fail-closed the serving promote. This version RESTORES the exact
-- proven-good v104 sent_fixed + LEAST logic (re-verified 0/0 violations on the
-- live writer) while PRESERVING the WS7 offer resolution. The only delta vs the
-- pre-WS7 prod view is `offer = COALESCE(co.offer, pc.product)`.

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
cd_cum AS (  -- v113 RESTORED: campaign-grain emails_sent (__ALL__ rollup) = 3rd sent fallback
  SELECT campaign_id, max(emails_sent) AS emails_sent
  FROM raw_pipeline_campaign_data
  WHERE step = '__ALL__' AND variant = '__ALL__'
  GROUP BY campaign_id
),
pipe_replies AS (  -- additive-safe distinct repliers from the lead-level mirror
  SELECT campaign_id, count(DISTINCT lower(lead_email)) AS unique_repliers
  FROM raw_pipeline_reply_data
  GROUP BY campaign_id
),
base AS (
  SELECT
    pc.campaign_id, pc.workspace_id, pc.workspace_name,
    pc.name AS campaign_name, pc.cm_name,
    -- WS7 [2026-06-21]: prefer the canonical resolved offer on core.campaign
    -- (Business Funding | R&D Credit | Pre-IPO | Section 125 | Tariffs), falling
    -- back to raw_pipeline_campaigns.product only when the campaign is unresolved /
    -- absent from core.campaign. Reverses the disproven "prefer pipeline product"
    -- rule for `offer` ONLY (pc.product is FUNDING-collapsed/poisoned).
    -- cm_name/infra_type/segment still prefer pipeline-derived (unchanged).
    COALESCE(co.offer, pc.product) AS offer,
    pc.infra_type, pc.segment, pc.status,
    regexp_matches(lower(pc.name), '\b(isaac|mca|cheap leads)\b') AS is_mca,
    a.reply_count_unique, a.reply_count_automatic_unique,
    a.total_opportunities, a.total_opportunity_value,
    a.bounced_count, a.completed_count, a.emails_sent_count,
    a.campaign_id AS analytics_cid, a._loaded_at AS analytics_loaded_at,
    pr.unique_repliers,
    -- SENT (v113): analytics first, additive daily sum, then __ALL__ rollup.
    COALESCE(a.emails_sent_count, NULLIF(ps.sent_additive, 0), NULLIF(cd.emails_sent, 0)) AS sent_fixed
  FROM pc
  LEFT JOIN raw_instantly_campaign_analytics a ON a.campaign_id = pc.campaign_id
  LEFT JOIN pipe_sent    ps ON ps.campaign_id = pc.campaign_id
  LEFT JOIN cd_cum       cd ON cd.campaign_id = pc.campaign_id
  LEFT JOIN pipe_replies pr ON pr.campaign_id = pc.campaign_id
  -- WS7 [2026-06-21]: canonical offer source. Join on campaign_id (stable id, NOT
  -- the slug); co.offer is the resolved 3-pass label written by entities/campaign.py
  -- _resolve_campaign_offer. LEFT JOIN so deleted/pipeline-only campaigns absent
  -- from core.campaign keep their pc.product fallback via the COALESCE above. 1:1.
  LEFT JOIN core.campaign co ON co.campaign_id = pc.campaign_id
)
SELECT
  campaign_id, workspace_id, workspace_name, campaign_name, cm_name, offer,
  infra_type, segment, status, is_mca,
  sent_fixed                                                                 AS sent,
  -- UNIQUE REPLIES (v113): analytics first; lead-level fallback CLAMPED to sent
  -- (LEAST) so cumulative replies can never exceed cumulative sent (canary C4).
  COALESCE(reply_count_unique, least(unique_repliers, sent_fixed))           AS unique_replies,
  reply_count_automatic_unique                                              AS auto_replies,
  total_opportunities                                                       AS opportunities,
  total_opportunity_value                                                   AS opportunity_value,
  bounced_count                                                             AS bounced,
  completed_count                                                           AS completed,
  CASE WHEN sent_fixed > 0
       THEN round(COALESCE(reply_count_unique, least(unique_repliers, sent_fixed))::DOUBLE
                  / sent_fixed, 5) END                                       AS reply_rate,
  CASE WHEN emails_sent_count > 0 AND total_opportunities IS NOT NULL
       THEN round(total_opportunities::DOUBLE / emails_sent_count, 5) END    AS opp_rate,
  -- positive-reply rate = opps / unique replies (the copy/offer conversion).
  CASE WHEN reply_count_unique > 0 AND total_opportunities IS NOT NULL
       THEN round(total_opportunities::DOUBLE / reply_count_unique, 4) END   AS positive_reply_rate,
  -- emails per opportunity (headline efficiency).
  CASE WHEN total_opportunities > 0
       THEN round(emails_sent_count::DOUBLE / total_opportunities, 0) END    AS email_per_opp,
  CASE WHEN analytics_cid IS NOT NULL THEN 'instantly_analytics'
       ELSE 'pipeline_fallback' END                                         AS metric_source,
  analytics_loaded_at
FROM base;
