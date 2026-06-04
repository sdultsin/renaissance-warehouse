-- Phase 3 / Layer 4: derived view — campaign opportunity rollup.
-- Applied at schema version 23 by scripts/setup_db.py / orchestrator DDL applier.
--
-- ⚠ CORRECTION (2026-06-02): SUM(unique_opportunities) is NOT campaign-canonical.
-- It only equals the Instantly UI when each opportunity-lead replied on a single
-- day (true for the tiny T-MailIn-GO=4 case the original note "verified"). For any
-- multi-day campaign it OVERCOUNTS: "Instantly - Short" sums to 98 vs the UI's 49.
-- The campaign-grain truth is now `v_campaign_metrics.opportunities` (sourced from
-- the Instantly campaigns/analytics endpoint, spec 32). This view's weekly
-- `opportunities` is an additive-approximation kept only for week-over-week TREND
-- shape — use v_campaign_metrics for any absolute/total opportunity number.
--
-- This is an AGGREGATE count (Instantly gives us the number of opportunities, not the
-- specific leads — the lead-level feed opportunity_webhook_log is empty; see GAPS B8).
-- It is distinct from `core.opportunity` (the lead-level warm-call/AIM surface).
--
-- Grain: campaign × ISO week. cm / offer / infra are attributes (pipeline-derived —
-- preferred over warehouse regex per the source-of-truth sub-rule). Roll up to
-- cm×week / offer×week / infra×week with a plain GROUP BY on top of this view.
--
-- A VIEW (not materialized): always fresh; re-resolves the latest mirror snapshot on
-- each query. Cheap (latest-run daily metrics is ~40k rows).

-- Tables are deduped to one row per natural _key since spec 15 — no _run_id filter
-- (filtering to the latest run would drop frozen rows the windowed mirror skipped).
CREATE OR REPLACE VIEW v_campaign_opportunities AS
WITH latest_m AS (
  SELECT * FROM raw_pipeline_campaign_daily_metrics
),
latest_c AS (
  SELECT * FROM raw_pipeline_campaigns
)
SELECT
  date_trunc('week', m.date)::DATE        AS week_start,
  m.campaign_id,
  c.name                                  AS campaign_name,
  c.cm_name                               AS cm,        -- pipeline-derived (preferred)
  c.product                               AS offer,     -- pipeline-derived (preferred)
  c.infra_type,                                          -- pipeline-derived sending ESP
  c.workspace_id,
  c.workspace_name,
  SUM(m.sent)                             AS sends,
  SUM(m.unique_replies)                   AS unique_replies,
  SUM(m.unique_opportunities)             AS opportunities,      -- ⭐ canonical (matches Instantly UI)
  SUM(m.opportunities)                    AS opportunities_raw,  -- cross-day double-count; reference only
  ROUND(SUM(m.unique_opportunities) * 1000.0 / NULLIF(SUM(m.sent), 0), 3) AS opp_per_1k,
  ROUND(SUM(m.unique_replies)       * 1000.0 / NULLIF(SUM(m.sent), 0), 2) AS reply_per_1k
FROM latest_m m
LEFT JOIN latest_c c ON c.campaign_id = m.campaign_id
GROUP BY
  date_trunc('week', m.date)::DATE, m.campaign_id, c.name, c.cm_name,
  c.product, c.infra_type, c.workspace_id, c.workspace_name;
