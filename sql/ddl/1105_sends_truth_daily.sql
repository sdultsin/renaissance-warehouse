-- @gate: add
-- Depends on 1101
-- Depends on 1102
-- ============================================================================
-- 1105_sends_truth_daily.sql — the day-grain MAX-stitch "sends truth" views.
--
-- WHY (kpi-corrected-weekly.md Defect E): Instantly's analytics API retroactively loses
-- whole days (campaign 3a4f57f4-2114… on 2026-05-07: API sent=0 vs warehouse frozen capture
-- 25,197 / 212 replies, adjacent days exact) while the warehouse frozen region (≤2026-05-12)
-- is missing up to ~89% of Feb→mid-Mar sends. NEITHER source is a superset ⇒ the only honest
-- day-grain reconstruction is GREATEST() across (a) the frozen API history escrow (DDL 1101),
-- (b) the live nightly API sync, (c) the warehouse pipeline fact. Lineage columns keep every
-- side visible (charter rule: keep sends_api_truth for lineage; use *_stitched).
--
-- HYGIENE baked in: the 8 synthetic '__ledger_recon__<slug>' campaign_ids (88 fact rows,
-- 66,209 fake sends — funding-scope-summary defect 2) are FILTERED here (consumer-side
-- eviction; raw rows untouched — never a raw deletion).
--
-- COVERAGE: API side = live-key workspaces only (see 1101 header); deleted workspaces are
-- warehouse-only rows (row_source='warehouse_only'). prospects-power campaign-grain API
-- history is partial (ws-grain complete). History tables are EMPTY until the first nightly
-- after ship runs entities/mof_bi_history.py — until then these views degrade gracefully to
-- live-sync + warehouse values.
--
-- Reversible: DROP VIEW core.v_campaign_sends_truth_daily / core.v_sends_truth_daily.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ── campaign × day ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_campaign_sends_truth_daily AS
WITH api_all AS (
  SELECT campaign_id, workspace_slug, date, sent, unique_replies,
         unique_replies_automatic, unique_opportunities
  FROM main.raw_instantly_campaign_daily_history
  UNION ALL
  SELECT campaign_id, workspace_slug, date, sent, unique_replies,
         unique_replies_automatic, unique_opportunities
  FROM main.raw_instantly_campaign_analytics_daily
),
api AS (  -- collapse escrow + live sync to one API row per campaign-day (max = settled)
  SELECT campaign_id, date,
         max(sent)                     AS sent_api,
         max(unique_replies)           AS replies_human_api,
         max(unique_replies_automatic) AS replies_auto_api,
         max(unique_opportunities)     AS opps_api,
         max(workspace_slug)           AS workspace_slug_api
  FROM api_all
  GROUP BY 1, 2
),
wh AS (
  SELECT campaign_id, date,
         sent                     AS sent_wh,
         unique_replies           AS replies_human_wh,
         unique_replies_automatic AS replies_auto_wh,
         unique_opportunities     AS opps_wh,
         workspace_id             AS workspace_raw
  FROM main.raw_pipeline_campaign_daily_metrics
  WHERE NOT contains(campaign_id, '__ledger_recon__')
)
SELECT
  COALESCE(a.campaign_id, w.campaign_id)                          AS campaign_id,
  COALESCE(a.date, w.date)                                        AS date,
  COALESCE(a.workspace_slug_api, wn.warehouse_slug, w.workspace_raw) AS workspace_slug,
  GREATEST(COALESCE(a.sent_api, 0),          COALESCE(w.sent_wh, 0))          AS sent_stitched,
  GREATEST(COALESCE(a.replies_human_api, 0), COALESCE(w.replies_human_wh, 0)) AS replies_human_stitched,
  GREATEST(COALESCE(a.replies_auto_api, 0),  COALESCE(w.replies_auto_wh, 0))  AS replies_auto_stitched,
  GREATEST(COALESCE(a.opps_api, 0),          COALESCE(w.opps_wh, 0))          AS opps_stitched,
  a.sent_api  AS sends_api_truth,   -- lineage; NULL/0 on API-lost days (Defect E) — never read alone
  w.sent_wh   AS sends_warehouse,   -- lineage; frozen-region holes — never read alone
  CASE WHEN a.campaign_id IS NULL THEN 'warehouse_only'
       WHEN w.campaign_id IS NULL THEN 'api_only'
       WHEN COALESCE(a.sent_api, 0) >= COALESCE(w.sent_wh, 0) THEN 'both_api_ge_wh'
       ELSE 'both_wh_gt_api'        -- Defect-E class: warehouse fills an API hole
  END AS row_source
FROM api a
FULL JOIN wh w ON a.campaign_id = w.campaign_id AND a.date = w.date
LEFT JOIN core.v_workspace_slug_norm wn ON wn.alias_lower = lower(w.workspace_raw);

-- ── workspace × day ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_sends_truth_daily AS
WITH api_ws_all AS (
  SELECT workspace_slug, date, sent, unique_replies, unique_replies_automatic,
         unique_opportunities
  FROM main.raw_instantly_ws_daily_history
  UNION ALL
  SELECT workspace_slug, date, sent, unique_replies, unique_replies_automatic,
         unique_opportunities
  FROM main.raw_instantly_workspace_analytics_daily
),
api_ws AS (
  SELECT workspace_slug, date,
         max(sent)                     AS sent_api,
         max(unique_replies)           AS replies_human_api,
         max(unique_replies_automatic) AS replies_auto_api,
         max(unique_opportunities)     AS opps_api
  FROM api_ws_all
  GROUP BY 1, 2
),
wh_ws AS (  -- pipeline fact rolled to workspace-day on the NORMALIZED slug (1102)
  SELECT COALESCE(wn.warehouse_slug, f.workspace_id, '(null-workspace)') AS workspace_slug,
         f.date,
         sum(f.sent)                     AS sent_wh,
         sum(f.unique_replies)           AS replies_human_wh,
         sum(f.unique_replies_automatic) AS replies_auto_wh,
         sum(f.unique_opportunities)     AS opps_wh
  FROM main.raw_pipeline_campaign_daily_metrics f
  LEFT JOIN core.v_workspace_slug_norm wn ON wn.alias_lower = lower(f.workspace_id)
  WHERE NOT contains(f.campaign_id, '__ledger_recon__')
  GROUP BY 1, 2
)
SELECT
  COALESCE(a.workspace_slug, w.workspace_slug) AS workspace_slug,
  COALESCE(a.date, w.date)                     AS date,
  GREATEST(COALESCE(a.sent_api, 0),          COALESCE(w.sent_wh, 0))          AS sent_stitched,
  GREATEST(COALESCE(a.replies_human_api, 0), COALESCE(w.replies_human_wh, 0)) AS replies_human_stitched,
  GREATEST(COALESCE(a.replies_auto_api, 0),  COALESCE(w.replies_auto_wh, 0))  AS replies_auto_stitched,
  GREATEST(COALESCE(a.opps_api, 0),          COALESCE(w.opps_wh, 0))          AS opps_stitched,
  a.sent_api AS sends_api_truth,
  w.sent_wh  AS sends_warehouse,
  CASE WHEN a.workspace_slug IS NULL THEN 'warehouse_only'
       WHEN w.workspace_slug IS NULL THEN 'api_only'
       WHEN COALESCE(a.sent_api, 0) >= COALESCE(w.sent_wh, 0) THEN 'both_api_ge_wh'
       ELSE 'both_wh_gt_api'
  END AS row_source
FROM api_ws a
FULL JOIN wh_ws w ON a.workspace_slug = w.workspace_slug AND a.date = w.date;
