-- @gate: add
-- Depends on 104
-- ============================================================================
-- 109_ws7_campaign_offer.sql  — WS7 Email offer split (RENUMBERED from 108).
-- Canonical per-campaign offer + provenance on core.campaign, the LLM cache,
-- and the per-offer performance views the Email-Performance dashboard reads.
-- Gated through the Schema-Moderator. Idempotent; safe to re-run.
--
-- DDL version 109 (RECONCILED-DEPLOY-PLAN §3, order 5).
-- Live MAX(core.schema_version)=104 at staging time (2026-06-21, snapshot
-- warehouse_20260621_063139_227.duckdb). 105-108 are the rest of this batch
-- (WS3/WS4/WS5/WS6); 109 is the WS7 slot. The original v2 design hardcoded 108
-- (when WS1 was assumed at 102) — re-numbered +1 because WS1=103, WS2=104 landed.
-- The moderator re-runs SELECT max(version) FROM core.schema_version immediately
-- before apply and bumps the whole remaining block by any delta if the nightly
-- moved the floor. apply_ddl_file PK-dedupes on version, so a taken version would
-- SILENTLY no-op the WHOLE migration — 109 verified free (0 rows) this session.
--
-- WS7 touches OFFER surfaces only; NO core.sending_account / account-state edit
-- (no merge folded in for this unit, per the reconciled plan — WS7 is DEPLOY-AS-IS).
-- ============================================================================

-- (a) Provenance column on the canonical campaign table (offer already exists).
ALTER TABLE core.campaign ADD COLUMN IF NOT EXISTS offer_source VARCHAR;
--   values: 'workspace_map' | 'name_regex' | 'llm_copy' | 'unresolved'
--   (no 'excluded_warmleads' — warm-leads campaigns are not in core.campaign)

COMMENT ON COLUMN core.campaign.offer IS
  'Canonical offer label: Business Funding | R&D Credit | Pre-IPO | Section 125 | Tariffs. '
  'Resolved 3-pass: workspace_map -> name_regex -> llm_copy (cached). Warm-leads excluded '
  '(not in core.campaign). Section 125 + Tariffs are FROZEN labels; their date-gating is '
  'deferred to WS9 (no campaign_daily history exists yet). READ THIS, never re-derive. WS7 2026-06-20.';

-- (b) LLM-on-copy cache (idempotency; re-run LLM only when copy changes).
CREATE TABLE IF NOT EXISTS core.campaign_offer_llm_cache (
  campaign_id    VARCHAR     NOT NULL,
  content_hash   VARCHAR     NOT NULL,   -- md5(name || char(10) || parsed_body_text), computed in generator
  offer          VARCHAR,                -- one of the 5, or NULL when model says __ambiguous__
  model          VARCHAR     NOT NULL,
  classified_at  TIMESTAMP WITH TIME ZONE NOT NULL,
  PRIMARY KEY (campaign_id, content_hash)
);

-- (c) Offer dimension. is_frozen carried; frozen DATE-GATING is DEFERRED to WS9
--     (Section 125 + Tariffs have zero campaign_daily rows -> no window to source).
--     active_from/to are honest MIN/MAX over whatever campaign_daily exists; for frozen
--     offers they are NULL today and frozen_window_source flags the deferral so the
--     dashboard HIDES is_frozen offers rather than rendering a dead NULL-bounds gate.
CREATE OR REPLACE VIEW core.v_offer_dim AS
SELECT
  c.offer,
  (c.offer IN ('Section 125','Tariffs'))                         AS is_frozen,
  CASE WHEN c.offer IN ('Section 125','Tariffs') THEN 'deferred_ws9'
       ELSE 'campaign_daily' END                                 AS frozen_window_source,
  MIN(cd.date)                                                   AS active_from,
  MAX(cd.date)                                                   AS active_to,
  COUNT(DISTINCT c.campaign_id)                                  AS campaigns,
  COALESCE(SUM(cd.sent),0)                                       AS sent_lifetime
FROM core.campaign c
LEFT JOIN core.campaign_daily cd USING (campaign_id)
WHERE c.offer IS NOT NULL
GROUP BY 1,2,3;

-- (d) Per-(offer, date) performance the dashboard reads. Warm-leads excluded by the
--     JOIN to core.campaign (its campaigns are absent). NEVER join on slug.
--     LIVE offers only flow to the dashboard split; frozen offers (is_frozen) are
--     hidden by the dashboard until WS9 lands their history (see read contract).
CREATE OR REPLACE VIEW core.v_offer_perf_daily AS
SELECT
  c.offer,
  (c.offer IN ('Section 125','Tariffs')) AS is_frozen,
  cd.date,
  SUM(cd.sent)            AS sent,
  SUM(cd.opportunities)   AS opportunities,
  SUM(cd.meetings_booked) AS meetings,
  SUM(cd.replies_human)   AS replies_human,
  SUM(cd.bounces)         AS bounces
FROM core.campaign_daily cd
JOIN core.campaign c ON c.campaign_id = cd.campaign_id   -- excludes warm-leads (unjoined)
WHERE c.offer IS NOT NULL                                -- drop unresolved
GROUP BY 1,2,3;

-- (e) 30-day offer rollup, SAME KPI math as v_workspace_perf_30d so the by-offer tab
--     reconciles definitionally to the by-workspace tab.
CREATE OR REPLACE VIEW core.v_offer_perf_30d AS
WITH win AS (SELECT MAX(date) AS w_end, (MAX(date) - 29) AS w_start FROM core.campaign_daily),
agg AS (
  SELECT p.offer, p.is_frozen,
         SUM(p.sent) AS sent, SUM(p.opportunities) AS opportunities,
         SUM(p.meetings) AS meetings, SUM(p.replies_human) AS replies_human
  FROM core.v_offer_perf_daily p, win
  WHERE p.date BETWEEN win.w_start AND win.w_end
  GROUP BY 1,2)
SELECT a.offer, a.is_frozen,
       (SELECT w_start FROM win) AS window_start,
       (SELECT w_end   FROM win) AS window_end,
       a.sent, a.opportunities, a.meetings, a.replies_human,
       ROUND(CAST(a.sent AS DOUBLE) / NULLIF(a.opportunities,0), 0) AS eop_windowed,
       ROUND(CAST(a.sent AS DOUBLE) / NULLIF(a.meetings,0),      0) AS kpi_emails_per_meeting
FROM agg a
ORDER BY a.sent DESC NULLS LAST;

-- ----------------------------------------------------------------------------
-- (f) Rewire core.v_campaign_metrics.offer to the canonical column.
--     CURRENT (live) line: `pc.product AS offer` (raw_pipeline_campaigns.product,
--     poisoned/FUNDING-collapsed). Change to prefer the resolved canonical offer:
--       LEFT JOIN core.campaign cc ON cc.campaign_id = pc.campaign_id
--       ... COALESCE(cc.offer, pc.product) AS offer
--     This file does NOT re-emit the whole 33_campaign_metrics_view.sql body — that
--     edit is applied to sql/ddl/33_campaign_metrics_view.sql in the same gate (see
--     REVIEW.md "Generator / view edits"). Kept as a documented coupled edit so the
--     dashboard's metric-source view reads canonical offer, not raw product.
-- ----------------------------------------------------------------------------

-- NOTE: the legacy-token migration (Funding->Business Funding, R&D->R&D Credit,
-- s125->Section 125) runs in the GENERATOR (entities/campaign.py
-- _resolve_campaign_offer), idempotently, as the SOLE writer of core.campaign.offer.
-- It is intentionally NOT a one-shot UPDATE in this DDL so re-runs stay deterministic
-- and the generator remains the single source of the offer column.
