-- @gate: add
-- Depends on 1101
-- Depends on 1102
-- Depends on 1103
-- Depends on 1105
-- Depends on 1106
-- ============================================================================
-- 1109_v_kpi_weekly_corrected.sql — derived.v_kpi_weekly_corrected: the corrected
-- weekly funding emails-per-meeting series (Ido's P0) as a LIVE view, reproducing
-- deliverables/2026-07-14-cold-email-bi/kpi-corrected-weekly.csv so the portal /
-- lens-KPI can read it nightly instead of a frozen CSV.
--
-- METHOD (kpi-corrected-weekly.md, Sam-confirmed 2026-07-14 "all good"):
-- calendar weeks Mon–Sun by event date; sends are FUNDING-SCOPE ONLY
-- (core.campaign_offer_scope); sends_corrected_stitched = sum of 3 components + band:
--   1. PURE-funding LIVE workspaces (workspace_alias_unified: funding_relevant='y',
--      status_class='live'): workspace-day MAX-stitch (core.v_sends_truth_daily)
--      MINUS the few out-of-scope campaigns' sends inside them (stitched campaign-day;
--      the CSV used API-side only — deviation, measured ~0 in-window ("trivial")).
--   2. MIXED live workspaces (funding_relevant='mixed': the-gatekeepers, the-eagles):
--      campaign-day MAX-stitch over funding-classified campaigns only (never guess a
--      ws-grain funding share).
--   3. DELETED workspaces + NULL-ws orphans: warehouse campaign-day, funding-classified
--      — a FLOOR (dead workspaces cannot be parity-checked; live ones showed ~40%
--      frozen-era undersync).
--   band: uncertainty_mixed_uncovered = mixed ws-day truth minus campaign-attributable
--      total (sends whose offer is unknowable) + UNRESOLVED-scope campaign sends there.
--
-- MEETINGS (denominator v2 = the truth-reconciled series):
--   weeks < 2026-06-01: core.v_meeting_truth era 'slack_era_portal' (portal rebuild,
--     entities/meeting_rebuilt.py) — email + funding + ours.
--   weeks >= 2026-06-01: core.v_meeting_truth post-cutover era (core.meeting sheet era,
--     verified superset of the portal there).
--   epm_old keeps the LEGACY lineage: warehouse-only sends ÷ legacy core.meeting email
--   count (the pre-correction view of the world — what any pre-backfill analysis showed).
--
-- HONESTY COLUMNS (charter §5 caveat): active_days_in_week (weekend-sends caveat — a
-- day is active if it carries ≥5% of the week's max day; recent weeks are 5-day),
-- meetings_source, is_partial_week, correction_factor, upper band.
--
-- EXPECTED VALUES (validation vs kpi-corrected-weekly.csv, on the analysis snapshot):
--   wk 2026-02-09: sends_corrected_stitched ≈ 2,293,915 (old 448,421; factor ~5.12) ·
--   wk 2026-05-04: ≈ 13,791,614, meetings_email_v2 = 551 · wk 2026-07-06: ≈ 12,621,962,
--   meetings 1,020, epm_corrected ≈ 12,374. Live values drift as sources resync —
--   week-over-week shape must match the CSV, not byte-identical totals.
--   NOTE: history tables (1101) load on the first nightly after ship; until then this
--   view serves the pre-correction (live-sync + warehouse) values.
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS derived;

CREATE OR REPLACE VIEW derived.v_kpi_weekly_corrected AS
WITH ws_class AS (  -- one row per canonical slug
  SELECT warehouse_slug,
         max(status_class)     AS status_class,
         max(funding_relevant) AS funding_relevant
  FROM core.workspace_alias_unified
  WHERE alias_kind = 'warehouse_slug'
  GROUP BY 1
),
pure_ws  AS (SELECT warehouse_slug FROM ws_class WHERE funding_relevant = 'y'     AND status_class = 'live'),
mixed_ws AS (SELECT warehouse_slug FROM ws_class WHERE funding_relevant = 'mixed' AND status_class = 'live'),
-- campaign-day stitch + scope + workspace class
cd AS (
  SELECT t.campaign_id, t.date,
         CAST(date_trunc('week', t.date) AS DATE) AS week_start,
         t.workspace_slug,
         t.sent_stitched, t.sends_api_truth, t.sends_warehouse,
         s.in_funding_scope,
         (p.warehouse_slug IS NOT NULL) AS is_pure_ws,
         (x.warehouse_slug IS NOT NULL) AS is_mixed_ws
  FROM core.v_campaign_sends_truth_daily t
  LEFT JOIN core.campaign_offer_scope s ON s.campaign_id = t.campaign_id
  LEFT JOIN pure_ws  p ON p.warehouse_slug = t.workspace_slug
  LEFT JOIN mixed_ws x ON x.warehouse_slug = t.workspace_slug
),
-- workspace-day stitch for live funding workspaces
wsd AS (
  SELECT w.workspace_slug, w.date,
         CAST(date_trunc('week', w.date) AS DATE) AS week_start,
         w.sent_stitched, w.sends_api_truth,
         (p.warehouse_slug IS NOT NULL) AS is_pure_ws,
         (x.warehouse_slug IS NOT NULL) AS is_mixed_ws
  FROM core.v_sends_truth_daily w
  LEFT JOIN pure_ws  p ON p.warehouse_slug = w.workspace_slug
  LEFT JOIN mixed_ws x ON x.warehouse_slug = w.workspace_slug
),
comp_pure AS (  -- component 1: ws-day stitch minus out-of-scope campaigns inside pure ws
  SELECT week_start, sum(sent_stitched) AS pure_gross, sum(COALESCE(sends_api_truth, 0)) AS pure_api
  FROM wsd WHERE is_pure_ws GROUP BY 1
),
comp_pure_minus AS (
  SELECT week_start, sum(sent_stitched) AS out_of_scope_in_pure
  FROM cd WHERE is_pure_ws AND in_funding_scope = FALSE GROUP BY 1
),
comp_mixed AS (  -- component 2: funding campaigns inside mixed live ws
  SELECT week_start,
         sum(CASE WHEN in_funding_scope THEN sent_stitched ELSE 0 END)            AS mixed_funding,
         sum(CASE WHEN in_funding_scope THEN COALESCE(sends_api_truth, 0) END)    AS mixed_funding_api,
         sum(CASE WHEN in_funding_scope IS NULL THEN sent_stitched ELSE 0 END)    AS mixed_unresolved,
         sum(sent_stitched)                                                       AS mixed_attributable_all
  FROM cd WHERE is_mixed_ws GROUP BY 1
),
comp_mixed_wsday AS (
  SELECT week_start, sum(sent_stitched) AS mixed_ws_truth FROM wsd WHERE is_mixed_ws GROUP BY 1
),
comp_deleted AS (  -- component 3: warehouse floor for deleted ws + NULL-ws orphans
  SELECT week_start, sum(COALESCE(sends_warehouse, 0)) AS deleted_wh_floor
  FROM cd
  WHERE NOT is_pure_ws AND NOT is_mixed_ws AND in_funding_scope = TRUE
  GROUP BY 1
),
old_wh AS (  -- what a pre-backfill analysis would have used (funding scope, warehouse only)
  SELECT week_start, sum(COALESCE(sends_warehouse, 0)) AS sends_warehouse_old
  FROM cd WHERE in_funding_scope = TRUE GROUP BY 1
),
active_days AS (  -- weekend caveat: a day is active if ≥5% of the week's max day
  SELECT week_start, count(*) AS active_days_in_week
  FROM (
    SELECT week_start, date, sum(sent_stitched) AS day_sent,
           max(sum(sent_stitched)) OVER (PARTITION BY week_start) AS max_day
    FROM wsd WHERE is_pure_ws OR is_mixed_ws
    GROUP BY 1, 2
  ) d
  WHERE day_sent >= 0.05 * max_day AND day_sent > 0
  GROUP BY 1
),
meetings_v2 AS (  -- truth denominator: email + funding + ours (era-correct source)
  SELECT CAST(date_trunc('week', meeting_date) AS DATE) AS week_start, count(*) AS meetings_email_v2
  FROM core.v_meeting_truth
  WHERE channel_norm = 'Email' AND is_ours AND COALESCE(in_funding_scope, TRUE)
  GROUP BY 1
),
meetings_legacy AS (  -- the OLD core.meeting email rule (lineage for epm_old)
  SELECT CAST(date_trunc('week', COALESCE(meeting_date, CAST(posted_at AS DATE))) AS DATE) AS week_start,
         count(*) AS meetings_email_legacy
  FROM core.meeting m
  WHERE (m.source = 'sheet' AND m.channel = 'Email')
     OR (m.source <> 'sheet'
         AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw, '') || ' ' || COALESCE(m.raw_text, '')),
                                'sendivo|\bsms\b|whatsapp|iskra'))
  GROUP BY 1
),
weeks AS (
  SELECT week_start FROM comp_pure
  UNION SELECT week_start FROM comp_mixed
  UNION SELECT week_start FROM comp_deleted
  UNION SELECT week_start FROM meetings_v2
),
maxdate AS (SELECT max(date) AS max_d FROM wsd)
SELECT
  w.week_start,
  COALESCE(ad.active_days_in_week, 0)                                       AS active_days_in_week,
  COALESCE(o.sends_warehouse_old, 0)                                        AS sends_warehouse_old,
  COALESCE(cp.pure_api, 0) - COALESCE(cpm_api.api_out, 0)
    + COALESCE(cm.mixed_funding_api, 0)                                     AS sends_api_truth,
  COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)
    + COALESCE(cm.mixed_funding, 0) + COALESCE(cdel.deleted_wh_floor, 0)    AS sends_corrected_stitched,
  COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)
    + COALESCE(cm.mixed_funding, 0) + COALESCE(cdel.deleted_wh_floor, 0)
    + GREATEST(COALESCE(cmw.mixed_ws_truth, 0) - COALESCE(cm.mixed_attributable_all, 0), 0)
    + COALESCE(cm.mixed_unresolved, 0)                                      AS sends_corrected_upper,
  COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)        AS comp_pure_live_stitched,
  COALESCE(cm.mixed_funding, 0)                                             AS comp_mixed_live_stitched,
  COALESCE(cdel.deleted_wh_floor, 0)                                        AS comp_deleted_wh_floor,
  GREATEST(COALESCE(cmw.mixed_ws_truth, 0) - COALESCE(cm.mixed_attributable_all, 0), 0)
    + COALESCE(cm.mixed_unresolved, 0)                                      AS uncertainty_mixed_uncovered,
  CAST(
    (COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)
     + COALESCE(cm.mixed_funding, 0) + COALESCE(cdel.deleted_wh_floor, 0)) AS DOUBLE)
    / NULLIF(o.sends_warehouse_old, 0)                                      AS correction_factor,
  m2.meetings_email_v2,
  ml.meetings_email_legacy,
  CASE WHEN w.week_start < DATE '2026-06-01'
       THEN 'portal_recon_v2 (core.meeting_rebuilt, slack era)'
       ELSE 'core.meeting sheet era (portal-verified superset)' END          AS meetings_source,
  CAST(COALESCE(o.sends_warehouse_old, 0) AS DOUBLE)
    / NULLIF(ml.meetings_email_legacy, 0)                                    AS epm_old,
  CAST(
    (COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)
     + COALESCE(cm.mixed_funding, 0) + COALESCE(cdel.deleted_wh_floor, 0)) AS DOUBLE)
    / NULLIF(m2.meetings_email_v2, 0)                                        AS epm_corrected,
  CAST(
    (COALESCE(cp.pure_gross, 0) - COALESCE(cpm.out_of_scope_in_pure, 0)
     + COALESCE(cm.mixed_funding, 0) + COALESCE(cdel.deleted_wh_floor, 0)
     + GREATEST(COALESCE(cmw.mixed_ws_truth, 0) - COALESCE(cm.mixed_attributable_all, 0), 0)
     + COALESCE(cm.mixed_unresolved, 0)) AS DOUBLE)
    / NULLIF(m2.meetings_email_v2, 0)                                        AS epm_corrected_upper,
  (w.week_start + INTERVAL 6 DAY >= (SELECT max_d FROM maxdate))             AS is_partial_week
FROM weeks w
LEFT JOIN comp_pure        cp  ON cp.week_start  = w.week_start
LEFT JOIN comp_pure_minus  cpm ON cpm.week_start = w.week_start
LEFT JOIN (SELECT week_start, sum(COALESCE(sends_api_truth, 0)) AS api_out
           FROM cd WHERE is_pure_ws AND in_funding_scope = FALSE GROUP BY 1) cpm_api
       ON cpm_api.week_start = w.week_start
LEFT JOIN comp_mixed       cm  ON cm.week_start  = w.week_start
LEFT JOIN comp_mixed_wsday cmw ON cmw.week_start = w.week_start
LEFT JOIN comp_deleted     cdel ON cdel.week_start = w.week_start
LEFT JOIN old_wh           o   ON o.week_start   = w.week_start
LEFT JOIN active_days      ad  ON ad.week_start  = w.week_start
LEFT JOIN meetings_v2      m2  ON m2.week_start  = w.week_start
LEFT JOIN meetings_legacy  ml  ON ml.week_start  = w.week_start;
