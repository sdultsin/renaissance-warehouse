-- @gate: add
-- Depends on: 33 (v_campaign_metrics), 1071 (core.campaign_infra)
-- v_campaign_scoreboard — THE standing campaign scoreboard. Version 1074. [2026-07-03]
--
-- WHY (TKT-2, handoffs/2026-07-01-campaign-scoreboard-standing-view.md — Sam:
--   "Need to start tracking, not so it's figured out impromptu every time, for each
--   campaign… sending infra… rigorously tracked moving forward"):
--   the scoreboard existed only as an ad-hoc proof query. Every consumer should get
--   the full campaign truth — performance + meetings + sending infra + recipient ESP
--   — with SELECT * FROM v_campaign_scoreboard.
--
-- FIX: one row per campaign in v_campaign_metrics (ALL campaigns incl. deleted —
--   raw_pipeline_campaigns is the superset base) extended with:
--   * workspace   — canonical core.workspace.name, NEVER the stale raw
--                   workspace_name codenames. v_campaign_metrics.workspace_id IS the
--                   slug for 3,043/3,048 campaigns; 4 legacy values are display names
--                   (name-join fallback resolves 2: 'Renaissance 7', 'The Eagles');
--                   'Outlook 1' / 'Renaissance 2' resolve to neither → raw value
--                   passes through, workspace_slug NULL (3 campaigns).
--   * is_dfy      — workspace_slug = 'renaissance-1' (Instantly DFY; excluded from
--                   CM rollups but visible).
--   * meetings    — core.meeting rows with matching campaign_id AND
--                   is_duplicate_of IS NULL (the canonical meeting source; never
--                   core.meeting_campaign_attribution).
--   * kpi         — sent / meetings (emails per meeting, THE campaign KPI);
--     opp_to_meeting_pct — 100 * meetings / opps; eop — email_per_opp passthrough.
--     Ratios are NULL when the denominator is 0 (never 0/∞).
--   * human_replies = Instantly-native unique_replies (reply_count is HUMAN,
--     excl. auto — reference_instantly_reply_count_is_human_not_total_20260630);
--     auto_replies passthrough.
--   * infra_*     — LEFT JOIN core.campaign_infra (DDL 1071 registry, populated by
--                   entities/campaign_infra.py): infra_vendor/esp, mixed_infra,
--                   derivation_source, recipient_esp(+share+sends), lifecycle.
--   * first/last_send_date — raw_pipeline_campaign_daily_metrics (sent > 0 days).
--   * flags       — meetings_gt_opps (meetings > opps AND opps NOT NULL: the
--                   BEN-CHEAP over-attribution class stays visible),
--                   infra_unknown (no registry row or vendor 'unknown').
--   segment/cm_name pass through as name-parsed hints (descoped by Sam: do not
--   improve). Opps stay cumulative-as-of-snapshot (v_campaign_metrics semantics).
--
-- GRAIN CHECK + verified read-only on serving snapshot
-- warehouse_20260703_043558_874.duckdb (exact view body, registry stubbed empty —
-- core.campaign_infra doesn't exist on serving until 1071 applies):
--   rows_total 3,048 == count(DISTINCT campaign_id) 3,048 (one row per campaign;
--   both core.workspace joins are unique: 23 rows, 23 distinct slugs, 23 names).
--   meetings joined 12,072 (of 12,081 attributed non-dup; 9 sit on campaign_ids
--   outside the v_campaign_metrics base) across 1,329 campaigns; kpi non-NULL
--   1,323; opp_to_meeting_pct non-NULL 631; meetings_gt_opps 28; is_dfy 123;
--   workspace_slug NULL 3.

CREATE OR REPLACE VIEW v_campaign_scoreboard AS
WITH mtg AS (
  SELECT campaign_id, count(*) AS meetings
  FROM core.meeting
  WHERE campaign_id IS NOT NULL AND is_duplicate_of IS NULL
  GROUP BY campaign_id
),
send_dates AS (
  SELECT campaign_id, min(date) AS first_send_date, max(date) AS last_send_date
  FROM raw_pipeline_campaign_daily_metrics
  WHERE sent > 0
  GROUP BY campaign_id
)
SELECT
  m.campaign_id,
  m.campaign_name                                          AS name,
  COALESCE(ws.name, wn.name, m.workspace_id)               AS workspace,
  COALESCE(ws.slug, wn.slug, ci.workspace_slug)            AS workspace_slug,
  COALESCE(COALESCE(ws.slug, wn.slug, ci.workspace_slug) = 'renaissance-1',
           false)                                          AS is_dfy,
  m.cm_name,
  m.segment,
  m.status,
  m.sent,
  m.unique_replies                                         AS human_replies,
  m.auto_replies,
  m.opportunities                                          AS opps,
  m.email_per_opp                                          AS eop,
  COALESCE(mt.meetings, 0)                                 AS meetings,
  CASE WHEN COALESCE(mt.meetings, 0) > 0
       THEN round(m.sent::DOUBLE / mt.meetings, 0) END     AS kpi,
  CASE WHEN m.opportunities > 0
       THEN round(100.0 * COALESCE(mt.meetings, 0)
                  / m.opportunities, 1) END                AS opp_to_meeting_pct,
  COALESCE(ci.infra_vendor, 'unknown')                     AS infra_vendor,
  COALESCE(ci.infra_esp, 'unknown')                        AS infra_esp,
  ci.mixed_infra,
  ci.derivation_source,
  COALESCE(ci.recipient_esp, 'unknown')                    AS recipient_esp,
  ci.recipient_esp_share,
  ci.recipient_sends_total,
  sd.first_send_date,
  sd.last_send_date,
  ci.first_seen_at,
  ci.last_seen_live_at,
  (m.opportunities IS NOT NULL
   AND COALESCE(mt.meetings, 0) > m.opportunities)         AS meetings_gt_opps,
  (ci.campaign_id IS NULL
   OR COALESCE(ci.infra_vendor, 'unknown') = 'unknown')    AS infra_unknown
FROM v_campaign_metrics m
LEFT JOIN core.workspace ws       ON ws.slug = m.workspace_id
LEFT JOIN core.workspace wn       ON wn.name = m.workspace_id
LEFT JOIN core.campaign_infra ci  ON ci.campaign_id = m.campaign_id
LEFT JOIN mtg mt                  ON mt.campaign_id = m.campaign_id
LEFT JOIN send_dates sd           ON sd.campaign_id = m.campaign_id;
