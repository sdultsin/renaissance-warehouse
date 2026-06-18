-- 81_workspace_performance.sql  [2026-06-18]  data-accuracy-fix chat (handoff B1-B4,B6,B10 at source).
-- Applied via apply_ddl_file(version=81). Idempotent CREATE OR REPLACE views only — no table writes.
--
-- Canonical PER-WORKSPACE email-performance serving layer. Fixes the per-workspace data-accuracy
-- bugs at the source so any dashboard/doc/query gets trustworthy sent/opps/meetings/EOP/KPI/opp-rate
-- per workspace WITHOUT hand-relabeling or silent undercounting:
--   B3  current Instantly workspace names (join core.workspace on slug; NEVER the stale stored
--       raw_pipeline_campaigns.workspace_name codenames). 5 unkeyed slugs get a curated override.
--   B4  NULL workspace_id in raw_pipeline_campaign_daily_metrics (~5% of rows) recovered by joining
--       the COMPLETE campaigns dim on campaign_id (the dim has 0 null workspace), never GROUP BY the
--       fact's own NULL column.
--   B2  meetings with NULL campaign_id (unattributable) surface as an explicit '(unattributed)'
--       workspace bucket so Σ per-workspace meetings + (unattributed) = the core.meeting email total
--       EXACTLY (the reconciliation identity). Per-workspace meeting counts are honest floors, never
--       implied complete. (The sheet-era residual is driven to ~0.5% by the email-reply campaign_id
--       backfill in entities/meeting.py; the pre-cutover Slack residual is irreducible -> bucket.)
--   B1  meetings come ONLY from core.meeting (channel-aware email filter, byte-identical to
--       v_kpi_email / derived.v_funnel) — never meetings_booked_raw.
--   B6  opps exposed BOTH ways, labeled: opps_windowed_trend (per-day-distinct sum, a TREND that
--       overcounts the cumulative truth ~30-56%) AND opps_cumulative_lifetime (exact, deduped, from
--       v_campaign_metrics — lifetime, NOT windowable).
--   B10 the rollup carries its own window_start/window_end (derived from the fact's max date, which
--       excludes today because sends lag) so date coverage is explicit, never implied.

-- =====================================================================================
-- v_workspace_name — slug -> CURRENT Instantly display name (B3). core.workspace.name is sourced
-- from the live per-workspace Instantly API keys, so it is the current name (e.g. renaissance-4 ->
-- 'Funding 1 (Samuel)'). 5 slugs appear in campaign data but have no Instantly key / core.workspace
-- row (warm-leads, the-dyad, outlook-2, renaissance-6/7) — give them a curated name so they RENDER
-- instead of collapsing into a "(various)" bucket (A2). Last-resort fallback = the slug itself.
-- =====================================================================================
CREATE OR REPLACE VIEW main.v_workspace_name AS
WITH override(slug, name) AS (
  VALUES ('warm-leads',   'Warm leads'),
         ('the-dyad',     'The Dyad'),
         ('outlook-2',    'Outlook 2'),
         ('renaissance-6','Renaissance 6'),
         ('renaissance-7','Renaissance 7')
),
slugs AS (
  SELECT DISTINCT workspace_id AS slug
  FROM main.raw_pipeline_campaigns
  WHERE workspace_id IS NOT NULL AND workspace_id <> ''
)
SELECT s.slug,
       COALESCE(w.name, o.name, s.slug) AS ws_name
FROM slugs s
LEFT JOIN core.workspace w ON w.slug = s.slug
LEFT JOIN override     o ON o.slug = s.slug;

-- =====================================================================================
-- v_workspace_daily — workspace x date primitive. Sum over any window (7d/MTD/30d) downstream.
-- Sends/opps/replies from the complete daily fact; meetings channel-aware from core.meeting with an
-- explicit (unattributed) bucket. workspace_id on BOTH sides resolved via the campaigns dim (B4).
-- =====================================================================================
CREATE OR REPLACE VIEW main.v_workspace_daily AS
WITH dims AS (
  SELECT DISTINCT ON (campaign_id) campaign_id, workspace_id AS slug
  FROM main.raw_pipeline_campaigns
  ORDER BY campaign_id, _loaded_at DESC
),
sends AS (
  SELECT COALESCE(d.slug, NULLIF(cd.workspace_id,'')) AS slug, cd.date,
         sum(cd.sent)                       AS sent,
         sum(cd.unique_opportunities)       AS opps_windowed,
         sum(cd.unique_replies)             AS replies_human,
         sum(cd.unique_replies_automatic)   AS replies_auto
  FROM main.raw_pipeline_campaign_daily_metrics cd
  LEFT JOIN dims d ON d.campaign_id = cd.campaign_id
  GROUP BY 1,2
),
mtg AS (  -- email filter byte-identical to v_kpi_email / derived.v_funnel
  SELECT CAST(m.posted_at AS DATE) AS date, d.slug AS slug, count(*) AS meetings
  FROM core.meeting m
  LEFT JOIN dims d ON d.campaign_id = m.campaign_id
  WHERE (m.source = 'sheet' AND m.channel = 'Email')
     OR (m.source <> 'sheet'
         AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                                'sendivo|\bsms\b|whatsapp|iskra'))
  GROUP BY 1,2
),
spine AS (
  SELECT COALESCE(s.slug, mt.slug) AS slug,
         COALESCE(s.date, mt.date) AS date,
         COALESCE(s.sent,0)            AS sent,
         COALESCE(s.opps_windowed,0)   AS opps_windowed,
         COALESCE(s.replies_human,0)   AS replies_human,
         COALESCE(s.replies_auto,0)    AS replies_auto,
         COALESCE(mt.meetings,0)       AS meetings
  FROM sends s
  FULL JOIN mtg mt
    ON s.slug IS NOT DISTINCT FROM mt.slug AND s.date = mt.date
)
SELECT CASE WHEN sp.slug IS NULL THEN '(unattributed)'
            ELSE COALESCE(wn.ws_name, sp.slug) END AS workspace,
       sp.slug, sp.date, sp.sent, sp.opps_windowed,
       sp.replies_human, sp.replies_auto, sp.meetings
FROM spine sp
LEFT JOIN main.v_workspace_name wn ON wn.slug = sp.slug;

-- =====================================================================================
-- v_workspace_perf_30d — the canonical last-30d per-workspace table (the DoD deliverable). Reconciles:
-- SUM(meetings_attributed) over ALL rows (incl the '(unattributed)' row) = total email meetings in
-- core.meeting for the same window. EOP / KPI / opp-rate are computed off the WINDOWED opp trend and
-- labeled as such; the exact lifetime opp count rides alongside (B6). window_start/end make coverage
-- explicit (B10): the window ends at the fact's max date, which EXCLUDES today (sends lag).
-- =====================================================================================
CREATE OR REPLACE VIEW main.v_workspace_perf_30d AS
WITH win AS (
  SELECT max(date) AS w_end, max(date) - 29 AS w_start
  FROM main.raw_pipeline_campaign_daily_metrics
),
agg AS (
  SELECT vd.workspace, vd.slug,
         sum(vd.sent)          AS sent,
         sum(vd.opps_windowed) AS opps_windowed_trend,
         sum(vd.replies_human) AS replies_human,
         sum(vd.meetings)      AS meetings_attributed
  FROM main.v_workspace_daily vd, win
  WHERE vd.date BETWEEN win.w_start AND win.w_end
  GROUP BY 1,2
),
cumopp AS (  -- exact, deduped lifetime opps per workspace (B6) — lifetime, NOT the 30d window
  SELECT d.slug, sum(vcm.opportunities) AS opps_cumulative_lifetime
  FROM main.v_campaign_metrics vcm
  JOIN (SELECT DISTINCT ON (campaign_id) campaign_id, workspace_id AS slug
        FROM main.raw_pipeline_campaigns ORDER BY campaign_id, _loaded_at DESC) d
    ON d.campaign_id = vcm.campaign_id
  GROUP BY 1
)
SELECT a.workspace, a.slug,
       (SELECT w_start FROM win) AS window_start,
       (SELECT w_end   FROM win) AS window_end,
       a.sent,
       a.opps_windowed_trend,
       c.opps_cumulative_lifetime,
       a.replies_human,
       a.meetings_attributed,
       round(a.sent::double      / NULLIF(a.opps_windowed_trend,0), 0) AS eop_windowed,
       round(a.sent::double      / NULLIF(a.meetings_attributed,0), 0) AS kpi_emails_per_meeting,
       round(a.meetings_attributed::double / NULLIF(a.opps_windowed_trend,0), 3) AS opp_to_meeting_rate_windowed
FROM agg a
LEFT JOIN cumopp c ON c.slug = a.slug
ORDER BY a.sent DESC NULLS LAST;
