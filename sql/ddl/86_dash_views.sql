-- ════════════════════════════════════════════════════════════════════════════
-- 86_dash_views.sql — per-dashboard "consumer contract" views (schema `dash`)
-- ════════════════════════════════════════════════════════════════════════════
-- Built by the `dashboard-views` bus chat [2026-06-19].
-- Handoff:  handoffs/2026-06-19-per-dashboard-views-handoff.md
-- Specs (per-view dependency map + verification, one file per dashboard):
--   <Renaissance>/deliverables/2026-06-19-per-dashboard-views/specs/<slug>.md
--
-- WHY: every portal Lens dashboard is STATIC — it renders a committed
-- data.json/.gz that a warehouse PUBLISHER script (renaissance-warehouse/scripts/*.py)
-- builds by running read-only SQL against the serving snapshot. So the real
-- warehouse CONSUMER for a dashboard is its publisher's source SELECT. Each dash.*
-- view below encapsulates exactly what one dashboard (or one of its tiles/grains)
-- reads = its formal consumer contract + blast-radius for the schema-moderator,
-- and a scoped per-dashboard workbench.
--
-- ALL views are READ-ONLY definitions over EXISTING warehouse objects (no data
-- copied). Every view body was validated against serving snapshot
-- warehouse_20260619_063042 via the read-API and reproduces its dashboard's data
-- (PASS / PASS-WITH-DRIFT — see the per-view spec). Schema-qualified throughout:
--   core.*  = curated tables/views        main.*  = raw_*, v_*, mv_* (default schema)
--
-- A dashboard whose feed is N independent datasets is NOT one clean view; it
-- decomposes into dash.<slug>__<part> views (documented in its spec). Heavy
-- client-side compute (period ratios, daily->weekly rollups, fuzzy meeting
-- attribution) stays in the dashboard/publisher — these views expose the BASE
-- MEASURES at their native grain.
--
-- BLAST-RADIUS NOTE for the schema-moderator: core.meeting, core.sending_account,
-- main.raw_pipeline_campaign_daily_metrics and main.raw_pipeline_campaigns are the
-- highest-fan-out base objects (read by 4-5 dashboards each). See the closeout
-- dependency matrix in the handoff for the full object -> dashboards map.
-- ════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS dash;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: warehouse-overview  ("Renaissance Overview")                    ║
-- ║ Publisher: scripts/portal_data.py  ->  portal_data.js (window.PORTAL_DATA) ║
-- ║ In nightly refresh_portal_feed.sh: YES.  4 part-views (3 grains + ESP).    ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- scalar KPI tile bundle (4 hero tiles + S/B + record day) — exactly ONE row.
-- HYBRID email filter is load-bearing: sheet rows (>=Jun-1) use channel='Email';
-- slack rows use the SMS-exclusion regex. period S/B = round(Σsent ÷ Σbooked).
CREATE OR REPLACE VIEW dash.warehouse_overview__kpi AS
SELECT
  (SELECT COUNT(*) FILTER (WHERE status = 'active')
     FROM core.sending_account)                                   AS active_inboxes,
  (SELECT COUNT(*) FROM core.sending_account
     WHERE status = 'active'
       AND lower(COALESCE(lifecycle_state,'')) LIKE '%warm%'
       AND lower(COALESCE(lifecycle_state,'')) <> 'warmed')        AS warmup_inboxes,
  (SELECT COUNT(*) FROM core.meeting m
     WHERE m.posted_at >= date_trunc('month', current_date)
       AND m.posted_at <  current_date + 1
       AND (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
                 ELSE NOT regexp_matches(
                        lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                        'sendivo|\bsms\b|whatsapp|iskra') END))     AS mtd_meetings,
  (SELECT COUNT(*) FROM core.meeting m
     WHERE (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
                 ELSE NOT regexp_matches(
                        lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                        'sendivo|\bsms\b|whatsapp|iskra') END))     AS all_time_meetings,
  (SELECT SUM(sent) FROM main.raw_pipeline_campaign_daily_metrics
     WHERE date >= date_trunc('month', current_date))             AS mtd_sent,
  round(
    (SELECT SUM(sent) FROM main.raw_pipeline_campaign_daily_metrics
       WHERE date >= date_trunc('month', current_date))::DOUBLE
    / NULLIF((SELECT COUNT(*) FROM core.meeting m
       WHERE m.posted_at >= date_trunc('month', current_date)
         AND m.posted_at <  current_date + 1
         AND (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
                   ELSE NOT regexp_matches(
                          lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                          'sendivo|\bsms\b|whatsapp|iskra') END)),0)) AS mtd_sb_ratio,
  (SELECT posted_at::DATE FROM core.meeting m
     WHERE (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
                 ELSE NOT regexp_matches(
                        lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                        'sendivo|\bsms\b|whatsapp|iskra') END)
     GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1)                    AS record_day,
  (SELECT COUNT(*) FROM core.meeting m
     WHERE (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
                 ELSE NOT regexp_matches(
                        lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                        'sendivo|\bsms\b|whatsapp|iskra') END)
     GROUP BY posted_at::DATE ORDER BY COUNT(*) DESC LIMIT 1)      AS record_day_meetings
;

-- partner leaderboard (MTD + all-time, label-normalized) — 1 row/partner.
CREATE OR REPLACE VIEW dash.warehouse_overview__partners AS
WITH em AS (
  SELECT
    CASE
      WHEN m.partner IN ('GreenBridge','GreenBridge Capital') THEN 'GreenBridge Capital'
      WHEN m.partner IN ('BTC','Big Think Capital')           THEN 'Big Think Capital'
      WHEN m.partner IN ('Qualifi','GoQualifi')               THEN 'GoQualifi'
      WHEN m.partner IN ('Llama','Llama Funding')             THEN 'Llama'
      WHEN m.partner IS NULL OR m.partner = ''                THEN '(unattributed)'
      ELSE m.partner END                                            AS partner,
    (m.posted_at >= date_trunc('month', current_date))              AS is_mtd
  FROM core.meeting m
  WHERE (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
              ELSE NOT regexp_matches(
                     lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                     'sendivo|\bsms\b|whatsapp|iskra') END)
)
SELECT partner,
       COUNT(*) FILTER (WHERE is_mtd) AS mtd_meetings,
       COUNT(*)                       AS all_time_meetings
FROM em
GROUP BY partner
ORDER BY all_time_meetings DESC
;

-- top CMs by all-time email meetings (CM resolved from core.meeting.cm or a trailing
-- "(TOKEN)" in raw_text; kept only if in the raw_pipeline_campaigns roster minus noise).
CREATE OR REPLACE VIEW dash.warehouse_overview__top_cms AS
WITH real_cms AS (
  SELECT DISTINCT UPPER(TRIM(cm_name)) AS cm
  FROM main.raw_pipeline_campaigns
  WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
    AND UPPER(TRIM(cm_name)) NOT IN ('INSTANTLY','INSTANTLY VIP','MAX','MAX (OUTREACH TODAY)')
)
SELECT
  COALESCE(NULLIF(UPPER(TRIM(m.cm)),''),
           NULLIF(UPPER(TRIM(regexp_extract(m.raw_text, '\(([^()]*)\)\s*$', 1))),'')) AS cm,
  COUNT(*) AS meetings
FROM core.meeting m
JOIN real_cms rc
  ON rc.cm = COALESCE(NULLIF(UPPER(TRIM(m.cm)),''),
                      NULLIF(UPPER(TRIM(regexp_extract(m.raw_text, '\(([^()]*)\)\s*$', 1))),''))
WHERE (CASE WHEN m.source = 'sheet' THEN m.channel = 'Email'
            ELSE NOT regexp_matches(
                   lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                   'sendivo|\bsms\b|whatsapp|iskra') END)
GROUP BY 1
ORDER BY meetings DESC
;

-- active-inbox ESP split (inline one-liner on the CM panel).
CREATE OR REPLACE VIEW dash.warehouse_overview__esp AS
SELECT esp,
       COUNT(*)               AS inboxes,
       SUM(daily_limit)       AS daily_capacity,
       COUNT(DISTINCT domain) AS domains
FROM core.sending_account
WHERE status = 'active' AND esp IS NOT NULL
GROUP BY esp
ORDER BY inboxes DESC
;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: lens-overview  ("Decision Matrix")                              ║
-- ║ Publisher: scripts/dashboard_data.py  ->  lens-overview/data.json          ║
-- ║ In nightly: YES.  Multi-tile hub -> 12 part-views over ~10 datasets.       ║
-- ║ NOTE: re-derives sending-truth / esp / sms inline (overlaps lens-sending-  ║
-- ║ truth + warehouse-overview + lens-sms — same base objects, by design).     ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- email performance rollup (campaign x week); facets by cm/offer/infra/weekly.
CREATE OR REPLACE VIEW dash.lens_overview__campaign_perf AS
SELECT
  week_start,
  cm,
  COALESCE(offer, '(unmatched)')      AS offer,
  COALESCE(infra_type, '(unknown)')   AS sender_esp,
  SUM(sends)            AS sends,
  SUM(unique_replies)   AS replies,
  SUM(opportunities)    AS opps          -- = SUM(unique_opportunities); TREND, overcounts deduped cumulative
FROM main.v_campaign_opportunities
GROUP BY week_start, cm, COALESCE(offer,'(unmatched)'), COALESCE(infra_type,'(unknown)')
;

-- inbox INVENTORY snapshot (current account-truth); facets totals/by_esp/by_workspace/by_lifecycle.
CREATE OR REPLACE VIEW dash.lens_overview__sending_truth_inventory AS
SELECT
  sa.esp,
  COALESCE(w.name, sa.workspace_slug) AS workspace,
  sa.lifecycle_state,
  sa.is_active,
  sa.daily_limit
FROM core.sending_account sa
LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
;

-- daily send ACTIVITY (last 45d), infra-split off CAMPAIGN infra_type (coarse — see spec).
CREATE OR REPLACE VIEW dash.lens_overview__sending_daily AS
WITH camp AS (   -- one row per campaign, LATEST metadata (deterministic). NB: the publisher
                 -- dashboard_data.py:108 filters _run_id=(latest) here, which silently drops
                 -- frozen/deleted-upstream campaigns from the infra split (-> all dumped to OTD)
                 -- once _run_id ever diverges (raw_pipeline_campaigns is an upsert mirror).
                 -- This view uses arg_max instead = same output today, no freeze bug. [flagged]
  SELECT campaign_id, arg_max(infra_type, _loaded_at) AS infra_type
  FROM main.raw_pipeline_campaigns
  GROUP BY campaign_id
)
SELECT
  m.date,
  COALESCE(m.workspace_name, '(unknown)')                          AS workspace,
  SUM(m.sent)                                                      AS sends,
  SUM(m.sent) FILTER (WHERE lower(c.infra_type)='google')          AS google,
  SUM(m.sent) FILTER (WHERE lower(c.infra_type)='outlook')         AS outlook,
  SUM(m.sent) FILTER (WHERE lower(c.infra_type) NOT IN ('google','outlook')
                          OR c.infra_type IS NULL)                 AS otd
FROM main.raw_pipeline_campaign_daily_metrics m
LEFT JOIN camp c ON c.campaign_id = m.campaign_id
WHERE m.date >= current_date - 45
GROUP BY m.date, COALESCE(m.workspace_name,'(unknown)')
;

-- ESP distribution (workspace x ESP, active inboxes).
CREATE OR REPLACE VIEW dash.lens_overview__esp_distribution AS
SELECT
  COALESCE(w.name, sa.workspace_slug) AS workspace,
  sa.esp,
  COUNT(*) AS inboxes
FROM core.sending_account sa
LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
WHERE sa.is_active AND sa.esp IS NOT NULL
GROUP BY 1, 2
;

-- SMS send funnel (Sendivo daily; rates re-derived volume-weighted by the publisher).
CREATE OR REPLACE VIEW dash.lens_overview__sms_send AS
SELECT metric_date, sms_sent, segments_sent, inbound_sms_received,
       delivery_rate, opt_out_rate, response_rate
FROM main.v_sms_performance
;

-- SMS-attributed booked meetings (Slack-channel-classified from raw_text).
-- (spend_billed = SUM(core.cost_ledger.total_usd WHERE vendor='sendivo') and
--  opportunities_unique = COUNT(core.opportunity WHERE source='sendivo' AND state<>'duplicate')
--  are scalar lookups the publisher reads directly — see dependency map in the spec.)
CREATE OR REPLACE VIEW dash.lens_overview__sms_meetings AS
SELECT posted_at, raw_text,
       (CASE WHEN lower(raw_text) LIKE '%whatsapp%' THEN 'whatsapp'
             WHEN lower(raw_text) LIKE '%sendivo%' OR lower(raw_text) LIKE '%sms%' THEN 'sms'
             WHEN lower(raw_text) LIKE '%linkedin%' THEN 'linkedin'
             WHEN lower(raw_text) LIKE '%sdr%' THEN 'sdr' ELSE 'email' END) AS channel
FROM core.meeting
;

-- deliverability: blacklist exposure per ESP (Spamhaus DBL headline, SURBL context).
CREATE OR REPLACE VIEW dash.lens_overview__deliverability_blacklist AS
SELECT
  esp,
  COUNT(*)                                                           AS domains,
  COUNT(*) FILTER (WHERE listed_on LIKE '%spamhaus_dbl%')            AS listed,
  ROUND(100.0*COUNT(*) FILTER (WHERE listed_on LIKE '%spamhaus_dbl%')/COUNT(*),2) AS pct_listed,
  COUNT(*) FILTER (WHERE listed_on LIKE '%surbl%')                   AS surbl_listed,
  ROUND(100.0*COUNT(*) FILTER (WHERE listed_on LIKE '%surbl%')/COUNT(*),1) AS pct_surbl
FROM core.domain
GROUP BY esp
;

-- deliverability: homogeneous-provisioning clusters (shared DNS signature; >100 domains).
CREATE OR REPLACE VIEW dash.lens_overview__deliverability_dns_clusters AS
SELECT dns_signature, COUNT(*) AS domains, MIN(domain) AS example, MAX(esp) AS esp
FROM core.domain WHERE dns_signature IS NOT NULL
GROUP BY dns_signature HAVING COUNT(*) > 100
;

-- deliverability: shared /24 IP blocks (>50 domains).
CREATE OR REPLACE VIEW dash.lens_overview__deliverability_ip24 AS
SELECT a_record_24, COUNT(*) AS domains, MAX(esp) AS esp
FROM core.domain WHERE a_record_24 IS NOT NULL
GROUP BY a_record_24 HAVING COUNT(*) > 50
;

-- meetings & partners base (channel from raw_text; partner labels via core.funding_partner).
CREATE OR REPLACE VIEW dash.lens_overview__meetings AS
SELECT
  m.posted_at,
  m.cm,
  m.partner,
  m.partner_key,
  m.raw_text,
  COALESCE(fp.display_name, m.partner, '(unattributed)') AS partner_label,
  fp.commercial_model,
  fp.tier,
  (CASE WHEN lower(m.raw_text) LIKE '%whatsapp%' THEN 'whatsapp'
        WHEN lower(m.raw_text) LIKE '%sendivo%' OR lower(m.raw_text) LIKE '%sms%' THEN 'sms'
        WHEN lower(m.raw_text) LIKE '%linkedin%' THEN 'linkedin'
        WHEN lower(m.raw_text) LIKE '%sdr%' THEN 'sdr' ELSE 'email' END) AS channel
FROM core.meeting m
LEFT JOIN core.funding_partner fp ON m.partner_key = fp.partner_key
;

-- ESP x ESP send matrix (publisher remap: sender unknown->otd; recipient yahoo/isp/apple/other->other).
-- ONLY `sends` is surfaced; the mv reply columns derive from a BROKEN source and MUST NOT be read.
CREATE OR REPLACE VIEW dash.lens_overview__esp_matrix AS
SELECT
  CASE WHEN sender_esp = 'unknown' THEN 'otd' ELSE sender_esp END AS sender_esp,
  CASE WHEN recipient_esp IN ('yahoo','isp','apple','other') THEN 'other'
       ELSE recipient_esp END                                     AS recipient_esp,
  SUM(sends)                                                      AS sends
FROM main.mv_esp_send_matrix
GROUP BY 1, 2
;

-- SMS call-opportunities (AIM-surfaced), pinned to the latest mirror run.
CREATE OR REPLACE VIEW dash.lens_overview__sms_opportunities AS
SELECT status, source
FROM main.raw_comms_call_opportunity
WHERE _run_id = (SELECT _run_id FROM main.raw_comms_call_opportunity ORDER BY _loaded_at DESC LIMIT 1)
  AND source = 'sendivo'
;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: lens-kpi  ("Emails per Meeting")                                ║
-- ║ Publisher: scripts/kpi_dashboard_data.py  ->  lens-kpi/data.json           ║
-- ║   *** publisher-of-record is the WORKTREE copy                             ║
-- ║       (.worktrees/warehouse-audit-fixes/scripts/kpi_dashboard_data.py),    ║
-- ║       NOT committed HEAD scripts/ — see spec + closeout flag. ***          ║
-- ║ In nightly: YES.  3 part-views (email / sms / esp).                        ║
-- ║ Views expose BASE MEASURES only; EOP/KPI ratios are recomputed CLIENT-SIDE.║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- email measures, date x infra x cm x is_mca, SCOPED to 5 Funding workspaces + 5 active CMs.
-- Source = raw_pipeline_campaign_daily_metrics (NEVER core.campaign_daily) + core.meeting.campaign_id.
CREATE OR REPLACE VIEW dash.lens_kpi__email AS
WITH filtered_rp AS (
  SELECT DISTINCT ON (campaign_id)
    campaign_id,
    COALESCE(NULLIF(infra_type,''),'unknown') AS infra,
    cm_name
  FROM main.raw_pipeline_campaigns
  WHERE workspace_id IN ('renaissance-4','renaissance-5','prospects-power','koi-and-destroy','renaissance-2')
    AND (cm_name IN ('IDO','LEO','SAM','EYVER','SAMUEL') OR cm_name IS NULL)
  ORDER BY campaign_id, _loaded_at DESC
),
dims AS (
  SELECT rp.campaign_id, rp.infra,
         COALESCE(NULLIF(rp.cm_name,''),'IDO') AS cm,   -- NULL-cm campaigns folded to IDO (publisher behaviour)
         COALESCE(c.is_mca, FALSE) AS is_mca
  FROM filtered_rp rp
  LEFT JOIN core.campaign c USING (campaign_id)
),
sends AS (
  SELECT cd.date, d.infra, d.cm, d.is_mca,
         SUM(cd.sent)                     AS sent,
         SUM(cd.unique_opportunities)     AS opportunities,   -- windowed unique_* (trend-grade, NOT deduped absolute)
         SUM(cd.unique_replies)           AS replies_human,
         SUM(cd.unique_replies_automatic) AS replies_auto
  FROM main.raw_pipeline_campaign_daily_metrics cd
  INNER JOIN dims d USING (campaign_id)               -- INNER = the scope filter
  GROUP BY 1,2,3,4
),
email_meetings AS (
  SELECT date, infra, cm, is_mca, SUM(meetings) AS meetings FROM (
    SELECT CAST(m.posted_at AS DATE) AS date, d.infra,
           COALESCE(NULLIF(d.cm,''),'IDO') AS cm, d.is_mca, COUNT(*) AS meetings
    FROM core.meeting m
    INNER JOIN dims d ON m.campaign_id = d.campaign_id
    WHERE m.source = 'slack'
      AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                             'sendivo|\bsms\b|whatsapp|iskra')
    GROUP BY 1,2,3,4
    UNION ALL
    SELECT CAST(m.posted_at AS DATE) AS date, 'google' AS infra,
           m.cm, FALSE AS is_mca, COUNT(*) AS meetings
    FROM core.meeting m
    WHERE m.source = 'slack' AND m.campaign_id IS NULL
      AND m.cm IN ('IDO','LEO','SAM','EYVER','SAMUEL')
      AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                             'sendivo|\bsms\b|whatsapp|iskra')
      AND NOT regexp_matches(COALESCE(m.campaign_name_raw,''), '^\d{1,2}/\d{1,2}/\d{4}')
    GROUP BY 1,2,3,4
  ) GROUP BY 1,2,3,4
)
SELECT
  COALESCE(s.date,  mt.date)            AS date,
  COALESCE(s.infra, mt.infra)           AS infra,
  COALESCE(s.cm,    mt.cm)              AS cm,
  COALESCE(s.is_mca, mt.is_mca)         AS is_mca,
  COALESCE(s.sent, 0)                   AS sent,
  COALESCE(s.opportunities, 0)          AS opportunities,
  COALESCE(s.replies_human, 0)          AS replies_human,
  COALESCE(s.replies_auto, 0)           AS replies_auto,
  COALESCE(mt.meetings, 0)              AS meetings
FROM sends s
FULL JOIN email_meetings mt
  ON s.date = mt.date AND s.infra = mt.infra AND s.cm = mt.cm
  AND s.is_mca IS NOT DISTINCT FROM mt.is_mca
;

-- SMS funnel (re-expose v_kpi_sms verbatim). Sendivo-campaign x date + a date-grain
-- SMS-meetings row (campaign_id NULL). "opportunities"=positive_replies, NOT email opps.
CREATE OR REPLACE VIEW dash.lens_kpi__sms AS
SELECT * FROM main.v_kpi_sms
;

-- ESP send mix (account grain) — the ONLY OTD/Google/Outlook-splittable surface.
CREATE OR REPLACE VIEW dash.lens_kpi__esp AS
SELECT
  date,
  COALESCE(esp,'unknown')        AS esp,
  SUM(actual_sends)              AS sends,
  COUNT(DISTINCT account_id)     AS active_accounts
FROM core.sending_account_daily
WHERE date >= current_date - 92          -- publisher reads a 92d window; bound the ~32M-row scan
GROUP BY 1,2
;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: lens-sms  ("SMS Campaign Performance")                          ║
-- ║ Publisher: scripts/sms_campaign_dashboard_data.py -> lens-sms/data/latest  ║
-- ║ In nightly: YES.  2 part-views (per-campaign / daily). Thin pass-through    ║
-- ║ over main.v_sms_campaign_performance. NO containment guard (faithful to     ║
-- ║ the live dashboard — carries the latent fan-out; see spec).                ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- per-campaign funnel, aggregated across the full available range.
CREATE OR REPLACE VIEW dash.lens_sms__campaign AS
SELECT
    campaign_id,
    any_value(campaign_name)                                  AS campaign_name,
    any_value(sub_account_name)                               AS sub_account,
    COALESCE(sum(sent), 0)::BIGINT                            AS sent,
    COALESCE(sum(delivered), 0)::BIGINT                       AS delivered,
    COALESCE(sum(failed), 0)::BIGINT                          AS failed,
    round(COALESCE(sum(cost_usd), 0), 2)                     AS cost_usd,
    COALESCE(sum(replies), 0)::BIGINT                         AS replies,
    COALESCE(sum(opt_outs), 0)::BIGINT                        AS opt_outs,
    COALESCE(sum(positive_replies), 0)::BIGINT               AS positive_replies,
    CASE WHEN sum(sent) > 0
         THEN round(100.0 * sum(delivered) / sum(sent), 2) END        AS delivery_rate,
    CASE WHEN sum(delivered) > 0
         THEN round(100.0 * sum(replies) / sum(delivered), 2) END     AS reply_rate,
    CASE WHEN sum(positive_replies) > 0
         THEN round(sum(cost_usd) / sum(positive_replies), 2) END     AS cost_per_positive
FROM main.v_sms_campaign_performance
GROUP BY campaign_id
HAVING sum(sent) > 0 OR sum(replies) > 0
ORDER BY sent DESC
;

-- daily send-volume trend (all campaigns collapsed), one row per metric_date.
CREATE OR REPLACE VIEW dash.lens_sms__daily AS
SELECT
    metric_date,
    COALESCE(sum(sent), 0)::BIGINT                            AS sent,
    COALESCE(sum(delivered), 0)::BIGINT                       AS delivered,
    COALESCE(sum(failed), 0)::BIGINT                          AS failed,
    round(COALESCE(sum(cost_usd), 0), 2)                     AS cost_usd,
    COALESCE(sum(replies), 0)::BIGINT                         AS replies,
    COALESCE(sum(opt_outs), 0)::BIGINT                        AS opt_outs,
    COALESCE(sum(positive_replies), 0)::BIGINT               AS positive_replies,
    CASE WHEN sum(sent) > 0
         THEN round(100.0 * sum(delivered) / sum(sent), 2) END        AS delivery_rate
FROM main.v_sms_campaign_performance
WHERE metric_date IS NOT NULL
GROUP BY metric_date
ORDER BY metric_date
;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: lens-campaign-performance  ("Campaign Performance (Email)")     ║
-- ║ Publisher: daily_performance_warehouse.py -> data/latest.json (in nightly);║
-- ║   workspaces.json = a SEPARATE droplet-only job (off-nightly).             ║
-- ║ 3 part-views. Heavy Python read-model (resolve_cm, fuzzy meeting match) —  ║
-- ║ these views are the SQL/blast-radius contract, not byte-exact JSON.        ║
-- ║ TRAP: raw_pipeline_campaigns.workspace_name carries MIXED slug/display/    ║
-- ║ casing forms -> MUST normalize lower(replace(...,'-',' ')) or rows vanish. ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- per (date x campaign) funnel for the 5 active Funding CMs.
CREATE OR REPLACE VIEW dash.lens_campaign_performance AS
WITH camp AS (   -- latest metadata row per campaign
  SELECT campaign_id, name, workspace_id, workspace_name, segment, cm_name, status,
         excluded_from_analysis
  FROM (SELECT *, row_number() OVER (PARTITION BY campaign_id
                                     ORDER BY _loaded_at DESC) rn
        FROM main.raw_pipeline_campaigns) WHERE rn = 1
),
metrics AS (     -- latest daily fact per (campaign, date)
  SELECT campaign_id, date, sent, unique_replies, unique_opportunities
  FROM (SELECT *, row_number() OVER (PARTITION BY campaign_id, date
                                     ORDER BY _loaded_at DESC) rn
        FROM main.raw_pipeline_campaign_daily_metrics) WHERE rn = 1
),
joined AS (
  SELECT m.date,
         m.campaign_id,
         c.name                                   AS campaign_name,
         c.segment,
         c.workspace_name                         AS workspace_raw,
         lower(replace(c.workspace_name,'-',' '))  AS ws_norm,
         upper(coalesce(c.cm_name,''))            AS cm_meta,
         m.sent, m.unique_replies, m.unique_opportunities
  FROM metrics m
  JOIN camp c USING (campaign_id)
  WHERE m.sent > 0
    AND (c.excluded_from_analysis IS NOT TRUE OR c.excluded_from_analysis IS NULL)
    AND (c.status IS NULL OR c.status <> 'deleted')
)
SELECT
  date,
  campaign_id,
  campaign_name,
  CASE ws_norm
    WHEN 'renaissance 4'   THEN 'Funding 1'
    WHEN 'renaissance 5'   THEN 'Funding 2'
    WHEN 'prospects power'  THEN 'Funding 3'
    WHEN 'koi and destroy'  THEN 'Funding 4'
    WHEN 'renaissance 2'   THEN 'Funding 5'
  END                                              AS workspace,        -- WORKSPACE_SLUG_TO_DISPLAY
  coalesce(segment, 'uncategorized')               AS industry,
  sent,
  unique_replies                                   AS replies,
  unique_opportunities                             AS opps
FROM joined
WHERE ws_norm IN ('renaissance 4','renaissance 5','prospects power',
                  'koi and destroy','renaissance 2')                  -- FUNDING_WORKSPACE_SLUGS
;

-- companion: per (date x campaign x resolved-CM) email meetings (NOT-LIKE SMS exclusion,
-- NOT v_kpi_email's regexp — adds linkedin+sdr, matches raw_text only).
CREATE OR REPLACE VIEW dash.lens_campaign_performance__meetings AS
SELECT
  CAST(m.posted_at AS DATE)                              AS date,
  m.campaign_id,
  coalesce(upper(m.cm), upper(c.cm_name))               AS cm,
  count(*)                                              AS meetings
FROM core.meeting m
LEFT JOIN (SELECT campaign_id, arg_max(cm_name, _loaded_at) cm_name   -- latest cm (deterministic; matches the funnel view)
           FROM main.raw_pipeline_campaigns GROUP BY 1) c
  ON c.campaign_id = m.campaign_id
WHERE m.is_duplicate_of IS NULL
  AND lower(coalesce(m.raw_text,'')) NOT LIKE '%whatsapp%'
  AND lower(coalesce(m.raw_text,'')) NOT LIKE '%sms%'
  AND lower(coalesce(m.raw_text,'')) NOT LIKE '%sendivo%'
  AND lower(coalesce(m.raw_text,'')) NOT LIKE '%linkedin%'
  AND lower(coalesce(m.raw_text,'')) NOT LIKE '%sdr%'
GROUP BY 1, 2, 3
;

-- per (date x workspace) totals, ALL workspaces, no CM filter (source for workspaces.json).
CREATE OR REPLACE VIEW dash.lens_campaign_performance__workspaces AS
WITH camp AS (
  SELECT campaign_id, workspace_name,
         row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) rn
  FROM main.raw_pipeline_campaigns
),
metrics AS (
  SELECT campaign_id, date, sent, unique_replies, unique_opportunities,
         row_number() OVER (PARTITION BY campaign_id, date ORDER BY _loaded_at DESC) rn
  FROM main.raw_pipeline_campaign_daily_metrics
),
sends AS (
  SELECT m.date,
         CASE lower(replace(coalesce(c.workspace_name,''),'-',' '))
           WHEN 'renaissance 4'  THEN 'Funding 1'
           WHEN 'renaissance 5'  THEN 'Funding 2'
           WHEN 'prospects power' THEN 'Funding 3'
           WHEN 'koi and destroy' THEN 'Funding 4'
           WHEN 'renaissance 2'  THEN 'Funding 5'
           WHEN ''               THEN '(new / unmapped)'
           ELSE coalesce(c.workspace_name, '(new / unmapped)')
         END                                       AS workspace,
         m.sent, m.unique_replies, m.unique_opportunities
  FROM metrics m
  LEFT JOIN camp c ON c.campaign_id = m.campaign_id AND c.rn = 1
  WHERE m.rn = 1 AND m.sent > 0
),
send_roll AS (
  SELECT date, workspace,
         SUM(sent) AS sent, SUM(unique_replies) AS replies,
         SUM(unique_opportunities) AS opps
  FROM sends GROUP BY 1,2
),
mtg_roll AS (
  SELECT CAST(mm.posted_at AS DATE) AS date,
         CASE lower(replace(coalesce(c.workspace_name,''),'-',' '))
           WHEN 'renaissance 4'  THEN 'Funding 1'
           WHEN 'renaissance 5'  THEN 'Funding 2'
           WHEN 'prospects power' THEN 'Funding 3'
           WHEN 'koi and destroy' THEN 'Funding 4'
           WHEN 'renaissance 2'  THEN 'Funding 5'
           WHEN ''               THEN '(new / unmapped)'
           ELSE coalesce(c.workspace_name, '(new / unmapped)')
         END                                       AS workspace,
         count(*) AS meetings
  FROM core.meeting mm
  LEFT JOIN (SELECT campaign_id, arg_max(workspace_name, _loaded_at) workspace_name   -- latest ws (deterministic)
             FROM main.raw_pipeline_campaigns GROUP BY 1) c USING (campaign_id)
  WHERE mm.is_duplicate_of IS NULL
    AND lower(coalesce(mm.raw_text,'')) NOT LIKE '%whatsapp%'
    AND lower(coalesce(mm.raw_text,'')) NOT LIKE '%sms%'
    AND lower(coalesce(mm.raw_text,'')) NOT LIKE '%sendivo%'
    AND lower(coalesce(mm.raw_text,'')) NOT LIKE '%linkedin%'
    AND lower(coalesce(mm.raw_text,'')) NOT LIKE '%sdr%'
  GROUP BY 1,2
)
SELECT coalesce(s.date, t.date) AS date,
       coalesce(s.workspace, t.workspace) AS workspace,
       coalesce(s.sent,0) AS sent, coalesce(s.replies,0) AS replies,
       coalesce(s.opps,0) AS opps, coalesce(t.meetings,0) AS meetings
FROM send_roll s
FULL OUTER JOIN mtg_roll t USING (date, workspace)
;


-- ╔══════════════════════════════════════════════════════════════════════════╗
-- ║ DASHBOARD: lens-sending-truth  ("Sending Volume Truth")                    ║
-- ║ *** This dashboard's CURRENT feed does NOT read the warehouse — it is      ║
-- ║ rebuilt live from the Instantly API into a standalone account_truth.duckdb ║
-- ║ (deliverables/2026-05-27-instantly-account-truth/). The views below are    ║
-- ║ the FORWARD/migration contract = the warehouse equivalent (core.sending_   ║
-- ║ account_daily mirror), verified within ~3% of the committed cube. ***      ║
-- ║ Tag/campaign tabs + setup_pending are OUT OF CONTRACT (not in warehouse).  ║
-- ╚══════════════════════════════════════════════════════════════════════════╝

-- account-day audit (configured/eligible/assigned/actual + gap + fulfillment + slice dims).
CREATE OR REPLACE VIEW dash.lens_sending_truth AS
WITH classified AS (
  SELECT
    d.date,
    d.account_id,
    d.workspace_slug,
    COALESCE(w.name, d.workspace_slug)              AS workspace_name,
    CASE d.esp WHEN 'google' THEN 'Google' WHEN 'outlook' THEN 'Outlook'
               WHEN 'otd' THEN 'OTD' ELSE COALESCE(d.esp,'(unknown)') END AS infra_type,
    sa.domain,
    sa.status                                       AS account_status,
    d.daily_limit,
    d.expected_sends,
    d.actual_sends,
    d.active_campaign_count,
    d.fulfillment,
    -- COALESCE(status,'') so a NULL status (LEFT-JOIN miss, ~40k missing-inventory rows) is
    -- FALSE not NULL — otherwise NOT is_eligible is NULL and the capacity drops from BOTH the
    -- excluded_capacity AND eligible_capacity buckets (configured != excluded + eligible).
    (COALESCE(sa.status,'') = 'active' AND COALESCE(d.expected_sends,0) > 0)              AS is_eligible,
    (COALESCE(sa.status,'') = 'active' AND COALESCE(d.expected_sends,0) > 0
        AND COALESCE(d.active_campaign_count,0) > 0)                                     AS is_campaign_assigned_eligible,
    CASE
      WHEN sa.status IS NULL OR sa.status = 'missing'        THEN 'missing_current_inventory'
      WHEN sa.status <> 'active'                              THEN 'bad_status'
      WHEN COALESCE(d.daily_limit,0) = 0                      THEN 'daily_limit_zero'
      WHEN COALESCE(d.active_campaign_count,0) = 0            THEN 'no_active_campaign'
      WHEN COALESCE(d.expected_sends,0) > 0
           AND COALESCE(d.actual_sends,0) >= d.expected_sends * 0.95 THEN 'fully_utilized'
      ELSE 'assigned_but_undersent'
    END                                             AS eligibility,
    CASE
      WHEN d.actual_sends = 0 AND d.expected_sends > 0           THEN 'zero'
      WHEN d.expected_sends > 0 AND d.fulfillment < 0.25         THEN 'under25'
      WHEN d.expected_sends > 0 AND d.fulfillment < 0.50         THEN 'under50'
      WHEN d.expected_sends > 0 AND d.fulfillment < 0.85         THEN 'under85'
      WHEN d.expected_sends > 0 AND d.fulfillment >= 0.85        THEN 'ok'
      ELSE 'none'
    END                                             AS fulfillment_bucket
  FROM core.sending_account_daily d
  LEFT JOIN core.sending_account sa ON sa.account_id = d.account_id
  LEFT JOIN core.workspace        w  ON w.slug       = d.workspace_slug
  WHERE d.date >= current_date - 92      -- bound the ~32M-row scan; account-truth feed is recent-only
)
SELECT
  date, account_id, workspace_slug, workspace_name, infra_type, domain,
  account_status, eligibility, fulfillment_bucket,
  daily_limit, expected_sends AS configured_capacity, actual_sends,
  active_campaign_count,
  CASE WHEN NOT is_eligible THEN expected_sends ELSE 0 END                  AS excluded_capacity,
  CASE WHEN is_eligible THEN expected_sends ELSE 0 END                      AS eligible_capacity,
  CASE WHEN is_campaign_assigned_eligible THEN expected_sends ELSE 0 END    AS campaign_assigned_capacity,
  GREATEST(CASE WHEN is_eligible THEN expected_sends ELSE 0 END - actual_sends, 0) AS eligible_gap
FROM classified
;

-- headline KPI strip (snapshot grain; SQL equivalent of the app's summarize()).
CREATE OR REPLACE VIEW dash.lens_sending_truth__totals AS
SELECT
  date,
  COUNT(*)                                      AS account_count,
  COUNT(*) FILTER (WHERE eligible_capacity > 0) AS eligible_account_count,
  SUM(configured_capacity)                      AS configured_capacity,
  SUM(excluded_capacity)                        AS excluded_capacity,
  SUM(eligible_capacity)                        AS eligible_capacity,
  SUM(campaign_assigned_capacity)               AS campaign_assigned_capacity,
  SUM(actual_sends)                             AS actual_sends,
  GREATEST(SUM(eligible_capacity) - SUM(actual_sends), 0) AS eligible_gap,
  SUM(actual_sends)::DOUBLE / NULLIF(SUM(eligible_capacity),0) AS eligible_fulfillment
FROM dash.lens_sending_truth
GROUP BY 1
;
