-- 63_audit_fixes.sql  [2026-06-13]  Warehouse data-integrity audit (WS-I + WS-G) fixes.
-- Applied via apply_ddl_file(version=63) — idempotent CREATE OR REPLACE. Standard SQL (migration-agnostic).
-- Fixes: D-L2-5 (v_campaign_metrics impossible rows), D-L2-1/2 (v_kpi_email reads the complete source),
-- WS-G (derived.v_funnel). SMS guard + sends_by_esp coordinated separately (sms-aim / dashboard script).

-- =====================================================================================
-- FIX 1 (D-L2-5): v_campaign_metrics — sent fallback no longer mixes a 45d-windowed sum with
-- an unbounded reply count. sent = analytics -> 45d-daily-sum -> all-time campaign_data cumulative
-- -> NULL (honest "unknown sends", never 0-with-replies). The pipeline reply fallback is clamped to
-- sent so unique_replies can never exceed sent. Verified: impossible rows 35->0, reply_rate>1 5->0.
-- =====================================================================================
CREATE OR REPLACE VIEW main.v_campaign_metrics AS
WITH pc AS (
  SELECT campaign_id, workspace_id, workspace_name, name, cm_name, product, infra_type, segment, status
  FROM main.raw_pipeline_campaigns
),
pipe_sent AS (SELECT campaign_id, sum(sent) AS sent_additive FROM main.raw_pipeline_campaign_daily_metrics GROUP BY campaign_id),
cd_cum    AS (SELECT campaign_id, max(emails_sent) AS emails_sent FROM main.raw_pipeline_campaign_data WHERE step='__ALL__' AND variant='__ALL__' GROUP BY campaign_id),
pipe_repl AS (SELECT campaign_id, count(DISTINCT lower(lead_email)) AS unique_repliers FROM main.raw_pipeline_reply_data GROUP BY campaign_id),
base AS (
  SELECT pc.campaign_id, pc.workspace_id, pc.workspace_name, pc.name AS campaign_name, pc.cm_name,
         pc.product AS offer, pc.infra_type, pc.segment, pc.status,
         regexp_matches(lower(pc.name), '\b(isaac|mca|cheap leads)\b') AS is_mca,
         a.reply_count_unique, a.reply_count_automatic_unique, a.total_opportunities, a.total_opportunity_value,
         a.bounced_count, a.completed_count, a.campaign_id AS analytics_cid, a._loaded_at AS analytics_loaded_at,
         a.emails_sent_count,
         -- canonical sent: analytics, else 45d daily sum, else all-time campaign_data cumulative, else NULL
         COALESCE(a.emails_sent_count, NULLIF(ps.sent_additive,0), NULLIF(cc.emails_sent,0)) AS sent_fixed,
         pr.unique_repliers
  FROM pc
  LEFT JOIN main.raw_instantly_campaign_analytics a ON a.campaign_id = pc.campaign_id
  LEFT JOIN pipe_sent ps ON ps.campaign_id = pc.campaign_id
  LEFT JOIN cd_cum    cc ON cc.campaign_id = pc.campaign_id
  LEFT JOIN pipe_repl pr ON pr.campaign_id = pc.campaign_id
)
SELECT campaign_id, workspace_id, workspace_name, campaign_name, cm_name, offer, infra_type, segment, status, is_mca,
       sent_fixed AS sent,
       -- clamp the pipeline reply fallback to sent (analytics reply count trusted as-is)
       COALESCE(reply_count_unique, LEAST(unique_repliers, sent_fixed)) AS unique_replies,
       reply_count_automatic_unique AS auto_replies,
       total_opportunities AS opportunities,
       total_opportunity_value AS opportunity_value,
       bounced_count AS bounced,
       completed_count AS completed,
       CASE WHEN sent_fixed > 0
            THEN round(COALESCE(reply_count_unique, LEAST(unique_repliers, sent_fixed))::double / sent_fixed, 5) END AS reply_rate,
       CASE WHEN emails_sent_count > 0 AND total_opportunities IS NOT NULL
            THEN round(total_opportunities::double / emails_sent_count, 5) END AS opp_rate,
       CASE WHEN reply_count_unique > 0 AND total_opportunities IS NOT NULL
            THEN round(total_opportunities::double / reply_count_unique, 4) END AS positive_reply_rate,
       CASE WHEN total_opportunities > 0
            THEN round(emails_sent_count::double / total_opportunities, 0) END AS email_per_opp,
       CASE WHEN analytics_cid IS NOT NULL THEN 'instantly_analytics' ELSE 'pipeline_fallback' END AS metric_source,
       analytics_loaded_at
FROM base;

-- =====================================================================================
-- FIX 2 (D-L2-1/2): v_kpi_email now sources sent/opps/replies(human+auto) from the COMPLETE
-- raw_pipeline_campaign_daily_metrics (was core.campaign_daily = ~284/2496 campaigns). Dims are
-- LEFT JOINed (orphan campaign-days keep their facts as '(no cm)'/'unknown' instead of being dropped).
-- bounces preserved from core.instantly_bounce_daily. Output columns unchanged.
-- NOTE: infra here is the CAMPAIGN-level infra_type (google-family vs outlook) — it does NOT split OTD.
-- The true OTD/Google/Outlook split is account-grain (core.sending_account_daily.esp); see sends_by_esp.
-- =====================================================================================
CREATE OR REPLACE VIEW main.v_kpi_email AS
WITH dims AS (
  SELECT rp.campaign_id,
         COALESCE(NULLIF(rp.infra_type,''),'unknown') AS infra,
         COALESCE(NULLIF(rp.cm_name,''), NULLIF(c.cm,''), '(no cm)') AS cm,
         COALESCE(c.is_mca, regexp_matches(lower(rp.name), '\b(isaac|mca|cheap leads)\b'), false) AS is_mca
  FROM main.raw_pipeline_campaigns rp
  LEFT JOIN core.campaign c ON c.campaign_id = rp.campaign_id
),
sends AS (
  SELECT cd.date,
         COALESCE(d.infra,'unknown') AS infra, COALESCE(d.cm,'(no cm)') AS cm, COALESCE(d.is_mca,false) AS is_mca,
         sum(cd.sent) AS sent, sum(cd.unique_opportunities) AS opportunities,
         sum(cd.unique_replies) AS replies_human, sum(cd.unique_replies_automatic) AS replies_auto
  FROM main.raw_pipeline_campaign_daily_metrics cd
  LEFT JOIN dims d ON d.campaign_id = cd.campaign_id
  GROUP BY 1,2,3,4
),
email_meetings AS (
  SELECT CAST(m.posted_at AS DATE) AS date,
         COALESCE(d.infra,'(unattributed)') AS infra,
         COALESCE(d.cm, NULLIF(m.cm,''), '(unattributed)') AS cm,
         COALESCE(d.is_mca,false) AS is_mca, count(*) AS meetings
  FROM core.meeting m LEFT JOIN dims d ON m.campaign_id = d.campaign_id
  WHERE m.source = 'slack'
    AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra')
  GROUP BY 1,2,3,4
),
spine AS (
  SELECT COALESCE(s.date, mt.date) AS date, COALESCE(s.infra, mt.infra) AS infra,
         COALESCE(s.cm, mt.cm) AS cm, COALESCE(s.is_mca, mt.is_mca) AS is_mca,
         COALESCE(s.sent,0) AS sent, COALESCE(s.opportunities,0) AS opportunities,
         COALESCE(s.replies_human,0) AS replies_human, COALESCE(s.replies_auto,0) AS replies_auto,
         COALESCE(mt.meetings,0) AS meetings
  FROM sends s
  FULL JOIN email_meetings mt
    ON s.date=mt.date AND s.infra=mt.infra AND s.cm=mt.cm AND s.is_mca IS NOT DISTINCT FROM mt.is_mca
),
bnc AS (
  SELECT b.date, COALESCE(d.cm,'(no cm)') AS cm, sum(b.bounced) AS bounces
  FROM core.instantly_bounce_daily b LEFT JOIN dims d ON d.campaign_id=b.campaign_id GROUP BY 1,2
)
SELECT sp.date, sp.infra, sp.cm, sp.is_mca, sp.sent, sp.opportunities, sp.replies_human, sp.replies_auto,
       COALESCE(bn.bounces,0) AS bounces, sp.meetings,
       CAST(sp.sent AS double)/NULLIF(sp.opportunities,0) AS eop,
       CAST(sp.sent AS double)/NULLIF(sp.meetings,0) AS kpi_emails_per_meeting,
       CAST(sp.meetings AS double)/NULLIF(sp.opportunities,0) AS opp_to_meeting_rate,
       CAST(sp.replies_human AS double)/NULLIF(sp.sent,0) AS reply_rate_human,
       CAST(sp.replies_auto AS double)/NULLIF(sp.sent,0) AS reply_rate_auto
FROM spine sp
LEFT JOIN bnc bn ON bn.date=sp.date AND bn.cm=sp.cm;

-- =====================================================================================
-- WS-G: derived.v_funnel — campaign-grain funnel lead -> sent -> reply(human/auto) -> opp ->
-- meeting -> (result). Populate only stages we have data for. meeting RESULT is present-but-EMPTY
-- (Close CRM not active — NEVER fabricate). Lead-grain funnel = derived.lead_intel.
-- =====================================================================================
CREATE SCHEMA IF NOT EXISTS derived;
CREATE OR REPLACE VIEW derived.v_funnel AS
WITH leads AS (SELECT campaign_id, max(lead_sequence_started) AS leads FROM main.raw_pipeline_campaign_data WHERE step='__ALL__' AND variant='__ALL__' GROUP BY 1),
sends AS (SELECT campaign_id, sum(sent) AS sends, sum(unique_replies) AS replies_human,
                 sum(unique_replies_automatic) AS replies_auto, sum(unique_opportunities) AS opps_windowed
          FROM main.raw_pipeline_campaign_daily_metrics GROUP BY 1),
mtg AS (SELECT campaign_id, count(*) AS meetings FROM core.meeting
        WHERE source='slack' AND campaign_id IS NOT NULL
          AND NOT regexp_matches(lower(COALESCE(campaign_name_raw,'')||' '||COALESCE(raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra')
        GROUP BY 1)
SELECT c.campaign_id, c.name AS campaign_name, c.cm_name, c.workspace_id, c.segment, c.product, c.status,
       COALESCE(l.leads,0)               AS leads,            -- lead_sequence_started (entered sequence)
       COALESCE(s.sends,0)               AS sends,
       COALESCE(s.replies_human,0)       AS replies_human,
       COALESCE(s.replies_auto,0)        AS replies_auto,
       vcm.opportunities                 AS opps_cumulative,  -- deduped (NULL for delisted)
       COALESCE(s.opps_windowed,0)       AS opps_windowed,    -- trend (overcounts cumulative)
       COALESCE(m.meetings,0)            AS meetings,
       CAST(NULL AS VARCHAR)             AS meeting_result    -- EMPTY slot: needs Close CRM (Sam decision)
FROM main.raw_pipeline_campaigns c
LEFT JOIN leads l ON l.campaign_id=c.campaign_id
LEFT JOIN sends s ON s.campaign_id=c.campaign_id
LEFT JOIN mtg   m ON m.campaign_id=c.campaign_id
LEFT JOIN main.v_campaign_metrics vcm ON vcm.campaign_id=c.campaign_id;
