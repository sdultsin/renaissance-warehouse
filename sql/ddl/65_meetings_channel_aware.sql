-- 65_meetings_channel_aware.sql  [2026-06-13]  WS-E meetings re-platform — make the email-meeting
-- surfaces channel-aware. Applied via apply_ddl_file(version=65). Idempotent CREATE OR REPLACE.
--
-- After the cutover (entities/meeting.py), core.meeting carries an explicit `channel` for sheet
-- rows (posted_at >= 2026-06-01). The "email funnel" now counts ONLY channel='Email' for those
-- rows instead of the fuzzy keyword split on raw_text (the P2 over-count root cause). Pre-cutover
-- Slack rows (channel NULL) keep the legacy keyword filter. This is the ONLY change vs DDL 63 —
-- both views are otherwise reproduced verbatim.

-- =====================================================================================
-- v_kpi_email — email-meeting count is now channel-aware (sheet: channel='Email'; slack: keyword).
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
  WHERE (m.source = 'sheet' AND m.channel = 'Email')
     OR (m.source <> 'sheet'
         AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra'))
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
bnc AS (  -- resolve bounces to the FULL spine grain (date,infra,cm,is_mca) to avoid fan-out (dp-review HIGH fix)
  SELECT b.date, COALESCE(d.infra,'unknown') AS infra, COALESCE(d.cm,'(no cm)') AS cm,
         COALESCE(d.is_mca,false) AS is_mca, sum(b.bounced) AS bounces
  FROM core.instantly_bounce_daily b LEFT JOIN dims d ON d.campaign_id=b.campaign_id GROUP BY 1,2,3,4
)
SELECT sp.date, sp.infra, sp.cm, sp.is_mca, sp.sent, sp.opportunities, sp.replies_human, sp.replies_auto,
       COALESCE(bn.bounces,0) AS bounces, sp.meetings,
       CAST(sp.sent AS double)/NULLIF(sp.opportunities,0) AS eop,
       CAST(sp.sent AS double)/NULLIF(sp.meetings,0) AS kpi_emails_per_meeting,
       CAST(sp.meetings AS double)/NULLIF(sp.opportunities,0) AS opp_to_meeting_rate,
       CAST(sp.replies_human AS double)/NULLIF(sp.sent,0) AS reply_rate_human,
       CAST(sp.replies_auto AS double)/NULLIF(sp.sent,0) AS reply_rate_auto
FROM spine sp
LEFT JOIN bnc bn ON bn.date=sp.date AND bn.infra=sp.infra AND bn.cm=sp.cm AND bn.is_mca IS NOT DISTINCT FROM sp.is_mca;

-- =====================================================================================
-- derived.v_funnel — meeting count channel-aware (same rule). Otherwise verbatim DDL 63.
-- =====================================================================================
CREATE SCHEMA IF NOT EXISTS derived;
CREATE OR REPLACE VIEW derived.v_funnel AS
WITH leads AS (SELECT campaign_id, max(lead_sequence_started) AS leads FROM main.raw_pipeline_campaign_data WHERE step='__ALL__' AND variant='__ALL__' GROUP BY 1),
sends AS (SELECT campaign_id, sum(sent) AS sends, sum(unique_replies) AS replies_human,
                 sum(unique_replies_automatic) AS replies_auto, sum(unique_opportunities) AS opps_windowed
          FROM main.raw_pipeline_campaign_daily_metrics GROUP BY 1),
mtg AS (SELECT campaign_id, count(*) AS meetings FROM core.meeting
        WHERE campaign_id IS NOT NULL
          AND ((source = 'sheet' AND channel = 'Email')
            OR (source <> 'sheet'
                AND NOT regexp_matches(lower(COALESCE(campaign_name_raw,'')||' '||COALESCE(raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra')))
        GROUP BY 1)
SELECT c.campaign_id, c.name AS campaign_name, c.cm_name, c.workspace_id, c.segment, c.product, c.status,
       COALESCE(l.leads,0)               AS leads,            -- lead_sequence_started (entered sequence)
       COALESCE(s.sends,0)               AS sends,
       COALESCE(s.replies_human,0)       AS replies_human,
       COALESCE(s.replies_auto,0)        AS replies_auto,
       vcm.opportunities                 AS opps_cumulative,  -- deduped (NULL for delisted)
       COALESCE(s.opps_windowed,0)       AS opps_windowed,    -- trend (overcounts cumulative)
       COALESCE(m.meetings,0)            AS meetings,
       CAST(NULL AS VARCHAR)             AS meeting_result    -- EMPTY slot: needs a funding-partner result source
FROM main.raw_pipeline_campaigns c
LEFT JOIN leads l ON l.campaign_id=c.campaign_id
LEFT JOIN sends s ON s.campaign_id=c.campaign_id
LEFT JOIN mtg   m ON m.campaign_id=c.campaign_id
LEFT JOIN main.v_campaign_metrics vcm ON vcm.campaign_id=c.campaign_id;

-- =====================================================================================
-- Item 5 (handoff residual): derived.v_funnel_detail — the BI-ready LEAD-GRAIN funnel detail,
-- modeled on RG_Leads_MTD_Business_Detail.xlsx (already integrated as core.lead_disposition /
-- DDL 41). Answers, per lead reply: what they replied WITH -> to which COPY (campaign/step/variant
-- + the actual variant body) -> did they book a meeting (email-linked, now possible via the sheet's
-- lead_email) -> partner-rep disposition (RG_Leads feedback) -> (future) the funded/declined RESULT.
--
-- STRUCTURE ONLY — NO backfill, NO fabricated outcomes:
--   * meeting_result is a typed-but-EMPTY slot (the funding-partner funded/declined outcome). No
--     source is wired yet; it is NEVER inferred from partner_disposition or Close. Append later.
--   * partner_disposition / _class / _rep ARE real (the integrated RG_Leads partner-rep feedback,
--     e.g. 'No show'/'DNQ'/'LIVE OPPORTUNITY') — carried as DISTINCT, clearly-labeled columns. They
--     are an intermediate hand-off disposition, NOT the funded/declined meeting result.
--
-- Reply-anchored grain: one row per (lead reply -> its exact copy). A lead with replies to several
-- campaigns/variants gets several rows. Meeting + disposition are deduped to one-per-lead so the
-- reply rows never fan out. Lead-grain identity intel lives in derived.lead_intel.
-- =====================================================================================
CREATE OR REPLACE VIEW derived.v_funnel_detail AS
WITH replies AS (
  SELECT lower(lead_email) AS lead_email, campaign_id, step, variant,
         reply_text, intent AS reply_intent, reply_timestamp, workspace_id
  FROM main.raw_pipeline_reply_data
  WHERE lead_email IS NOT NULL AND lead_email <> ''
),
mtg AS (  -- one (most-recent) email-channel sheet meeting per lead; email linkage = the sheet's lead_email
  SELECT lead_email, mtg_campaign_id, meeting_at, meeting_partner FROM (
    SELECT lower(lead_email) AS lead_email, campaign_id AS mtg_campaign_id,
           posted_at AS meeting_at, partner AS meeting_partner,
           ROW_NUMBER() OVER (PARTITION BY lower(lead_email) ORDER BY posted_at DESC) AS rn
    FROM core.meeting
    WHERE source='sheet' AND channel='Email' AND lead_email IS NOT NULL AND lead_email <> ''
  ) WHERE rn=1
),
disp AS (  -- latest partner disposition per lead (lead_disposition PK = lead_email, source_period)
  SELECT lead_email, disposition, disposition_class, rep, business_name, id_confidence FROM (
    SELECT lower(lead_email) AS lead_email, disposition, disposition_class, rep, business_name, id_confidence,
           ROW_NUMBER() OVER (PARTITION BY lower(lead_email) ORDER BY resolved_at DESC NULLS LAST) AS rn
    FROM core.lead_disposition WHERE lead_email IS NOT NULL AND lead_email <> ''
  ) WHERE rn=1
),
vc AS (  -- one copy row per (campaign,step,variant): variant_copy has dup keys (~159) -> dedup so the
         -- reply-grain rows can never fan out (the audit's fan-out class). Latest by synced_at wins.
  SELECT campaign_id, step, variant, subject_unspintaxed, body_unspintaxed, content_hash FROM (
    SELECT campaign_id, step, variant, subject_unspintaxed, body_unspintaxed, content_hash,
           ROW_NUMBER() OVER (PARTITION BY campaign_id, step, variant ORDER BY synced_at DESC NULLS LAST) AS rn
    FROM main.raw_pipeline_variant_copy
  ) WHERE rn=1
)
SELECT
  r.lead_email,
  r.campaign_id,
  c.name                       AS campaign_name,
  c.cm_name,
  c.workspace_id,
  r.step,
  r.variant,
  vc.subject_unspintaxed       AS copy_subject,        -- the COPY the lead replied to
  vc.body_unspintaxed          AS copy_body,
  vc.content_hash              AS copy_content_hash,    -- stable copy/variant key for "which copy -> which result"
  r.reply_text,
  r.reply_intent,
  r.reply_timestamp,
  (m.lead_email IS NOT NULL)   AS booked_meeting,
  m.meeting_at,
  m.meeting_partner,
  d.disposition                AS partner_disposition,       -- RG_Leads partner-rep feedback (NOT the funded/declined result)
  d.disposition_class          AS partner_disposition_class,
  d.rep                        AS partner_rep,
  CAST(NULL AS VARCHAR)        AS meeting_result             -- EMPTY typed slot: funded/declined; no source wired — never fabricate
FROM replies r
LEFT JOIN main.raw_pipeline_campaigns c   ON c.campaign_id = r.campaign_id
LEFT JOIN vc ON vc.campaign_id = r.campaign_id AND vc.step = r.step AND vc.variant = r.variant
LEFT JOIN mtg  m ON m.lead_email = r.lead_email
LEFT JOIN disp d ON d.lead_email = r.lead_email;
