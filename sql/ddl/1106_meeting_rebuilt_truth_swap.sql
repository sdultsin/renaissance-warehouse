-- @gate: add
-- Depends on 1102
-- Depends on 1103
-- Depends on 1105
-- ============================================================================
-- 1106_meeting_rebuilt_truth_swap.sql — Slack-era meeting rebuild from the portal
-- archive + the core.v_meeting_truth swap view + repoint of the two email-meeting
-- consumer views (main.v_kpi_email, derived.v_funnel).
--
-- WHY (meetings-truth-reconciliation.md, 2026-07-14/15 verdict): the Slack feed that
-- populates core.meeting for the pre-2026-06-01 era MISSED meetings chronically —
-- weekly capture 66.9–90.9% Feb–Apr, worst in May (~831 email meetings = 31% missing);
-- its rows carry NO lead identity (lead_email NULL on all 2,424 May slack rows) and no
-- channel labels. Portal `raw_im_bookings` deduped by email/phone + day (NEVER row id —
-- standing Sam rule) is the measured superset with identity + channel/workspace/partner,
-- and its June series agrees with core.meeting sheet-era within 0.5–1.7% (two independent
-- pipelines) ⇒ portal = meeting SoT; slack rows demoted to corroborating evidence.
--
-- REVERSIBLE BY DESIGN (charter D):
--   * core.meeting is NOT touched. entities/meeting.py is NOT touched.
--   * core.meeting_rebuilt is a NEW table, rebuilt idempotently each nightly by
--     entities/meeting_rebuilt.py (era split below).
--   * Consumers read core.v_meeting_truth (the swap view). ROLLBACK = repoint this one
--     view back to legacy core.meeting (rollback SQL at the bottom of this file,
--     commented) — v_kpi_email / v_funnel then serve exactly the old numbers.
--
-- ERA SPLIT (in meeting_rebuilt, no date overlaps possible):
--   meeting_date <  2026-06-01 -> portal rebuild: latest raw_im_bookings nightly snapshot,
--       deduped lower(email)|phone-digits + day, channel classified (real `channel` where
--       present, else workspace/campaign-string rules measured in the reconciliation),
--       lead identity + partner + advisor carried, BTC client-sheet backfill included
--       (portal rows source='btc_sheet_backfill_20260713', 1,240 rows, 2024-01-15→2025-12-06),
--       best-effort campaign attribution via main.norm_campaign_name (match_method
--       'portal_norm' | 'unmatched').
--   meeting_date >= 2026-06-01 -> core.meeting sheet-era rows VERBATIM (verified superset
--       of the portal there; keeps campaign attribution, cm, channel, lead_email).
--
-- HYGIENE also folded into the two consumer views (funding-scope-summary defect 2):
-- sends CTEs exclude the 8 synthetic '__ledger_recon__' campaign_ids.
--
-- BLAST RADIUS of the consumer repoint (enumerated 2026-07-15, full census in ship notes):
--   IN THIS FILE: main.v_kpi_email (portal KPI tab + scripts/portal_data.py +
--     scripts/dashboard_data.py read it), derived.v_funnel.
--   NOT repointed (still read core.meeting / core.v_meeting_canonical directly — deliberate,
--   one consumer at a time, expand/contract): entities/conversion_event.py, entities/
--   lead_spine.py, scripts/portal_data.py raw core.meeting queries, scripts/dashboard_data.py,
--   scripts/render_mtd.py, scripts/kpi_dashboard_data.py, scripts/sms_comms_performance_daily.py,
--   scripts/qa_workspace_accuracy.py, core.v_meeting_canonical (107/1091) and the ~25 other
--   DDL views listed in the ship notes. They keep serving legacy numbers until each is
--   migrated with its own DDL.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.meeting_rebuilt (
  meeting_key        VARCHAR PRIMARY KEY,  -- era1 'portal:<lead_key>:<day>' | era2 core.meeting.meeting_id
  era                VARCHAR NOT NULL,     -- 'slack_era_portal' | 'post_cutover_core_meeting'
  meeting_date       DATE,                 -- business/booking day (dedup day for era1)
  posted_at          TIMESTAMPTZ,          -- era2 core.meeting.posted_at; era1 = meeting_date midnight
  lead_email         VARCHAR,
  lead_phone         VARCHAR,
  channel_raw        VARCHAR,              -- portal channel column verbatim (mostly NULL pre-June)
  channel_norm       VARCHAR NOT NULL,     -- Email|SMS|WhatsApp|LinkedIn|Call|Other (classified)
  channel_basis      VARCHAR,              -- native_channel | workspace_label | campaign_string | default_email
  workspace_label_raw VARCHAR,
  workspace_slug     VARCHAR,              -- via core.workspace_alias_unified (NULL if unmapped)
  offer              VARCHAR,
  in_funding_scope   BOOLEAN,              -- workspace-label grain era1; offer/campaign-scope era2
  is_ours            BOOLEAN,              -- FALSE for ISKRA partner-outbound bookings
  partner            VARCHAR,
  advisor            VARCHAR,
  campaign_string    VARCHAR,              -- IM-entered campaign string (fuzzy)
  campaign_id        VARCHAR,              -- attributed campaign (best-effort era1)
  match_method       VARCHAR,              -- era1: portal_norm|unmatched · era2: core.meeting.match_method
  cm                 VARCHAR,
  source             VARCHAR NOT NULL,     -- era1: 'portal_rebuild' · era2: core.meeting.source
  portal_source      VARCHAR,              -- raw_im_bookings.source (e.g. btc_sheet_backfill_20260713)
  raw_text           VARCHAR,              -- era2 audit passthrough; NULL era1
  _rebuilt_at        TIMESTAMPTZ DEFAULT now(),
  _run_id            VARCHAR
);

-- ── THE SWAP VIEW — the single repoint target for meeting consumers ───────────
-- Era-2 reads core.meeting LIVE (never stale even if the rebuild entity hasn't run);
-- era-1 reads the portal rebuild. Before the first entity run after ship, era-1 rows are
-- absent (pre-June meetings read 0) — run entities/meeting_rebuilt.py once at apply time
-- (ship notes) or wait one nightly.
CREATE OR REPLACE VIEW core.v_meeting_truth AS
SELECT meeting_key, era, meeting_date, posted_at, lead_email, channel_norm, channel_basis,
       workspace_slug, offer, in_funding_scope, is_ours, partner, campaign_string,
       campaign_id, match_method, cm, source, portal_source, raw_text
FROM core.meeting_rebuilt
WHERE era = 'slack_era_portal'
UNION ALL
SELECT m.meeting_id AS meeting_key,
       'post_cutover_core_meeting' AS era,
       m.meeting_date,
       m.posted_at,
       m.lead_email,
       CASE WHEN m.channel IN ('Email','SMS','WhatsApp','LinkedIn','Call') THEN m.channel
            WHEN m.channel IS NULL OR m.channel = '' THEN 'Other'
            ELSE 'Other' END AS channel_norm,
       'native_channel' AS channel_basis,
       m.workspace_slug,
       m.offer,
       CASE WHEN COALESCE(m.offer,'') = 'Pre-IPO' THEN FALSE
            WHEN cos.in_funding_scope IS NOT NULL THEN cos.in_funding_scope
            ELSE TRUE END AS in_funding_scope,
       NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'') || ' ' || COALESCE(m.raw_text,'')), 'iskra') AS is_ours,
       m.partner_key AS partner,
       m.campaign_name_raw AS campaign_string,
       m.campaign_id,
       m.match_method,
       m.cm,
       m.source,
       NULL AS portal_source,
       m.raw_text
FROM core.meeting m
LEFT JOIN core.campaign_offer_scope cos ON cos.campaign_id = m.campaign_id
WHERE m.source = 'sheet' AND m.meeting_date >= DATE '2026-06-01';

-- ── main.v_kpi_email — reproduced VERBATIM from DDL 65 except: ────────────────
--   (1) email_meetings now reads core.v_meeting_truth (channel_norm='Email' AND is_ours,
--       dated by meeting_date) instead of core.meeting's sheet/keyword split;
--   (2) sends CTE excludes the synthetic __ledger_recon__ campaign rows.
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
  WHERE NOT contains(cd.campaign_id, '__ledger_recon__')
  GROUP BY 1,2,3,4
),
email_meetings AS (
  SELECT mt.meeting_date AS date,
         COALESCE(d.infra,'(unattributed)') AS infra,
         COALESCE(d.cm, NULLIF(mt.cm,''), '(unattributed)') AS cm,
         COALESCE(d.is_mca,false) AS is_mca, count(*) AS meetings
  FROM core.v_meeting_truth mt LEFT JOIN dims d ON mt.campaign_id = d.campaign_id
  WHERE mt.channel_norm = 'Email' AND mt.is_ours
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

-- ── derived.v_funnel — reproduced VERBATIM from DDL 65 except the same two changes ──
CREATE SCHEMA IF NOT EXISTS derived;
CREATE OR REPLACE VIEW derived.v_funnel AS
WITH leads AS (SELECT campaign_id, max(lead_sequence_started) AS leads FROM main.raw_pipeline_campaign_data WHERE step='__ALL__' AND variant='__ALL__' GROUP BY 1),
sends AS (SELECT campaign_id, sum(sent) AS sends, sum(unique_replies) AS replies_human,
                 sum(unique_replies_automatic) AS replies_auto, sum(unique_opportunities) AS opps_windowed
          FROM main.raw_pipeline_campaign_daily_metrics
          WHERE NOT contains(campaign_id, '__ledger_recon__')
          GROUP BY 1),
mtg AS (SELECT campaign_id, count(*) AS meetings FROM core.v_meeting_truth
        WHERE campaign_id IS NOT NULL AND channel_norm = 'Email' AND is_ours
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

-- ── ROLLBACK (do NOT run — ship as a new numbered DDL if ever needed) ─────────
-- CREATE OR REPLACE VIEW core.v_meeting_truth AS
-- SELECT m.meeting_id AS meeting_key,
--        CASE WHEN m.source='sheet' THEN 'post_cutover_core_meeting' ELSE 'slack_era_legacy' END AS era,
--        m.meeting_date, m.posted_at, m.lead_email,
--        CASE WHEN m.source='sheet' AND m.channel='Email' THEN 'Email'
--             WHEN m.source<>'sheet' AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),'sendivo|\bsms\b|whatsapp|iskra') THEN 'Email'
--             ELSE 'Other' END AS channel_norm,
--        'legacy_keyword_rule' AS channel_basis, m.workspace_slug, m.offer, TRUE AS in_funding_scope,
--        TRUE AS is_ours, m.partner_key AS partner, m.campaign_name_raw AS campaign_string,
--        m.campaign_id, m.match_method, m.cm, m.source, NULL AS portal_source, m.raw_text
-- FROM core.meeting m;
