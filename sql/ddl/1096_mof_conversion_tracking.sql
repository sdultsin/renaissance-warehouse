-- @gate: add
-- Depends on 1091
-- ============================================================================
-- 1096: MOF conversion-tracking layer (dash.v_mof_*) — the measurement half of
--       the 07-12 MOF DoD: "what % of opportunities, by channel, reached a
--       call / form / meeting this week, and how fast."
--
-- Three ADDITIVE views (no existing object touched):
--   dash.v_mof_campaign_offer  campaign_id -> offer (the OFFER-TAG-SPEC join point)
--   dash.v_mof_funnel_daily    day x channel x workspace funnel fact family
--   dash.v_mof_opp_outcomes    opp-cohort day x channel conversion windows
--
-- Consumers: scripts/mof_daily_digest.py (#cc-sam morning digest), Ben KPI pack,
-- daily report v2 extensions. Spec: Renaissance deliverables/2026-07-08-mof-
-- orchestrator/SCOPING-SYNTHESIS.md §1+§6.
--
-- SEMANTIC CONTRACT (SCOPING-SYNTHESIS §1 — the traps are load-bearing):
--   * "Opportunity" = 3 DIFFERENT definitions by channel; the views LABEL them
--     (opp_definition column) and never blend them into one cross-channel number:
--       email    = Instantly-native `opportunities` ~= ENGAGED (any non-negative),
--                  workspace-grain raw_instantly_workspace_analytics_daily —
--                  the D-1-FINAL source (daily report v2 §1; 07-08 final = 1,320).
--                  Day-of values harden overnight (intraday syncs stop early).
--       sms      = Qwen-strict positive (derived.sms_reply_is_positive_qwen) via
--                  v_sms_workspace_funnel. Channel PAUSED since 2026-07-07
--                  (TCPA/DNC scrub) — near-zero is REAL, not a broken feed.
--       whatsapp = warm-call-queue capture (core.opportunity source='iskra';
--                  vendor-tag -> Haiku classifier, WA-OPP-CLASSIFIER-QA 07-10).
--                  NOT Qwen — semantics differ from SMS.
--   * core.opportunity is CAPTURE-grain (entry into the warm-call queue), NOT
--     channel-native occurrence grain. v_mof_opp_outcomes builds on it BY DESIGN
--     (it is the person-level population that calls/forms/meetings act on);
--     its opps_captured must NEVER be reconciled against v_mof_funnel_daily.opps.
--   * Per-day-distinct email facts (opportunities/unique_replies) are reported
--     per day and are NEVER summed across days as an absolute (30-56% overcount).
--     Consumers that need multi-day email opps must label them a trend.
--   * All legs bounded >= DATE '2026-06-01': every source starts there (workspace
--     analytics 06-01, core.opportunity 06-01/06-23, sheet-era meetings 06-01),
--     and all v_meeting_canonical rows in-window are sheet-source with a
--     populated channel column (no slack-era regex hybrid needed).
--
-- KNOWN DATA ARTIFACTS (documented, not hidden):
--   * WA cohort 2026-07-10 carries a one-time backfill lump (~+410 recovered R0
--     opps stamped opened_at=07-10 by the 07-10 capture fix) — that day's WA
--     opps/opps_captured is inflated vs the true ~25-30/day run rate.
--   * v_meeting_canonical.booked_at_ts is the portal row-ENTRY time with a
--     +1..+4d late-entry tail (v1091 header) — cohort booking windows are
--     therefore conservative (a late-entered booking lands in a later bucket).
--   * Email meetings: workspace attribution via the raw sheet workspace_name
--     free-text mapped by the SAME alias rules as render_daily_v2.ws_alias (the
--     ONE mapper); non-roster / unmappable rows land in '(unattributed)' —
--     visible, never dropped, so Σ meetings == canonical channel totals.
--
-- OFFER DIMENSION (OFFER-TAG-SPEC 2026-07-10): offer is derived from Instantly
-- campaign tags 'Offer: <X>' (raw_instantly_campaign_analytics_daily.tag_labels
-- x raw_instantly_tag_def — the CURRENT v1061 nightly tag surface, NOT the
-- frozen 06-14 core.campaign_sending_tag). Untagged = 'Funding' (zero backfill
-- by design — all existing campaigns are Funding). As of 2026-07-10 ZERO
-- 'Offer:' tags exist, so v_mof_funnel_daily carries offer='Funding' as a
-- constant on every leg; dash.v_mof_campaign_offer is the shipped JOIN POINT —
-- when the first PoC offer campaign launches (~wk of 07-13), extend the email
-- leg to campaign grain through it (documented at the leg).
--
-- Verified on snapshot warehouse_20260710_154218_460.duckdb (pre-flight, view
-- bodies run raw through the query API):
--   * meetings partition EXACT: 2026-07-08 email 224 / total 256; 07-09 email
--     206 / total 226 (== core.v_meeting_canonical channel totals).
--   * email opps 2026-07-08 = 1,320 (workspace-grain final == daily report v2).
--   * SMS opps == derived.sms_reply_is_positive_qwen daily counts (886/590/...).
-- READ-ONLY over existing objects. CREATE OR REPLACE VIEW only.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS dash;

-- ----------------------------------------------------------------------------
-- dash.v_mof_campaign_offer — campaign_id -> offer. THE campaign-tag join point
-- (OFFER-TAG-SPEC). Latest tag_labels per campaign from the nightly campaign-
-- analytics mirror; first 'Offer: <X>' label wins; untagged/never-seen =
-- 'Funding'. Deterministic only — no LLM anywhere in offer attribution.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dash.v_mof_campaign_offer AS
WITH latest AS (
  SELECT campaign_id, workspace_slug, campaign_name, tag_labels,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY date DESC) AS rn
  FROM raw_instantly_campaign_analytics_daily
)
SELECT
  campaign_id,
  workspace_slug,
  campaign_name,
  -- tag_labels is a JSON array string, e.g. ["Offer: HELOC","Reseller Active"];
  -- extract the first 'Offer: ' label. COALESCE(tag,'Funding') per spec §2.
  COALESCE(NULLIF(regexp_extract(COALESCE(tag_labels, ''), 'Offer: ([^"]+)', 1), ''),
           'Funding') AS offer
FROM latest
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- dash.v_mof_funnel_daily — one row per (metric_date x channel x workspace_name).
-- Long-format UNION of per-channel legs; every measure sits at its NATIVE
-- attribution grain (no fabricated joins), so channel totals partition exactly:
--   channel='email'    workspace = core.workspace.name (8-roster) | '(unattributed)' meetings bucket
--   channel='sms'      workspace = Sendivo sub-account ('Renaissance 1/2', ...) — PAUSED
--   channel='whatsapp' workspace = '(channel)' (Iskra has no workspace concept)
--   channel='call'     workspace = '(channel)' — the warm-caller lane (dialed/connected/meetings)
--   channel='form'     workspace = the form surface ('GBC application' | 'Apply-now')
--   channel='other'    workspace = the raw meeting channel ('LinkedIn' | '(no channel)') —
--                      off-channel meetings stay VISIBLE so Σ == canonical day total
--                      (daily-report-v2 hard-rule #6).
-- Column semantics:
--   opps           per-channel definition — READ opp_definition; never cross-sum channels
--   sent/delivered channel volume context (email sent = Instantly; sms/wa = vendor)
--   replies        email = HUMAN replies (Instantly-native unique_replies);
--                  sms/whatsapp = TOTAL inbound replies (no human/auto split exists)
--   dialed         warm-caller dials that ET-day (core.call, all directions counted
--                  outbound+inbound; matches daily report §3 "dials")
--   connected      calls >= 60s (== daily report v2 §3 "connects" definition)
--   form_fills     submitted applications (GBC application.submitted / apply-now rows)
--   meetings       core.v_meeting_canonical by channel (meeting_date grain)
--   channel_status 'PAUSED' renders a zero as EXPECTED, not broken (SMS since
--                  2026-07-07, TCPA/DNC — mirror of render_daily_v2.CHANNEL_STATUS;
--                  flip here + there together when SMS resumes; promote to a
--                  warehouse ops table when ops flips are wired — v2 gap #8).
-- Day grains are leg-native and documented: email/sms/wa facts = source day;
-- calls + forms = ET day (matches daily report v2); meetings = meeting_date.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dash.v_mof_funnel_daily AS
WITH
-- ── email: workspace-grain Instantly-native analytics x sheet meetings ──
email_meet AS (
  SELECT
    meeting_date AS d,
    -- the ONE free-text -> roster-name mapper (mirror of render_daily_v2.ws_alias;
    -- outputs == core.workspace.name for the 8 roster workspaces)
    CASE
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'funding 1%' OR lower(trim(COALESCE(workspace_name,''))) = 'f1' THEN 'Funding 1 (Samuel)'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'funding 2%' OR lower(trim(COALESCE(workspace_name,''))) = 'f2' THEN 'Funding 2 (Ido)'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'funding 3%' OR lower(trim(COALESCE(workspace_name,''))) = 'f3' THEN 'Funding 3 (Leo)'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'funding 4%' OR lower(trim(COALESCE(workspace_name,''))) = 'f4' THEN 'Funding 4 (Sam)'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'funding 5%' OR lower(trim(COALESCE(workspace_name,''))) = 'f5' THEN 'Funding 5 (Eyver)'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'warm%' THEN 'Warm leads'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE 'max%' OR lower(trim(COALESCE(workspace_name,''))) LIKE '%gatekeeper%' THEN 'Max''s workspace'
      WHEN lower(trim(COALESCE(workspace_name,''))) LIKE '%renaissance 1%'
        OR lower(trim(COALESCE(workspace_name,''))) IN ('r1','instantly')
        OR lower(trim(COALESCE(workspace_name,''))) LIKE '%sendivo%' THEN 'Renaissance 1 (Instantly)'
      ELSE '(unattributed)'
    END AS ws,
    COUNT(*) AS meetings
  FROM core.v_meeting_canonical
  WHERE channel = 'Email' AND meeting_date >= DATE '2026-06-01'
  GROUP BY 1, 2
),
email_an AS (
  SELECT
    a.date AS d,
    COALESCE(w.name, a.workspace_slug) AS ws,
    SUM(a.sent)           AS sent,
    SUM(a.opportunities)  AS opps,
    SUM(a.unique_replies) AS replies
  FROM raw_instantly_workspace_analytics_daily a
  LEFT JOIN core.workspace w ON w.slug = a.workspace_slug
  WHERE a.date >= DATE '2026-06-01'
  GROUP BY 1, 2
),
email_leg AS (
  SELECT
    COALESCE(an.d, em.d)   AS metric_date,
    'email'                AS channel,
    'ACTIVE'               AS channel_status,
    COALESCE(an.ws, em.ws) AS workspace_name,
    an.opps                AS opps,
    'instantly_native_opportunities~engaged (workspace-grain, D-1-final)' AS opp_definition,
    an.sent                AS sent,
    CAST(NULL AS BIGINT)   AS delivered,
    an.replies             AS replies,
    CAST(NULL AS BIGINT)   AS dialed,
    CAST(NULL AS BIGINT)   AS connected,
    CAST(NULL AS BIGINT)   AS form_fills,
    em.meetings            AS meetings
  FROM email_an an
  FULL OUTER JOIN email_meet em ON em.d = an.d AND em.ws = an.ws
),
-- ── sms: pre-reconciled per-sub-account funnel (v1025/v1068) — PAUSED ──
sms_leg AS (
  SELECT
    metric_date                            AS metric_date,
    'sms'                                  AS channel,
    'PAUSED'                               AS channel_status,   -- since 2026-07-07 (TCPA/DNC scrub)
    COALESCE(sub_account, '(unattributed)') AS workspace_name,
    opps                                   AS opps,
    'qwen_strict_positive_replies'         AS opp_definition,
    sent                                   AS sent,
    delivered                              AS delivered,
    replies                                AS replies,          -- TOTAL inbound (no human/auto split)
    CAST(NULL AS BIGINT)                   AS dialed,
    CAST(NULL AS BIGINT)                   AS connected,
    CAST(NULL AS BIGINT)                   AS form_fills,
    meetings                               AS meetings
  FROM v_sms_workspace_funnel
  WHERE metric_date >= DATE '2026-06-01'
),
-- ── whatsapp: warm-call-queue captures x Iskra vendor volume x meetings ──
wa_opps AS (
  SELECT CAST(opened_at AS DATE) AS d, COUNT(*) AS opps
  FROM core.opportunity
  WHERE source = 'iskra' AND state <> 'duplicate'
    AND opened_at >= DATE '2026-06-01'
  GROUP BY 1
),
wa_send AS (
  SELECT metric_date AS d, sent, delivered, replies_total AS replies
  FROM main.v_sms_dash_wa_daily
  WHERE channel = 'whatsapp' AND metric_date >= DATE '2026-06-01'
),
wa_meet AS (
  SELECT meeting_date AS d, COUNT(*) AS meetings
  FROM core.v_meeting_canonical
  WHERE channel = 'WhatsApp' AND meeting_date >= DATE '2026-06-01'
  GROUP BY 1
),
wa_leg AS (
  SELECT
    COALESCE(o.d, s.d, m.d) AS metric_date,
    'whatsapp'              AS channel,
    'ACTIVE'                AS channel_status,
    '(channel)'             AS workspace_name,
    o.opps                  AS opps,
    'warm_call_queue_capture (Iskra vendor-tag->Haiku / 2026-07-10 carries a one-time +~410 R0-backfill lump)' AS opp_definition,
    s.sent                  AS sent,
    s.delivered             AS delivered,
    s.replies               AS replies,
    CAST(NULL AS BIGINT)    AS dialed,
    CAST(NULL AS BIGINT)    AS connected,
    CAST(NULL AS BIGINT)    AS form_fills,
    m.meetings              AS meetings
  FROM wa_opps o
  FULL OUTER JOIN wa_send s ON s.d = o.d
  FULL OUTER JOIN wa_meet m ON m.d = COALESCE(o.d, s.d)
),
-- ── call: the warm-caller lane (ET day, == daily report v2 §3 definitions) ──
call_daily AS (
  SELECT
    CAST(occurred_at AT TIME ZONE 'America/New_York' AS DATE) AS d,
    COUNT(*) AS dialed,
    COUNT(*) FILTER (WHERE duration_seconds >= 60) AS connected
  FROM core.call
  WHERE occurred_at >= DATE '2026-06-01'
  GROUP BY 1
),
call_meet AS (
  SELECT meeting_date AS d, COUNT(*) AS meetings
  FROM core.v_meeting_canonical
  WHERE channel = 'Call' AND meeting_date >= DATE '2026-06-01'
  GROUP BY 1
),
call_leg AS (
  SELECT
    COALESCE(c.d, m.d)   AS metric_date,
    'call'               AS channel,
    'ACTIVE'             AS channel_status,
    '(channel)'          AS workspace_name,
    CAST(NULL AS BIGINT) AS opps,
    CAST(NULL AS VARCHAR) AS opp_definition,
    CAST(NULL AS BIGINT) AS sent,
    CAST(NULL AS BIGINT) AS delivered,
    CAST(NULL AS BIGINT) AS replies,
    c.dialed             AS dialed,
    c.connected          AS connected,
    CAST(NULL AS BIGINT) AS form_fills,
    m.meetings           AS meetings
  FROM call_daily c
  FULL OUTER JOIN call_meet m ON m.d = c.d
),
-- ── form: submitted applications (ET day; a fill ~= meeting-equivalent) ──
form_leg AS (
  SELECT
    CAST(submitted_at AT TIME ZONE 'America/New_York' AS DATE) AS metric_date,
    'form'            AS channel,
    'ACTIVE'          AS channel_status,
    'GBC application' AS workspace_name,
    CAST(NULL AS BIGINT) AS opps, CAST(NULL AS VARCHAR) AS opp_definition,
    CAST(NULL AS BIGINT) AS sent, CAST(NULL AS BIGINT) AS delivered,
    CAST(NULL AS BIGINT) AS replies, CAST(NULL AS BIGINT) AS dialed,
    CAST(NULL AS BIGINT) AS connected,
    COUNT(*)          AS form_fills,
    CAST(NULL AS BIGINT) AS meetings
  FROM raw_comms_gbc_application
  WHERE submitted_at >= DATE '2026-06-01'
  GROUP BY 1
  UNION ALL
  SELECT
    CAST(created_at AT TIME ZONE 'America/New_York' AS DATE) AS metric_date,
    'form', 'ACTIVE', 'Apply-now',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    COUNT(*),
    NULL
  FROM raw_comms_lead_application
  WHERE created_at >= DATE '2026-06-01'
  GROUP BY 1
),
-- ── other: off-channel meetings stay visible (Σ meetings == canonical total) ──
other_leg AS (
  SELECT
    meeting_date AS metric_date,
    'other'      AS channel,
    'ACTIVE'     AS channel_status,
    COALESCE(channel, '(no channel)') AS workspace_name,
    CAST(NULL AS BIGINT) AS opps, CAST(NULL AS VARCHAR) AS opp_definition,
    CAST(NULL AS BIGINT) AS sent, CAST(NULL AS BIGINT) AS delivered,
    CAST(NULL AS BIGINT) AS replies, CAST(NULL AS BIGINT) AS dialed,
    CAST(NULL AS BIGINT) AS connected, CAST(NULL AS BIGINT) AS form_fills,
    COUNT(*)     AS meetings
  FROM core.v_meeting_canonical
  WHERE meeting_date >= DATE '2026-06-01'
    AND (channel IS NULL OR channel NOT IN ('Email', 'SMS', 'WhatsApp', 'Call'))
  GROUP BY 1, 4
)
SELECT
  metric_date, channel, channel_status, workspace_name,
  -- OFFER: constant 'Funding' until the first 'Offer:' campaign tag exists
  -- (0 today). Join point = dash.v_mof_campaign_offer; when PoC offer campaigns
  -- launch, extend the email leg to campaign grain through it (header note).
  'Funding' AS offer,
  opps, opp_definition, sent, delivered, replies,
  dialed, connected, form_fills, meetings
FROM (
  SELECT * FROM email_leg
  UNION ALL SELECT * FROM sms_leg
  UNION ALL SELECT * FROM wa_leg
  UNION ALL SELECT * FROM call_leg
  UNION ALL SELECT * FROM form_leg
  UNION ALL SELECT * FROM other_leg
);

-- ----------------------------------------------------------------------------
-- dash.v_mof_opp_outcomes — per (cohort_date x channel) conversion outcomes of
-- the warm-call-queue opp population: % booked <=24h/<=72h/<=7d/any, % called,
-- % connected, % form-filled, and how fast (median hours to book).
--
-- POPULATION = core.opportunity (CAPTURE grain — entry into the warm-call
-- queue; state <> 'duplicate'). This is the person-level population the
-- engine acts on. It is NOT the channel-native opp count:
--   email 'instantly' captures = the subset routed to calling (plus backfills)
--   — do NOT reconcile opps_captured against v_mof_funnel_daily.opps.
--
-- JOIN COVERAGE (honest, per channel — measured 14d on 2026-07-10):
--   identity: email ~100% have lead_email | sms ~72% | whatsapp ~12%.
--   BOOKING joins are EMAIL-KEYED ONLY (v_meeting_canonical carries lead_email,
--   no phone) -> rows without an email are counted in booking_unjoinable and
--   ALL pct_booked_* are % of email_joinable, never of the total (phone-keyed
--   rows are never silently dropped — they are the unjoinable bucket).
--   CALL + FORM joins use email OR phone (right-10 digit match; forms and
--   core.call both carry phone), so pct_called/pct_form are % of ALL captures.
-- TIMING: outcome must be AT/AFTER capture (>= opened_at). booked_at_ts has a
-- +1..+4d entry-lag tail (v1091) -> windows are conservative. Cohorts younger
-- than 7 days have an incomplete 7d window — filter cohort_7d_complete.
-- 'called' = the queue's own called_at stamp (raw_comms_call_opportunity;
-- talked_to_human/last_call_at are dead columns there — verified all-NULL —
-- so 'connected' comes from core.call >= 60s, the daily-report §3 definition).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dash.v_mof_opp_outcomes AS
WITH spine AS (
  SELECT
    co.opportunity_id,
    CASE co.source WHEN 'instantly' THEN 'email'
                   WHEN 'sendivo'   THEN 'sms'
                   WHEN 'iskra'     THEN 'whatsapp'
                   ELSE co.source END AS channel,
    CAST(co.opened_at AS DATE) AS cohort_date,
    co.opened_at,
    NULLIF(lower(trim(co.lead_email)), '') AS em,
    NULLIF(right(regexp_replace(COALESCE(rco.phone_e164, ''), '[^0-9]', '', 'g'), 10), '') AS ph10,
    rco.called_at,
    rco.close_lead_id
  FROM core.opportunity co
  LEFT JOIN raw_comms_call_opportunity rco
    ON rco.source = co.source AND CAST(rco.id AS VARCHAR) = co.source_event_id
  WHERE co.state <> 'duplicate'
    AND co.opened_at IS NOT NULL
    AND co.opened_at >= DATE '2026-06-01'
),
-- first meeting booked at/after capture (email-keyed; MIN kills meeting fan-out)
first_book AS (
  SELECT s.opportunity_id, MIN(mt.booked_at_ts) AS first_booked_ts
  FROM spine s
  JOIN core.v_meeting_canonical mt
    ON lower(trim(mt.lead_email)) = s.em
   AND mt.booked_at_ts >= s.opened_at
  WHERE s.em IS NOT NULL
  GROUP BY 1
),
-- first form fill at/after capture (email OR phone keyed)
form_events AS (
  SELECT NULLIF(lower(trim(applicant_email)), '') AS em,
         NULLIF(right(regexp_replace(COALESCE(applicant_phone, ''), '[^0-9]', '', 'g'), 10), '') AS ph10,
         submitted_at AS ts
  FROM raw_comms_gbc_application
  WHERE submitted_at IS NOT NULL
  UNION ALL
  SELECT NULLIF(lower(trim(email)), ''),
         NULLIF(right(regexp_replace(COALESCE(prospect_number, ''), '[^0-9]', '', 'g'), 10), ''),
         created_at
  FROM raw_comms_lead_application
),
first_form AS (
  SELECT opportunity_id, MIN(ts) AS first_form_ts
  FROM (
    SELECT s.opportunity_id, f.ts
    FROM spine s JOIN form_events f ON f.em = s.em
    WHERE s.em IS NOT NULL AND f.ts >= s.opened_at
    UNION ALL
    SELECT s.opportunity_id, f.ts
    FROM spine s JOIN form_events f ON f.ph10 = s.ph10
    WHERE s.ph10 IS NOT NULL AND f.ts >= s.opened_at
  )
  GROUP BY 1
),
-- first connected call (>= 60s) at/after capture (close_lead_id OR phone keyed)
call_events AS (
  SELECT close_lead_id,
         NULLIF(right(regexp_replace(COALESCE(phone_e164, ''), '[^0-9]', '', 'g'), 10), '') AS ph10,
         occurred_at
  FROM core.call
  WHERE duration_seconds >= 60
),
first_connect AS (
  SELECT opportunity_id, MIN(occurred_at) AS first_connect_ts
  FROM (
    SELECT s.opportunity_id, c.occurred_at
    FROM spine s JOIN call_events c ON c.close_lead_id = s.close_lead_id
    WHERE s.close_lead_id IS NOT NULL AND c.occurred_at >= s.opened_at
    UNION ALL
    SELECT s.opportunity_id, c.occurred_at
    FROM spine s JOIN call_events c ON c.ph10 = s.ph10
    WHERE s.ph10 IS NOT NULL AND c.occurred_at >= s.opened_at
  )
  GROUP BY 1
)
SELECT
  s.cohort_date,
  s.channel,
  COUNT(*)                                          AS opps_captured,
  COUNT(s.em)                                       AS email_joinable,
  COUNT(*) - COUNT(s.em)                            AS booking_unjoinable,   -- phone-only: bookings can't match (email-keyed canonical)
  ROUND(100.0 * COUNT(s.em) / COUNT(*), 1)          AS pct_email_joinable,
  -- booking windows (counts; conservative — see header)
  COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 24 HOUR) AS booked_24h,
  COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 72 HOUR) AS booked_72h,
  COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 7 DAY)   AS booked_7d,
  COUNT(fb.first_booked_ts)                                                   AS booked_any,
  -- booking rates = % of email_joinable (NEVER of total — unjoinable is visible above)
  ROUND(100.0 * COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 24 HOUR)
        / NULLIF(COUNT(s.em), 0), 1) AS pct_booked_24h,
  ROUND(100.0 * COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 72 HOUR)
        / NULLIF(COUNT(s.em), 0), 1) AS pct_booked_72h,
  ROUND(100.0 * COUNT(*) FILTER (WHERE fb.first_booked_ts < s.opened_at + INTERVAL 7 DAY)
        / NULLIF(COUNT(s.em), 0), 1) AS pct_booked_7d,
  ROUND(100.0 * COUNT(fb.first_booked_ts) / NULLIF(COUNT(s.em), 0), 1) AS pct_booked_any,
  ROUND(MEDIAN(date_diff('minute', s.opened_at, fb.first_booked_ts)) / 60.0, 1) AS median_hours_to_book,
  -- call + form outcomes (% of ALL captures — these joins carry phone too)
  COUNT(s.called_at)                                 AS called_n,
  ROUND(100.0 * COUNT(s.called_at) / COUNT(*), 1)    AS pct_called,
  COUNT(fc.first_connect_ts)                         AS connected_n,
  ROUND(100.0 * COUNT(fc.first_connect_ts) / COUNT(*), 1) AS pct_connected,
  COUNT(ff.first_form_ts)                            AS form_n,
  ROUND(100.0 * COUNT(ff.first_form_ts) / COUNT(*), 1) AS pct_form,
  (s.cohort_date <= current_date - 7)                AS cohort_7d_complete
FROM spine s
LEFT JOIN first_book    fb ON fb.opportunity_id = s.opportunity_id
LEFT JOIN first_form    ff ON ff.opportunity_id = s.opportunity_id
LEFT JOIN first_connect fc ON fc.opportunity_id = s.opportunity_id
GROUP BY 1, 2;
