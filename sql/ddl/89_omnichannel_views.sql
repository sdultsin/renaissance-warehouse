-- 89 — Omnichannel performance parity layer (Email + SMS + WhatsApp), daily × channel grain.
-- @gate: add
-- Depends on 04 (raw_pipeline_campaign_daily_metrics) 34 (v_sms_campaign_performance)
--   55 (core.instantly_bounce_daily) 65 (core.meeting channel) 82 (raw_iskra_messages/meetings)
--
-- GOAL (Sam, 2026-06-20): bring SMS (Sendivo) and WhatsApp (Iskra) up to the Instantly/email
-- funnel-sync bar in DuckDB, structured so each channel can be pulled INDEPENDENTLY (channel-pure
-- views) AND COMBINED (one unified view), with the Google Sheet staying the MEETING source-of-truth
-- and the platforms used to cross-reference/verify it (v_meeting_capture_reconcile).
--
-- ARCHITECTURE DECISION (resolved empirically 2026-06-20, the "pivotal unknown"):
--   * Iskra carries ZERO SMS traffic: GET /v1/messages/sms = 0 rows; stats channel=sms all-zero in
--     every window; WhatsApp channel full (50,844 sent MTD). => the SMS funnel comes from SENDIVO,
--     NOT a channel=sms parameterization of Iskra. Iskra stays WhatsApp-only.
--   * The SMS message/reply ingest ALREADY EXISTS (DDL 25/34/52/66; entities/sendivo*.py): per-
--     campaign outbound funnel (raw_sendivo_campaign_daily) + recovered inbound replies
--     (raw_sendivo_inbound, opt-out flagged). So this DDL is NOT new ingest — it is the VIEW layer
--     that lines the three channels up column-for-column.
--
-- GRAIN. The three platforms have different native grains (email: date×campaign; SMS: date×campaign;
-- WhatsApp: agency snapshot per window — Iskra exposes NO campaign/blast object on our key). The
-- only grain ALL THREE share cleanly is DAILY × CHANNEL, so that is the parity grain here. Campaign-
-- level detail stays in the native per-channel views (v_kpi_email, v_sms_campaign_performance);
-- WhatsApp has none to give (documented gap → Arseny).
--
-- NON-BREAKING NAMING. The existing v_sms_performance (DDL 25, agency delivery-metrics tile) and
-- v_whatsapp_performance (DDL 82, Iskra stats tile) are LEFT UNTOUCHED — they remain the native
-- per-platform surfaces and the reconciliation SoT rows. This file ADDS a parallel `v_omni_*`
-- contract on top of them; clobbering the existing names would break the dash.* consumers (DDL 86)
-- and the publisher. All objects here are CREATE OR REPLACE VIEW (idempotent; setup_db globs
-- sql/ddl/*.sql in order, so 89 applies after its deps 25/34/62/65/66/81/82).
--
-- THE PARITY CONTRACT (every v_omni_*_performance view emits exactly these columns, same order):
--   channel, metric_date, sent, delivered, failed, replies_total, replies_human, replies_auto,
--   positive_replies, negative_replies, opt_outs, meetings_booked, deals_won, cost_usd,
--   delivery_rate, reply_rate, positive_rate, positive_signal
-- A column a channel genuinely cannot supply is surfaced as NULL with the reason documented inline
-- (that NULL map IS the gap analysis — see deliverables/.../GAP-ANALYSIS.md). Rates use /sent as the
-- common denominator so the channels are comparable (native per-channel views may use /delivered).
--
-- positive_signal documents HOW positive_replies is derived per channel — it is HETEROGENEOUS by
-- design and must NOT be cross-summed naively:
--   email    -> 'instantly_unique_opportunities'  (Instantly-native opp = the email truth)
--   sms      -> 'sendivo_not_opt_out_INTERIM'      (replied AND not STOP — OVERCOUNTS: includes
--                                                    Block/Not-relevant/wrong-number. Real SMS
--                                                    sentiment is a Phase-2 LLM-classifier gap.)
--   whatsapp -> 'iskra_ai_reply_sentiment_positive' (Iskra's AI conversation sentiment tag)

-- =====================================================================================
-- EMAIL — v_omni_email_performance. Source: raw_pipeline_campaign_daily_metrics (Instantly via the
-- pipeline-Supabase mirror) + core.instantly_bounce_daily (bounces) + core.meeting (sheet SoT).
-- Email is the parity BAR: it has native human/auto reply split + Instantly opportunities.
-- =====================================================================================
CREATE OR REPLACE VIEW v_omni_email_performance AS
WITH sends AS (
  SELECT date AS metric_date,
         sum(sent)                        AS sent,
         sum(unique_replies)              AS replies_human,
         sum(unique_replies_automatic)    AS replies_auto,
         sum(unique_opportunities)        AS opportunities
  FROM raw_pipeline_campaign_daily_metrics
  GROUP BY 1
),
bounces AS (
  SELECT date AS metric_date, sum(bounced) AS bounced
  FROM core.instantly_bounce_daily GROUP BY 1
),
mtg AS (  -- meetings = Google Sheet SoT, channel='Email' (the current channel-aware sync)
  SELECT posted_at::date AS metric_date, count(*) AS meetings
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'Email'
  GROUP BY 1
),
-- date spine: union all dates from the three sources so a meeting-only or bounce-only day still
-- surfaces (keeps email structurally symmetric with the SMS/WhatsApp views' FULL OUTER JOINs).
dates AS (
  SELECT metric_date FROM sends
  UNION SELECT metric_date FROM bounces
  UNION SELECT metric_date FROM mtg
)
SELECT
  'email'                                                  AS channel,
  d.metric_date,
  s.sent,
  (s.sent - COALESCE(b.bounced, 0))                        AS delivered,   -- email "delivered" = sent − bounces; NULL on a no-send day
  COALESCE(b.bounced, 0)                                   AS failed,      -- bounces
  (COALESCE(s.replies_human,0) + COALESCE(s.replies_auto,0)) AS replies_total,
  s.replies_human,                                                          -- Instantly-native human reply
  s.replies_auto,                                                           -- Instantly-native auto reply
  s.opportunities                                          AS positive_replies,  -- Instantly opp = email positive truth
  CAST(NULL AS BIGINT)                                     AS negative_replies,  -- NULL: Instantly exposes no negative tag
  CAST(NULL AS BIGINT)                                     AS opt_outs,          -- NULL: opt-out is an SMS-only concept
  COALESCE(m.meetings, 0)                                  AS meetings_booked,
  CAST(NULL AS BIGINT)                                     AS deals_won,         -- NULL: funded/deals = separate BoF instrument (Close/sheet), not Instantly
  CAST(NULL AS DOUBLE)                                     AS cost_usd,          -- NULL: email cost lives in core.cost_ledger at a different grain
  CAST(s.sent - COALESCE(b.bounced,0) AS DOUBLE) / nullif(s.sent,0)  AS delivery_rate,
  CAST(COALESCE(s.replies_human,0)+COALESCE(s.replies_auto,0) AS DOUBLE) / nullif(s.sent,0) AS reply_rate,
  CAST(s.opportunities AS DOUBLE) / nullif(s.sent,0)       AS positive_rate,
  'instantly_unique_opportunities'                         AS positive_signal
FROM dates d
LEFT JOIN sends s   ON s.metric_date = d.metric_date
LEFT JOIN bounces b ON b.metric_date = d.metric_date
LEFT JOIN mtg m     ON m.metric_date = d.metric_date;

-- =====================================================================================
-- SMS — v_omni_sms_performance. Source: v_sms_campaign_performance (Sendivo /sms/logs outbound +
-- recovered inbound replies) aggregated to day + core.meeting (sheet SoT, channel='SMS').
-- GAP (documented): positive_replies is INTERIM (not-opt-out) — Sendivo has no sentiment, and the
-- cross-channel LLM positive/intent classifier is email-only today. negative_replies uses opt_outs
-- as the only hard-negative signal available. Human/auto split is NULL (not classified for SMS).
-- =====================================================================================
CREATE OR REPLACE VIEW v_omni_sms_performance AS
WITH sms AS (
  SELECT metric_date,
         sum(sent)             AS sent,
         sum(delivered)        AS delivered,
         sum(failed)           AS failed,
         sum(replies)          AS replies_total,
         sum(opt_outs)         AS opt_outs,
         sum(positive_replies) AS not_opt_out_replies,   -- INTERIM positive proxy
         sum(cost_usd)         AS cost_usd
  FROM v_sms_campaign_performance
  GROUP BY 1
),
mtg AS (
  SELECT posted_at::date AS metric_date, count(*) AS meetings
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'SMS'
  GROUP BY 1
)
SELECT
  'sms'                                              AS channel,
  COALESCE(s.metric_date, m.metric_date)             AS metric_date,
  s.sent,
  s.delivered,
  s.failed,
  s.replies_total,
  CAST(NULL AS BIGINT)                               AS replies_human,   -- NULL: SMS replies not human/auto-classified (Phase-2 gap)
  CAST(NULL AS BIGINT)                               AS replies_auto,    -- NULL: same
  s.not_opt_out_replies                              AS positive_replies,-- INTERIM (see header) — overcounts
  s.opt_outs                                         AS negative_replies,-- opt-out = the only hard SMS negative signal
  s.opt_outs                                         AS opt_outs,
  COALESCE(m.meetings, 0)                            AS meetings_booked,
  CAST(NULL AS BIGINT)                               AS deals_won,       -- NULL: separate BoF instrument
  s.cost_usd,
  CAST(s.delivered AS DOUBLE) / nullif(s.sent,0)     AS delivery_rate,
  CAST(s.replies_total AS DOUBLE) / nullif(s.sent,0) AS reply_rate,
  CAST(s.not_opt_out_replies AS DOUBLE) / nullif(s.sent,0) AS positive_rate,
  'sendivo_not_opt_out_INTERIM'                      AS positive_signal
FROM sms s
FULL OUTER JOIN mtg m ON m.metric_date = s.metric_date;

-- =====================================================================================
-- WHATSAPP — v_omni_whatsapp_performance. Source: raw_iskra_messages (per-message; reconciles
-- 0-delta vs Iskra stats at the window level) for the send/reply funnel + raw_iskra_meetings
-- (Iskra AI sentiment/meeting tags) for positive/negative + core.meeting (sheet SoT, channel=
-- 'WhatsApp') for meetings_booked. WhatsApp HAS conversation sentiment (its parity strength) but
-- has NO campaign object on our API key and NO auto-reply concept.
-- =====================================================================================
CREATE OR REPLACE VIEW v_omni_whatsapp_performance AS
WITH msg AS (
  SELECT (created_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE direction='outbound')                                AS sent,
         count(*) FILTER (WHERE direction='outbound' AND status IN ('delivered','read')) AS delivered,
         count(*) FILTER (WHERE direction='outbound' AND status = 'failed')          AS failed,
         count(*) FILTER (WHERE direction='inbound')                                 AS replies_total
  FROM raw_iskra_messages
  GROUP BY 1
),
sent_tag AS (  -- Iskra AI conversation sentiment, attributed by tag day
  SELECT (tagged_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE reply_sentiment='positive') AS positive_replies,
         count(*) FILTER (WHERE reply_sentiment='negative') AS negative_replies
  FROM raw_iskra_meetings WHERE tagged_at IS NOT NULL
  GROUP BY 1
),
mtg AS (
  SELECT posted_at::date AS metric_date, count(*) AS meetings
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'WhatsApp'
  GROUP BY 1
)
SELECT
  'whatsapp'                                          AS channel,
  COALESCE(g.metric_date, t.metric_date, m.metric_date) AS metric_date,
  g.sent,
  g.delivered,
  g.failed,
  g.replies_total,
  CAST(NULL AS BIGINT)                                AS replies_human,   -- NULL: WhatsApp inbound has no human/auto flag on our key
  CAST(NULL AS BIGINT)                                AS replies_auto,    -- NULL: same
  t.positive_replies,                                                      -- Iskra AI sentiment = WhatsApp positive truth
  t.negative_replies,
  CAST(NULL AS BIGINT)                                AS opt_outs,        -- NULL: opt-out is an SMS-only concept
  COALESCE(m.meetings, 0)                             AS meetings_booked,
  CAST(NULL AS BIGINT)                                AS deals_won,       -- NULL: Iskra stats.deals_won=0; funded = separate BoF instrument
  CAST(NULL AS DOUBLE)                                AS cost_usd,        -- NULL: WhatsApp cost not in warehouse (no per-message price on our key)
  CAST(g.delivered AS DOUBLE) / nullif(g.sent,0)      AS delivery_rate,
  CAST(g.replies_total AS DOUBLE) / nullif(g.sent,0)  AS reply_rate,
  CAST(t.positive_replies AS DOUBLE) / nullif(g.sent,0) AS positive_rate,
  'iskra_ai_reply_sentiment_positive'                 AS positive_signal
FROM msg g
FULL OUTER JOIN sent_tag t ON t.metric_date = g.metric_date
FULL OUTER JOIN mtg m      ON m.metric_date = COALESCE(g.metric_date, t.metric_date);

-- =====================================================================================
-- UNIFIED — v_omnichannel_performance. UNION of the three channel-pure views (identical contract),
-- with a `channel` column so dashboards can slice ONE channel (disconnected by choice) or combine
-- ALL THREE (connected). This is the single cross-channel funnel surface.
-- =====================================================================================
CREATE OR REPLACE VIEW v_omnichannel_performance AS
SELECT * FROM v_omni_email_performance
UNION ALL
SELECT * FROM v_omni_sms_performance
UNION ALL
SELECT * FROM v_omni_whatsapp_performance;

-- =====================================================================================
-- MEETING RECONCILE — v_meeting_capture_reconcile (THE SoT CONTRACT). The Google Sheet stays the
-- meeting source-of-truth; this view CROSS-REFERENCES the sheet against each platform's own meeting
-- signal per channel × day so the GAP can be CHASED toward sheet completeness. It does NOT, and must
-- not, auto-write platform meetings into core.meeting (that would make platforms co-sources and
-- contradict "sheet is SoT"). Auto-supplement is a Sam-gated Phase-2 option, surfaced not built.
--
-- platform_meetings availability (this IS the per-channel meeting-verification gap):
--   email    -> NULL  (Instantly exposes no meeting object — sheet is the sole source)
--   sms      -> NULL  (Sendivo API exposes no meeting/conversation/deal endpoint — /conversations,
--                      /inbox, deal-status all 404 on our key — sheet is the sole source)
--   whatsapp -> count(raw_iskra_meetings where meeting_status='booked'), by tag day (Iskra AI tag)
-- So today only WhatsApp has an INDEPENDENT platform meeting count to verify the sheet against.
-- =====================================================================================
CREATE OR REPLACE VIEW v_meeting_capture_reconcile AS
WITH sheet AS (
  SELECT channel,
         lower(channel)        AS channel_key,
         posted_at::date       AS metric_date,
         count(*)              AS sheet_meetings
  FROM core.meeting
  WHERE is_duplicate_of IS NULL AND source = 'sheet'
    AND channel IN ('Email','SMS','WhatsApp','Call','LinkedIn')
  GROUP BY 1,2,3
),
wa_platform AS (
  SELECT (tagged_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE meeting_status='booked') AS platform_meetings
  FROM raw_iskra_meetings WHERE tagged_at IS NOT NULL
  GROUP BY 1
)
SELECT
  s.metric_date,
  s.channel,
  s.sheet_meetings,
  CASE WHEN s.channel_key = 'whatsapp' THEN COALESCE(w.platform_meetings, 0)
       ELSE CAST(NULL AS BIGINT) END                         AS platform_meetings,
  CASE WHEN s.channel_key = 'whatsapp'
       THEN s.sheet_meetings - COALESCE(w.platform_meetings, 0)
       ELSE CAST(NULL AS BIGINT) END                         AS sheet_minus_platform,
  CASE
    WHEN s.channel_key = 'whatsapp' THEN 'iskra_ai_meeting_status_booked'
    WHEN s.channel_key = 'email'    THEN 'no_platform_meeting_object (Instantly) — sheet is sole source'
    WHEN s.channel_key = 'sms'      THEN 'no_platform_meeting_endpoint (Sendivo 404) — sheet is sole source'
    ELSE 'no_platform_meeting_source — sheet is sole source'
  END                                                         AS platform_meeting_source
FROM sheet s
LEFT JOIN wa_platform w
  ON s.channel_key = 'whatsapp' AND w.metric_date = s.metric_date;
