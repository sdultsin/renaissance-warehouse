-- Omnichannel overview + WhatsApp per-launch performance [2026-06-23]
--
-- Adds the cross-channel "real opportunities" column the omni parity layer (DDL 89) was missing, and
-- a per-launch (pipeline) WhatsApp funnel for copy attribution. Additive VIEWS only — no tables, no
-- ingest, does not touch the live email path.
--
-- Depends on: v_omnichannel_performance (DDL 89), core.opportunity, v_whatsapp_conversation_performance (DDL 82)

-- @gate: additive_views

-- v_omnichannel_overview — the single per-channel side-by-side Sam reads: sent · replies · positive ·
-- OPPORTUNITIES · meetings, daily × channel. "warm_call_opps" = the REAL cross-channel opportunities
-- that reached the warm-call queue (core.opportunity, the comms/Close mirror: instantly→email,
-- sendivo→sms, iskra→whatsapp), excluding dedup-artifact rows. This is the opp grain the callers
-- actually work, distinct from the channel-native positive_replies signal (which stays as-is).
--
-- deals_won is intentionally LEFT NULL: funded deals cannot be reliably attributed to a channel today
-- (core.deal_funded.channel is NULL for all funded rows, and they join to core.meeting with a NULL
-- channel) — per the 100%-or-wipe data rule we do NOT ship a misleading sparse column. Revisit when
-- the deal→channel key exists.
CREATE OR REPLACE VIEW v_omnichannel_overview AS
WITH opps AS (
  SELECT
    CASE source WHEN 'instantly' THEN 'email' WHEN 'sendivo' THEN 'sms' WHEN 'iskra' THEN 'whatsapp' END AS channel,
    CAST(opened_at AS DATE) AS metric_date,
    count(*) AS warm_call_opps
  FROM core.opportunity
  WHERE state <> 'duplicate' AND opened_at IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  b.channel,
  b.metric_date,
  b.sent,
  b.delivered,
  b.failed,
  b.replies_total,
  b.replies_human,
  b.replies_auto,
  b.positive_replies,
  b.negative_replies,
  b.opt_outs,
  COALESCE(o.warm_call_opps, 0) AS warm_call_opps,   -- the cross-channel opp count (Close warm-call queue)
  b.meetings_booked,
  b.cost_usd,
  b.delivery_rate,
  b.reply_rate,
  b.positive_rate,
  b.positive_signal
FROM v_omnichannel_performance b
LEFT JOIN opps o
  ON o.channel = b.channel AND o.metric_date = b.metric_date;

-- v_whatsapp_pipeline_performance — per-launch (Iskra pipeline_id = the only campaign-ish dimension)
-- WhatsApp funnel: the copy-attribution grain for WhatsApp (conversations → replies → positive →
-- meetings booked), rolled up from the conversation-grain view. pipeline_id is a coarse sending WAVE,
-- not a per-variant id (no per-send template id on our key — Arseny vendor ask); good enough to
-- compare active vs dead waves. NULL pipeline_id = the small unattributed residual.
CREATE OR REPLACE VIEW v_whatsapp_pipeline_performance AS
SELECT
  pipeline_id,
  count(*)                                  AS conversations,
  count(*) FILTER (WHERE replied)           AS replied,
  count(*) FILTER (WHERE is_positive_reply) AS positive_replies,
  count(*) FILTER (WHERE meeting_booked)    AS meetings_booked,
  round(count(*) FILTER (WHERE replied)           * 1.0 / NULLIF(count(*), 0), 4) AS reply_rate,
  round(count(*) FILTER (WHERE is_positive_reply) * 1.0 / NULLIF(count(*), 0), 4) AS positive_rate
FROM v_whatsapp_conversation_performance
GROUP BY pipeline_id;
