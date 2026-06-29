-- v_omnichannel_overview_by_offer — repoint the SMS leg to the copy-classified offer map [2026-06-29]
--
-- DDL 1012 added the SMS leg but derived its offer from raw_comms_brand.offer_type, which is WRONG:
-- ~81% NULL (760/937 brands) and mis-labels funding brands as pre_ipo ("FUNDING 4 YOU LLC" tagged
-- pre_ipo) — sub-account / campaign-name / brand are all throwaway funding-skinned names with no offer
-- signal; the only reliable signal is the message COPY. This repoints the SMS leg to the
-- copy-classified core.sms_campaign_offer (PR #101, the offer-attribution chat's map):
--   * sends   → v_sms_campaign_performance.campaign_id = core.sms_campaign_offer.campaign_id → offer
--   * replies → raw_sendivo_inbound.our_number ≈ brand.sender_number → sendivo_campaign_id (brand registry,
--               number<->campaign linkage only) → core.sms_campaign_offer.campaign_id → offer
-- Both fall back to '(offer-unknown)' when the campaign isn't classified, so per-offer SUMs still
-- reconcile to the blended totals. Email + WhatsApp legs are UNCHANGED.
--
-- SMS MEETINGS cannot be offer-split (verified 2026-06-24): the Funding Form has no brand/phone key
-- and Sendivo opps are phone-first with sparse email (0 overlap with the meeting emails), so an SMS
-- meeting can't be joined to its originating offer. They roll up under '(offer-unknown)' — NOT
-- mis-assigned to funding. The real fix is an offer field on the Funding Form (Grace) — flagged.
-- Email + WhatsApp meetings remain offer-attributed (campaign_id / WhatsApp=100%-funding).
--
-- Pattern unchanged from 1009: each metric in its own (offer × date) CTE, then a per-channel KEY
-- SPINE LEFT JOINs them so no meeting/positive is dropped. Mirrors v_omni_sms_performance's inbound
-- dedup + qwen positive join, plus the offer dimension.
--
-- @gate: add
-- Depends on 1012 1044
-- Depends on: v_omnichannel_overview_by_offer (DDL 1012, the SMS leg this replaces), core.sms_campaign_offer
--   (DDL 1044, the copy-classified offer map), v_sms_campaign_performance, raw_sendivo_inbound,
--   raw_comms_brand (number<->campaign bridge only), derived.sms_reply_is_positive_qwen, core.meeting

CREATE OR REPLACE VIEW v_omnichannel_overview_by_offer AS
WITH email_send AS (
  SELECT COALESCE(c.offer, '(unmapped)') AS offer, m.date AS metric_date,
         sum(m.sent) AS sent, sum(m.unique_replies) AS replies_human,
         sum(m.unique_replies_automatic) AS replies_auto, sum(m.unique_opportunities) AS positive_replies
  FROM raw_pipeline_campaign_daily_metrics m
  LEFT JOIN core.campaign c ON c.campaign_id = m.campaign_id
  GROUP BY 1, 2
),
email_mtg AS (
  SELECT COALESCE(c.offer, COALESCE(NULLIF(mt.offer, ''), '(unmapped)')) AS offer,
         CAST(mt.posted_at AS DATE) AS metric_date, count(*) AS meetings_booked
  FROM core.meeting mt
  LEFT JOIN core.campaign c ON c.campaign_id = mt.campaign_id
  WHERE mt.is_duplicate_of IS NULL AND mt.source = 'sheet' AND mt.channel = 'Email'
  GROUP BY 1, 2
),
email_keys AS (SELECT offer, metric_date FROM email_send UNION SELECT offer, metric_date FROM email_mtg),
email_final AS (
  SELECT 'email' AS channel, k.offer, k.metric_date,
         COALESCE(s.sent, 0) AS sent, s.replies_human, s.replies_auto,
         COALESCE(s.positive_replies, 0) AS positive_replies, COALESCE(mt.meetings_booked, 0) AS meetings_booked
  FROM email_keys k
  LEFT JOIN email_send s  USING (offer, metric_date)
  LEFT JOIN email_mtg  mt USING (offer, metric_date)
),
-- ── WhatsApp ──
wa_send AS (
  SELECT COALESCE(co.offer, CASE WHEN co.offer_kind IN ('operational','test') THEN '(non-sales)' ELSE '(unmapped)' END) AS offer,
         CAST(timezone('UTC', msg.created_at) AS DATE) AS metric_date,
         count(*) FILTER (WHERE msg.direction = 'outbound') AS sent
  FROM raw_iskra_messages msg
  LEFT JOIN core.v_whatsapp_conversation_offer co ON co.conversation_id = msg.conversation_id
  GROUP BY 1, 2
),
wa_pos AS (
  SELECT COALESCE(co.offer, '(non-sales)') AS offer, CAST(timezone('UTC', mt.tagged_at) AS DATE) AS metric_date,
         count(*) FILTER (WHERE mt.reply_sentiment = 'positive') AS positive_replies
  FROM raw_iskra_meetings mt
  LEFT JOIN core.v_whatsapp_conversation_offer co ON co.conversation_id = mt.conversation_id
  WHERE mt.tagged_at IS NOT NULL
  GROUP BY 1, 2
),
wa_mtg AS (
  SELECT 'Business Funding' AS offer, CAST(posted_at AS DATE) AS metric_date, count(*) AS meetings_booked
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'WhatsApp'
  GROUP BY 1, 2
),
wa_keys AS (
  SELECT offer, metric_date FROM wa_send
  UNION SELECT offer, metric_date FROM wa_pos
  UNION SELECT offer, metric_date FROM wa_mtg
),
wa_final AS (
  SELECT 'whatsapp' AS channel, k.offer, k.metric_date,
         COALESCE(s.sent, 0) AS sent, CAST(NULL AS BIGINT) AS replies_human, CAST(NULL AS BIGINT) AS replies_auto,
         COALESCE(p.positive_replies, 0) AS positive_replies, COALESCE(mt.meetings_booked, 0) AS meetings_booked
  FROM wa_keys k
  LEFT JOIN wa_send s  USING (offer, metric_date)
  LEFT JOIN wa_pos  p  USING (offer, metric_date)
  LEFT JOIN wa_mtg  mt USING (offer, metric_date)
),
-- ── SMS — offer from the COPY-CLASSIFIED core.sms_campaign_offer (repointed 2026-06-29 off the wrong
--    raw_comms_brand.offer_type: ~81% NULL and mis-labels funding brands as pre_ipo — verified). The
--    canonical key is campaign_id. The brand registry is kept ONLY as the sender_number ->
--    sendivo_campaign_id bridge for INBOUND (inbound rows carry our_number, not campaign_id); the
--    number<->campaign linkage is sound — only brand.offer_type was wrong. Anything unmapped -> '(offer-unknown)'.
sms_num_campaign AS (
  SELECT regexp_replace(COALESCE(sender_number,''), '[^0-9]', '', 'g') AS sn_digits,
         any_value(sendivo_campaign_id) AS campaign_id
  FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC, _run_id DESC) AS rn
        FROM raw_comms_brand) WHERE rn = 1 AND sendivo_campaign_id IS NOT NULL
  GROUP BY 1
),
sms_send AS (
  SELECT COALESCE(o.offer, '(offer-unknown)') AS offer, s.metric_date, sum(s.sent) AS sent
  FROM v_sms_campaign_performance s
  LEFT JOIN core.sms_campaign_offer o ON o.campaign_id = s.campaign_id
  WHERE s.sent IS NOT NULL
  GROUP BY 1, 2
),
sms_inb_dedup AS (
  SELECT inbound_message_id, received_at, is_opt_out,
         regexp_replace(COALESCE(our_number,''), '[^0-9]', '', 'g') AS our_digits
  FROM (SELECT *, row_number() OVER (PARTITION BY inbound_message_id ORDER BY _loaded_at DESC, received_at) AS rn
        FROM raw_sendivo_inbound) WHERE rn = 1
),
sms_inb AS (
  -- replies_human + replies_auto computed as EXPLICIT, mutually-exclusive filters in the SAME
  -- (offer, date) bucket — no cross-CTE subtraction. auto = non-opt-out & qwen-classified non-human;
  -- human = everything else (opt-outs + non-opt-out where is_human is true OR unclassified/NULL).
  -- human + auto = count(*) exactly, so the leg reconciles. Semantics match v_omni_sms_performance
  -- (unclassified replies fall to human, same as its replies_total - replies_auto).
  SELECT COALESCE(o.offer, '(offer-unknown)') AS offer, CAST(i.received_at AS DATE) AS metric_date,
         count(*) FILTER (WHERE (NOT i.is_opt_out) AND q.is_human = CAST('f' AS BOOLEAN)) AS replies_auto,
         count(*) FILTER (WHERE i.is_opt_out OR q.is_human IS DISTINCT FROM CAST('f' AS BOOLEAN)) AS replies_human,
         count(*) FILTER (WHERE (NOT i.is_opt_out) AND q.is_positive = CAST('t' AS BOOLEAN)) AS positive_replies
  FROM sms_inb_dedup i
  LEFT JOIN sms_num_campaign nc ON nc.sn_digits = i.our_digits   -- number -> sendivo campaign_id
  LEFT JOIN core.sms_campaign_offer o ON o.campaign_id = nc.campaign_id   -- campaign_id -> copy-classified offer
  LEFT JOIN derived.sms_reply_is_positive_qwen q ON q.reply_id = i.inbound_message_id
  GROUP BY 1, 2
),
sms_mtg AS (
  -- SMS meetings can't be offer-split (no brand/phone key on the Funding Form) → offer-unknown, never funding.
  SELECT '(offer-unknown)' AS offer, CAST(posted_at AS DATE) AS metric_date, count(*) AS meetings_booked
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'SMS'
  GROUP BY 1, 2
),
sms_keys AS (
  SELECT offer, metric_date FROM sms_send
  UNION SELECT offer, metric_date FROM sms_inb
  UNION SELECT offer, metric_date FROM sms_mtg
),
sms_final AS (
  SELECT 'sms' AS channel, k.offer, k.metric_date,
         COALESCE(s.sent, 0) AS sent,
         COALESCE(i.replies_human, 0) AS replies_human,
         COALESCE(i.replies_auto, 0) AS replies_auto,
         COALESCE(i.positive_replies, 0) AS positive_replies,
         COALESCE(mt.meetings_booked, 0) AS meetings_booked
  FROM sms_keys k
  LEFT JOIN sms_send s  USING (offer, metric_date)
  LEFT JOIN sms_inb  i  USING (offer, metric_date)
  LEFT JOIN sms_mtg  mt USING (offer, metric_date)
)
SELECT * FROM email_final
UNION ALL SELECT * FROM wa_final
UNION ALL SELECT * FROM sms_final;
