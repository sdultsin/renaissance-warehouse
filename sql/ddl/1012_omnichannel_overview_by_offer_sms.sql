-- v_omnichannel_overview_by_offer — add the SMS leg [2026-06-24]
--
-- DDL 1009 shipped this offer-sliced overview with EMAIL + WhatsApp legs only. SMS is where pre-IPO
-- volume actually concentrates (the top SMS senders are the 4 pre_ipo brands), so an offer-blind SMS
-- was the biggest remaining blend. Now that raw_comms_brand carries offer_type (DDL 1009 + nightly),
-- add SMS — sends & replies attributed to offer via the brand:
--   * sends   → v_sms_campaign_performance.campaign_id = brand.sendivo_campaign_id → offer_type
--   * replies → raw_sendivo_inbound.our_number ≈ brand.sender_number (digits) → offer_type
-- Both fall back to a '(offer-unknown)' bucket when the brand can't be resolved (rotated/retired
-- numbers ≈16% of sends, ≈37% of inbound) so per-offer SUMs still reconcile to the blended totals.
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
-- Depends on 1009 16 34 90
-- Depends on: v_omnichannel_overview_by_offer (DDL 1009), raw_comms_brand.offer_type, v_sms_campaign_performance,
--   raw_sendivo_inbound, derived.sms_reply_is_positive_qwen, core.meeting

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
-- ── SMS (NEW) ──
-- deduped brand → canonical offer, keyed by BOTH sendivo_campaign_id (sends) and sender_number digits (inbound)
sms_brand AS (
  SELECT id, sendivo_campaign_id,
         regexp_replace(COALESCE(sender_number,''), '[^0-9]', '', 'g') AS sn_digits,
         CASE WHEN offer_type = 'pre_ipo' THEN 'Pre-IPO'
              WHEN offer_type = 'funding' THEN 'Business Funding'
              ELSE offer_type END AS offer
  FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC, _run_id DESC) AS rn
        FROM raw_comms_brand) WHERE rn = 1 AND offer_type IS NOT NULL
),
sms_send AS (
  SELECT COALESCE(b.offer, '(offer-unknown)') AS offer, s.metric_date, sum(s.sent) AS sent
  FROM v_sms_campaign_performance s
  LEFT JOIN sms_brand b ON b.sendivo_campaign_id = s.campaign_id
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
  SELECT COALESCE(b.offer, '(offer-unknown)') AS offer, CAST(i.received_at AS DATE) AS metric_date,
         count(*) FILTER (WHERE (NOT i.is_opt_out) AND q.is_human = CAST('f' AS BOOLEAN)) AS replies_auto,
         count(*) AS replies_total,
         count(*) FILTER (WHERE (NOT i.is_opt_out) AND q.is_positive = CAST('t' AS BOOLEAN)) AS positive_replies
  FROM sms_inb_dedup i
  LEFT JOIN sms_brand b ON b.sn_digits = i.our_digits
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
         (COALESCE(i.replies_total, 0) - COALESCE(i.replies_auto, 0)) AS replies_human,
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
