-- 114_v_positive_lead_alltime.sql
-- Chat: classify-all-time-positives (2026-06-22). Full writeup:
--   deliverables/2026-06-22-classify-all-time-positives/REPORT.md
--
-- PURPOSE: the COMPLETE all-time positive-LEAD set (email + SMS), lead-grain, deduped, for the
--   Ken lead-sale / retargeting. JOIN-READY to core.meeting by lower(lead_email) so booked-meeting
--   leads can be excluded at sale time (partner exclusivity). This is a LEAD surface, not a count.
--
-- Reconciled to the live "Close CRM Opportunity Classifier" (STRICT-positive) definition. The
-- historical email tail (536,620 replies the live classifier never saw) was backfilled with the
-- established qwen strict method (qwen/qwen3-30b-a3b-instruct via OpenRouter) into
-- derived.reply_is_positive_strict (DDL 93 seed) — calibrated at 79.2% agreement vs the live-Haiku
-- opps on the overlap; the UNION below retains all Haiku opps regardless, so recall is guaranteed.
--
-- EMAIL tiers (best-signal precedence high->low):
--   live_haiku_opp            = core.opportunity source='instantly'           (the live definition)
--   strict_qwen               = derived.reply_is_positive_strict is_positive ∩ current core.reply
--   pipeline_positive_intent  = raw_pipeline_reply_data.intent='positive'      (legacy tag, weakest)
-- SMS = raw_comms_call_opportunity source='sendivo' (phone-grain; short history, fully live-era).
--
-- ⛔ NOT A COUNT SURFACE for replies/opps. This is distinct positive LEADS. booked_meeting_email_match
--   is exact for the post-2026-06-01 sheet era (100% lead_email); pre-June Slack meetings carry no
--   lead_email and SMS positives are mostly phone-only — see REPORT.md "join verdict" for the gaps.
CREATE OR REPLACE VIEW derived.v_positive_lead_alltime AS
WITH sig AS (
  SELECT lower(lead_email) AS e, opened_at AS ts, 1 AS pri, 'live_haiku_opp' AS src
    FROM core.opportunity
    WHERE source = 'instantly' AND lead_email IS NOT NULL AND lead_email <> ''
  UNION ALL
  SELECT lower(r.lead_email), r.reply_timestamp, 2, 'strict_qwen'
    FROM derived.reply_is_positive_strict s
    JOIN core.reply r ON r.reply_id = s.reply_id
    WHERE s.is_positive AND r.lead_email IS NOT NULL AND r.lead_email <> ''
  UNION ALL
  SELECT lower(lead_email), reply_timestamp, 3, 'pipeline_positive_intent'
    FROM main.raw_pipeline_reply_data
    WHERE intent = 'positive' AND lead_email IS NOT NULL AND lead_email <> ''
),
email_lead AS (
  SELECT 'email' AS channel, e AS lead_email, CAST(NULL AS VARCHAR) AS phone_e164,
         min(ts) AS first_positive_at, arg_min(src, pri) AS best_signal
  FROM sig GROUP BY e
),
sms_lead AS (
  SELECT 'sms' AS channel, NULLIF(lower(max(email)), '') AS lead_email, phone_e164,
         min(opportunity_marked_at) AS first_positive_at, 'live_haiku_opp' AS best_signal
  FROM main.raw_comms_call_opportunity
  WHERE source = 'sendivo' AND phone_e164 IS NOT NULL AND phone_e164 <> ''
  GROUP BY phone_e164
),
mtg AS (
  SELECT DISTINCT lower(lead_email) AS e
  FROM core.meeting WHERE lead_email IS NOT NULL AND lead_email <> ''
)
SELECT l.channel, l.lead_email, l.phone_e164, l.first_positive_at, l.best_signal,
       (m.e IS NOT NULL) AS booked_meeting_email_match
FROM (SELECT * FROM email_lead UNION ALL BY NAME SELECT * FROM sms_lead) l
LEFT JOIN mtg m ON m.e = l.lead_email;
