-- 116_v_positive_lead_alltime_v2.sql
-- Supersedes the view body from DDL 114, adding the SMS phone-keyed booked-exclusion flag.
-- Chat: classify-all-time-positives (2026-06-23). Writeup:
--   deliverables/2026-06-22-classify-all-time-positives/REPORT.md
--
-- ADDS `sms_booked_exclude`: core.meeting has no phone column, so SMS positives (phone-keyed)
-- can't be matched to booked meetings by email. We resolve SMS-meeting lead phones two ways —
-- comms (raw_comms_conversation.prospect_number, in-warehouse) ∪ enriched
-- (derived.sms_meeting_phone_enriched, DDL 115 / LeadMagic email->phone) — and flag any SMS
-- positive whose phone matches (last-10-digit normalized). At sale time exclude booked leads
-- via booked_meeting_email_match (email channel) OR sms_booked_exclude (SMS channel).
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
),
-- resolved SMS-meeting phones (last-10-digit), comms ∪ enriched
sms_mtg_phone AS (
  SELECT DISTINCT right(regexp_replace(c.prospect_number, '[^0-9]', '', 'g'), 10) AS d10
    FROM core.meeting m
    JOIN main.raw_comms_conversation c ON lower(c.prospect_email) = lower(m.lead_email)
    WHERE m.channel = 'SMS' AND c.prospect_number IS NOT NULL AND c.prospect_number <> ''
  UNION
  SELECT DISTINCT right(regexp_replace(phone_e164, '[^0-9]', '', 'g'), 10)
    FROM derived.sms_meeting_phone_enriched WHERE phone_e164 IS NOT NULL AND phone_e164 <> ''
)
SELECT l.channel, l.lead_email, l.phone_e164, l.first_positive_at, l.best_signal,
       (m.e IS NOT NULL) AS booked_meeting_email_match,
       (l.channel = 'sms'
        AND right(regexp_replace(l.phone_e164, '[^0-9]', '', 'g'), 10) IN (SELECT d10 FROM sms_mtg_phone))
         AS sms_booked_exclude
FROM (SELECT * FROM email_lead UNION ALL BY NAME SELECT * FROM sms_lead) l
LEFT JOIN mtg m ON m.e = l.lead_email;
