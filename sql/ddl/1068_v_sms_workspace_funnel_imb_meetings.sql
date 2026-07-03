-- SMS per-workspace funnel — repoint the MEETING leg to the native per-booking sub-account [2026-07-02]
--
-- WHY (flag warehouse-flags #14): since the im_bookings meeting-source cutover (2026-06-29, PR #115 /
-- entities/meeting.py step 2-IMB), v_sms_workspace_funnel showed meetings=0 for Renaissance 1 on every
-- day >= 2026-06-29, while core.meeting held 58/43/66 (Jun29/30/Jul1). Anyone reading the SMS dashboard
-- saw a meeting collapse that did not happen (this fed the "50 meetings from 1M sends" misread on the
-- 2026-07-01 Sam×Ido call).
--
-- ROOT CAUSE: DDL 1025's meeting leg bridged each SMS meeting to a sub-account by joining
-- core.meeting.source_event_id -> the retired Funding-Form Google Sheet (raw_sheets_funding_form_data,
-- the ffph CTE) -> Funding-Form phone -> our_number -> v_sendivo_number_campaign.sub_account. Post-cutover
-- SMS meetings are 'imb:%' rows whose source_event_id is the im_bookings id (NOT a sheet Submission ID),
-- so that join is DEAD -> every recent meeting fell to '(unattributed)' and Renaissance 1 read 0.
--
-- FIX: im_bookings meetings carry the Sendivo sub-account NATIVELY on core.meeting.sendivo_sub_account
-- (entities/meeting.py D7, both the FF branch and the im_bookings branch derive it), so key the meeting
-- leg directly on that column — no funding-form sheet, no phone/number bridge to break. Rows whose
-- Sendivo workspace label was not recognized (e.g. 'Sendivo R1'/'SMS R1', ~7-9/day) keep an explicit
-- '(unattributed)' bucket — never fabricated into a workspace. The retired ffph/inb sheet-bridge CTEs are
-- dropped. sends + opps legs are UNCHANGED.
--
-- Verified read-only on serving snapshot warehouse_20260703_031137_972.duckdb:
--   * Renaissance 1 meetings 2026-06-29..07-02 = 51 / 33 / 64 / 116 (was 0 / 0 / 0 / 0) — non-zero,
--     small honest drift vs core.meeting's offer-scoped 58/43/66 (the delta is the unmapped-workspace rows).
--   * Pre-cutover complete week Jun08-14: proposed total == core.meeting total (601 == 601) — no regression.
--
-- @gate: add
-- Depends on 1025
CREATE OR REPLACE VIEW v_sms_workspace_funnel AS
WITH sa AS (
  SELECT DISTINCT sub_account_id, sub_account_name FROM raw_sendivo_campaign_daily
),
nc AS (
  SELECT right(regexp_replace(our_number, '[^0-9]', '', 'g'), 10) AS onum, sub_account_id
  FROM v_sendivo_number_campaign WHERE sub_account_id IS NOT NULL GROUP BY 1, 2
),
ibm AS (
  SELECT inbound_message_id,
         any_value(right(regexp_replace(our_number, '[^0-9]', '', 'g'), 10)) AS onum
  FROM raw_sendivo_inbound GROUP BY 1
),
sends AS (
  SELECT metric_date AS d, COALESCE(sub_account_name, '(unmapped)') AS ws,
         SUM(sent) AS sent, SUM(delivered) AS delivered, SUM(replies) AS replies,
         SUM(cost_usd) AS cost_usd
  FROM v_sms_campaign_performance GROUP BY 1, 2
),
opps AS (
  SELECT CAST(q.received_at AS DATE) AS d,
         COALESCE(sa.sub_account_name, '(unattributed)') AS ws, COUNT(*) AS opps
  FROM derived.sms_reply_is_positive_qwen q
  JOIN ibm ON ibm.inbound_message_id = q.reply_id
  LEFT JOIN nc ON nc.onum = ibm.onum
  LEFT JOIN sa ON sa.sub_account_id = nc.sub_account_id
  WHERE q.is_positive AND q.received_at IS NOT NULL GROUP BY 1, 2
),
mtg AS (  -- native per-booking sub-account (im_bookings cutover); no funding-form sheet / phone bridge.
  SELECT m.meeting_date AS d,
         COALESCE(m.sendivo_sub_account, '(unattributed)') AS ws,
         COUNT(DISTINCT m.meeting_id) AS meetings
  FROM core.meeting m
  WHERE m.channel = 'SMS' AND m.is_duplicate_of IS NULL AND m.meeting_date IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  COALESCE(s.d, o.d, t.d)   AS metric_date,
  COALESCE(s.ws, o.ws, t.ws) AS sub_account,
  COALESCE(s.sent, 0)       AS sent,
  COALESCE(s.delivered, 0)  AS delivered,
  COALESCE(s.replies, 0)    AS replies,
  ROUND(COALESCE(s.cost_usd, 0), 2) AS cost_usd,
  COALESCE(o.opps, 0)       AS opps,
  COALESCE(t.meetings, 0)   AS meetings,
  CASE WHEN COALESCE(t.meetings, 0) > 0
       THEN ROUND(COALESCE(s.sent, 0) * 1.0 / t.meetings) END AS sent_per_meeting
FROM sends s
FULL JOIN opps o ON o.d = s.d AND o.ws = s.ws
FULL JOIN mtg  t ON t.d = COALESCE(s.d, o.d) AND t.ws = COALESCE(s.ws, o.ws);
