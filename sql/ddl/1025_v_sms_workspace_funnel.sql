-- SMS per-workspace (Sendivo sub-account) funnel — sends · opps · meetings · KPI, ONE pre-joined source.
--
-- WHY: the portal's per-workspace SMS table mixed two incompatible workspace keys and produced an
-- impossible row (Sam, 2026-06-26): "Renaissance 3 = ~0 sent but 1,694 opps + 400 meetings, KPI 6,980".
-- Root cause: SMS SENDS key on the Sendivo `sub_account` (closed set: Renaissance 1 / 2 / a DEAD
-- Renaissance 3 / NULL), but SMS OPPS are a channel total (derived.sms_reply_is_positive_qwen, no
-- workspace key) and SMS MEETINGS carry only the IM-typed Funding-Form label (~99% the single constant
-- "Sendivo (Renaissance 1)", never "Renaissance 3"). The portal overlaid the channel-total opps/meetings
-- onto a per-sub-account send table with no join key and mis-binned the bulk into a phantom Renaissance-3
-- row that sent 0. (Detail: deliverables/2026-06-26-revops-funnel-deep-dive/SMS-WORKSPACE-ATTRIBUTION.md)
--
-- FIX: re-key ALL THREE metrics to the SEND-side sub_account via the Sendivo number->campaign bridge,
-- so a single pre-reconciled per-(sub_account x day) row feeds the portal — no client-side overlay of
-- mismatched keys is possible. Rows that don't bridge fall to an explicit '(unattributed)' bucket; the
-- phantom "Renaissance 3" row disappears (its real sends are ~15k lifetime, last 2026-06-06).
--
-- BRIDGES (validated 2026-06-26, window 06-22..06-26):
--   sends    : v_sms_campaign_performance grouped by sub_account_name.            (100%)
--   opps     : sms_reply_is_positive_qwen.reply_id -> raw_sendivo_inbound (dedup by inbound_message_id)
--              -> our_number(right-10) -> v_sendivo_number_campaign.sub_account.  (100%)
--   meetings : core.meeting(channel='SMS') -> source_event_id == Funding-Form submission_id
--              -> FF phone(right-10) -> raw_sendivo_inbound.prospect_number -> our_number
--              -> v_sendivo_number_campaign.sub_account.                          (~67-99%; residual -> '(unattributed)')
--
-- GRAIN: one row per (metric_date x sub_account). Consumers window/aggregate client-side.
-- COVERAGE CAVEAT (read): the meeting bridge is only ~67% via canonical core.meeting (the rest land in
-- '(unattributed)', never fabricated). The meeting builder itself still hard-labels SMS meetings to the
-- Funding-Form constant, so the Ren-1/Ren-2 meeting SPLIT here reflects which sub-account's number the
-- lead replied to. A meeting-builder re-key (entities/meeting.py D7) is the upstream follow-on.
--
-- @gate: non-destructive (new view; touches no existing object grain)

CREATE OR REPLACE VIEW v_sms_workspace_funnel AS
WITH sa AS (
  SELECT DISTINCT sub_account_id, sub_account_name FROM raw_sendivo_campaign_daily
),
nc AS (
  SELECT right(regexp_replace(our_number, '[^0-9]', '', 'g'), 10) AS onum, sub_account_id
  FROM v_sendivo_number_campaign WHERE sub_account_id IS NOT NULL GROUP BY 1, 2
),
inb AS (
  SELECT DISTINCT right(regexp_replace(prospect_number, '[^0-9]', '', 'g'), 10) AS ph,
                  right(regexp_replace(our_number,      '[^0-9]', '', 'g'), 10) AS onum
  FROM raw_sendivo_inbound
),
ibm AS (
  SELECT inbound_message_id,
         any_value(right(regexp_replace(our_number, '[^0-9]', '', 'g'), 10)) AS onum
  FROM raw_sendivo_inbound GROUP BY 1
),
ffph AS (
  SELECT json_extract_string(row_json, '$[1]')  AS sid,
         right(regexp_replace(json_extract_string(row_json, '$[10]'), '[^0-9]', '', 'g'), 10) AS ph
  FROM raw_sheets_funding_form_data
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
mtg AS (
  SELECT m.meeting_date AS d,
         COALESCE(sa.sub_account_name, '(unattributed)') AS ws,
         COUNT(DISTINCT m.meeting_id) AS meetings
  FROM core.meeting m
  LEFT JOIN ffph ON ffph.sid = m.source_event_id
  LEFT JOIN inb  ON inb.ph   = ffph.ph
  LEFT JOIN nc   ON nc.onum  = inb.onum
  LEFT JOIN sa   ON sa.sub_account_id = nc.sub_account_id
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
