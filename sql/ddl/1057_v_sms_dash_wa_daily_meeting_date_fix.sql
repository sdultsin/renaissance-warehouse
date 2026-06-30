-- 1057 — v_sms_dash_wa_daily: bucket WhatsApp meetings by meeting_date, not posted_at  [2026-06-30]
-- @gate: alter-type   (CREATE OR REPLACE VIEW — fix meeting bucketing; column contract UNCHANGED)
-- Depends on 110
--
-- DDL 110 bucketed the WhatsApp meeting count on core.meeting.posted_at::date (the day a partner LOGGED
-- the meeting to the sheet) because core.meeting.meeting_date did not exist yet — its header flagged this
-- as a one-line WS5-gated follow-up: "When WS5 lands core.meeting.meeting_date, repoint the meeting CTE
-- from posted_at::date to meeting_date." That column now EXISTS (DATE, verified live 2026-06-30), and the
-- posted_at bucketing is the source of the daily report's inflated WhatsApp meeting count (e.g. 9 on
-- 06-29, most of which were 06-28 meetings logged late) — the "9 vs 3" discrepancy in the
-- 2026-06-30 WhatsApp handoff. Every OTHER channel in the daily report already counts meetings by
-- meeting_date; this makes WhatsApp consistent and removes the over-count.
--
-- ONLY the `mtg` CTE changes (posted_at::date -> meeting_date, + drop the now-redundant outer ::date cast on
-- m.metric_date since meeting_date is already DATE). Output columns are byte-for-byte identical to DDL 110,
-- so every consumer (v_sms_dash_daily 'all' rollup, the daily-report renderer, scripts/sms_dashboard_data.py)
-- is unaffected except for the corrected meeting number. delivered/failed were already exposed by DDL 110
-- (the renderer simply was not reading them — fixed on the renderer side).

CREATE OR REPLACE VIEW main.v_sms_dash_wa_daily AS
WITH msg AS (
  SELECT (created_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE direction='outbound')                                     AS sent,
         count(*) FILTER (WHERE direction='outbound' AND status IN ('delivered','read'))  AS delivered,
         count(*) FILTER (WHERE direction='outbound' AND status='failed')                 AS failed,
         count(*) FILTER (WHERE direction='inbound')                                      AS replies_total
  FROM raw_iskra_messages GROUP BY 1
),
tag AS (
  SELECT (tagged_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE reply_sentiment='positive') AS positive_replies
  FROM raw_iskra_meetings WHERE tagged_at IS NOT NULL GROUP BY 1
),
mtg AS (   -- SOURCE OF TRUTH: partner-logged WhatsApp meetings, bucketed by meeting_date (DDL 1057 fix)
  SELECT meeting_date AS metric_date, count(*) AS meetings_booked
  FROM core.meeting
  WHERE is_duplicate_of IS NULL AND source='sheet' AND channel='WhatsApp' AND meeting_date IS NOT NULL
  GROUP BY 1
)
SELECT 'whatsapp'                                          AS channel,
       COALESCE(g.metric_date, t.metric_date, m.metric_date)::DATE AS metric_date,
       COALESCE(g.sent,0)                                  AS sent,
       COALESCE(g.delivered,0)                             AS delivered,
       COALESCE(g.failed,0)                                AS failed,
       COALESCE(g.replies_total,0)                         AS replies_total,
       COALESCE(t.positive_replies,0)                      AS positive_replies,
       CAST(NULL AS BIGINT)                                AS opt_outs,
       COALESCE(m.meetings_booked,0)                       AS meetings_booked,
       CAST(NULL AS DOUBLE)                                AS cost_usd,
       CAST(g.delivered AS DOUBLE)/nullif(g.sent,0)        AS delivery_rate,
       CAST(g.replies_total AS DOUBLE)/nullif(g.sent,0)    AS reply_rate,
       CAST(t.positive_replies AS DOUBLE)/nullif(g.sent,0) AS positive_rate,
       'iskra_ai_reply_sentiment_positive'                 AS positive_signal
FROM msg g
FULL OUTER JOIN tag t ON t.metric_date = g.metric_date
FULL OUTER JOIN mtg m ON m.metric_date = COALESCE(g.metric_date, t.metric_date);
