-- 110 — SMS dashboard serving layer (3-tab: All / SMS=Sendivo / WhatsApp=Iskra). WS8, portal-data-rebuild.
-- @gate: add
-- VERSION 110. RENUMBERED from the source design's "109" (CONTRACT C1). Live MAX(core.schema_version)=104
--   this session (WS1=103, WS2=104 already deployed); the reconciled plan assigns WS8 the v110 slot.
--   Verified free: 0 rows at version 110 in core.schema_version. The Schema-Moderator re-runs
--   SELECT max(version)+1 immediately before apply and bumps the whole remaining block by any delta if the
--   nightly moved the floor — so a stale/duplicate number cannot silently no-op via apply_ddl_file PK-dedupe.
-- Depends on: 34 (v_sms_campaign_performance), 66 (v_sms_failure_summary / v_sendivo_asset_health /
--   v_sendivo_phone_inventory), 82 (raw_iskra_messages / raw_iskra_meetings / v_whatsapp_number_health),
--   89+90/91 (v_omni_sms_performance — ALREADY LIVE, qwen-strict positive: positive_signal='sendivo_qwen_strict'
--   VERIFIED live this session, so CONTRACT C6 is already satisfied — no further repoint needed).
-- DEPLOY DEP: WS5 (v107, core.meeting.meeting_date) is a NON-BLOCKING FOLLOW-ON, not a precondition. This DDL
--   buckets meetings on posted_at::date today (Jun-19 SMS=53, VERIFIED) and SURFACES the +8 vs Grace col-A (45).
--   When WS5/v107 lands core.meeting.meeting_date, repoint the 3 inline meeting CTEs (msg/mtg/wh) from
--   posted_at::date to meeting_date and flip ASSERT-B's literal to =45 AND (45-45)=0 (one-line WS5-gated follow-up).
--   VERIFIED this session: core.meeting has NO meeting_date column yet → this DDL MUST ship on posted_at::date.
-- SELF-CONTAINED: WhatsApp daily + combined + meeting-reconcile are defined INLINE here (NOT a re-apply of the
--   89 file) so this gate has no cross-DDL ordering dependency and cannot clobber the live qwen SMS view.
--
-- The nightly generator (scripts/sms_dashboard_data.py) READS these views and NEVER re-derives
-- (persist-everything rule). All date cols are DATE/VARCHAR; no raw TIMESTAMPTZ is exposed (the read-only HTTP
-- query API throws pytz on TIMESTAMPTZ; all casts (... AT TIME ZONE 'UTC')::date happen INSIDE the views).
--
-- positive_signal is HETEROGENEOUS by channel (SMS=sendivo_qwen_strict, WhatsApp=iskra_ai_reply_sentiment);
-- it is carried per-row and the 'all' tab's positive is a documented mixed sum — footnoted, never compared
-- cross-channel as like-for-like.

-- (0) WhatsApp daily (INLINE — v_omni_whatsapp_performance is not live; reproduce its logic, same contract).
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
mtg AS (
  SELECT posted_at::date AS metric_date, count(*) AS meetings_booked
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source='sheet' AND channel='WhatsApp' GROUP BY 1
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

-- (1) DAILY TIMELINE — per channel ('sms','whatsapp') AND combined ('all'), one row per (tab, date).
--     SMS reads the LIVE qwen-strict v_omni_sms_performance; WhatsApp reads the inline view above.
CREATE OR REPLACE VIEW main.v_sms_dash_daily AS
WITH chan AS (
  SELECT 'sms' AS channel, metric_date, sent, delivered, failed, replies_total,
         positive_replies, opt_outs, meetings_booked, cost_usd,
         delivery_rate, reply_rate, positive_rate, positive_signal
  FROM v_omni_sms_performance
  UNION ALL
  SELECT channel, metric_date, sent, delivered, failed, replies_total,
         positive_replies, opt_outs, meetings_booked, cost_usd,
         delivery_rate, reply_rate, positive_rate, positive_signal
  FROM main.v_sms_dash_wa_daily
),
combined AS (
  SELECT 'all' AS channel, metric_date,
         sum(sent) AS sent, sum(delivered) AS delivered, sum(failed) AS failed,
         sum(replies_total) AS replies_total,
         sum(positive_replies) AS positive_replies,         -- heterogeneous mixed sum; see header
         sum(opt_outs) AS opt_outs, sum(meetings_booked) AS meetings_booked, sum(cost_usd) AS cost_usd,
         CAST(sum(delivered) AS DOUBLE)/nullif(sum(sent),0)        AS delivery_rate,
         CAST(sum(replies_total) AS DOUBLE)/nullif(sum(sent),0)    AS reply_rate,
         CAST(sum(positive_replies) AS DOUBLE)/nullif(sum(sent),0) AS positive_rate,
         'mixed: sms_sendivo_qwen_strict + whatsapp_iskra_ai_sentiment' AS positive_signal
  FROM chan GROUP BY 2
)
SELECT channel AS tab, metric_date::DATE AS metric_date, sent, delivered, failed, replies_total,
       positive_replies, opt_outs, meetings_booked, cost_usd,
       delivery_rate, reply_rate, positive_rate, positive_signal FROM chan
UNION ALL
SELECT channel AS tab, metric_date::DATE, sent, delivered, failed, replies_total,
       positive_replies, opt_outs, meetings_booked, cost_usd,
       delivery_rate, reply_rate, positive_rate, positive_signal FROM combined;

-- (2) SMS PER-CAMPAIGN — lifetime grain. campaign_id is BIGINT -> CAST to VARCHAR before any string bucket.
--     A row is a REAL campaign only if it has outbound (sent>0 OR delivered>0); every inbound-only row
--     (NULL campaign_id orphans AND non-NULL outbound-empty ids like 3219) collapses into ONE explicit
--     '(inbound-only / unmapped number)' bucket -> the lifetime invariant holds for every real-campaign row.
CREATE OR REPLACE VIEW main.v_sms_dash_campaign AS
WITH tagged AS (
  SELECT CASE WHEN campaign_id IS NOT NULL AND (sent>0 OR delivered>0)
              THEN CAST(campaign_id AS VARCHAR) ELSE '__inbound_only_unmapped__' END AS campaign_key,
         campaign_name, sub_account_name, sent, delivered, failed, replies, opt_outs,
         positive_replies, cost_usd
  FROM v_sms_campaign_performance
)
SELECT
  campaign_key                                                                  AS campaign_id,
  CASE WHEN campaign_key='__inbound_only_unmapped__' THEN '(inbound-only / unmapped number)'
       ELSE COALESCE(any_value(campaign_name), '(unnamed campaign)') END        AS campaign_name,
  any_value(sub_account_name)                                                   AS sub_account,
  COALESCE(sum(sent),0)::BIGINT                                                 AS sent,
  COALESCE(sum(delivered),0)::BIGINT                                            AS delivered,
  COALESCE(sum(failed),0)::BIGINT                                               AS failed,
  COALESCE(sum(replies),0)::BIGINT                                              AS replies,
  COALESCE(sum(opt_outs),0)::BIGINT                                             AS opt_outs,
  COALESCE(sum(positive_replies),0)::BIGINT                                     AS positive_replies,
  round(COALESCE(sum(cost_usd),0),2)                                            AS cost_usd,
  CASE WHEN sum(sent)>0      THEN round(100.0*sum(delivered)/sum(sent),2) END              AS delivery_rate,
  CASE WHEN sum(delivered)>0 THEN round(100.0*sum(replies)/nullif(sum(delivered),0),2) END AS reply_rate,
  CASE WHEN sum(positive_replies)>0 THEN round(sum(cost_usd)/sum(positive_replies),2) END  AS cost_per_positive
FROM tagged
GROUP BY campaign_key
HAVING sum(sent) > 0 OR sum(replies) > 0;

-- (3) SMS PER-SUB-ACCOUNT rollup (Renaissance 1/2/3).
CREATE OR REPLACE VIEW main.v_sms_dash_subaccount AS
SELECT COALESCE(sub_account_name, '(unmapped)') AS sub_account,
       count(DISTINCT campaign_id)             AS campaigns,
       COALESCE(sum(sent),0)::BIGINT            AS sent,
       COALESCE(sum(delivered),0)::BIGINT       AS delivered,
       COALESCE(sum(replies),0)::BIGINT         AS replies,
       COALESCE(sum(positive_replies),0)::BIGINT AS positive_replies,
       round(COALESCE(sum(cost_usd),0),2)       AS cost_usd
FROM v_sms_campaign_performance GROUP BY 1;

-- (4) SMS PLATFORM panel — Sendivo asset health (one wide row) + top failure reasons.
CREATE OR REPLACE VIEW main.v_sms_dash_platform_sms AS
SELECT campaigns_total, campaigns_approved, campaigns_pending, campaigns_in_review, campaigns_rejected,
       numbers_total, numbers_messaging_active AS numbers_active, numbers_unassigned,
       brands_total, brands_verified
FROM v_sendivo_asset_health;

CREATE OR REPLACE VIEW main.v_sms_dash_failure_reasons AS
SELECT reason, sum(n_messages)::BIGINT AS n_messages
FROM v_sms_failure_summary GROUP BY 1 ORDER BY 2 DESC;

-- (5) WHATSAPP PLATFORM panel — Iskra number health rollup.
CREATE OR REPLACE VIEW main.v_sms_dash_platform_whatsapp AS
SELECT status, count(*)::BIGINT AS n_numbers
FROM v_whatsapp_number_health GROUP BY 1 ORDER BY 2 DESC;

-- (6) MEETING RECONCILE — sheet (SoT) vs warehouse meeting bucket, per channel × day, INLINE (no dep on
--     the not-live v_meeting_capture_reconcile). Surfaces the warehouse posted_at over-count + the WhatsApp
--     platform-capture gap; sheet stays SoT, nothing auto-written.
CREATE OR REPLACE VIEW main.v_sms_dash_meeting_reconcile AS
WITH wh AS (   -- warehouse meeting bucket (posted_at) by channel/day — the number the dashboard shows today
  SELECT posted_at::date AS metric_date, channel, count(*) AS warehouse_meetings
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source='sheet' AND channel IN ('SMS','WhatsApp')
  GROUP BY 1,2
),
wa_platform AS (  -- Iskra platform-side booked tags (independent WhatsApp meeting signal)
  SELECT (tagged_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE meeting_status='booked') AS platform_meetings
  FROM raw_iskra_meetings WHERE tagged_at IS NOT NULL GROUP BY 1
)
SELECT wh.metric_date, wh.channel, wh.warehouse_meetings,
       CASE WHEN wh.channel='WhatsApp' THEN COALESCE(w.platform_meetings,0) ELSE CAST(NULL AS BIGINT) END
         AS platform_meetings,
       CASE WHEN wh.channel='WhatsApp' THEN wh.warehouse_meetings - COALESCE(w.platform_meetings,0)
            ELSE CAST(NULL AS BIGINT) END AS warehouse_minus_platform,
       CASE WHEN wh.channel='WhatsApp' THEN 'iskra_ai_meeting_status_booked'
            WHEN wh.channel='SMS'      THEN 'no_platform_meeting_endpoint (Sendivo 404) — sheet is sole source'
            ELSE 'sheet is sole source' END AS platform_meeting_source
FROM wh LEFT JOIN wa_platform w ON wh.channel='WhatsApp' AND w.metric_date = wh.metric_date;

-- NOTE: no manual INSERT into core.schema_version here. The Schema-Moderator's apply_ddl_file stamps the
--   version row itself (version, applied_at, sql_file) from the NN_ filename prefix — that stamping IS the
--   PK-dedupe that no-ops a duplicate version. A hand-written INSERT would (a) use wrong columns (live table is
--   version/applied_at/sql_file, NOT name) and (b) race the applier's own stamp. Leave registration to the gate.

