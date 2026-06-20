-- 91_sms_reply_sentiment_views.sql — wire the SMS qwen sentiment signal into the funnel views,
-- dedup the inflated inbound counts, and fix the stale SMS-meetings source.
-- @gate: replace
-- Depends on 34 (v_sms_campaign_performance / v_sendivo_number_campaign) 62 (v_kpi_sms)
--   65 (core.meeting channel) 89 (v_omni_sms_performance) 90 (derived.sms_reply_is_positive_qwen)
--
-- THREE changes, all CREATE OR REPLACE VIEW (idempotent; same column lists/types — no schema break):
--
--   (A) v_omni_sms_performance — REPOINT positive/negative + human/auto to the qwen classifier.
--       Before: positive_replies = not-opt-out (INTERIM proxy, overcounts ~by every non-opt-out
--       reply incl. Block/Not-relevant/wrong-number); replies_human/auto = NULL. After:
--       positive_replies = qwen STRICT-positive among non-opt-out; negative_replies = opt-outs +
--       non-opt-out classified-negative; replies_auto = deterministic SMS auto-replies; positive_
--       signal = 'sendivo_qwen_strict'. Inbound metrics are sourced from a DEDUPED raw_sendivo_inbound
--       base (distinct inbound_message_id, latest copy) — raw_sendivo_inbound re-ingests the same
--       message across _run_id, so the old sum(replies)/sum(positive_replies) over it were inflated.
--
--   (B) v_sms_campaign_performance — DEDUP the inbound CTE (root bug fix). The outbound side already
--       picks the latest _run_id (run_rank), but the inbound side did a raw count(*) over
--       raw_sendivo_inbound with NO latest-run filter, so replies/opt_outs/positive_replies were
--       inflated by per-day _run_id duplication (1.1x recent, up to ~5x older; all-time 670,579 raw
--       vs 73,382 distinct). Ingest only DELETEs its own _run_id then re-inserts, so copies
--       accumulate. Columns/semantics unchanged (positive_replies stays = not-opt-out here) — pure
--       VALUE correction (removes inflation). Downstream consumers (v_kpi_sms, dash.* SMS) inherit
--       the corrected, deduped counts. HISTORICAL SMS reply/opt-out/opp counts will DROP to their
--       true deduped values.
--
--   (C) v_kpi_sms — FIX the stale meetings source. It pulled SMS meetings from the legacy
--       source='slack' + regexp('sendivo|sms|whatsapp|iskra'); the current sync writes meetings as
--       source='sheet', channel='SMS' (DDL 65 channel-aware), so the old clause missed them all
--       (1,161 MTD). Repointed to is_duplicate_of IS NULL AND source='sheet' AND channel='SMS',
--       matching v_omni_sms_performance / v_omni_email_performance.

-- =====================================================================================
-- (B) v_sms_campaign_performance — dedup the inbound side to distinct inbound_message_id.
-- =====================================================================================
CREATE OR REPLACE VIEW v_sms_campaign_performance AS
WITH
run_rank AS (
  SELECT metric_date, _run_id,
         ROW_NUMBER() OVER (PARTITION BY metric_date ORDER BY max(_loaded_at) DESC) rn
  FROM raw_sendivo_campaign_daily GROUP BY metric_date, _run_id
),
out_day AS (
  SELECT cd.* FROM raw_sendivo_campaign_daily cd
  JOIN run_rank r ON cd.metric_date=r.metric_date AND cd._run_id=r._run_id AND r.rn=1
),
outbound AS (
  SELECT metric_date, campaign_id, any_value(campaign_name) campaign_name,
         any_value(sub_account_name) sub_account_name,
         sum(n_messages) sent,
         sum(n_messages) FILTER (WHERE status_group='DELIVERED') delivered,
         sum(n_messages) FILTER (WHERE status_group IN ('UNDELIVERABLE','REJECTED','EXPIRED')) failed,
         sum(n_messages) FILTER (WHERE status_group='PENDING') pending,
         sum(segments) segments, sum(cost_usd) cost_usd
  FROM out_day GROUP BY metric_date, campaign_id
),
-- inbound DEDUPED to one row per inbound_message_id (latest copy) — fixes _run_id re-ingestion inflation.
inb_dedup AS (
  SELECT inbound_message_id, received_at, our_number, is_opt_out
  FROM (
    SELECT inbound_message_id, received_at, our_number, is_opt_out,
           -- Latest-copy wins, tiebroken by our_number/received_at so attribution is deterministic
           -- (not _loaded_at-order dependent). Sendivo payloads for a given message_id are immutable,
           -- so is_opt_out/received_at are stable across copies; latest = current payload regardless.
           ROW_NUMBER() OVER (PARTITION BY inbound_message_id
                              ORDER BY _loaded_at DESC, our_number, received_at) rn
    FROM raw_sendivo_inbound
  ) WHERE rn = 1
),
inbound AS (
  SELECT i.received_at::date metric_date, nc.campaign_id,
         count(*) replies,
         count(*) FILTER (WHERE i.is_opt_out) opt_outs,
         count(*) FILTER (WHERE NOT i.is_opt_out) positive_replies
  FROM inb_dedup i
  LEFT JOIN v_sendivo_number_campaign nc ON '+'||ltrim(i.our_number,'+') = nc.our_number
  GROUP BY 1,2
)
SELECT
  COALESCE(o.metric_date, n.metric_date) AS metric_date,
  COALESCE(o.campaign_id, n.campaign_id) AS campaign_id,
  o.campaign_name, o.sub_account_name,
  o.sent, o.delivered, o.failed, o.pending, o.segments, o.cost_usd,
  CASE WHEN o.sent>0 THEN round(100.0*o.delivered/o.sent,2) END AS delivery_rate,
  n.replies, n.opt_outs, n.positive_replies,
  CASE WHEN o.delivered>0 THEN round(100.0*n.replies/o.delivered,2) END AS reply_rate
FROM outbound o
FULL OUTER JOIN inbound n ON o.metric_date=n.metric_date AND o.campaign_id=n.campaign_id;

-- =====================================================================================
-- (A) v_omni_sms_performance — qwen strict-positive + human/auto, deduped inbound base.
-- 18-column parity contract (channel, metric_date, sent, delivered, failed, replies_total,
-- replies_human, replies_auto, positive_replies, negative_replies, opt_outs, meetings_booked,
-- deals_won, cost_usd, delivery_rate, reply_rate, positive_rate, positive_signal).
-- =====================================================================================
CREATE OR REPLACE VIEW v_omni_sms_performance AS
WITH
-- outbound funnel + cost: from the per-campaign view (outbound is correctly run-deduped).
out_day AS (
  SELECT metric_date, sum(sent) AS sent, sum(delivered) AS delivered,
         sum(failed) AS failed, sum(cost_usd) AS cost_usd
  FROM v_sms_campaign_performance
  GROUP BY 1
),
-- inbound DEDUPED to distinct inbound_message_id, LEFT JOIN the qwen classifier.
-- UNCLASSIFIED semantics: positive = non-opt-out labeled positive; negative = opt-outs + non-opt-out
-- labeled NEGATIVE (q.is_positive = false, NOT a COALESCE of the LEFT-JOIN miss). A non-opt-out reply
-- with NO qwen label (q.is_positive IS NULL — e.g. a reply newer than the backfill, before the
-- incremental classifier runs) is counted in replies_total but in NEITHER positive NOR negative, so
-- it cannot silently inflate negatives. Therefore positive_replies + negative_replies <= replies_total
-- (the gap = unclassified non-opt-out). The seed (~73,382) covers the residual through the backfill
-- date; net-new replies are classified incrementally so the gap stays ~0 going forward.
inb_dedup AS (
  SELECT inbound_message_id, received_at, is_opt_out
  FROM (
    SELECT inbound_message_id, received_at, is_opt_out,
           ROW_NUMBER() OVER (PARTITION BY inbound_message_id
                              ORDER BY _loaded_at DESC, received_at) rn
    FROM raw_sendivo_inbound
  ) WHERE rn = 1
),
inb AS (
  SELECT i.received_at::date AS metric_date,
         count(*)                                                              AS replies_total,
         count(*) FILTER (WHERE i.is_opt_out)                                  AS opt_outs,
         count(*) FILTER (WHERE NOT i.is_opt_out AND q.is_positive = true)     AS positive_replies,
         count(*) FILTER (WHERE i.is_opt_out
                          OR (NOT i.is_opt_out AND q.is_positive = false))      AS negative_replies,
         count(*) FILTER (WHERE NOT i.is_opt_out AND q.is_human = false)       AS replies_auto
  FROM inb_dedup i
  LEFT JOIN derived.sms_reply_is_positive_qwen q ON q.reply_id = i.inbound_message_id
  GROUP BY 1
),
mtg AS (  -- meetings = Google Sheet SoT, channel='SMS' (the current channel-aware sync)
  SELECT posted_at::date AS metric_date, count(*) AS meetings
  FROM core.meeting WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'SMS'
  GROUP BY 1
),
dates AS (
  SELECT metric_date FROM out_day
  UNION SELECT metric_date FROM inb
  UNION SELECT metric_date FROM mtg
)
SELECT
  'sms'                                                 AS channel,
  d.metric_date,
  o.sent,
  o.delivered,
  o.failed,
  i.replies_total,
  (i.replies_total - COALESCE(i.replies_auto,0))        AS replies_human,   -- human = total − deterministic auto-replies
  i.replies_auto,                                                           -- carrier/auto-responder/DND auto-replies (deterministic)
  i.positive_replies,                                                       -- qwen STRICT-positive (genuine interest), non-opt-out
  i.negative_replies,                                                       -- opt-outs + non-opt-out classified-negative
  i.opt_outs,
  COALESCE(m.meetings, 0)                               AS meetings_booked,
  CAST(NULL AS BIGINT)                                  AS deals_won,       -- NULL: separate BoF instrument
  o.cost_usd,
  CAST(o.delivered AS DOUBLE) / nullif(o.sent,0)        AS delivery_rate,
  CAST(i.replies_total AS DOUBLE) / nullif(o.sent,0)    AS reply_rate,
  CAST(i.positive_replies AS DOUBLE) / nullif(o.sent,0) AS positive_rate,
  'sendivo_qwen_strict'                                 AS positive_signal
FROM dates d
LEFT JOIN out_day o ON o.metric_date = d.metric_date
LEFT JOIN inb i     ON i.metric_date = d.metric_date
LEFT JOIN mtg m     ON m.metric_date = d.metric_date;

-- =====================================================================================
-- (C) v_kpi_sms — fix the stale SMS-meetings source (slack-regex -> sheet/channel='SMS').
-- Send-side unchanged (inherits the deduped v_sms_campaign_performance from (B)).
-- =====================================================================================
CREATE OR REPLACE VIEW v_kpi_sms AS
SELECT
    p.metric_date                AS date,
    p.campaign_id,
    p.campaign_name,
    p.sub_account_name,
    p.sent,
    p.delivered,
    p.replies,
    p.positive_replies           AS opportunities,
    p.opt_outs,
    p.cost_usd,
    0                            AS meetings,
    CAST(p.positive_replies AS DOUBLE) / NULLIF(p.delivered, 0) AS opp_rate,
    CAST(p.replies AS DOUBLE)          / NULLIF(p.delivered, 0) AS reply_rate
FROM v_sms_campaign_performance p
UNION ALL
SELECT
    CAST(m.posted_at AS DATE)    AS date,
    NULL                         AS campaign_id,
    '(sms meetings)'             AS campaign_name,
    NULL                         AS sub_account_name,
    0 AS sent, 0 AS delivered, 0 AS replies, 0 AS opportunities, 0 AS opt_outs,
    0.0 AS cost_usd,
    COUNT(*)                     AS meetings,
    NULL AS opp_rate, NULL AS reply_rate
FROM core.meeting m
WHERE m.is_duplicate_of IS NULL AND m.source = 'sheet' AND m.channel = 'SMS'
GROUP BY 1;
