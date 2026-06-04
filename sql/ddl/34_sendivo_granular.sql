-- 34 — Sendivo PER-CAMPAIGN granularity (the Instantly-parity layer).
--
-- Two new raw families that the agency-aggregate delivery-metrics (DDL 25) could not give us:
--   * raw_sendivo_campaign_daily  — OUTBOUND funnel per (campaign, sub_account, day, status_group),
--       rolled up on ingest from GET /sms/logs (the only per-campaign source). We do NOT store the
--       ~500-630k raw rows/day — we aggregate during the paged pull. (entities/sendivo_logs.py)
--   * raw_sendivo_inbound         — INBOUND replies, recovered from comms.webhook_receipt
--       (webhook_type='sendivo_inbound') whose raw_payload we already store. Lets us attribute
--       replies/opt-outs to a campaign via the sending number, independent of the (currently broken)
--       comms worker. (entities/sendivo_inbound.py)
--
-- Both feed v_sms_campaign_performance — per-campaign send + reply funnel for the dashboard,
-- the SMS analogue of v_campaign_metrics for Instantly. See specs/14-source-sendivo.md (granular addendum).

-- OUTBOUND: one row per (campaign, sub_account, day, status_group, run). Aggregated on ingest.
CREATE TABLE IF NOT EXISTS raw_sendivo_campaign_daily (
    metric_date       DATE,
    campaign_id       BIGINT,        -- NULL for system messages (opt-out confirmations etc.)
    campaign_name     VARCHAR,
    sub_account_id    BIGINT,
    sub_account_name  VARCHAR,
    status_group      VARCHAR,       -- DELIVERED / UNDELIVERABLE / REJECTED / EXPIRED / PENDING
    n_messages        BIGINT,
    segments          BIGINT,
    cost_usd          DOUBLE,        -- sum(price_per_message) for the group
    _loaded_at        TIMESTAMPTZ NOT NULL,
    _run_id           VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_campday_date ON raw_sendivo_campaign_daily (metric_date);
CREATE INDEX IF NOT EXISTS ix_sv_campday_camp ON raw_sendivo_campaign_daily (campaign_id);

-- INBOUND: one row per recovered inbound reply (from webhook_receipt.raw_payload).
CREATE TABLE IF NOT EXISTS raw_sendivo_inbound (
    inbound_message_id     VARCHAR,    -- payload data.message_id (dedup key)
    received_at            TIMESTAMPTZ,
    prospect_number        VARCHAR,    -- data.from
    our_number             VARCHAR,    -- data.to  (-> maps to a 10DLC campaign)
    message                VARCHAR,    -- data.message (reply text)
    is_opt_out             BOOLEAN,    -- message ~ STOP/UNSUBSCRIBE/etc.
    sub_account_name       VARCHAR,    -- data.sub_account_name
    sendivo_conversation_id BIGINT,    -- data.conversation_id
    contact_email          VARCHAR,
    contact_first_name     VARCHAR,
    contact_last_name      VARCHAR,
    webhook_receipt_id     BIGINT,     -- source row in comms.webhook_receipt
    processed_by_worker    BOOLEAN,    -- did the comms worker process it (vs dropped)?
    _loaded_at             TIMESTAMPTZ NOT NULL,
    _run_id                VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_inbound_recv ON raw_sendivo_inbound (received_at);
CREATE INDEX IF NOT EXISTS ix_sv_inbound_num  ON raw_sendivo_inbound (our_number);

-- Number -> campaign map, derived from the latest /campaigns snapshot (phone_numbers JSON array).
CREATE OR REPLACE VIEW v_sendivo_number_campaign AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) rn
  FROM raw_sendivo_campaigns
)
SELECT
  json_extract_string(pn.unnest, '$.phone_number') AS our_number,
  campaign_id, name AS campaign_name, brand_name, sub_account_id
FROM latest, UNNEST(from_json(phone_numbers, '["json"]')) AS pn(unnest)
WHERE rn = 1 AND phone_numbers IS NOT NULL AND phone_numbers <> 'null';

-- PER-CAMPAIGN performance: outbound funnel (latest run) + inbound replies/opt-outs, by campaign x day.
CREATE OR REPLACE VIEW v_sms_campaign_performance AS
WITH
-- pick the latest RUN per metric_date so re-pulls of a day supersede.
-- (dedup by _run_id, NOT _loaded_at: executemany stamps now() per-row, so _loaded_at is not a run marker.)
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
inbound AS (
  SELECT i.received_at::date metric_date, nc.campaign_id,
         count(*) replies,
         count(*) FILTER (WHERE i.is_opt_out) opt_outs,
         count(*) FILTER (WHERE NOT i.is_opt_out) positive_replies
  FROM raw_sendivo_inbound i
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
