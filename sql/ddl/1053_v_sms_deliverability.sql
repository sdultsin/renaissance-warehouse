-- v_sms_deliverability — SMS deliverability tracking by campaign / brand / workspace [2026-06-29]
-- @gate: add
-- Depends on 1052 (main.v_sms_campaign_performance, main.raw_sendivo_campaigns)
--
-- Ask (Ido + Sam, 2026-06-29): track EVERY SMS that goes out and what is DELIVERED vs FAILED — we pay
-- for undelivered messages too. Target 90-95% delivery; trailing-14d overall ~91%, but individual
-- brands run 78-82%. Report by campaign + brand + workspace, NO reason-attribution (Sam: you can't be
-- sure WHY a message failed — bad numbers / T-Mobile limits / brand reputation / scripts — so just
-- track sent vs delivered vs failed and let the breakdown surface the low ones).
--   * delivered / failed come from main.v_sms_campaign_performance (per campaign x day, from /sms/logs).
--   * brand_name from raw_sendivo_campaigns (in this setup each campaign == one brand persona, ~1:1).
--   * LIST-level deliverability is NOT yet possible — list_name is not in the warehouse (it lives only
--     in the Sendivo /blasts API). Tracked as a follow-on: sync /blasts(list_name, sms_sent, delivery_rate).

-- Atomic grain (campaign x day) with brand + workspace — supports any window + any rollup + history.
CREATE OR REPLACE VIEW main.v_sms_deliverability_daily AS
SELECT
    p.metric_date,
    p.campaign_id,
    p.campaign_name,
    c.brand_name,
    p.sub_account_name,
    p.sent,
    p.delivered,
    p.failed,
    ROUND(100.0 * p.delivered / NULLIF(p.sent, 0), 1) AS deliv_pct
FROM main.v_sms_campaign_performance AS p
LEFT JOIN main.raw_sendivo_campaigns AS c USING (campaign_id)
WHERE p.sent > 0;

-- Trailing-14d rolling summary by campaign (carries brand + workspace); flags the sub-90% floor.
CREATE OR REPLACE VIEW main.v_sms_deliverability_14d AS
SELECT
    p.campaign_id,
    any_value(p.campaign_name)    AS campaign_name,
    any_value(c.brand_name)       AS brand_name,
    any_value(p.sub_account_name) AS sub_account_name,
    SUM(p.sent)                   AS sent,
    SUM(p.delivered)              AS delivered,
    SUM(p.failed)                 AS failed,
    ROUND(100.0 * SUM(p.delivered) / NULLIF(SUM(p.sent), 0), 1)      AS deliv_pct,
    ROUND(100.0 * SUM(p.delivered) / NULLIF(SUM(p.sent), 0), 1) < 90 AS below_target_90
FROM main.v_sms_campaign_performance AS p
LEFT JOIN main.raw_sendivo_campaigns AS c USING (campaign_id)
WHERE p.metric_date >= CURRENT_DATE - INTERVAL 14 DAY
  AND p.sent > 0
GROUP BY p.campaign_id;
