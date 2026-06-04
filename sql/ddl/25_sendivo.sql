-- Phase 3: Sendivo SMS send-side source (spec 14).
-- Applied at schema version 25 by scripts/setup_db.py / orchestrator DDL applier.
--
-- Sendivo API (https://app.sendivo.io/api/v1, Bearer SENDIVO_API_KEY) — the SEND side of
-- SMS (volume/delivery/opt-out/response + cost), complementing the comms-orchestration
-- reply/opportunity layer we already mirror. Standard raw_* append-only convention.
-- Cost folds into core.cost_ledger (no new canonical table); v_sms_performance feeds the
-- dashboard SMS tile. See specs/14-source-sendivo.md.

-- /delivery-metrics — one row per (scope, metric_date, run). scope='agency' in v1.
CREATE TABLE IF NOT EXISTS raw_sendivo_delivery_metrics (
    scope                 VARCHAR,
    metric_date           DATE,
    sms_sent              BIGINT,
    segments_sent         BIGINT,
    inbound_sms_received  BIGINT,
    delivery_rate         DOUBLE,
    opt_out_rate          DOUBLE,
    response_rate         DOUBLE,
    _loaded_at            TIMESTAMPTZ NOT NULL,
    _run_id               VARCHAR
);

-- /campaigns — full snapshot each run (small). Doubles as a UI correctness check (30/16/14).
CREATE TABLE IF NOT EXISTS raw_sendivo_campaigns (
    campaign_id     BIGINT,
    name            VARCHAR,
    status          VARCHAR,
    brand_id        BIGINT,
    brand_name      VARCHAR,
    phone_numbers   VARCHAR,   -- JSON array
    sub_account_id  BIGINT,
    created_at      TIMESTAMPTZ,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- /brands — full snapshot each run.
CREATE TABLE IF NOT EXISTS raw_sendivo_brands (
    brand_id            BIGINT,
    name                VARCHAR,
    legal_company_name  VARCHAR,
    verification_status VARCHAR,
    registration_state  VARCHAR,
    campaigns_count     INTEGER,
    sub_account_id      BIGINT,
    created_at          TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

-- /billing/report — one row per (sub_account, period, run). Itemized fees + raw_json.
CREATE TABLE IF NOT EXISTS raw_sendivo_billing (
    sub_account_id        BIGINT,
    location_id           VARCHAR,
    period_start          DATE,
    period_end            DATE,
    total_spend           DOUBLE,
    sms_fee_qty           BIGINT,
    sms_fee_usd           DOUBLE,
    carrier_fee_qty       BIGINT,
    carrier_fee_usd       DOUBLE,
    campaign_setup_usd    DOUBLE,
    campaign_renewal_usd  DOUBLE,
    brand_fee_usd         DOUBLE,
    phone_setup_usd       DOUBLE,
    phone_renewal_usd     DOUBLE,
    raw_json              VARCHAR,
    _loaded_at            TIMESTAMPTZ NOT NULL,
    _run_id               VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_raw_sendivo_dm_date ON raw_sendivo_delivery_metrics (metric_date);

-- ------------------------------------------------------------------
-- v_sms_performance — the SMS dashboard surface. Latest snapshot per (scope, date).
-- ------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sms_performance AS
WITH latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY scope, metric_date ORDER BY _loaded_at DESC) AS rn
  FROM raw_sendivo_delivery_metrics
)
SELECT metric_date, scope, sms_sent, segments_sent, inbound_sms_received,
       delivery_rate, opt_out_rate, response_rate
FROM latest WHERE rn = 1;
