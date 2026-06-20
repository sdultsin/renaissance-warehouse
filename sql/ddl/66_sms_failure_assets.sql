-- 66 — Sendivo SMS: failure-reason breakdown (G1), intraday grain (G3),
--      phone-number inventory (G2), sub-account dim (G7) + richer brand/campaign metadata (G6).
--
-- Closes the highest-value gaps from deliverables/2026-06-14-sms-sync-audit/REPORT.md.
-- Source-side changes live in entities/sendivo_logs.py (G1/G3) + entities/sendivo.py (G2/G6/drift)
-- + sources/sendivo.py (phone_numbers + pagination guard). This file owns the new RAW tables that
-- entities/sendivo.py relies on (it has no CREATE TABLE of its own), the additive columns, and the
-- consumer views. All statements are idempotent (IF NOT EXISTS / OR REPLACE / ADD COLUMN IF NOT
-- EXISTS) so the whole file applies once in a single transaction and is safe to re-run.
-- Confirmed live 2026-06-14: /blasts, /conversations, /inbox, /sub-accounts -> 404 (no per-blast or
-- deal-status endpoint on our key; G4/G8 are not buildable Sendivo-side). /phone-numbers, /brands,
-- /campaigns return the full set un-paginated. delivery-metrics honours sub_account_id.

-- ---------------------------------------------------------------------------
-- New RAW tables (entities create these too via IF NOT EXISTS; declared here so
-- setup_db materialises them before the views below on a fresh DB).
-- ---------------------------------------------------------------------------

-- G1: granular status/error breakdown, per (day, sub, campaign, status, status_group, status_name,
--     error_description). error_description is the actionable account-health signal.
CREATE TABLE IF NOT EXISTS raw_sendivo_failure_daily (
    metric_date        DATE,
    sub_account_id     BIGINT,
    sub_account_name   VARCHAR,
    campaign_id        BIGINT,
    campaign_name      VARCHAR,
    status             VARCHAR,    -- delivered / undeliverable / rejected / expired / ...
    status_group       VARCHAR,    -- DELIVERED / UNDELIVERABLE / REJECTED / EXPIRED / PENDING
    status_name        VARCHAR,    -- DELIVERED_TO_HANDSET / REJECTED_PREFIX_MISSING / ...
    error_description  VARCHAR,    -- "End user out of prepay credit" / "Invalid destination address" / ...
    n_messages         BIGINT,
    segments           BIGINT,
    cost_usd           DOUBLE,
    _loaded_at         TIMESTAMPTZ NOT NULL,
    _run_id            VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_faild_date ON raw_sendivo_failure_daily (metric_date);

-- G3: intraday OUTBOUND grain (UTC hour). Reply-by-hour is derived from raw_sendivo_inbound directly.
CREATE TABLE IF NOT EXISTS raw_sendivo_hourly (
    metric_date        DATE,
    hour_utc           INTEGER,
    sub_account_id     BIGINT,
    sub_account_name   VARCHAR,
    campaign_id        BIGINT,
    campaign_name      VARCHAR,
    n_messages         BIGINT,
    delivered_messages BIGINT,
    segments           BIGINT,
    _loaded_at         TIMESTAMPTZ NOT NULL,
    _run_id            VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_hourly_date ON raw_sendivo_hourly (metric_date);

-- G2: /phone-numbers inventory snapshot (full set each run, ~262 rows).
CREATE TABLE IF NOT EXISTS raw_sendivo_phone_numbers (
    phone_number_id    BIGINT,
    phone_number       VARCHAR,
    friendly_name      VARCHAR,
    number_status      VARCHAR,
    messaging_status   VARCHAR,
    phone_number_type  VARCHAR,
    is_default         BOOLEAN,
    campaign_id        BIGINT,
    campaign_name      VARCHAR,
    campaign_status    VARCHAR,
    brand_id           BIGINT,
    brand_name         VARCHAR,
    sub_account_id     BIGINT,
    tags               VARCHAR,
    purchase_date      TIMESTAMPTZ,
    renewal_date       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ NOT NULL,
    _run_id            VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sv_phone_num ON raw_sendivo_phone_numbers (phone_number);

-- ---------------------------------------------------------------------------
-- G6: additive metadata columns on the existing snapshots (DDL 25).
-- ---------------------------------------------------------------------------
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS description     VARCHAR;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS tcr_status      VARCHAR;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS campaign_type   VARCHAR;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS use_case        VARCHAR;  -- JSON array
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS is_default      BOOLEAN;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS registered_on   VARCHAR;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS expiration_date VARCHAR;
ALTER TABLE raw_sendivo_campaigns ADD COLUMN IF NOT EXISTS auto_renew      BOOLEAN;

ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS dba_name              VARCHAR;
ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS country               VARCHAR;
ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS vertical_type         VARCHAR;
ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS website               VARCHAR;
ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS vetting_score         VARCHAR;
ALTER TABLE raw_sendivo_brands ADD COLUMN IF NOT EXISTS brand_identity_status VARCHAR;

-- ---------------------------------------------------------------------------
-- Consumer views.
-- ---------------------------------------------------------------------------

-- G1 — full status/error breakdown, latest run per metric_date (re-pulls supersede).
CREATE OR REPLACE VIEW v_sms_failure_reasons AS
WITH run_rank AS (
  SELECT metric_date, _run_id,
         ROW_NUMBER() OVER (PARTITION BY metric_date ORDER BY max(_loaded_at) DESC) rn
  FROM raw_sendivo_failure_daily GROUP BY metric_date, _run_id
),
latest AS (
  SELECT f.* FROM raw_sendivo_failure_daily f
  JOIN run_rank r ON f.metric_date = r.metric_date AND f._run_id = r._run_id AND r.rn = 1
)
SELECT metric_date, sub_account_id, sub_account_name, campaign_id, campaign_name,
       status, status_group, status_name, error_description,
       sum(n_messages) AS n_messages, sum(segments) AS segments, sum(cost_usd) AS cost_usd
FROM latest
GROUP BY ALL;

-- G1 — "top failure reasons" table: non-delivered only, collapsed to the human reason.
CREATE OR REPLACE VIEW v_sms_failure_summary AS
SELECT metric_date, sub_account_id, sub_account_name, status_group,
       COALESCE(NULLIF(error_description, 'No Error'), status_name) AS reason,
       sum(n_messages) AS n_messages
FROM v_sms_failure_reasons
WHERE status_group IS DISTINCT FROM 'DELIVERED'
GROUP BY ALL;

-- G3 — OUTBOUND send volume by UTC hour, latest run per metric_date.
CREATE OR REPLACE VIEW v_sms_send_by_hour AS
WITH run_rank AS (
  SELECT metric_date, _run_id,
         ROW_NUMBER() OVER (PARTITION BY metric_date ORDER BY max(_loaded_at) DESC) rn
  FROM raw_sendivo_hourly GROUP BY metric_date, _run_id
),
latest AS (
  SELECT h.* FROM raw_sendivo_hourly h
  JOIN run_rank r ON h.metric_date = r.metric_date AND h._run_id = r._run_id AND r.rn = 1
)
SELECT metric_date, hour_utc, sub_account_id, sub_account_name,
       sum(n_messages) AS sent, sum(delivered_messages) AS delivered, sum(segments) AS segments
FROM latest
GROUP BY ALL;

-- G3 — INBOUND replies by UTC hour (free from raw_sendivo_inbound.received_at). Feeds the heatmap.
CREATE OR REPLACE VIEW v_sms_reply_by_hour AS
SELECT received_at::date AS metric_date,
       extract('hour' FROM received_at)::int AS hour_utc,
       sub_account_name,
       count(*) AS replies,
       count(*) FILTER (WHERE is_opt_out) AS opt_outs,
       count(*) FILTER (WHERE NOT is_opt_out) AS positive_replies
FROM raw_sendivo_inbound
GROUP BY ALL;

-- G7 — sub-account id -> name dim (no /sub-accounts endpoint exists; recovered from /sms/logs rows).
CREATE OR REPLACE VIEW dim_sendivo_sub_account AS
SELECT sub_account_id, any_value(sub_account_name) AS sub_account_name
FROM (
  SELECT sub_account_id, sub_account_name FROM raw_sendivo_campaign_daily
  WHERE sub_account_id IS NOT NULL AND sub_account_name IS NOT NULL
  UNION ALL
  SELECT sub_account_id, sub_account_name FROM raw_sendivo_failure_daily
  WHERE sub_account_id IS NOT NULL AND sub_account_name IS NOT NULL
)
GROUP BY sub_account_id;

-- G2 — current phone-number inventory (latest snapshot per number).
CREATE OR REPLACE VIEW v_sendivo_phone_inventory AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY phone_number_id ORDER BY _loaded_at DESC) rn
  FROM raw_sendivo_phone_numbers
)
SELECT phone_number_id, phone_number, friendly_name, number_status, messaging_status,
       phone_number_type, is_default, campaign_id, campaign_name, campaign_status,
       brand_id, brand_name, sub_account_id, tags, purchase_date, renewal_date
FROM latest WHERE rn = 1;

-- G2/G6 — sending-asset health: single-row snapshot of campaign-approval + number + brand state.
CREATE OR REPLACE VIEW v_sendivo_asset_health AS
WITH camp AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) rn
    FROM raw_sendivo_campaigns
  ) WHERE rn = 1
),
num AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY phone_number_id ORDER BY _loaded_at DESC) rn
    FROM raw_sendivo_phone_numbers
  ) WHERE rn = 1
),
brd AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand_id ORDER BY _loaded_at DESC) rn
    FROM raw_sendivo_brands
  ) WHERE rn = 1
)
SELECT
  (SELECT count(*) FROM camp)                                          AS campaigns_total,
  (SELECT count(*) FROM camp WHERE status = 'Carriers Approved')       AS campaigns_approved,
  (SELECT count(*) FROM camp WHERE status = 'Carriers Pending')        AS campaigns_pending,
  (SELECT count(*) FROM camp WHERE status = 'Sendivo Review')          AS campaigns_in_review,
  (SELECT count(*) FROM camp WHERE status = 'Carriers Rejected')       AS campaigns_rejected,
  (SELECT count(*) FROM num)                                           AS numbers_total,
  (SELECT count(*) FROM num WHERE messaging_status = 'active')         AS numbers_messaging_active,
  (SELECT count(*) FROM num WHERE campaign_id IS NULL)                 AS numbers_unassigned,
  (SELECT count(*) FROM brd)                                           AS brands_total,
  (SELECT count(*) FROM brd WHERE verification_status = 'verified')    AS brands_verified;
