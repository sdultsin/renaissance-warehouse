-- Phase 3: core.recipient_domain + ESP×ESP send matrix (spec 08).
-- Applied at schema version 24 by scripts/setup_db.py / orchestrator DDL applier.
--
-- core.recipient_domain — one row per recipient (lead) domain we send to, classified
-- to a receiving ESP. Two-tier classification (avoids a 3.25M-domain full MX sweep):
--   1. consumer-ESP map  — gmail/outlook/yahoo/icloud/isp constants (no MX needed;
--      gmail alone is ~36% of send volume). classification_method='consumer_map'.
--   2. MX lookup         — for the top company domains BY SEND VOLUME only; reuses
--      sources/dns.py resolve_mx (MX-only, fast). classification_method='mx_lookup'.
--   Long tail (millions of 1-2 send company domains) stays recipient_esp='unknown'
--   (classification_method='unclassified') — low volume, not worth sweeping (GAP F5).
-- Shape mirrors the proven recipient_esp_lookup table from the prior May-13 esp_rr job.
--
-- raw_recipient_mx — append-only MX sweep output (domain, mx_host, provider, run).
--
-- The ESP×ESP matrix itself is a materialized table built by entities/esp_matrix.py
-- (postgres_scanner aggregation of contact_frequency_campaign_daily — NOT mirrored raw).

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS raw_recipient_mx (
    domain        VARCHAR,
    mx_host       VARCHAR,       -- top (lowest-priority) MX hostname
    mx_provider   VARCHAR,       -- google | outlook | mimecast | barracuda | other | none
    mx_error      VARCHAR,
    _loaded_at    TIMESTAMPTZ NOT NULL,
    _run_id       VARCHAR
);

CREATE TABLE IF NOT EXISTS core.recipient_domain (
    domain                VARCHAR PRIMARY KEY,
    recipient_esp         VARCHAR NOT NULL,   -- google | microsoft | yahoo | apple | isp | other | unknown
    mx_host               VARCHAR,            -- NULL for consumer-map rows
    mx_provider           VARCHAR,            -- raw MX classification (mx_lookup rows)
    classification_method VARCHAR NOT NULL,   -- consumer_map | mx_lookup | unclassified
    send_volume           BIGINT,             -- SUM(sent_count) over the scored window (prioritization + coverage)
    resolved_at           TIMESTAMPTZ,
    last_error            VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_core_recipient_domain_esp ON core.recipient_domain (recipient_esp);
CREATE INDEX IF NOT EXISTS ix_raw_recipient_mx_domain   ON raw_recipient_mx (domain);

-- ESP×ESP send matrix (materialized by entities/esp_matrix.py in the 'derived' phase).
-- One row per (week, sender_esp, recipient_esp). sends from contact_frequency; replies
-- attributed via reply_data (lead_email→domain→recipient_esp). Opportunities per cell
-- are deferred (aggregate-only at campaign grain — can't split by recipient ESP cleanly).
CREATE TABLE IF NOT EXISTS mv_esp_send_matrix (
    week_start       DATE,
    sender_esp       VARCHAR,        -- google | outlook | otd (raw_pipeline_campaigns.infra_type)
    recipient_esp    VARCHAR,        -- core.recipient_domain.recipient_esp
    sends            BIGINT,
    human_replies    BIGINT,
    reply_per_1k     DOUBLE,
    domains_covered  BIGINT,         -- distinct recipient domains in this cell that were classified
    _resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_mv_esp_matrix ON mv_esp_send_matrix (sender_esp, recipient_esp, week_start);
