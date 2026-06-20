-- Version 92 (2026-06-20) — core.account_registry: formalize and fully seed from
-- FINAL DATA - ALL.csv (2,656,099 rows, one row per sending account email address).
--
-- History: the table was created ad-hoc before DDL tracking; it existed in the
-- warehouse with 158,772 rows (MailIn + MilkBox only, no DDL record). This file
-- formalizes the schema and upserts the full fleet from seed_data/final_data_registry.csv.
--
-- Columns sourced from FINAL DATA: email, domain, rg_tag (RG# Tag), vendor (Provider Tag),
-- batch_tag, workspace_label. The remaining columns (first_name, last_name, gender,
-- inbox_type, panel, offer, cohort, source_tab, email_tag, rg_range, status) are
-- preserved from existing rows via COALESCE — they are populated by other pipelines
-- and must not be overwritten with NULL.
--
-- Conflict strategy: ON CONFLICT (email) DO UPDATE with COALESCE so that:
--   - warehouse value always wins: existing non-null values are never overwritten.
--   - FINAL DATA fills in only when the warehouse column is NULL.
--   - All other columns are untouched (excluded.* is NULL for those).
--   - _staged_at is always refreshed.
--
-- @gate: add

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.account_registry (
    email            VARCHAR NOT NULL,
    domain           VARCHAR,
    first_name       VARCHAR,
    last_name        VARCHAR,
    rg_tag           VARCHAR,
    rg_range         VARCHAR,
    email_tag        VARCHAR,
    vendor           VARCHAR,
    batch_tag        VARCHAR,
    workspace_label  VARCHAR,
    inbox_type       VARCHAR,
    status           VARCHAR,
    gender           VARCHAR,
    panel            VARCHAR,
    offer            VARCHAR,
    cohort           VARCHAR,
    source_tab       VARCHAR,
    _staged_at       TIMESTAMPTZ,
    PRIMARY KEY (email)
);

INSERT INTO core.account_registry (email, domain, rg_tag, vendor, batch_tag, workspace_label, _staged_at)
SELECT
    email,
    NULLIF(domain, '')          AS domain,
    NULLIF(rg_tag, '')          AS rg_tag,
    NULLIF(vendor, '')          AS vendor,
    NULLIF(batch_tag, '')       AS batch_tag,
    NULLIF(workspace_label, '') AS workspace_label,
    now()                       AS _staged_at
FROM read_csv_auto('seed_data/final_data_registry.csv', header=true, nullstr='')
WHERE (SELECT count(*) FROM glob('seed_data/final_data_registry.csv')) > 0
ON CONFLICT (email) DO UPDATE SET
    domain          = COALESCE(core.account_registry.domain,          NULLIF(excluded.domain, '')),
    rg_tag          = COALESCE(core.account_registry.rg_tag,          NULLIF(excluded.rg_tag, '')),
    vendor          = COALESCE(core.account_registry.vendor,          NULLIF(excluded.vendor, '')),
    batch_tag       = COALESCE(core.account_registry.batch_tag,       NULLIF(excluded.batch_tag, '')),
    workspace_label = COALESCE(core.account_registry.workspace_label, NULLIF(excluded.workspace_label, '')),
    _staged_at      = excluded._staged_at;
