-- core.lead_attrs — pre-built scraped attrs for signal leads (Spec 16, WS-F Task 3).
--
-- Materialized nightly by scripts/prebuild_lead_attrs.py (runs at 02:30 UTC, before the
-- 03:30+ nightly orchestrator). Populated from mirror.leads_current in lead_mirror.duckdb,
-- filtered to only the ~247k signal leads already in core.lead — keeping the ATTACH join
-- outside the nightly writer lock window.
--
-- lead_spine.py joins against this table (fast in-warehouse, no ATTACH during nightly) to
-- populate first_name/company/segment/industry/lead_source on core.lead.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.lead_attrs (
    email               VARCHAR PRIMARY KEY,
    first_name          VARCHAR,
    company_name        VARCHAR,
    general_industry    VARCHAR,
    specific_industry   VARCHAR,
    seniority           VARCHAR,
    company_size        BIGINT,
    city                VARCHAR,
    state               VARCHAR,
    source              VARCHAR,
    source_list_name    VARCHAR
);
