-- comms-orchestration mirror gap-close (Spec 16 — WS-E): the three comms tables
-- that the v1 mirror (16_comms_mirror.sql) intentionally left out but which carry
-- real analytic value: close_sync (Close.com API audit), gbc_application (GBC
-- partner webform submissions), app_link_check (CRM application-status poll log).
--
-- ADDITIVE ONLY: this file ADDS three new raw_comms_* tables. It does NOT touch
-- any table defined in 16_comms_mirror.sql (additive invariant, Spec 16 §3).
--
-- webhook_receipt (6.18M rows) is STILL excluded by design — it is a raw
-- pre-processing webhook log (noise, no analytic value); see the note in
-- entities/comms_mirror.py.
--
-- Mirrored via DuckDB postgres_scanner (see entities/comms_mirror.py), same
-- conventions as 16_comms_mirror.sql:
--   * raw_* tables hold exactly ONE full snapshot (REPLACE-style since
--     2026-07-01, warehouse-flags#12; one row per source id).
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * Mirror DELETEs the whole table then INSERTs a fresh snapshot (atomic per
--     table, idempotent).
--
-- Column names/types verified 2026-06-08 against the live source via the
-- comms-orchestration Supabase MCP (list_tables verbose, schema comms).
--
-- Type conventions (Postgres -> DuckDB), identical to 16_comms_mirror.sql:
--   text                       -> VARCHAR
--   integer (int4)             -> INTEGER
--   bigint  (int8)             -> BIGINT
--   boolean                    -> BOOLEAN
--   timestamp with time zone   -> TIMESTAMPTZ
--   jsonb                      -> VARCHAR  (CAST in the SELECT)

-- comms.close_sync  (audit log of every Close.com API call; ~16.5k rows)
-- request_payload / response_payload are jsonb -> cast to VARCHAR.
CREATE TABLE IF NOT EXISTS raw_comms_close_sync (
    id                  BIGINT,
    opportunity_id      BIGINT,
    action              VARCHAR,
    request_payload     VARCHAR,    -- jsonb -> text
    response_payload    VARCHAR,    -- jsonb -> text
    http_status         INTEGER,
    success             BOOLEAN,
    error_message       VARCHAR,
    attempted_at        TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

-- comms.gbc_application  (GBC partner webform submissions; small, ~tens of rows)
-- raw_payload / suppression_log are jsonb -> cast to VARCHAR.
CREATE TABLE IF NOT EXISTS raw_comms_gbc_application (
    id                      BIGINT,
    application_id          VARCHAR,
    received_env            VARCHAR,
    event                   VARCHAR,
    form_type               VARCHAR,
    ref                     VARCHAR,
    submitted_at            TIMESTAMPTZ,
    applicant_email         VARCHAR,
    applicant_phone         VARCHAR,
    applicant_first_name    VARCHAR,
    applicant_last_name     VARCHAR,
    applicant_company       VARCHAR,
    applicant_company_ein   VARCHAR,
    raw_payload             VARCHAR,    -- jsonb -> text
    signature_valid         BOOLEAN,
    source_ip               VARCHAR,
    received_at             TIMESTAMPTZ,
    suppression_log         VARCHAR,    -- jsonb -> text
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- comms.app_link_check  (audit log of every CRM poll for application status;
-- currently 0 rows but go-forward signal for the app-link funnel)
-- raw_response is jsonb -> cast to VARCHAR.
CREATE TABLE IF NOT EXISTS raw_comms_app_link_check (
    id                  BIGINT,
    conversation_id     BIGINT,
    checked_at          TIMESTAMPTZ,
    result              VARCHAR,
    raw_response        VARCHAR,    -- jsonb -> text
    error_message       VARCHAR,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);
