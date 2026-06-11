-- comms.lead_application mirror (2026-06-11): web-form funding applications from
-- the Lumara apply form (lumara-capital.com/apply-now/, Fluent Forms #4) routed
-- to funding partners (GBC today; round-robin w/ Ken planned). This is the SMS
-- AIM app-link funnel's CONVERSION event — the action data Sam wants in the
-- warehouse alongside the rest of the comms mirror.
--
-- ADDITIVE ONLY: adds one new raw_comms_* table; touches nothing else
-- (additive invariant, Spec 16 §3).
--
-- Mirrored via DuckDB postgres_scanner (entities/comms_mirror.py), same
-- conventions as 16_comms_mirror.sql / 47_comms_mirror_gaps.sql:
--   * raw_* tables are append-only snapshots keyed by _run_id.
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * Mirror DELETEs by _run_id then INSERTs (idempotent within a run).
--
-- Column names/types verified 2026-06-11 against the live source (psql
-- information_schema, comms schema). Source rows include:
--   * partner_status='sent'              — pushed to the partner CRM (GBC 23T)
--   * partner_status='error'             — push failed; replayed by the worker's
--                                          /scheduled-gbc-retry sweep
--   * partner_status='skipped_backfill' + partner='none'
--                                        — 114 historical Fluent Forms entries
--                                          (2026-03-03..2026-06-08) backfilled
--                                          2026-06-11; NEVER sent to any partner.
--   * raw (jsonb -> VARCHAR) = full normalized application (every form field,
--     incl. ones with no partner-CRM slot, e.g. monthly sales band; backfilled
--     rows also carry ff_entry_id / ff_status / tz_assumed).

-- comms.lead_application  (id uuid -> VARCHAR cast; raw jsonb -> VARCHAR cast)
CREATE TABLE IF NOT EXISTS raw_comms_lead_application (
    id                  VARCHAR,        -- uuid -> text
    created_at          TIMESTAMPTZ,
    prospect_number     VARCHAR,
    email               VARCHAR,
    business_name       VARCHAR,
    src                 VARCHAR,
    partner             VARCHAR,
    partner_lead_id     VARCHAR,
    partner_status      VARCHAR,
    partner_error       VARCHAR,
    raw                 VARCHAR,        -- jsonb -> text
    retry_count         INTEGER,
    last_retry_at       TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);
