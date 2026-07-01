-- comms-orchestration bulk mirror: raw_comms_* tables.
-- Source: comms-orchestration Supabase (Sendivo SMS + warm-call + AIM).
-- Mirrored via DuckDB postgres_scanner (see entities/comms_mirror.py).
--
-- Column names/types verified 2026-05-30 against the live source via the
-- comms-orchestration Supabase MCP (list_tables verbose). Do NOT guess columns;
-- the real schema differs significantly from analogous warehouse tables.
--
-- Convention (REPLACE-style since 2026-07-01, warehouse-flags#12):
--   * raw_* tables hold exactly ONE full snapshot (one row per source id).
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * Mirror DELETEs the whole table then INSERTs a fresh snapshot, atomically
--     per table (idempotent; prior runs are NOT preserved — the original
--     keep-history design stacked 12-34 snapshots and inflated naive SUMs).
--
-- Type conventions (Postgres -> DuckDB):
--   text                       -> VARCHAR
--   integer (int4)             -> INTEGER
--   bigint  (int8)             -> BIGINT
--   numeric                    -> DECIMAL(38, 12)
--   boolean                    -> BOOLEAN
--   timestamp with time zone   -> TIMESTAMPTZ
--   USER-DEFINED enum          -> VARCHAR  (CAST in the SELECT)
--   jsonb                      -> VARCHAR  (CAST in the SELECT)
--   ARRAY (text[])             -> VARCHAR  (CAST in the SELECT)
--
-- Source schemas: comms.* (most) and audit.* (ai_decision_log).

-- comms.brand  (id is TEXT, not a uuid)
CREATE TABLE IF NOT EXISTS raw_comms_brand (
    id                       VARCHAR,
    legal_name               VARCHAR,
    sender_number            VARCHAR,
    sendivo_campaign_id      BIGINT,
    persona_first_name       VARCHAR,
    active                   BOOLEAN,
    created_at               TIMESTAMPTZ,
    updated_at               TIMESTAMPTZ,
    sendivo_phone_number_id  BIGINT,
    _loaded_at               TIMESTAMPTZ NOT NULL,
    _run_id                  VARCHAR
);

-- comms.conversation  (id BIGINT; brand_id TEXT; state is an enum)
CREATE TABLE IF NOT EXISTS raw_comms_conversation (
    id                          BIGINT,
    brand_id                    VARCHAR,
    prospect_number             VARCHAR,
    prospect_name               VARCHAR,
    prospect_email              VARCHAR,
    prospect_timezone           VARCHAR,
    prospect_state              VARCHAR,
    state                       VARCHAR,    -- enum comms.conversation_state -> text
    next_check_at               TIMESTAMPTZ,
    follow_up_count             INTEGER,
    last_inbound_at             TIMESTAMPTZ,
    last_outbound_at            TIMESTAMPTZ,
    app_link_sent_at            TIMESTAMPTZ,
    app_link_url                VARCHAR,
    app_submitted_at            TIMESTAMPTZ,
    calendly_event_uri          VARCHAR,
    calendly_booked_at          TIMESTAMPTZ,
    shadow_mode                 BOOLEAN,
    metadata                    VARCHAR,    -- jsonb -> text
    created_at                  TIMESTAMPTZ,
    updated_at                  TIMESTAMPTZ,
    manual_intervention_at      TIMESTAMPTZ,
    manual_intervention_by      VARCHAR,
    last_followup_deferred_to   TIMESTAMPTZ,
    prospect_first_name         VARCHAR,
    prospect_company_name       VARCHAR,
    last_proposed_slots         VARCHAR,    -- jsonb -> text
    _loaded_at                  TIMESTAMPTZ NOT NULL,
    _run_id                     VARCHAR
);

-- comms.message  (id BIGINT; conversation_id BIGINT; content, not body)
CREATE TABLE IF NOT EXISTS raw_comms_message (
    id                  BIGINT,
    conversation_id     BIGINT,
    direction           VARCHAR,
    source              VARCHAR,
    content             VARCHAR,
    segments            INTEGER,
    sendivo_message_id  VARCHAR,
    sendivo_inbound_id  VARCHAR,
    sendivo_status      VARCHAR,
    sendivo_error       VARCHAR,
    ai_decision_id      BIGINT,
    created_at          TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

-- comms.suppression  (PK prospect_number; no surrogate id)
CREATE TABLE IF NOT EXISTS raw_comms_suppression (
    prospect_number        VARCHAR,
    reason                 VARCHAR,
    suppressed_at          TIMESTAMPTZ,
    expires_at             TIMESTAMPTZ,
    source_conversation_id BIGINT,
    triggering_message_id  BIGINT,
    notes                  VARCHAR,
    _loaded_at             TIMESTAMPTZ NOT NULL,
    _run_id                VARCHAR
);

-- comms.escalation
CREATE TABLE IF NOT EXISTS raw_comms_escalation (
    id                     BIGINT,
    conversation_id        BIGINT,
    reason                 VARCHAR,
    triggering_message_id  BIGINT,
    notified_at            TIMESTAMPTZ,
    slack_message_ts       VARCHAR,
    resolved_at            TIMESTAMPTZ,
    resolved_by            VARCHAR,
    resolution_notes       VARCHAR,
    _loaded_at             TIMESTAMPTZ NOT NULL,
    _run_id                VARCHAR
);

-- comms.call_opportunity  (id BIGINT; unified warm-call queue)
CREATE TABLE IF NOT EXISTS raw_comms_call_opportunity (
    id                          BIGINT,
    source                      VARCHAR,
    source_workspace_id         VARCHAR,
    source_lead_id              VARCHAR,
    conversation_id             BIGINT,
    email                       VARCHAR,
    phone_e164                  VARCHAR,
    first_name                  VARCHAR,
    full_name                   VARCHAR,
    company                     VARCHAR,
    linkedin_url                VARCHAR,
    status                      VARCHAR,
    enrichment_failure_reason   VARCHAR,
    close_lead_id               VARCHAR,
    duplicate_of                BIGINT,
    opportunity_marked_at       TIMESTAMPTZ,
    enrichment_attempted_at     TIMESTAMPTZ,
    ready_at                    TIMESTAMPTZ,
    queued_at                   TIMESTAMPTZ,
    called_at                   TIMESTAMPTZ,
    closed_at                   TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ,
    updated_at                  TIMESTAMPTZ,
    call_outcome                VARCHAR,
    call_status                 VARCHAR,
    call_duration_seconds       INTEGER,
    talked_to_human             BOOLEAN,
    last_call_at                TIMESTAMPTZ,
    call_attempt_count          INTEGER,
    close_contact_id            VARCHAR,
    thread_synced_to_close_at   TIMESTAMPTZ,
    thread_sync_message_count   INTEGER,
    thread_sync_error           VARCHAR,
    _loaded_at                  TIMESTAMPTZ NOT NULL,
    _run_id                     VARCHAR
);

-- comms.phone_enrichment  (Prospeo enrich audit; opportunity_id, not conversation_id)
CREATE TABLE IF NOT EXISTS raw_comms_phone_enrichment (
    id              BIGINT,
    opportunity_id  BIGINT,
    provider        VARCHAR,
    input_kind      VARCHAR,
    input_value     VARCHAR,
    http_status     INTEGER,
    mobile_e164     VARCHAR,
    mobile_status   VARCHAR,
    found_email     VARCHAR,
    raw_response    VARCHAR,    -- jsonb -> text
    credits_spent   INTEGER,
    error_code      VARCHAR,
    attempted_at    TIMESTAMPTZ,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- comms.instantly_message  (to_emails is text[]; raw_payload jsonb)
CREATE TABLE IF NOT EXISTS raw_comms_instantly_message (
    id                      BIGINT,
    opportunity_id          BIGINT,
    instantly_workspace_id  VARCHAR,
    instantly_lead_id       VARCHAR,
    instantly_message_id    VARCHAR,
    direction               VARCHAR,
    subject                 VARCHAR,
    body_text               VARCHAR,
    body_html               VARCHAR,
    from_email              VARCHAR,
    to_emails               VARCHAR,    -- text[] -> text
    date_sent               TIMESTAMPTZ,
    raw_payload             VARCHAR,    -- jsonb -> text
    pulled_at               TIMESTAMPTZ,
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- audit.ai_decision_log  (immutable Sonnet-call log; trigger + state enums)
CREATE TABLE IF NOT EXISTS raw_comms_ai_decision_log (
    id                          BIGINT,
    conversation_id             BIGINT,
    trigger                     VARCHAR,
    state_before                VARCHAR,    -- enum -> text
    state_after                 VARCHAR,    -- enum -> text
    system_prompt_hash          VARCHAR,
    conversation_history_tokens INTEGER,
    user_input                  VARCHAR,
    ai_output                   VARCHAR,
    sent_output                 VARCHAR,
    shadow_mode                 BOOLEAN,
    human_decision              VARCHAR,
    human_edited_output         VARCHAR,
    reviewed_by                 VARCHAR,
    reviewed_at                 TIMESTAMPTZ,
    model                       VARCHAR,
    input_tokens                INTEGER,
    output_tokens               INTEGER,
    cost_usd                    DECIMAL(38, 12),
    latency_ms                  INTEGER,
    resulted_in_message_id      BIGINT,
    resulted_in_escalation      BOOLEAN,
    created_at                  TIMESTAMPTZ,
    _loaded_at                  TIMESTAMPTZ NOT NULL,
    _run_id                     VARCHAR
);

-- comms.enrichment_vendor_pricing (editable $/credit rate table for dollarizing
-- phone-enrichment spend; see derived.enrichment_cost in 35_enrichment_cost.sql).
-- usd_per_credit is the single source of truth for the dollar conversion — update
-- it in the comms-orchestration Postgres when a vendor plan changes; this mirror
-- and the derived views pick it up on the next nightly run.
CREATE TABLE IF NOT EXISTS raw_comms_enrichment_vendor_pricing (
    provider                    VARCHAR,
    usd_per_credit              DOUBLE,
    plan_note                   VARCHAR,
    updated_at                  TIMESTAMPTZ,
    _loaded_at                  TIMESTAMPTZ NOT NULL,
    _run_id                     VARCHAR
);
