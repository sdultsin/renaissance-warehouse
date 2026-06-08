-- Phase 3: account_truth ingest -> core.sending_account (spec 06).
-- Applied at schema version 19 by scripts/setup_db.py / orchestrator DDL applier.
--
-- THREE objects:
--   raw_account_truth_accounts        — copy-through snapshot of the account_truth
--                                       duckdb's `account_inventory` table.
--   core.sending_account              — one canonical row per Instantly inbox.
--   core.sending_account_state_event  — append-only lifecycle transition log.
--
-- Source: /root/archive/mac-offload/account_truth_<date>.duckdb (the same file the
-- Sending Truth Vercel app reads). We absorb its `account_inventory` table rather
-- than re-deriving inbox state from scratch (per spec 06 "Things to NOT do").
--
-- Convention (matches 04_pipeline_mirror.sql / 16_comms_mirror.sql):
--   * raw_* tables are append-only snapshots keyed by _run_id.
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * Mirror DELETEs by _run_id then INSERTs (idempotent within a run); prior
--     snapshots are preserved.
--
-- DEVIATION FROM SPEC 06 (deliberate, accuracy-first): the account_truth snapshot
-- carries `workspace_slug`, NOT the Instantly workspace UUID. Two of its workspaces
-- (`warm-leads`, `the-dyad`) are not in core.workspace, so a UUID join is NULL for
-- ~3.2k rows. We therefore store `workspace_slug` (always present, NOT NULL) as the
-- reliable key and resolve `workspace_id` via a LEFT JOIN to core.workspace.slug
-- (nullable). This matches the warehouse's slug-is-the-join-key principle.

CREATE SCHEMA IF NOT EXISTS core;

-- --------------------------------------------------------------------------
-- raw_account_truth_accounts  (copy-through of account_inventory)
-- Source column names preserved verbatim. created_at/updated_at are VARCHAR in
-- the source snapshot (ISO strings) — kept as-is; canonical TRY_CASTs them.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_account_truth_accounts (
    workspace_slug   VARCHAR,
    workspace_name   VARCHAR,
    email            VARCHAR,
    domain           VARCHAR,
    status           INTEGER,
    status_label     VARCHAR,
    daily_limit      INTEGER,
    provider_code    INTEGER,
    infra_type       VARCHAR,
    setup_pending    BOOLEAN,
    warmup_status    INTEGER,
    warmup_score     DOUBLE,
    sending_gap      INTEGER,
    created_at       VARCHAR,   -- source-side ISO string
    updated_at       VARCHAR,   -- source-side ISO string
    raw_json         VARCHAR,
    _snapshot_file   VARCHAR NOT NULL,  -- provenance: which account_truth_*.duckdb
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
);

-- --------------------------------------------------------------------------
-- core.sending_account  (canonical, one row per inbox)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sending_account (
    account_id          VARCHAR PRIMARY KEY,   -- email (account_truth has no Instantly UUID; email is unique)
    email               VARCHAR NOT NULL,
    domain              VARCHAR NOT NULL,       -- FK to core.domain (future); from snapshot or split_part(email)
    workspace_slug      VARCHAR NOT NULL,       -- reliable join key (account_truth side)
    workspace_id        VARCHAR,                -- FK to core.workspace.workspace_id; NULL for unmatched/dead workspaces

    -- Classification
    esp                 VARCHAR,                -- 'google' | 'outlook' | 'otd' | NULL (unknown/missing)
    infra_provider      VARCHAR,                -- 'OTD' resolvable here; other vendor brands need domain/tag resolution -> NULL for now

    -- Lifecycle state machine (spec 06)
    lifecycle_state     VARCHAR NOT NULL,       -- created | warming | warmed | ramping | active | paused | retired
    rotation_state      VARCHAR,                -- on | off | NULL (not derivable from account_truth -> NULL)

    -- Lifecycle transition timestamps (only created_at is directly observed in the
    -- snapshot; the rest fill in as nightly snapshot-diffs detect transitions).
    created_at          TIMESTAMPTZ,
    warmup_started_at   TIMESTAMPTZ,
    warmup_completed_at TIMESTAMPTZ,
    rampup_started_at   TIMESTAMPTZ,
    rampup_completed_at TIMESTAMPTZ,
    paused_at           TIMESTAMPTZ,
    retired_at          TIMESTAMPTZ,

    -- Operational state from the snapshot
    status              VARCHAR,                -- active | paused | connection_error | missing
    warmup_phase        VARCHAR,                -- warmed | not_warmed | degraded (label of warmup_status)
    warmup_score        DOUBLE,                 -- 0-100 deliverability/warmup score from account_truth
    daily_limit         INTEGER,                -- configured target daily limit
    daily_limit_used    INTEGER,                -- supplement-only (Instantly /accounts) -> NULL in v1

    -- Cost projection columns (spec 13). NULL until v3 derivation populates.
    cost_per_day_usd_estimated  DOUBLE,
    vendor_billing_cycle        VARCHAR,        -- monthly | annual | pay_as_you_go

    is_active           BOOLEAN NOT NULL,       -- FALSE when status_label='Missing Current Inventory' (gone from Instantly)
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_core_sending_account_domain    ON core.sending_account (domain);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_workspace ON core.sending_account (workspace_slug);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_esp       ON core.sending_account (esp);
CREATE INDEX IF NOT EXISTS ix_core_sending_account_lifecycle ON core.sending_account (lifecycle_state);

-- --------------------------------------------------------------------------
-- core.sending_account_state_event  (append-only lifecycle timeline)
-- v1 seeds 'created' events from created_at. Transition events (warmup_started,
-- paused, retired, ...) accumulate going forward via nightly snapshot diffing.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sending_account_state_event (
    account_id      VARCHAR NOT NULL,
    event_type      VARCHAR NOT NULL,   -- created | warmup_started | warmup_completed | rampup_started | rampup_completed | paused | resumed | rotation_on | rotation_off | retired
    event_at        TIMESTAMPTZ NOT NULL,
    previous_state  VARCHAR,
    new_state       VARCHAR,
    notes           VARCHAR,
    _detected_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, event_type, event_at)
);
