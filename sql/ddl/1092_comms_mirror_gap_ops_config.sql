-- @gate: add
-- Intent: comms-orchestration mirror gap-close #2 (MIRROR-COVERAGE-AUDIT 2026-07-09 §1, gap #3):
--         add the 7 remaining analytically/forensically valuable comms-hub tables to the
--         nightly comms mirror — campaign_blast_template (SMS cold-blast copy provenance),
--         meeting_reminder (reminder/no-show funnel), sendivo_outbound_log (blast-id
--         reconciliation source), and the 4 config.* tables (ops/config forensics).
-- Depends on: 47
--
-- ADDITIVE ONLY: this file ADDS seven new raw_comms_* tables. It does NOT touch any
-- existing table/view (additive invariant §3). Loaded by entities/comms_mirror.py
-- (same postgres_scanner full-refresh REPLACE pattern as 16/47/56):
--   * each raw_* table holds exactly ONE full snapshot (REPLACE-style,
--     warehouse-flags#12) — DELETE all + INSERT fresh, atomic per table, idempotent.
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * jsonb columns are CAST to VARCHAR in the SELECT (raw layer stays flat text).
--
-- Column names/types verified 2026-07-09 against the LIVE comms Postgres
-- (information_schema via read-only postgres_scanner attach; in-memory DuckDB).
--
-- SECURITY: config.worker_config holds live secrets in `value` (worker_secret,
-- cc_slack_bot_token). The mirror REDACTS `value` for secret-pattern keys in the
-- SELECT (see _COLUMN_EXPRS in entities/comms_mirror.py) — secrets never land in
-- the warehouse, which is served broadly via the read-only query API.
--
-- comms.webhook_receipt (~6M rows) is STILL excluded by design (noise; its analytic
-- slice already lands via raw_comms_sendivo_outbound). comms.alert_throttle and
-- comms.iskra_conversation stay excluded (throttle state / rebuildable cache of a
-- mirrored API). comms.retargeting_enrollment does not exist yet — whatever PR
-- creates it must add it to the mirror list.

-- comms.campaign_blast_template — the canonical SMS cold-blast copy per brand
-- (feeds Close "Original Message" fallback). Copy provenance: loss = can't
-- reconstruct what copy a lead got. Tiny (1 row live 2026-07-09).
CREATE TABLE IF NOT EXISTS raw_comms_campaign_blast_template (
    id              BIGINT,
    brand_id        VARCHAR,
    campaign_id     BIGINT,
    message         VARCHAR,
    active          BOOLEAN,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- comms.meeting_reminder — the ONE meeting-window orchestration table (comms mig 029):
-- T-1h email / T-30m SMS reminder latches + Close call-window stamps. This is the
-- attempted→delivered→confirm→show reminder-funnel rung the RevOps no-show tracker
-- names as a no-ledger gap. 0 rows live 2026-07-09 (slot capture pending) — mirrored
-- go-forward so the funnel lands from day one.
CREATE TABLE IF NOT EXISTS raw_comms_meeting_reminder (
    id                      BIGINT,
    meeting_id              VARCHAR,
    conversation_id         BIGINT,
    close_lead_id           VARCHAR,
    channel                 VARCHAR,
    partner                 VARCHAR,
    contact_handle          VARCHAR,
    prospect_phone_e164     VARCHAR,
    prospect_email          VARCHAR,
    prospect_name           VARCHAR,
    meeting_slot_at         TIMESTAMPTZ,
    status                  VARCHAR,
    email_reminder_fired_at TIMESTAMPTZ,
    sms_reminder_fired_at   TIMESTAMPTZ,
    call_window_opened_at   TIMESTAMPTZ,
    call_window_cleared_at  TIMESTAMPTZ,
    close_task_id           VARCHAR,
    metadata                VARCHAR,    -- jsonb -> text
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- comms.sendivo_outbound_log — standalone /sms/logs reconciliation mirror (comms mig
-- 010 + blast_id/blast_name in 011). Content overlaps the webhook-view mirror
-- (raw_comms_sendivo_outbound) but the blast enrichment on THIS table is not
-- guaranteed mirrored elsewhere. 0 rows live 2026-07-09 (populated by backfill /
-- reconciliation jobs when they run) — mirrored for custody.
CREATE TABLE IF NOT EXISTS raw_comms_sendivo_outbound_log (
    sendivo_log_id      BIGINT,
    to_number           VARCHAR,
    to_phone10          VARCHAR,
    from_number         VARCHAR,
    message_content     VARCHAR,
    segments            INTEGER,
    status              VARCHAR,
    status_group_name   VARCHAR,
    campaign_id         BIGINT,
    campaign_name       VARCHAR,
    sub_account_id      BIGINT,
    sub_account_name    VARCHAR,
    sent_at             TIMESTAMPTZ,
    raw                 VARCHAR,    -- jsonb -> text
    ingested_at         TIMESTAMPTZ,
    blast_id            BIGINT,
    blast_name          VARCHAR,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

-- config.kill_switch — single-row global worker pause flag. When/why the worker was
-- killed is ops-forensics data (config-history).
CREATE TABLE IF NOT EXISTS raw_comms_kill_switch (
    id              INTEGER,
    enabled         BOOLEAN,
    reason          VARCHAR,
    enabled_by      VARCHAR,
    enabled_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- config.worker_config — cron→worker config KV. `value` is REDACTED in the mirror
-- SELECT for secret-pattern keys (worker_secret, cc_slack_bot_token, …): the
-- warehouse copy carries key names + non-secret values + updated_at/updated_by
-- (the forensics), never the secret material itself.
CREATE TABLE IF NOT EXISTS raw_comms_worker_config (
    key             VARCHAR,
    value           VARCHAR,    -- '<redacted>' for secret-pattern keys
    updated_at      TIMESTAMPTZ,
    updated_by      VARCHAR,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);

-- config.brand_followup_cap — per-brand daily cap / spend-governance flags (35 rows
-- live 2026-07-09).
CREATE TABLE IF NOT EXISTS raw_comms_brand_followup_cap (
    brand_id                VARCHAR,
    daily_partner_cap       INTEGER,
    partner_count_today     INTEGER,
    count_reset_at          DATE,
    routing_mode_after_cap  VARCHAR,
    updated_at              TIMESTAMPTZ,
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- config.iskra_watchdog_state — single-row Iskra watchdog heartbeat/consec-failure
-- state (rides along free in the same batch).
CREATE TABLE IF NOT EXISTS raw_comms_iskra_watchdog_state (
    id              INTEGER,
    consec_bad      INTEGER,
    last_fire_at    TIMESTAMPTZ,
    _loaded_at      TIMESTAMPTZ NOT NULL,
    _run_id         VARCHAR
);
