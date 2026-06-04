-- Workstream I — unified cross-channel reply/intent layer (the "Palantir" substrate).
-- Applied at schema version 29 by scripts/setup_db.py (glob *.sql, sorted by NN_ prefix).
--
-- PURPOSE: This file is the canonical home for the two raw reply mirrors that feed
-- derived.reply_intent (see 30_unified_replies.sql):
--   * raw_pipeline_reply_data  — COLD EMAIL replies  (source: pipeline-supabase public.reply_data)
--   * raw_pipeline_reply_intent_classifications — canonical COLD EMAIL reply intent bridge
--   * raw_comms_message        — SMS inbound+outbound (source: comms-orchestration comms.message)
--
-- IMPORTANT — both base tables ALREADY EXIST in the warehouse:
--   * raw_pipeline_reply_data is created in 04_pipeline_mirror.sql (lines ~148-163) and
--     populated by entities/pipeline_mirror.py (reply_data is in SLIM_TABLES, full mirror).
--   * raw_comms_message is created in 16_comms_mirror.sql (lines ~77-92) and populated by
--     entities/comms_mirror.py (comms.message is in _TABLES).
--   The CREATE TABLE statements below are duplicated here with IF NOT EXISTS purely so this
--   migration is self-describing and idempotent if applied on a fresh DB out of order. They
--   are byte-identical to the originals — do NOT diverge them. The originals remain the
--   source of truth for the column lists the mirror entities read.
--
-- WHAT THIS FILE ADDS that the originals do not: reply/intent-specific indexes that the
-- unified view and the dashboard reply summary scan on (intent, reply_timestamp,
-- conversation_id, direction). 16_comms_mirror.sql ships with no indexes at all.
--
-- Type + idempotency conventions match 04_pipeline_mirror.sql / 16_comms_mirror.sql:
-- raw_* are append-only _run_id-keyed snapshots; the mirror DELETEs by _run_id then INSERTs.

-- ── COLD EMAIL replies (mirror of pipeline-supabase public.reply_data) ──────────────
-- Schema verified 2026-05-31 against live source via pipeline-supabase MCP.
-- intent ∈ {other, unsubscribe, negative, positive, auto_reply, NULL}. reply_text = body;
-- subject present; lead_email = prospect identifier; email replies are inherently inbound
-- (reply_data only stores prospect→us replies, so there is no direction column).
-- Window: FULL mirror (no date filter) — windowing was dropped 2026-05-31 per Sam; the
-- pipeline retains history Instantly discards, so full retention is near-zero-cost. Today
-- this is ~380k rows/run; source reaches back to 2026-03-26.
CREATE TABLE IF NOT EXISTS raw_pipeline_reply_data (
  id              BIGINT,
  campaign_id     VARCHAR,
  lead_email      VARCHAR,
  reply_text      VARCHAR,
  reply_timestamp TIMESTAMPTZ,
  workspace_id    VARCHAR,
  intent          VARCHAR,
  from_name       VARCHAR,
  subject         VARCHAR,
  synced_at       TIMESTAMPTZ,
  step            INTEGER,
  variant         VARCHAR,
  _loaded_at      TIMESTAMPTZ NOT NULL,
  _run_id         VARCHAR NOT NULL
);

-- ── COLD EMAIL reply intent bridge (source of truth for current email intent) ───────
-- Built in pipeline-supabase on 2026-06-02 because reply_data.intent stopped receiving
-- native/LLM labels after mid-April. source_table='conversation_messages' is the primary
-- email event stream; source_table='reply_data' is retained for legacy compatibility.
CREATE TABLE IF NOT EXISTS raw_pipeline_reply_intent_classifications (
  source_table          VARCHAR,
  source_id             VARCHAR,
  workspace_id          VARCHAR,
  campaign_id           VARCHAR,
  lead_email            VARCHAR,
  sender_email          VARCHAR,
  recipient_email       VARCHAR,
  reply_timestamp       TIMESTAMPTZ,
  intent                VARCHAR,
  intent_source         VARCHAR,
  is_auto_reply         BOOLEAN,
  auto_reply_source     VARCHAR,
  auto_reply_confidence DECIMAL(18, 6),
  classifier_version    VARCHAR,
  classified_at         TIMESTAMPTZ,
  _loaded_at            TIMESTAMPTZ NOT NULL,
  _run_id               VARCHAR NOT NULL
);

-- ── SMS replies (mirror of comms-orchestration comms.message) ───────────────────────
-- Schema verified 2026-05-31 against live source via comms-orchestration MCP.
-- direction ∈ {inbound, outbound}; source ∈ {prospect, ai}; content = body text;
-- ai_decision_id → audit.ai_decision_log; conversation_id → comms.conversation
-- (which carries prospect_number / prospect_email — message itself has no prospect id).
-- comms.message has no intent column; SMS intent is inferred at the view layer from the
-- conversation state enum (see 30_unified_replies.sql).
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

-- ── reply/intent-specific indexes ───────────────────────────────────────────────────
-- The _run_id index on raw_pipeline_reply_data already exists (04_pipeline_mirror.sql).
-- These accelerate the latest-run + intent + time-window scans the unified view does.
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_data_intent  ON raw_pipeline_reply_data (intent);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_data_ts      ON raw_pipeline_reply_data (reply_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_run   ON raw_pipeline_reply_intent_classifications (_run_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_intent ON raw_pipeline_reply_intent_classifications (intent);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_ts    ON raw_pipeline_reply_intent_classifications (reply_timestamp);

-- raw_comms_message ships with no indexes from 16_comms_mirror.sql; add them here.
CREATE INDEX IF NOT EXISTS ix_raw_comms_message_run           ON raw_comms_message (_run_id);
CREATE INDEX IF NOT EXISTS ix_raw_comms_message_conv          ON raw_comms_message (conversation_id);
CREATE INDEX IF NOT EXISTS ix_raw_comms_message_created       ON raw_comms_message (created_at);
