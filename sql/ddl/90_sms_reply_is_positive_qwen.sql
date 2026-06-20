-- 90_sms_reply_is_positive_qwen.sql — SMS reply strict-positive + human/auto label table (persistence).
-- @gate: add
-- Depends on 34 (raw_sendivo_inbound)
--
-- WHAT: derived.sms_reply_is_positive_qwen holds the offline qwen LLM classification of the
-- recovered SMS inbound replies (raw_sendivo_inbound) — one row per DISTINCT inbound_message_id
-- among the NON-opt-out residual, with is_positive (genuine interest, the STRICT rubric — the SAME
-- prompt the email positive-reply-bi run used, locked verbatim for cross-channel comparability) plus
-- a deterministic is_human flag (false = carrier/auto-responder/DND auto-reply). ~73,382 rows at
-- load time (the 670,579 raw residual is ~9x inflated by raw_sendivo_inbound _run_id re-ingestion;
-- the dedup key is inbound_message_id — see sql/ddl/34 comment).
--
-- WHY THIS FILE EXISTS (same lesson as 83_reply_is_positive_qwen): a one-off CREATE OR REPLACE TABLE
-- into the primary would be DROPPED by the next nightly rebuild and never reach the gated serving
-- snapshot. This DDL declares the schema idempotently (CREATE TABLE IF NOT EXISTS) so setup_db
-- always has the shape; entities/sms_reply_is_positive_qwen.py (the `derived` phase) re-materializes
-- the ROWS from the out-of-band seed JSONL every nightly via CREATE OR REPLACE, so the labels survive
-- every rebuild. The seed file is the LLM artifact
-- (seed_data/sms-reply-is-positive-qwen/sms_seed.jsonl) — gitignored, local/box only (same
-- out-of-band pattern as 83 / cost_seed / partner-feedback).
--
-- This is NOT re-classification — the labels already exist in the seed; this layer is purely
-- persistence. The table is additive and NOT in the gate's required_schema, so it can never block a
-- snapshot promote. v_omni_sms_performance (DDL 91) reads is_positive/is_human from here, replacing
-- the not-opt-out INTERIM proxy that overcounts SMS "positive".
--
-- positive_signal of the consuming view -> 'sendivo_qwen_strict'.
--
-- Source artifact schema (one JSON object per line):
--   reply_id (varchar = inbound_message_id) · is_positive (bool) · is_human (bool) ·
--   reason (varchar) · received_at (timestamptz, inbound receipt) · model (varchar).
--   classified_at is added at load time (load watermark).

CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS derived.sms_reply_is_positive_qwen (
    reply_id       VARCHAR PRIMARY KEY,   -- inbound_message_id (Sendivo payload data.message_id)
    is_positive    BOOLEAN,               -- STRICT rubric: genuine interest/opportunity (torn -> false)
    is_human       BOOLEAN,               -- deterministic: false = carrier/auto-responder/DND auto-reply
    reason         VARCHAR,               -- <=160 char model rationale
    received_at    TIMESTAMPTZ,           -- inbound received_at (day attribution; dedup-immune)
    model          VARCHAR,
    classified_at  TIMESTAMPTZ
);
