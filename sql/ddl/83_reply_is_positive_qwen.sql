-- 83_reply_is_positive_qwen.sql — qwen positive-reply label table (persistence).
--
-- WHAT: derived.reply_is_positive_qwen holds the offline qwen LLM classification of
-- inbound replies — one row per reply_id, with is_positive plus the secondary
-- is_question / is_referral / is_later flags. 741,785 rows at load time.
--
-- WHY THIS FILE EXISTS: the table was originally loaded as a ONE-OFF
-- `CREATE OR REPLACE TABLE` directly into the warehouse primary. It was NOT in the
-- DDL/sources, so the nightly rebuild (core/sync_run via core.orchestrator) would
-- DROP/lose it on the next run, and it never reached the gated serving snapshot
-- (/opt/duckdb/warehouse_current.duckdb) the read-API serves.
--
-- HOW IT NOW PERSISTS:
--   * This DDL declares the schema idempotently (CREATE TABLE IF NOT EXISTS), so a
--     fresh `scripts/setup_db.py` on a clean box always has the table shape, even
--     before the seed loads. Applied + version-tracked (schema_version=83) by the
--     nightly's setup_db.py step.
--   * entities/reply_is_positive_qwen.py (the `derived` phase) re-materializes the
--     ROWS from the out-of-band seed JSONL every nightly via CREATE OR REPLACE, so
--     the labels survive every rebuild. The seed file is the LLM artifact
--     (seed_data/reply-is-positive-qwen/full_qwen.partial.jsonl) — gitignored,
--     local/box only (same out-of-band pattern as cost_seed / partner-feedback).
--   * The 06:30 UTC snapshot publisher byte-copies the whole primary -> serving, so
--     once the table is in primary it reaches serving automatically (no publisher
--     change needed).
--
-- This re-classification is NOT re-run here — the labels already exist; this layer is
-- purely persistence. The table is additive and is NOT in the gate's required_schema,
-- so it can never block a snapshot promote.
--
-- Source artifact schema (one JSON object per line):
--   reply_id (uuid) · is_positive (bool) · confidence (json, currently null) ·
--   reason (varchar) · is_question (bool) · is_referral (bool) · is_later (bool) ·
--   model (varchar). classified_at is added at load time (load watermark).

CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS derived.reply_is_positive_qwen (
    reply_id       UUID PRIMARY KEY,
    is_positive    BOOLEAN,
    confidence     JSON,
    reason         VARCHAR,
    is_question    BOOLEAN,
    is_referral    BOOLEAN,
    is_later       BOOLEAN,
    model          VARCHAR,
    classified_at  TIMESTAMPTZ
);
