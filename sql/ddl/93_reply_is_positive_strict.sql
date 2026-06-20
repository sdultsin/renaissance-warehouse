-- @gate: add
-- Depends on 83
-- 93_reply_is_positive_strict.sql — strict positive-reply label table (persistence).
--
-- WHAT: derived.reply_is_positive_strict holds the offline "strict" LLM classification of
-- inbound replies — one row per reply_id, with is_positive + the model's reason. 741,785
-- rows at load time. This is the canonical strict-positive signal referenced by the
-- warehouse reply-truth layer (see memory: reply_is_positive_strict + reply_offer were the
-- two one-shot DIRECT writes a from-scratch rebuild would have lost).
--
-- WHY THIS FILE EXISTS: the table was originally loaded as a ONE-OFF direct write into the
-- warehouse primary with NO durable DDL/entity/seed. It was NOT in the DDL/sources, so the
-- nightly rebuild (core/sync_run via core.orchestrator) would DROP/lose it on the next run,
-- and it would never reach the gated serving snapshot (/opt/duckdb/warehouse_current.duckdb)
-- the read-API serves. This is the same failure the qwen table (DDL 83) was hardened against.
--
-- HOW IT NOW PERSISTS (identical pattern to DDL 83 / reply_is_positive_qwen):
--   * This DDL declares the schema idempotently (CREATE TABLE IF NOT EXISTS), so a fresh
--     scripts/setup_db.py on a clean box always has the table shape, even before the seed
--     loads. Applied + version-tracked by the nightly's setup_db.py step.
--   * entities/reply_is_positive_strict.py (the `derived` phase) re-materializes the ROWS
--     from the out-of-band seed JSONL every nightly via CREATE OR REPLACE, asserting
--     committed == attempted, so the labels survive every rebuild and a truncated load
--     fails loud.  Seed = seed_data/reply-is-positive-strict/strict_full_labels.jsonl
--     (gitignored — *.jsonl + seed_data/ in .gitignore; local/box only).
--   * The 06:30 UTC snapshot publisher byte-copies the whole primary -> serving, so once
--     the table is in primary it reaches serving automatically (no publisher change needed).
--
-- This is NOT a re-classification — the labels already exist in the seed; this layer is pure
-- persistence. The table is additive and is NOT in the gate's required_schema, so it can
-- never block a snapshot promote.
--
-- Source artifact schema (one JSON object per line):
--   reply_id (uuid) · is_positive (bool) · reason (varchar) · model (varchar).
-- The seed's `model` maps to column `strict_model`; classified_at is added at load (watermark).

CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS derived.reply_is_positive_strict (
    reply_id       UUID PRIMARY KEY,
    is_positive    BOOLEAN,
    reason         VARCHAR,
    strict_model   VARCHAR,
    classified_at  TIMESTAMPTZ
);
