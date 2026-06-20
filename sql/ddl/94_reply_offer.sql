-- @gate: add
-- Depends on 83
-- 94_reply_offer.sql — per-reply offer attribution table (persistence).
--
-- WHAT: derived.reply_offer maps each inbound reply_id to the offer it belongs to
-- (campaign_id, workspace_id, offer, offer_source) — one row per reply_id, 741,785 rows at
-- load time. Pairs with derived.reply_is_positive_strict (DDL 93): strict gives WHETHER a
-- reply is positive, reply_offer gives WHICH offer the reply is about.
--
-- WHY THIS FILE EXISTS: like the strict table, reply_offer was originally a ONE-OFF direct
-- write into the warehouse primary with NO durable DDL/entity/seed — the next nightly rebuild
-- would have DROPped/lost it and it would never reach the gated serving snapshot. This
-- hardens it the same way DDL 83 (qwen) hardened reply_is_positive_qwen.
--
-- HOW IT NOW PERSISTS (identical pattern to DDL 83 / 93):
--   * Idempotent CREATE TABLE IF NOT EXISTS here -> schema always exists on a clean box,
--     applied + version-tracked by setup_db.py.
--   * entities/reply_offer.py (the `derived` phase) re-materializes the ROWS from the
--     out-of-band seed JSONL every nightly via CREATE OR REPLACE, asserting
--     committed == attempted (truncated load fails loud).  Seed =
--     seed_data/reply-offer/reply_offer.jsonl (gitignored; local/box only).
--   * The 06:30 UTC snapshot publisher byte-copies primary -> serving, so the table reaches
--     serving automatically.
--
-- NOT a re-classification — pure persistence of an existing artifact. Additive; not in the
-- gate's required_schema, so it can never block a snapshot promote.
--
-- Source artifact schema (one JSON object per line):
--   reply_id (uuid, PK) · campaign_id (uuid, nullable — 18 rows have no campaign) ·
--   workspace_id (varchar) · offer (varchar) · offer_source (varchar).
-- classified_at is added at load (watermark).

CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS derived.reply_offer (
    reply_id       UUID PRIMARY KEY,
    campaign_id    UUID,
    workspace_id   VARCHAR,
    offer          VARCHAR,
    offer_source   VARCHAR,
    classified_at  TIMESTAMPTZ
);
