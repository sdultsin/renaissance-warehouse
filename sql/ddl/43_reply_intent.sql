-- Spec 16 (BI / Lead-Intent layer) — canonical reply fact + rich LLM intent. Version 43.
--
-- WS-C. Two new objects, both additive (CREATE … IF NOT EXISTS), no ALTER/DROP of any
-- pre-existing table or view:
--
--   core.reply         — ONE canonical row per inbound human reply. Consolidates
--                        raw_instantly_email (PRIMARY, current source, Instantly-wins) +
--                        raw_pipeline_reply_data (historical fallback for pre-cutover replies
--                        that predate the direct-Instantly ingest). Deduped on
--                        (lead_email, thread_id, reply_timestamp). Carries the recovered
--                        `variant` (consumed from v_reply_enriched when that view exists;
--                        NULL otherwise) and a derived `is_auto_reply`.
--
--   core.reply_intent  — ONE row per reply_id, produced by an LLM (Haiku-class) classifier
--                        over reply_text (+ subject + step). Rich, multi-signal, versioned —
--                        SUPERSEDES the dead raw_pipeline_reply_intent_classifications
--                        (a deterministic keyword/native-label heuristic, frozen ~2026-06-07,
--                        part of the pipeline-Supabase retirement).
--
-- ⚠ PII: reply_text + lead_email are PII and this repo is public. The DDL is fine to commit
-- (no verbatims), but no reply verbatims are ever written to a git-tracked file.

CREATE SCHEMA IF NOT EXISTS core;

-- ── CANONICAL: one reply fact ────────────────────────────────────────────────
-- reply_id = the Instantly email_id when the row came from raw_instantly_email;
-- for pipeline-only historical rows (no email_id) it is a stable md5 hash over the
-- dedup key so re-runs are deterministic and idempotent.
CREATE TABLE IF NOT EXISTS core.reply (
    reply_id        VARCHAR PRIMARY KEY,
    lead_email      VARCHAR,
    campaign_id     VARCHAR,
    workspace_id    VARCHAR,
    step            INTEGER,
    variant         VARCHAR,        -- recovered from v_reply_enriched; NULL when unrecoverable
    subject         VARCHAR,
    reply_text      VARCHAR,
    reply_timestamp TIMESTAMPTZ,
    is_auto_reply   BOOLEAN,        -- derived (OOO / autoresponder / bounce-ish heuristic)
    source          VARCHAR,        -- 'instantly' (primary) | 'pipeline' (historical fallback)
    _loaded_at      TIMESTAMPTZ,
    _run_id         VARCHAR
);

-- ── CANONICAL: rich LLM intent (one row per reply_id) ─────────────────────────
-- Columns are exactly per spec 16 §5.2. primary_intent ∈ the fixed §6 enum:
--   interested | info_request | objection_price | objection_timing | objection_trust |
--   objection_no_need | not_decision_maker | unsubscribe | auto_reply | hostile | neutral_other
CREATE TABLE IF NOT EXISTS core.reply_intent (
    reply_id          VARCHAR PRIMARY KEY,   -- → core.reply.reply_id
    primary_intent    VARCHAR,               -- fixed enum (§6), never NULL after classify
    intent_tags       VARCHAR[],             -- secondary labels (multi)
    sentiment         VARCHAR,               -- positive | neutral | negative
    is_question       BOOLEAN,
    is_objection      BOOLEAN,
    objection_type    VARCHAR,               -- price | timing | trust | not_dm | already_have | no_need | NULL
    is_unsubscribe    BOOLEAN,
    is_referral       BOOLEAN,
    is_wrong_person   BOOLEAN,
    summary           VARCHAR,               -- one-line what-they-said
    classifier_model  VARCHAR,               -- e.g. 'claude-haiku-4-5'
    classifier_version INTEGER,              -- bump to force a full re-classify
    confidence        DOUBLE,                -- 0–1
    classified_at     TIMESTAMPTZ
);
