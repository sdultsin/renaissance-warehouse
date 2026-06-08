-- Spec 16 (BI / Lead-Intent layer), WS-I — derived.lead_intel + insight surfaces. Version 46.
--
-- THE PAYOFF LAYER. derived.lead_intel = ONE wide row per SIGNAL lead (core.lead), with
-- every downstream intent/disposition/funnel signal folded onto that single identity. It is
-- the join-once surface the BI views and the chatbot read instead of re-joining 5 source
-- tables every question.
--
-- Built by entities/lead_intel.py in the `derived` phase (registry.add_phase('derived',
-- 'lead_intel', run)), AFTER canonical (core.lead, core.reply, core.opportunity,
-- core.conversion_event) and intent (core.reply_intent) have populated. Full DELETE+INSERT
-- rebuild each run — idempotent.
--
-- Identity: keyed on core.lead.lead_key (the WS-F spine surrogate). lead_email is carried
-- for the email-keyed joins to the signal tables (core.reply / core.lead_disposition /
-- core.opportunity are all keyed by lead_email; core.conversion_event joins on lead_key first,
-- lead_email fallback).
--
-- profile_summary (LLM one-paragraph synthesis) is intentionally left NULL for now — a
-- follow-up workstream fills it. The column exists so adding it later is a backfill, not a
-- schema change.
--
-- ⚠ PII: lead_email + last_reply_text + partner_notes are PII and this repo is public. The
-- DDL is safe to commit (no verbatims); no row data is ever written to a git-tracked file.
--
-- Additive only. CREATE … IF NOT EXISTS. No ALTER/DROP/rename of any pre-existing object.

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS derived.lead_intel (
    -- ── identity ─────────────────────────────────────────────────────────────
    lead_key              VARCHAR PRIMARY KEY,  -- core.lead.lead_key (md5 of email|phone)
    lead_email            VARCHAR,              -- core.lead.email (NULL for phone-only)
    phone_e164            VARCHAR,              -- core.lead.phone_e164 (carries identity if email NULL)
    first_name            VARCHAR,
    company               VARCHAR,
    segment               VARCHAR,
    industry              VARCHAR,
    lead_source           VARCHAR,

    -- ── reply behaviour (aggregated over core.reply ⋈ core.reply_intent) ──────
    n_replies             BIGINT,               -- count of human replies (is_auto_reply = false)
    first_reply_at        TIMESTAMPTZ,
    last_reply_at         TIMESTAMPTZ,
    dominant_intent       VARCHAR,              -- mode of reply_intent.primary_intent
    all_intent_tags       VARCHAR[],            -- distinct union of primary_intent + intent_tags
    has_question          BOOLEAN,              -- any reply is_question
    has_objection         BOOLEAN,              -- any reply is_objection
    top_objection_type    VARCHAR,              -- mode of reply_intent.objection_type (where is_objection)
    last_reply_text       VARCHAR,              -- text of the most-recent reply (PII)
    last_sentiment        VARCHAR,              -- sentiment of the most-recent classified reply

    -- ── partner disposition (latest core.lead_disposition for this email) ─────
    partner_disposition   VARCHAR,              -- raw disposition string, verbatim
    disposition_class     VARCHAR,              -- tidy enum (live / no_show / disqualified / …)
    partner_rep           VARCHAR,
    partner_business_name VARCHAR,
    partner_id_confidence VARCHAR,
    partner_notes         VARCHAR,              -- rep free text (PII)

    -- ── funnel (opportunity + conversion_event) ──────────────────────────────
    is_opportunity        BOOLEAN,              -- has any core.opportunity row
    is_meeting            BOOLEAN,              -- has any core.conversion_event row
    conversion_agent      VARCHAR,              -- agent of the latest conversion_event (im / warm_caller / …)
    funnel_stage          VARCHAR,              -- derived: lead < replied < opportunity < meeting < disposition_outcome

    -- ── score + (deferred) LLM synthesis ─────────────────────────────────────
    engagement_score      INTEGER,              -- 0–100 deterministic composite (see lead_intel.py)
    profile_summary       VARCHAR,              -- LLM one-paragraph synthesis — DEFERRED, NULL for now

    resolved_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_lead_intel_email   ON derived.lead_intel (lead_email);
CREATE INDEX IF NOT EXISTS ix_lead_intel_intent  ON derived.lead_intel (dominant_intent);
CREATE INDEX IF NOT EXISTS ix_lead_intel_funnel  ON derived.lead_intel (funnel_stage);
