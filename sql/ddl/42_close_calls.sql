-- Spec 16 (BI / Lead-Intent layer), WS-B — Close CRM warm-call ingest. Version 42.
--
-- Pulls warm-call activity from Close (GET /api/v1/activity/call/) into the warehouse,
-- STRUCTURED only (no transcripts — that is WS-H, which fills core.call_transcript).
-- This is the BOF "warm caller" object: a call -> Close lead -> Campaign/Source (via the
-- Close lead custom fields) -> our existing core.campaign ontology.
--
-- ⚠ Sendivo (SMS) leads are PHONE-ONLY (no email) — empirically every recent call in the
-- org is a Sendivo lead, so lead_email is frequently NULL and phone_e164 carries identity.
-- The lead spine (WS-F) keys on email OR phone for exactly this reason.
--
-- raw_close_call    — one row per Close call id (UPSERT on id), typed + api_response_raw.
-- core.call         — canonical call fact (DELETE+INSERT rebuild from raw).
-- core.warm_caller  — per-(user) rollups + an aggregate 'ALL' row (MVP entity).
-- core.call_outcome — disposition-only outcome_class (voicemail/no_answer/answered). Note is
--                     ground truth for intent; read it directly, do not classify it.
-- core.call_transcript — EMPTY here; WS-H (local Whisper) fills it. PII -> droplet-only.
--
-- Additive only. No ALTER/DROP/rename of any pre-existing table or view.

CREATE SCHEMA IF NOT EXISTS core;

-- ── RAW ──────────────────────────────────────────────────────────────────────
-- One row per Close call activity id. UPSERT on id (idempotent re-pull).
CREATE TABLE IF NOT EXISTS raw_close_call (
    id                 VARCHAR PRIMARY KEY,   -- acti_… call activity id
    _type              VARCHAR,               -- 'Call'
    lead_id            VARCHAR,               -- lead_… (joins to a Close lead)
    contact_id         VARCHAR,               -- cont_…
    direction          VARCHAR,               -- inbound | outbound
    disposition        VARCHAR,               -- answered | no-answer | vm-left | error | …
    status             VARCHAR,               -- completed | …
    duration           INTEGER,              -- seconds (call connect duration)
    recording_duration INTEGER,              -- seconds of recording
    recording_url      VARCHAR,               -- authed MP3 endpoint (WS-H downloads)
    has_recording      BOOLEAN,
    voicemail_url      VARCHAR,
    voicemail_duration INTEGER,
    outcome_id         VARCHAR,
    outcome_reason     VARCHAR,
    note               VARCHAR,               -- rep free-text (gold for outcome/intent)
    note_html          VARCHAR,
    cost               VARCHAR,               -- Close returns this as a string
    local_phone        VARCHAR,               -- our dialer number
    remote_phone       VARCHAR,               -- the lead's number (E.164)
    phone              VARCHAR,               -- Close's `phone` field (== remote_phone)
    user_id            VARCHAR,               -- the warm caller (Close user)
    user_name          VARCHAR,
    source             VARCHAR,               -- e.g. 'Close.io'
    date_created       TIMESTAMPTZ,
    date_answered      TIMESTAMPTZ,
    date_updated       TIMESTAMPTZ,           -- incremental watermark
    organization_id    VARCHAR,
    api_response_raw   JSON,
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);

-- ── CANONICAL: core.call ─────────────────────────────────────────────────────
-- One row per call. lead_email / phone_e164 / source_campaign / source_channel are
-- resolved by fetching the Close lead (contacts[].emails, contacts[].phones, custom.cf_*).
-- warm_caller_id = 'ALL' for the MVP aggregate, but the real user_id/user_name are kept
-- so the per-rep split is a backfill, not a schema change (spec §1).
CREATE TABLE IF NOT EXISTS core.call (
    call_id          VARCHAR PRIMARY KEY,
    close_lead_id    VARCHAR,
    lead_email       VARCHAR,        -- from the Close lead (NULL for phone-only/Sendivo)
    phone_e164       VARCHAR,        -- the remote/lead phone
    warm_caller_id   VARCHAR,        -- 'ALL' (MVP aggregate)
    user_id          VARCHAR,        -- real Close user id (for the later per-rep split)
    user_name        VARCHAR,
    caller_name      VARCHAR,        -- parsed from note prefix (rep types their first name)
    direction        VARCHAR,
    disposition      VARCHAR,
    duration_seconds INTEGER,
    has_recording    BOOLEAN,
    recording_url    VARCHAR,
    cost             DOUBLE,
    occurred_at      TIMESTAMPTZ,    -- date_answered ?? date_created
    source_campaign  VARCHAR,        -- Close lead custom field (Campaign); id from env CLOSE_CF_CAMPAIGN
    source_channel   VARCHAR,        -- Close lead custom field (Instantly|Sendivo); id from env CLOSE_CF_SOURCE
    resolved_at      TIMESTAMPTZ
);
-- Additive migration: add caller_name to existing deployments.
ALTER TABLE core.call ADD COLUMN IF NOT EXISTS caller_name VARCHAR;

-- ── CANONICAL: core.warm_caller ──────────────────────────────────────────────
-- One row per warm_caller_id: the aggregate 'ALL' row AND one row per real Close user.
-- appt_set_calls is left 0/NULL here (WS-G ConversionEvents fills the appt-set signal).
CREATE TABLE IF NOT EXISTS core.warm_caller (
    warm_caller_id   VARCHAR PRIMARY KEY,   -- 'ALL' or the real user_id
    user_id          VARCHAR,
    user_name        VARCHAR,
    calls            BIGINT,
    connected_calls  BIGINT,                -- disposition = 'answered'
    connect_rate     DOUBLE,
    appt_set_calls   BIGINT,                -- WS-G fills (0/NULL for now)
    resolved_at      TIMESTAMPTZ
);

-- ── CANONICAL: core.call_outcome ─────────────────────────────────────────────
-- One row per call_id. outcome_class is disposition-only — three values:
--   voicemail  — vm-left disposition, voicemail_url, or voicemail_duration > 0
--   no_answer  — no-answer, busy, error, canceled (not connected)
--   answered   — connected in any way
-- note is the rep's verbatim Close note — the ground truth for intent/outcome.
-- Read note directly; do not classify it with keywords.
CREATE TABLE IF NOT EXISTS core.call_outcome (
    call_id       VARCHAR PRIMARY KEY,
    outcome_class VARCHAR NOT NULL,
    note          VARCHAR,            -- verbatim rep note; ground truth for intent
    resolved_at   TIMESTAMPTZ
);
ALTER TABLE core.call_outcome DROP COLUMN IF EXISTS needs_llm;

-- ── CANONICAL: core.call_transcript (EMPTY — WS-H fills) ──────────────────────
-- Created here so the table exists; WS-H (local Whisper on the droplet) populates it.
-- PII (transcript text) is droplet-only / git-ignored; never materialized to git.
CREATE TABLE IF NOT EXISTS core.call_transcript (
    call_id          VARCHAR PRIMARY KEY,
    transcript       VARCHAR,
    model            VARCHAR,
    lang             VARCHAR,
    duration_seconds INTEGER,
    transcribed_at   TIMESTAMPTZ
);
