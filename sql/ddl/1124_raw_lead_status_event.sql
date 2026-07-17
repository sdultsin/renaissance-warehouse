-- @gate: add
-- Depends on 1110
-- ============================================================================
-- 1124_raw_lead_status_event.sql — the NON-REPLY lead status EVENT STREAM
-- (append-only, CRM-style; same ledger family as main.raw_reply_label_event).
-- Phase-1 of the lead-tracking build (R34a meeting_booked events + R26 call events).
--
-- WHY: the n=1 lead model (R13/R34a) needs statuses beyond reply labels appended to
-- the same per-lead stream: a booked meeting demotes an "opportunity" in CURRENT
-- state automatically (terminal-positive), and warm-caller call activity (Close CRM)
-- becomes part of the lead's history. Reply-label events stay in raw_reply_label_event
-- (DDL 1110, untouched); this table carries every OTHER status event; the union view
-- below is the single chronological stream.
--
-- EVENT TYPES (phase-1): 'meeting_booked' (source core.v_meeting_truth — portal
-- im_bookings SoT, all eras/channels, deduped email/phone+day) and 'call_logged'
-- (source Close CRM via the existing nightly `close` phase: core.call +
-- core.v_call_outcome_final).
--
-- DATA / LOAD PATH: entities/lead_status_event.py, `derived` phase (after canonical,
-- so core.meeting/meeting_rebuilt + core.call are same-night fresh). Idempotent
-- anti-join on the uniqueness grain (event_type, source, source_ref) — the FIRST run
-- IS the full backfill (all historical bookings + all Close activity), every later
-- nightly appends the increment. Rows are never updated or deleted: if an upstream
-- meeting/call row later disappears, its event stays (append-only ledger semantics).
--
-- HONESTY: lead_email is NULL where the source cannot resolve one (Close resolves
-- ~78% of calls; phone_e164 always carried for calls). v_meeting_truth rows without a
-- recoverable lead_email (~42 of ~49.9k) cannot join the ledger and are NOT ingested —
-- the meetings lane still counts them.
--
-- Reversible: DROP TABLE (upstream sources retain everything; the entity rebuilds
-- the full history on the next run).
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.raw_lead_status_event (
    event_id       VARCHAR PRIMARY KEY,             -- deterministic md5(event_type|source|source_ref)
    event_type     VARCHAR NOT NULL,                -- 'meeting_booked' | 'call_logged' (family open)
    workspace_slug VARCHAR,                         -- where attributable (bookings mostly; calls NULL)
    lead_email     VARCHAR,                         -- lowercased; NULL only when source has no email
    phone_e164     VARCHAR,                         -- calls; NULL for bookings
    event_ts       TIMESTAMP WITH TIME ZONE,        -- meeting: booking day (1114 convention: meeting_date, posted_at fallback); call: occurred_at
    source         VARCHAR NOT NULL,                -- 'v_meeting_truth' | 'close'
    source_ref     VARCHAR NOT NULL,                -- meeting_key | Close call activity id
    outcome        VARCHAR,                         -- calls: v_call_outcome_final.outcome_class (disposition fallback); NULL for meetings
    detail         VARCHAR,                         -- JSON string (channel/era/offer/direction/duration/…)
    _loaded_at     TIMESTAMPTZ DEFAULT now(),
    _run_id        VARCHAR,
    UNIQUE (event_type, source, source_ref)
);

CREATE SCHEMA IF NOT EXISTS core;

-- The single chronological per-lead status stream: reply-label events (1110) UNION
-- non-reply status events (this table). Gate classes ('auto'/'bot') and
-- 'labeler_error' are excluded exactly as in every label-stat view.
-- IDENTITY: the two source tables mint their own event_id (uuid4 escrow vs md5
-- here) — the view therefore emits a BRANCH-PREFIXED event_id ('reply:…' /
-- 'status:…') so the stream key is globally unique BY CONSTRUCTION; consumers
-- needing the source-table key strip the prefix or read the source table.
CREATE OR REPLACE VIEW core.v_lead_status_event AS
SELECT
    'reply:' || event_id     AS event_id,
    'reply_label'            AS event_type,
    workspace_slug,
    lower(lead_email)        AS lead_email,
    CAST(NULL AS VARCHAR)    AS phone_e164,
    message_ts               AS event_ts,
    'raw_reply_label_event'  AS source,
    message_ref_id           AS source_ref,
    label                    AS outcome,
    CAST(NULL AS VARCHAR)    AS detail,
    labeler_version,
    _loaded_at
FROM main.raw_reply_label_event
WHERE label IN ('opportunity', 'engagement', 'confused', 'not_interested')
UNION ALL
SELECT
    'status:' || event_id    AS event_id,
    event_type,
    workspace_slug,
    lower(lead_email)        AS lead_email,
    phone_e164,
    event_ts,
    source,
    source_ref,
    outcome,
    detail,
    CAST(NULL AS VARCHAR)    AS labeler_version,
    _loaded_at
FROM main.raw_lead_status_event;
