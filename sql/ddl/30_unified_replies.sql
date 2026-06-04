-- Workstream I — derived.reply_intent: the unified cross-channel reply/intent view.
-- Applied at schema version 30 by scripts/setup_db.py.
--
-- This is the cross-channel intent substrate: one row per reply event across ALL channels,
-- normalized into a single shape so intent can be queried "any way imaginable" (by channel,
-- by intent, by prospect email/phone, by campaign, by time) without knowing which underlying
-- source it came from. Built ON TOP OF the raw mirrors (29_reply_mirror.sql):
--   email → raw_pipeline_reply_intent_classifications
--           (pipeline-supabase public.reply_intent_classifications)
--   sms   → raw_comms_message + raw_comms_conversation (comms-orchestration)
--
-- A VIEW (not materialized): always reflects the latest mirror snapshot. Each source is
-- pinned to its own latest _run_id so a partial nightly run never blends snapshots.
--
-- UNIFIED SHAPE (every channel maps into exactly this):
--   channel        'email' | 'sms'                 -- (WhatsApp 'whatsapp' slots in later, see below)
--   prospect_email prospect's email (NULL for most SMS — SMS keys on phone)
--   prospect_phone prospect's E.164 phone (NULL for email)
--   direction      'inbound' | 'outbound'
--   replied_at     event timestamp (TIMESTAMPTZ)
--   intent         normalized intent vocabulary (see mapping below)
--   body           the reply/message text
--   campaign_id    originating campaign (email only; NULL for SMS — Sendivo has no campaign_id on the message)
--   source_id      surrogate row id within the source table (for drill-through)
--
-- INTENT VOCABULARY (canonical = the email reply_data set):
--   other | unsubscribe | negative | positive | auto_reply | NULL
-- EMAIL: uses pipeline-supabase's canonical reply-intent bridge. That bridge is built
--   from Instantly native labels/statuses plus deterministic reply-body rules after
--   reply_data.intent stopped receiving non-other labels in April/May 2026.
-- SMS: comms.message has NO per-message intent. We infer a conversation-level intent from
--   the comms.conversation state enum and stamp it on every message in that conversation:
--     opted_out            -> 'unsubscribe'
--     declined             -> 'negative'
--     booked               -> 'positive'
--     escalated            -> 'positive'   (escalated = human-worthy interest)
--     engaged / NULL / *   -> NULL          (in-flight; AIM has not resolved a terminal intent)
--   This is a coarse proxy, not a per-message classifier. A future refinement can join
--   audit.ai_decision_log (raw_comms_ai_decision_log) for message-level AI classification;
--   left as a documented follow-up so the view stays cheap today.

CREATE SCHEMA IF NOT EXISTS derived;

CREATE OR REPLACE VIEW derived.reply_intent AS
WITH email_latest AS (
  -- reply_intent_classifications is deduped by _key since spec 15 (no _run_id filter).
  SELECT * FROM raw_pipeline_reply_intent_classifications
  WHERE source_table = 'conversation_messages'
),
msg_latest AS (
  SELECT * FROM raw_comms_message
  WHERE _run_id = (SELECT _run_id FROM raw_comms_message
                   ORDER BY _loaded_at DESC LIMIT 1)
),
conv_latest AS (
  SELECT * FROM raw_comms_conversation
  WHERE _run_id = (SELECT _run_id FROM raw_comms_conversation
                   ORDER BY _loaded_at DESC LIMIT 1)
)

-- ── EMAIL replies ───────────────────────────────────────────────────────────────────
SELECT
  'email'                       AS channel,
  e.lead_email                  AS prospect_email,
  CAST(NULL AS VARCHAR)         AS prospect_phone,
  'inbound'                     AS direction,        -- reply_data stores prospect→us only
  e.reply_timestamp             AS replied_at,
  e.intent                      AS intent,
  CAST(NULL AS VARCHAR)         AS body,             -- body stays in source mirrors; this view is the canonical intent surface
  e.campaign_id                 AS campaign_id,
  e.source_table || ':' || e.source_id AS source_id
FROM email_latest e

UNION ALL

-- ── SMS messages (inbound + outbound), intent inferred from conversation state ───────
SELECT
  'sms'                         AS channel,
  c.prospect_email              AS prospect_email,    -- usually NULL for SMS
  c.prospect_number             AS prospect_phone,
  m.direction                   AS direction,         -- 'inbound' | 'outbound'
  m.created_at                  AS replied_at,
  CASE c.state
    WHEN 'opted_out' THEN 'unsubscribe'
    WHEN 'declined'  THEN 'negative'
    WHEN 'booked'    THEN 'positive'
    WHEN 'escalated' THEN 'positive'
    ELSE NULL                                         -- engaged / in-flight / unmapped
  END                           AS intent,
  m.content                     AS body,
  CAST(NULL AS VARCHAR)         AS campaign_id,        -- Sendivo messages carry no campaign_id
  CAST(m.id AS VARCHAR)         AS source_id
FROM msg_latest m
LEFT JOIN conv_latest c ON c.id = m.conversation_id

-- ── WhatsApp (Iskra) — KNOWN GAP, NOT YET MIRRORED ──────────────────────────────────
-- WhatsApp has no API/store today (the mcp__whatsapp__* surface is a personal-account
-- bridge, not a logged corpus we mirror). When a raw_whatsapp_message mirror lands, add a
-- third UNION ALL branch here with channel='whatsapp', mapping phone→prospect_phone and the
-- WhatsApp body→body, intent NULL (or classified) — no other branch needs to change.
;
