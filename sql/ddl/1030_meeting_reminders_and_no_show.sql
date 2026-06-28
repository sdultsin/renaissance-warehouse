-- 1030_meeting_reminders_and_no_show.sql  [2026-06-27]  Meeting reminder/confirmation-system foundation.
-- @gate: add
-- Depends on 65
-- Depends on 64
-- Depends on 41
--
-- Two read-only serving VIEWS for the 1-hour meeting reminder + confirmation system
-- (deliverables/2026-06-26-revops-funnel-deep-dive/REMINDER-SYSTEM-ARCHITECTURE.md). Both are
-- CREATE OR REPLACE VIEW — idempotent, non-destructive, no data written.
--
--   core.v_meeting_reminders — the per-booked-meeting SPINE the reminder scheduler reads. One row per
--     booked meeting (core.meeting, source='sheet'), enriched with phone + the ORIGINAL SENDING ACCOUNT
--     (funding-form column idx16 "Our Email"; deliberately NOT core.reply.eaccount, which is only 56%
--     populated per REPLY-SYNC-QA Part 5). meeting_slot_at — the T-minus reminder anchor (date+time) —
--     is a TYPED-BUT-EMPTY slot today: core.meeting.meeting_date is DATE only (the form carries no
--     time-of-day). It is filled GOING FORWARD by capture-at-confirmation (the SMS AIM / IM writes the
--     slot at booking) and, for email, by the reply-thread datetime parse (Part 5, ~94.6%). It is
--     NEVER inferred/fabricated here — a NULL meeting_slot_at means "not yet sourced", per 100%-or-wipe.
--
--   core.v_no_show_ledger — the measurement BACKBONE: the booked denominator (core.meeting) LEFT JOIN
--     the only outcome feed we have today (core.lead_disposition). outcome ∈ {no_show, reschedule,
--     cancelled, disqualified, live_opportunity, ...} or NULL when there is no feedback. There is NO
--     "showed" signal yet — absence of a no-show disposition is NOT a show — so has_outcome gates any
--     rate (no_show_rate = no_show / has_outcome, never / all booked). As partner feedback expands
--     (all partners, daily; ideally the meeting_id handshake) this view fills automatically; when a
--     real showed+no_show feed exists it graduates to a physical core.no_show table.

-- =====================================================================================
-- core.v_meeting_reminders — the reminder-system spine (one row per booked meeting).
-- =====================================================================================
CREATE OR REPLACE VIEW core.v_meeting_reminders AS
WITH ff AS (  -- funding-form Data tab; row_json is a POSITIONAL array (idx per entities/meeting.py).
  SELECT json_extract_string(row_json, '$[1]')                          AS submission_id,   -- = core.meeting.source_event_id
         NULLIF(trim(json_extract_string(row_json, '$[10]')), '')        AS phone,           -- idx10 = Phone
         NULLIF(lower(trim(json_extract_string(row_json, '$[16]'))), '') AS original_sending_account, -- idx16 = "Our Email"
         row_number() OVER (PARTITION BY json_extract_string(row_json, '$[1]')
                            ORDER BY _loaded_at DESC)                     AS rn  -- latest edit of a Submission ID wins
  FROM main.raw_sheets_funding_form_data
  WHERE _tab = 'Data'
)
SELECT
  m.meeting_id,
  m.source,
  m.posted_at,
  m.lead_email,
  COALESCE(m.partner_key, m.partner)         AS partner,
  m.channel,
  m.advisor,                                  -- "<PREFIX>: <Full Name>"
  m.advisor_name,
  m.advisor_partner,
  m.inbox_manager,
  m.campaign_id,
  m.campaign_name_raw,
  m.workspace_name,
  m.workspace_slug,
  m.offer,
  f.phone,
  f.original_sending_account,
  m.meeting_date,                             -- DATE only (the form has no time-of-day)
  CAST(NULL AS TIME)      AS meeting_time,    -- TYPED-EMPTY: filled by capture-at-confirmation / email parse
  CAST(NULL AS TIMESTAMP) AS meeting_slot_at  -- TYPED-EMPTY: the T-minus reminder anchor (date+time)
FROM core.meeting m
LEFT JOIN ff f ON f.submission_id = m.source_event_id AND f.rn = 1
WHERE m.source = 'sheet';

-- =====================================================================================
-- core.v_no_show_ledger — booked denominator + the (sparse) outcome feed; measurement backbone.
-- =====================================================================================
CREATE OR REPLACE VIEW core.v_no_show_ledger AS
WITH disp AS (  -- one disposition per lead (latest), the only outcome feed we have today.
  SELECT lower(lead_email)        AS lead_email,
         disposition,
         disposition_class,
         rep,
         source_period,
         row_number() OVER (PARTITION BY lower(lead_email)
                            ORDER BY resolved_at DESC NULLS LAST) AS rn
  FROM core.lead_disposition
)
SELECT
  m.meeting_id,
  m.lead_email,
  COALESCE(m.partner_key, m.partner) AS partner,
  m.channel,
  m.advisor_name,
  m.inbox_manager,
  m.campaign_id,
  m.meeting_date,
  m.posted_at,
  CASE
    WHEN d.disposition_class IS NULL          THEN NULL              -- no feedback (NOT "showed")
    WHEN d.disposition_class = 'no_show'      THEN 'no_show'
    WHEN d.disposition_class = 'reschedule'   THEN 'reschedule'
    WHEN d.disposition_class = 'cancelled'    THEN 'cancelled'
    WHEN d.disposition_class = 'disqualified' THEN 'disqualified'
    WHEN d.disposition_class = 'live'         THEN 'live_opportunity'
    ELSE d.disposition_class
  END                                AS outcome,
  d.disposition                      AS disposition_raw,
  d.disposition_class,
  (d.disposition_class IS NOT NULL)  AS has_outcome,            -- gate any rate on this (absence != show)
  CASE WHEN d.lead_email IS NOT NULL THEN 'lead_disposition' END AS outcome_source,
  CASE WHEN d.lead_email IS NOT NULL THEN 'email' END            AS outcome_match_method,
  d.rep,
  d.source_period
FROM core.meeting m
LEFT JOIN disp d ON d.lead_email = lower(m.lead_email) AND d.rn = 1
WHERE m.source = 'sheet';
