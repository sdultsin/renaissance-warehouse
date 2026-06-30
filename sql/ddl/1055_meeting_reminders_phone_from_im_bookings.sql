-- @gate: add
-- Depends on 1049
-- 1055_meeting_reminders_phone_from_im_bookings.sql  [2026-06-30]
-- Re-point core.v_meeting_reminders.phone + original_sending_account to im_bookings for the post-cutover
-- Funding rows. Companion to the core.meeting im_bookings rewire
-- (handoffs/2026-06-29-meeting-source-rewire-im-bookings-BUILD.md): meetings on/after 2026-06-29 now
-- come from the bookings-portal im_bookings mirror with meeting_id 'imb:<id>' (NOT the retired
-- Funding-Form Google Sheet 'sheet:<submission_id>').
--
-- The reminder spine sourced phone + original_sending_account ("Our Email") from the Funding-Form sheet
-- by joining the sheet's Submission ID to core.meeting.source_event_id. For 'imb:%' rows that join is
-- DEAD (source_event_id is the im_bookings id, not a sheet Submission ID) -> phone would silently go
-- NULL and the SMS reminder would have no number to text. im_bookings carries phone + our_email
-- natively, so we add an id-keyed CTE (imb_contact) and COALESCE: frozen FF rows (<=06-28, meeting_id
-- 'sheet:%') keep their sheet-sourced phone; im_bookings rows (>=06-29) get phone/our_email from the
-- portal. meeting_slot_at (DDL 1049) already joins im_bookings on email+booking-date and is unchanged.
--
-- CREATE OR REPLACE VIEW — idempotent, non-destructive, no data written. Reads the latest LIVE
-- im_bookings snapshot, so it tracks the nightly mirror. Everything below is verbatim from DDL 1049
-- except the new imb_contact CTE + the two COALESCE'd output columns.

CREATE OR REPLACE VIEW core.v_meeting_reminders AS
WITH ff AS (  -- funding-form Data tab; row_json is a POSITIONAL array (idx per entities/meeting.py).
  SELECT json_extract_string(row_json, '$[1]')                          AS submission_id,   -- = core.meeting.source_event_id
         NULLIF(trim(json_extract_string(row_json, '$[10]')), '')        AS phone,           -- idx10 = Phone
         NULLIF(lower(trim(json_extract_string(row_json, '$[16]'))), '') AS original_sending_account, -- idx16 = "Our Email"
         row_number() OVER (PARTITION BY json_extract_string(row_json, '$[1]')
                            ORDER BY _loaded_at DESC)                     AS rn  -- latest edit of a Submission ID wins
  FROM main.raw_sheets_funding_form_data
  WHERE _tab = 'Data'
),
imb_contact AS (  -- im_bookings phone + sending inbox, id-keyed (= core.meeting.source_event_id for 'imb:%').
  SELECT
    CAST(id AS VARCHAR)                  AS source_event_id,
    NULLIF(trim(phone), '')              AS phone,
    NULLIF(lower(trim(our_email)), '')   AS original_sending_account,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY TRY_CAST(created_at AS TIMESTAMP) DESC NULLS LAST) AS rn
  FROM raw_im_bookings
  WHERE _source = 'portal_im_bookings_nightly'
    AND _snapshot_date = (SELECT max(_snapshot_date) FROM raw_im_bookings
                          WHERE _source = 'portal_im_bookings_nightly')
    AND deleted_at IS NULL
),
imb AS (  -- latest LIVE im_bookings snapshot; the slot-time source. Dedup same-(email,booking-date).
  SELECT
    lower(trim(email))                  AS lead_email,
    TRY_CAST("date" AS DATE)            AS booking_date,   -- = core.meeting.meeting_date (col A)
    NULLIF(trim(meeting_date), '')      AS slot_date,      -- the future APPOINTMENT date
    NULLIF(trim(meeting_time), '')      AS slot_time,
    NULLIF(trim(meeting_tz), '')        AS slot_tz,
    ROW_NUMBER() OVER (
      PARTITION BY lower(trim(email)), TRY_CAST("date" AS DATE)
      ORDER BY TRY_CAST(created_at AS TIMESTAMP) DESC NULLS LAST
    )                                   AS rn
  FROM raw_im_bookings
  WHERE _source = 'portal_im_bookings_nightly'
    AND _snapshot_date = (SELECT max(_snapshot_date) FROM raw_im_bookings
                          WHERE _source = 'portal_im_bookings_nightly')
    AND deleted_at IS NULL              -- exclude cancelled bookings — never remind on a dead slot
),
slot AS (  -- typed + zone-mapped slot, computed once.
  SELECT
    i.lead_email, i.booking_date,
    TRY_CAST(i.slot_date AS DATE)                                                   AS slot_d,
    COALESCE(TRY_STRPTIME(i.slot_time, '%I:%M %p'), TRY_STRPTIME(i.slot_time, '%H:%M'))::TIME AS slot_t,
    i.slot_tz,
    CASE
      WHEN upper(i.slot_tz) IN ('ET','EST','EDT') THEN 'America/New_York'
      WHEN upper(i.slot_tz) IN ('CT','CST','CDT') THEN 'America/Chicago'
      WHEN upper(i.slot_tz) IN ('MT','MST','MDT') THEN 'America/Denver'
      WHEN upper(i.slot_tz) IN ('PT','PST','PDT') THEN 'America/Los_Angeles'
    END                                                                            AS slot_zone
  FROM imb i
  WHERE i.rn = 1
)
SELECT
  m.meeting_id,
  m.source,
  m.posted_at,
  m.lead_email,
  COALESCE(m.partner_key, m.partner)         AS partner,
  m.channel,
  m.advisor,
  m.advisor_name,
  m.advisor_partner,
  m.inbox_manager,
  m.campaign_id,
  m.campaign_name_raw,
  m.workspace_name,
  m.workspace_slug,
  m.offer,
  COALESCE(f.phone, ik.phone)                                  AS phone,                    -- FF (<=06-28) | im_bookings (>=06-29)
  COALESCE(f.original_sending_account, ik.original_sending_account) AS original_sending_account,
  m.meeting_date,
  -- meeting_time / meeting_slot_at: filled from im_bookings (100%-or-wipe). (DDL 1049, unchanged.)
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN s.slot_t END                                          AS meeting_time,
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN ((s.slot_d + s.slot_t) AT TIME ZONE s.slot_zone) AT TIME ZONE 'UTC'
  END                                                             AS meeting_slot_at,  -- naive UTC = firing key
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN (s.slot_d + s.slot_t) END                             AS meeting_slot_local,
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN s.slot_zone END                                       AS meeting_tz
FROM core.meeting m
LEFT JOIN ff          f  ON f.submission_id = m.source_event_id AND f.rn = 1
LEFT JOIN imb_contact ik ON ik.source_event_id = m.source_event_id AND ik.rn = 1
                            AND m.meeting_id LIKE 'imb:%'
LEFT JOIN slot        s  ON s.lead_email = lower(m.lead_email) AND s.booking_date = m.meeting_date
WHERE m.source = 'sheet';
