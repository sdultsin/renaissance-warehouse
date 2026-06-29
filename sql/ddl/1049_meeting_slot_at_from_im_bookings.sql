-- @gate: add
-- Depends on 1030
-- Depends on 1048
-- 1049_meeting_slot_at_from_im_bookings.sql  [2026-06-29]
-- Fill core.v_meeting_reminders.meeting_slot_at — the T-minus reminder anchor that has been a
-- TYPED-BUT-EMPTY slot since DDL 1030 (the Funding-Form sheet carries no time-of-day).
--
-- SOURCE (the gap-closer): the IM booking form now collects Meeting Date / Time / Timezone, which
-- land in the bookings-portal Supabase im_bookings table and are mirrored to raw_im_bookings (DDL
-- 1048 cols). The Funding-Form Google SHEET does NOT carry them, so im_bookings is the only path.
-- The sheet STAYS source-of-truth for the meeting FACT (dedup/double-bookings/attribution); this
-- pulls ONLY the slot-time from im_bookings and joins it on.
--
-- JOIN: no shared key exists (the sheet's Submission ID is a UUID; im_bookings.id != that, and
-- im_bookings.booking_id is NULL). Validated empirically (2026-06-29): lower(email) + booking-date is
-- 99.8% unique in im_bookings, and im_bookings.date == core.meeting.meeting_date for matched emails
-- (59/60 on a 06-27 sample). So we join lower(email)=lead_email AND im_bookings.date=meeting_date,
-- taking the latest created_at to break the ~0.2% same-(email,date) re-bookings. Cancelled bookings
-- (deleted_at not null) are excluded so a reminder never fires on a dead slot.
--
-- NORMALIZATION (validated against live values): meeting_time arrives as 12h ("11:00 AM") OR 24h
-- ("10:00"/"15:30"); meeting_tz as one of 8 variants {ET,EST,CT,CST,MT,MST,PT,PST}. We map the 8 -> 4
-- DST-aware IANA zones (EST and ET both -> America/New_York: IMs mean "Eastern", and a future booking
-- in summer is EDT) and convert (local wall-clock AT TIME ZONE zone) AT TIME ZONE 'UTC' -> a naive
-- UTC timestamp = the firing key. 100%-or-wipe: meeting_slot_at/local stay NULL unless date, time AND
-- tz are all present and parse (a NULL means "not yet sourced" — never a guessed reminder time).
--
-- ADDITIVE: meeting_time/meeting_slot_at keep their existing positions+types (TIME / naive-UTC
-- TIMESTAMP); meeting_slot_local + meeting_tz are appended. CREATE OR REPLACE VIEW — idempotent, no
-- data written. Reads the latest LIVE im_bookings snapshot, so it tracks the nightly mirror.

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
  f.phone,
  f.original_sending_account,
  m.meeting_date,
  -- meeting_time / meeting_slot_at: was TYPED-EMPTY, now filled from im_bookings (100%-or-wipe).
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN s.slot_t END                                          AS meeting_time,
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN ((s.slot_d + s.slot_t) AT TIME ZONE s.slot_zone) AT TIME ZONE 'UTC'
  END                                                             AS meeting_slot_at,  -- naive UTC = firing key
  -- appended (additive): the prospect-local wall-clock slot + the resolved IANA zone.
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN (s.slot_d + s.slot_t) END                             AS meeting_slot_local,
  CASE WHEN s.slot_d IS NOT NULL AND s.slot_t IS NOT NULL AND s.slot_zone IS NOT NULL
       THEN s.slot_zone END                                       AS meeting_tz
FROM core.meeting m
LEFT JOIN ff   f ON f.submission_id = m.source_event_id AND f.rn = 1
LEFT JOIN slot s ON s.lead_email = lower(m.lead_email) AND s.booking_date = m.meeting_date
WHERE m.source = 'sheet';
