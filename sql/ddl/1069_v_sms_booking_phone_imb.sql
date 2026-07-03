-- core.v_sms_booking_phone — recover booking phone from im_bookings after the meeting-source cutover [2026-07-02]
--
-- WHY (flag warehouse-flags #15): core.v_sms_booking_attribution went 94% 'unattributed_no_phone' for
-- bookings >= 2026-06-29 (243/258; was 58% attributed at the PR #110 launch). 'unattributed_no_phone'
-- fires when core.v_sms_booking_phone.phone10 IS NULL, so "which blast booked this meeting" is
-- unanswerable for anything recent — blocking per-blast copy KPI + the Jun-29 1.23M-send verification.
--
-- ROOT CAUSE: DDL 1034's v_sms_booking_phone recovered phone10 by joining core.meeting -> the retired
-- Funding-Form Google Sheet (raw_sheets_funding_form_data) BY EMAIL. Post-cutover SMS bookings are
-- im_bookings 'imb:%' rows whose lead_email is not in the frozen sheet, so ff.phone10 is NULL for all of
-- them. im_bookings carries phone NATIVELY (measured coverage for recent SMS bookings: 58/58, 48/69,
-- 79/83, 133/138 on Jun29..Jul2), keyed by id == core.meeting.source_event_id for 'imb:%'.
--
-- FIX: add an id-keyed im_bookings phone leg (imb_ph, right-10 normalized like every other SMS surface)
-- and COALESCE it ahead of the legacy sheet leg — im_bookings phone for 'imb:%' rows, frozen sheet phone
-- for pre-cutover 'sheet:%' rows. Mirrors the exact pattern DDL 1055 used for core.v_meeting_reminders.
-- Output columns unchanged (v_sms_booking_attribution / v_sms_blast_performance depend on them).
--
-- Verified read-only on serving snapshot warehouse_20260703_031137_972.duckdb:
--   * phone coverage for booking_ts >= 2026-06-29: 333 with-phone / 63 without (was 73 / 323).
--   * no row fan-out: proposed rows == current rows == distinct meeting_id (396 == 396 == 396).
-- NOTE: 'first_reply'/'last_outbound' attribution for bookings on/after 2026-07-01 additionally needs the
-- raw_sendivo_outbound_message backfill (flag #16) — with phone restored here, those otherwise land in
-- 'unattributed_no_blast' (not 'no_phone') until the outbound mirror catches up on the nightly.
--
-- @gate: add
-- Depends on 1034 1051 1055
CREATE OR REPLACE VIEW core.v_sms_booking_phone AS
WITH ff AS (  -- legacy Funding-Form SMS phone by email (pre-cutover 'sheet:%' bookings)
    SELECT
        lower(trim(json_extract_string(row_json, '$[9]')))                       AS email,
        right(regexp_replace(json_extract_string(row_json, '$[10]'), '[^0-9]', '', 'g'), 10) AS phone10
    FROM main.raw_sheets_funding_form_data
    WHERE _run_id = (SELECT max(_run_id) FROM main.raw_sheets_funding_form_data)
      AND upper(trim(json_extract_string(row_json, '$[3]'))) = 'SMS'
),
imb_ph AS (  -- im_bookings native phone, id-keyed (= core.meeting.source_event_id for 'imb:%')
    SELECT
        CAST(id AS VARCHAR)                                                  AS source_event_id,
        right(regexp_replace(phone, '[^0-9]', '', 'g'), 10)                  AS phone10,
        row_number() OVER (PARTITION BY id
                           ORDER BY TRY_CAST(created_at AS TIMESTAMP) DESC NULLS LAST) AS rn
    FROM main.raw_im_bookings
    WHERE _source = 'portal_im_bookings_nightly'
      AND _snapshot_date = (SELECT max(_snapshot_date) FROM main.raw_im_bookings
                            WHERE _source = 'portal_im_bookings_nightly')
      AND deleted_at IS NULL
)
SELECT DISTINCT
    m.meeting_id,
    lower(m.lead_email)                                     AS email,
    COALESCE(NULLIF(ip.phone10, ''), ff.phone10)            AS phone10,   -- im_bookings ('imb:%') | frozen sheet
    m.program,
    m.sendivo_sub_account,
    m.meeting_date,
    coalesce(m.submission_ts, (m.meeting_date)::TIMESTAMP)  AS booking_ts
FROM core.meeting m
LEFT JOIN ff     ON ff.email = lower(m.lead_email)
LEFT JOIN imb_ph ip ON ip.source_event_id = m.source_event_id AND ip.rn = 1 AND m.meeting_id LIKE 'imb:%'
WHERE m.channel = 'SMS';
