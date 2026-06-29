-- @gate: add
-- Depends on 27
-- 1048_im_bookings_meeting_fields.sql  [2026-06-29]
-- Capture the booking-SLOT + lifecycle columns the bookings portal added AFTER the original
-- 22-col raw_im_bookings snapshot (sql/ddl/27). The IM booking form
-- (generalrenaissance.github.io/booking-form -> Supabase im_bookings) now collects, per booking:
--   meeting_date / meeting_time / meeting_tz -- the appointment SLOT (the reminder anchor). Added
--                                               ~2026-06-29; the Funding-Form Google Sheet does NOT
--                                               carry these (verified: 38-col sheet has Date +
--                                               Submission time only), so im_bookings is the ONLY
--                                               warehouse path to meeting_slot_at. NB: im_bookings.date
--                                               is the BOOKING date (= core.meeting.meeting_date / sheet
--                                               col A); meeting_date here is the future APPOINTMENT date.
--   created_at / deleted_at                  -- booking lifecycle. deleted_at gates out cancelled
--                                               bookings downstream so a reminder never fires on a dead
--                                               slot; created_at is the dedup tiebreak for same-(email,
--                                               date) re-bookings (~0.2%).
--   lead_type / subject_line                 -- booking detail (parity with the form).
-- entities/im_bookings.py adds these to SOURCE_COLUMNS in the SAME change (its INSERT lists columns
-- explicitly, so the column ORDER here is irrelevant — only the names must match). All VARCHAR: the
-- raw layer keeps source values verbatim; typing/normalization (12h|24h time, 8 tz variants -> 4
-- DST-aware IANA zones, -> UTC) happens downstream in core.v_meeting_reminders (DDL 1049). The mirror
-- also moves to the portal SERVICE-ROLE key in the same change (the anon GRANT was revoked ~2026-06-29
-- -> the nightly pull now 401s; see entities/im_bookings.py). Idempotent (ADD COLUMN IF NOT EXISTS).
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS meeting_date  VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS meeting_time  VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS meeting_tz    VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS created_at    VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS deleted_at    VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS lead_type     VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS subject_line  VARCHAR;
