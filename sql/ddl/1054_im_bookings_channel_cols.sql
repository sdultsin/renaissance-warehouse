-- @gate: add
-- Depends on 27
-- Depends on 1048
-- 1054_im_bookings_channel_cols.sql  [2026-06-30]
-- Capture the per-booking CHANNEL + provenance columns the bookings portal exposes after the
-- 2026-06-30 all-offer-booking-form migration. These are the columns core.meeting needs to source
-- Funding meetings DIRECTLY from im_bookings (the Funding-Form Google Sheet is retired as of
-- ~2026-06-29 6PM ET; im_bookings is now the channel-rich canonical booking source —
-- handoffs/2026-06-29-meeting-source-rewire-im-bookings-BUILD.md).
--   channel             -- Email / SMS / WhatsApp / Call (verified populated, 0 NULL, on the live
--                          >=2026-06-29 window: 54 Email / 49 SMS / 20 Call / 1 WhatsApp). This is
--                          the attribution split entities/meeting.py reads (replaces the Funding-Form
--                          col-D Channel). Pre-migration historical rows carry NULL (channel was added
--                          ~2026-06-29); the cutover only sources >=2026-06-29 from im_bookings, where
--                          channel is fully populated.
--   source              -- booking provenance ('fillout_sheet' etc.) — mirrored for audit.
--   booking_id          -- portal-side booking key (now populated; was NULL pre-migration). Mirrored
--                          for audit/cross-reference; the meeting projection keys on id (= imb:<id>).
--   industry            -- prospect industry (sparse) — mirrored for downstream segmentation.
--   inbox_manager_email -- the IM's email (sparse), companion to inbox_manager (name).
-- entities/im_bookings.py adds these to SOURCE_COLUMNS in the SAME change (its INSERT lists columns
-- explicitly, so the column ORDER here is irrelevant — only the names must match). All VARCHAR: the
-- raw layer keeps source values verbatim; typing/normalization happens downstream. Idempotent
-- (ADD COLUMN IF NOT EXISTS). The mirror re-pulls these on the next nightly (the existing rows get the
-- new columns backfilled from the portal then); the column ADD itself is non-destructive and applies
-- live now.
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS channel             VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS source              VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS booking_id          VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS industry            VARCHAR;
ALTER TABLE raw_im_bookings ADD COLUMN IF NOT EXISTS inbox_manager_email VARCHAR;
