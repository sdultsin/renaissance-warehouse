-- Scope A (IM conversion attribution, 2026-06-09): core.conversion_event.feeder. Version 54.
--
-- Adds a FEEDER provenance dim so the new identity-bearing portal feeder
-- (raw_im_bookings -> conversion_agent='im') can coexist with the original identity-less
-- Slack feeder (core.meeting -> conversion_agent='im') without ambiguity.
--
--   feeder: slack_meeting | close_call | portal_im_bookings | …  (free-text, like the
--           other dims — a new feeder inserts a new string, NO DDL change)
--
-- ⚠ COUNTING RULE: IM meetings are now DOUBLE-CARRIED — the same physical booking can
-- appear once via 'slack_meeting' (count feed, lead_email/phone NULL) and once via
-- 'portal_im_bookings' (identity feed, email ~99.9%). There is no row-level join key
-- between the two feeds. NEVER count(*) across feeders for "total meetings"; always
-- GROUP BY / filter feeder. Per-lead analyses (lead_intel.is_meeting, response-time ×
-- conversion) are unaffected: only portal rows carry identity, so identity joins
-- naturally hit one feed.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS; backfill UPDATE is a pure projection (re-runnable).
-- Additive only — follows the sql/ddl/26_meeting_partner_key.sql ALTER pattern.

ALTER TABLE core.conversion_event ADD COLUMN IF NOT EXISTS feeder VARCHAR;

-- Backfill provenance for rows built before this column existed (entity now stamps it
-- directly; this only matters for a warehouse where conversion_event hasn't rebuilt yet).
UPDATE core.conversion_event
SET feeder = CASE
  WHEN conversion_agent = 'im'          AND feeder IS NULL THEN 'slack_meeting'
  WHEN conversion_agent = 'warm_caller' AND feeder IS NULL THEN 'close_call'
  ELSE feeder
END
WHERE feeder IS NULL;

CREATE INDEX IF NOT EXISTS ix_core_conv_event_feeder ON core.conversion_event (feeder);
