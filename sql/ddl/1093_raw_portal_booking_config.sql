-- @gate: add
-- Intent: nightly mirror of the bookings-portal booking-FORM CONFIG tables
--         (MIRROR-COVERAGE-AUDIT 2026-07-09 §2, gap #4): booking_partners
--         (partner→offer→Slack-channel routing) + booking_options (form option
--         config). Original, Darcy-entered config that exists nowhere else —
--         custody requires a warehouse copy.
-- Depends on: 27
--
-- ADDITIVE ONLY: two new raw tables; nothing existing is touched.
--
-- Source: the bookings-portal Supabase (same PostgREST source as raw_im_bookings,
-- credentials by name: IM_BOOKINGS_SUPABASE_URL + SERVICE_ROLE/ANON key). Loaded
-- nightly by entities/portal_booking_config.py (im_bookings phase): tiny tables
-- (11 + 3 rows live 2026-07-09), full-refresh REPLACE — each run keeps exactly ONE
-- live snapshot; a 0-row pull refuses to replace (key/RLS breakage guard).
--
-- Column names/types verified 2026-07-09 against the live PostgREST OpenAPI spec.
-- Metadata columns follow the raw_im_bookings convention:
--   _snapshot_date DATE — the pull date; _source VARCHAR — loader tag;
--   _loaded_at TIMESTAMPTZ — ingestion wall-clock.

-- portal.booking_partners — partner→offer→Slack-channel routing config for the
-- booking form (the routing truth behind im_bookings partner attribution).
CREATE TABLE IF NOT EXISTS raw_portal_booking_partners (
    partner             VARCHAR,
    offer               VARCHAR,
    slack_channel_id    VARCHAR,
    slack_channel_name  VARCHAR,
    active              BOOLEAN,
    created_at          TIMESTAMPTZ,
    _snapshot_date      DATE        NOT NULL,
    _source             VARCHAR     NOT NULL,
    _loaded_at          TIMESTAMPTZ NOT NULL
);

-- portal.booking_options — booking-form option config (field/scope/value rows).
CREATE TABLE IF NOT EXISTS raw_portal_booking_options (
    id                  BIGINT,
    field               VARCHAR,
    scope               VARCHAR,
    value               VARCHAR,
    active              BOOLEAN,
    created_by          VARCHAR,
    created_at          TIMESTAMPTZ,
    _snapshot_date      DATE        NOT NULL,
    _source             VARCHAR     NOT NULL,
    _loaded_at          TIMESTAMPTZ NOT NULL
);
