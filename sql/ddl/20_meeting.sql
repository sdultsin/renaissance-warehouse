-- Phase 3: core.meeting canonical entity (spec 09).
-- Applied at schema version 20 by scripts/setup_db.py / orchestrator DDL applier.
--
-- One row per booked meeting. v1 source is Slack only (the success-channel posts),
-- mirrored into raw_pipeline_meetings_booked_raw by data-pipeline-v2's cron and then
-- bulk-copied into this warehouse by entities/pipeline_mirror.py.
--
-- Per Sam: Slack is the source of truth. Calendly + Close are v1.5 (do NOT build them).
--
-- Type conventions match the rest of the warehouse:
--   text                       -> VARCHAR
--   timestamp with time zone   -> TIMESTAMPTZ
--   numeric / double           -> DOUBLE   (match_confidence + cost projection)
--
-- Idempotent upsert semantics live in entities/meeting.py: insert net-new meeting_id,
-- update last-seen fields for rows that already exist. The table is canonical (core
-- schema), NOT append-only — raw_pipeline_meetings_booked_raw remains the immutable
-- snapshot history.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.meeting (
  meeting_id            VARCHAR PRIMARY KEY,    -- derived: '{channel_id}:{message_ts}:{line_index}'
  source                VARCHAR NOT NULL,       -- 'slack' (v1) | 'calendly' | 'close' (v1.5+)
  source_event_id       VARCHAR,                -- original ID from source (raw_pipeline_meetings_booked_raw.id)
  posted_at             TIMESTAMPTZ NOT NULL,
  partner               VARCHAR,                -- funding partner, Slack channel-derived
  campaign_id           VARCHAR,                -- nullable; see match_method / match_confidence
  campaign_name_raw     VARCHAR,                -- literal campaign name as it appeared in the post
  cm                    VARCHAR,                -- derived: campaign join first, post-parse fallback
  match_method          VARCHAR,                -- exact | alias | manual | unmatched (carried from source)
  match_confidence      DOUBLE,
  is_duplicate_of       VARCHAR,                -- v1.5 cross-source dedup target; NULL in v1
  -- Cost projection column (spec 13). NULL until v3 derivation logic populates it.
  cost_per_meeting_usd_estimated   DOUBLE,
  raw_text              VARCHAR,
  -- Bookkeeping: which run first created / last touched this canonical row.
  _first_run_id         VARCHAR,
  _last_run_id          VARCHAR,
  _first_seen_at        TIMESTAMPTZ,
  _last_seen_at         TIMESTAMPTZ
);

-- Common access paths: per-CM monthly rollups and partner breakdowns.
CREATE INDEX IF NOT EXISTS ix_core_meeting_posted_at ON core.meeting (posted_at);
CREATE INDEX IF NOT EXISTS ix_core_meeting_cm        ON core.meeting (cm);
CREATE INDEX IF NOT EXISTS ix_core_meeting_campaign  ON core.meeting (campaign_id);
