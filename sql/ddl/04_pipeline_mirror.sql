-- Phase 2 Track B: slim mirror of pipeline-supabase analytical tables.
-- Applied at version 4 by scripts/setup_db.py.
--
-- Each raw_pipeline_* table lives in the default (main) schema, not `core`.
-- Every row carries a surrogate _key, _loaded_at (when the mirror wrote it),
-- and _run_id (which orchestrator run last touched it). Since spec 15 these
-- tables are sync-mode mirrors, not append snapshots: immutable event tables
-- insert once, copy tables insert content-hashed versions, and dimension/daily
-- tables upsert by _key. Missing upstream rows are never deleted
-- (freeze-on-delete).
--
-- Type conventions:
--   text                       -> VARCHAR
--   integer                    -> INTEGER
--   bigint                     -> BIGINT
--   numeric                    -> DECIMAL(38, 12)
--   boolean                    -> BOOLEAN
--   date                       -> DATE
--   timestamp with time zone   -> TIMESTAMPTZ
--   ARRAY (text[])             -> VARCHAR  (JSON-encoded by the mirror)
--   jsonb                      -> VARCHAR  (JSON-encoded by the mirror)

-- public.campaigns (full mirror)
CREATE TABLE IF NOT EXISTS raw_pipeline_campaigns (
  _key                   VARCHAR NOT NULL,
  campaign_id            VARCHAR,
  workspace_id           VARCHAR,
  workspace_name         VARCHAR,
  name                   VARCHAR,
  status                 VARCHAR,
  cm_name                VARCHAR,
  industry               VARCHAR,
  bounced_count          INTEGER,
  contacted_count        INTEGER,
  leads_count            INTEGER,
  completed_count        INTEGER,
  unsubscribed_count     INTEGER,
  instantly_created_at   TIMESTAMPTZ,
  synced_at              TIMESTAMPTZ,
  tags                   VARCHAR,          -- ARRAY -> JSON
  lead_source            VARCHAR,
  rg_batch_ids           VARCHAR,          -- ARRAY -> JSON
  segment                VARCHAR,
  timestamp_updated      TIMESTAMPTZ,
  daily_limit            INTEGER,
  product                VARCHAR,
  excluded_from_analysis BOOLEAN,
  exclusion_reason       VARCHAR,
  infra_type             VARCHAR,
  _loaded_at             TIMESTAMPTZ NOT NULL,
  _run_id                VARCHAR NOT NULL
);

-- public.campaign_data (full mirror, per campaign × step × variant)
CREATE TABLE IF NOT EXISTS raw_pipeline_campaign_data (
  _key                       VARCHAR NOT NULL,
  campaign_id                VARCHAR,
  campaign_name              VARCHAR,
  workspace_id               VARCHAR,
  workspace_name             VARCHAR,
  cm_name                    VARCHAR,
  segment                    VARCHAR,
  product                    VARCHAR,
  infra_type                 VARCHAR,
  status                     VARCHAR,
  date_launched              TIMESTAMPTZ,
  daily_limit                INTEGER,
  lead_source                VARCHAR,
  tags                       VARCHAR,      -- ARRAY -> JSON
  excluded_from_analysis     BOOLEAN,
  exclusion_reason           VARCHAR,
  step                       VARCHAR,
  variant                    VARCHAR,
  emails_sent                INTEGER,
  replies                    INTEGER,
  opportunities              INTEGER,
  analytics_sequence_started INTEGER,
  leads_closed               INTEGER,
  e_op                       DECIMAL(38, 12),
  reply_rate                 DECIMAL(38, 12),
  close_rate                 DECIMAL(38, 12),
  campaign_score             DECIMAL(38, 12),
  subject                    VARCHAR,
  body                       VARCHAR,
  subject_preview            VARCHAR,
  body_preview               VARCHAR,
  signature                  VARCHAR,
  v_disabled                 BOOLEAN,
  synced_at                  TIMESTAMPTZ,
  meetings_booked            INTEGER,
  rg_batch_tags              VARCHAR,      -- ARRAY -> JSON
  pair_tag                   VARCHAR,
  sender_tags                VARCHAR,      -- ARRAY -> JSON
  other_tags                 VARCHAR,      -- ARRAY -> JSON
  total_leads                INTEGER,
  leads_completed            INTEGER,
  leads_bounced              INTEGER,
  leads_unsubscribed         INTEGER,
  lead_sequence_started      INTEGER,
  content_hash               VARCHAR,
  _loaded_at                 TIMESTAMPTZ NOT NULL,
  _run_id                    VARCHAR NOT NULL
);

-- public.campaign_daily_metrics (last 90 days)
CREATE TABLE IF NOT EXISTS raw_pipeline_campaign_daily_metrics (
  _key                     VARCHAR NOT NULL,
  campaign_id              VARCHAR,
  date                     DATE,
  sent                     INTEGER,
  contacted                INTEGER,
  new_leads_contacted      INTEGER,
  opened                   INTEGER,
  unique_opened            INTEGER,
  replies                  INTEGER,
  unique_replies           INTEGER,
  replies_automatic        INTEGER,
  unique_replies_automatic INTEGER,
  clicks                   INTEGER,
  unique_clicks            INTEGER,
  opportunities            INTEGER,
  unique_opportunities     INTEGER,
  synced_at                TIMESTAMPTZ,
  workspace_id             VARCHAR,
  workspace_name           VARCHAR,
  _loaded_at               TIMESTAMPTZ NOT NULL,
  _run_id                  VARCHAR NOT NULL
);

-- public.meetings_booked_raw (full)
CREATE TABLE IF NOT EXISTS raw_pipeline_meetings_booked_raw (
  _key                VARCHAR NOT NULL,
  id                  BIGINT,
  channel_id          VARCHAR,
  channel_name        VARCHAR,
  partner             VARCHAR,
  message_ts          VARCHAR,
  line_index          INTEGER,
  posted_by           VARCHAR,
  posted_at           TIMESTAMPTZ,
  raw_text            VARCHAR,
  booking_number      INTEGER,
  campaign_name_raw   VARCHAR,
  campaign_id         VARCHAR,
  match_method        VARCHAR,
  match_confidence    DECIMAL(38, 12),
  synced_at           TIMESTAMPTZ,
  posted_by_slack_id  VARCHAR,
  raw_line            VARCHAR,
  _loaded_at          TIMESTAMPTZ NOT NULL,
  _run_id             VARCHAR NOT NULL
);

-- public.reply_data (last 90 days, filter on reply_timestamp)
CREATE TABLE IF NOT EXISTS raw_pipeline_reply_data (
  _key            VARCHAR NOT NULL,
  id              BIGINT,
  campaign_id     VARCHAR,
  lead_email      VARCHAR,
  reply_text      VARCHAR,
  reply_timestamp TIMESTAMPTZ,
  workspace_id    VARCHAR,
  intent          VARCHAR,
  from_name       VARCHAR,
  subject         VARCHAR,
  synced_at       TIMESTAMPTZ,
  step            INTEGER,
  variant         VARCHAR,
  _loaded_at      TIMESTAMPTZ NOT NULL,
  _run_id         VARCHAR NOT NULL
);

-- public.reply_intent_classifications (canonical row-level reply intent bridge)
CREATE TABLE IF NOT EXISTS raw_pipeline_reply_intent_classifications (
  _key                  VARCHAR NOT NULL,
  source_table          VARCHAR,
  source_id             VARCHAR,
  workspace_id          VARCHAR,
  campaign_id           VARCHAR,
  lead_email            VARCHAR,
  sender_email          VARCHAR,
  recipient_email       VARCHAR,
  reply_timestamp       TIMESTAMPTZ,
  intent                VARCHAR,
  intent_source         VARCHAR,
  is_auto_reply         BOOLEAN,
  auto_reply_source     VARCHAR,
  auto_reply_confidence DECIMAL(18, 6),
  classifier_version    VARCHAR,
  classified_at         TIMESTAMPTZ,
  _loaded_at            TIMESTAMPTZ NOT NULL,
  _run_id               VARCHAR NOT NULL
);

-- public.reply_auto_reconciliation (aggregate vs row-level auto coverage)
CREATE TABLE IF NOT EXISTS raw_pipeline_reply_auto_reconciliation (
  _key                  VARCHAR NOT NULL,
  date                  DATE,
  campaign_id           VARCHAR,
  aggregate_unique_auto INTEGER,
  row_level_auto        INTEGER,
  coverage_pct          DECIMAL(18, 6),
  source_notes          VARCHAR,
  checked_at            TIMESTAMPTZ,
  _loaded_at            TIMESTAMPTZ NOT NULL,
  _run_id               VARCHAR NOT NULL
);

-- public.lead_events (last 90 days, filter on event_timestamp)
CREATE TABLE IF NOT EXISTS raw_pipeline_lead_events (
  _key            VARCHAR NOT NULL,
  id              BIGINT,
  lead_email      VARCHAR,
  campaign_id     VARCHAR,
  event_type      VARCHAR,
  workspace_id    VARCHAR,
  event_timestamp TIMESTAMPTZ,
  event_data      VARCHAR,                 -- jsonb -> JSON string
  synced_at       TIMESTAMPTZ,
  _loaded_at      TIMESTAMPTZ NOT NULL,
  _run_id         VARCHAR NOT NULL
);

-- public.variant_copy (full)
CREATE TABLE IF NOT EXISTS raw_pipeline_variant_copy (
  _key                 VARCHAR NOT NULL,
  campaign_id          VARCHAR,
  step                 INTEGER,
  variant              VARCHAR,
  subject              VARCHAR,
  body                 VARCHAR,
  synced_at            TIMESTAMPTZ,
  body_resolved        VARCHAR,
  subject_resolved     VARCHAR,
  v_disabled           BOOLEAN,
  body_unspintaxed     VARCHAR,
  subject_unspintaxed  VARCHAR,
  content_hash         VARCHAR,
  _loaded_at           TIMESTAMPTZ NOT NULL,
  _run_id              VARCHAR NOT NULL
);

-- public.bounce_suppression (full)
CREATE TABLE IF NOT EXISTS raw_pipeline_bounce_suppression (
  _key              VARCHAR NOT NULL,
  id                BIGINT,
  email             VARCHAR,
  domain            VARCHAR,
  bounce_type       VARCHAR,
  first_bounced_at  TIMESTAMPTZ,
  last_seen_at      TIMESTAMPTZ,
  workspaces_seen   VARCHAR,               -- ARRAY -> JSON
  source_campaigns  VARCHAR,               -- ARRAY -> JSON
  raw_reason        VARCHAR,
  lead_first_name   VARCHAR,
  lead_last_name    VARCHAR,
  lead_company      VARCHAR,
  created_at        TIMESTAMPTZ,
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR NOT NULL
);

-- Unique _key indexes back ON CONFLICT (_key) in entities/pipeline_mirror.py.
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_campaigns_key                 ON raw_pipeline_campaigns               (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_campaign_data_key             ON raw_pipeline_campaign_data           (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_campaign_daily_metrics_key    ON raw_pipeline_campaign_daily_metrics  (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_meetings_booked_raw_key       ON raw_pipeline_meetings_booked_raw     (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_reply_data_key                ON raw_pipeline_reply_data              (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_reply_intent_key              ON raw_pipeline_reply_intent_classifications (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_reply_auto_recon_key          ON raw_pipeline_reply_auto_reconciliation (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_lead_events_key               ON raw_pipeline_lead_events             (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_variant_copy_key              ON raw_pipeline_variant_copy            (_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_bounce_suppression_key        ON raw_pipeline_bounce_suppression      (_key);

-- Helper indexes for common joins and watermarks.
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_campaigns_campaign                  ON raw_pipeline_campaigns               (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_campaign_data_campaign              ON raw_pipeline_campaign_data           (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_campaign_daily_metrics_campaign     ON raw_pipeline_campaign_daily_metrics  (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_meetings_booked_raw_campaign        ON raw_pipeline_meetings_booked_raw     (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_data_campaign                 ON raw_pipeline_reply_data              (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_campaign               ON raw_pipeline_reply_intent_classifications (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_auto_recon_campaign           ON raw_pipeline_reply_auto_reconciliation (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_lead_events_campaign                ON raw_pipeline_lead_events             (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_variant_copy_campaign               ON raw_pipeline_variant_copy            (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_ts               ON raw_pipeline_reply_intent_classifications (reply_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_intent_campaign_ts      ON raw_pipeline_reply_intent_classifications (campaign_id, reply_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_meetings_booked_raw_posted_at ON raw_pipeline_meetings_booked_raw     (posted_at);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_reply_data_reply_ts           ON raw_pipeline_reply_data              (reply_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_lead_events_event_ts          ON raw_pipeline_lead_events             (event_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_bounce_suppression_last_seen  ON raw_pipeline_bounce_suppression      (last_seen_at);
