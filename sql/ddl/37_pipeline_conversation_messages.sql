-- Live migration: add raw_pipeline_conversation_messages to the slim
-- pipeline-supabase mirror. Fresh installs get the same table from
-- sql/ddl/04_pipeline_mirror.sql; this version-37 file applies it to the
-- already-initialized live warehouse (DDL 04 is version 4 and will never
-- re-run there). Keep the two definitions identical.
--
-- public.conversation_messages = full Instantly email thread bodies
-- (~17.1M rows, growing). Immutable events: a sent/received email never
-- mutates in place, so this mirrors with sync mode `insert` keyed on the
-- Instantly message id and pulls incrementally by a message_timestamp
-- watermark. See entities/pipeline_mirror.py SPECS["conversation_messages"].
--
-- LINEAGE: source today is pipeline-supabase public.conversation_messages,
-- which itself mirrors Instantly Unibox conversations. Once pipeline-supabase
-- is retired this mirror's source should swap to a direct Instantly
-- conversation sync (per-workspace Unibox/emails endpoints) feeding the same
-- raw_pipeline_conversation_messages shape. Until then this is the canonical
-- full-body thread source for BI.

CREATE TABLE IF NOT EXISTS raw_pipeline_conversation_messages (
  _key              VARCHAR NOT NULL,
  id                VARCHAR,
  thread_id         VARCHAR,
  campaign_id       VARCHAR,
  workspace_id      VARCHAR,
  lead_email        VARCHAR,
  sender_email      VARCHAR,
  sender_name       VARCHAR,
  recipient_email   VARCHAR,
  recipient_name    VARCHAR,
  direction         VARCHAR,
  ue_type           INTEGER,
  body_text         VARCHAR,
  body_html         VARCHAR,
  subject           VARCHAR,
  message_timestamp TIMESTAMPTZ,
  step_raw          VARCHAR,
  step              INTEGER,
  variant           VARCHAR,
  is_unread         BOOLEAN,
  interest_status   INTEGER,
  ai_interest_value INTEGER,
  content_preview   VARCHAR,
  eaccount          VARCHAR,
  subsequence_id    VARCHAR,
  synced_at         TIMESTAMPTZ,
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR NOT NULL
);

-- Unique _key backs ON CONFLICT (_key) DO NOTHING in entities/pipeline_mirror.py.
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pipeline_conversation_messages_key
  ON raw_pipeline_conversation_messages (_key);

-- Helper indexes consistent with sibling event tables (*_campaign, *_ts) plus
-- thread/lead access paths for BI thread reconstruction.
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_conversation_messages_campaign
  ON raw_pipeline_conversation_messages (campaign_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_conversation_messages_ts
  ON raw_pipeline_conversation_messages (message_timestamp);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_conversation_messages_thread
  ON raw_pipeline_conversation_messages (thread_id);
CREATE INDEX IF NOT EXISTS ix_raw_pipeline_conversation_messages_lead
  ON raw_pipeline_conversation_messages (lead_email);
