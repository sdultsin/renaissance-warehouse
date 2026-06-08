-- Direct-Instantly inbound reply mirror. Version 36.
--
-- WHY THIS EXISTS (2026-06-07, Pipeline-Supabase retirement):
--   Today the warehouse gets cold-email replies via the slim mirror of
--   pipeline-supabase.public.reply_data (entities/pipeline_mirror.py -> the
--   raw_pipeline_reply_data table). But pipeline-supabase.reply_data is produced
--   by an n8n webhook collector we do NOT own (see deliverables/
--   2026-03-23-data-landscape-audit.md "reply_data Population"); it also has the
--   known INSERT-vs-UPSERT duplicate-key bug that drops ~430 replies/day.
--
--   The retirement plan eliminates pipeline-supabase as ingestion staging. For
--   replies the lower-risk path is (A): have the warehouse pull replies DIRECTLY
--   from Instantly (GET /api/v2/emails?email_type=received), the same source the
--   n8n collector ultimately reads. This removes the dependency on a producer we
--   don't control AND fixes the dedupe bug (we upsert on the Instantly email id).
--
-- GRAIN: one row per Instantly inbound email (reply). PRIMARY KEY = email_id
--   (the Instantly /emails item id), so the entity UPSERTs and aggregates are safe
--   with no _run_id filter. ue_type = 2 for received replies (1 = sent, 3 = manual).
--
-- PARITY-FIRST: this is ADDITIVE. It does NOT replace raw_pipeline_reply_data yet.
--   v_reply_source_parity (below) is the gate: once email-id-level coverage and
--   per-campaign distinct-replier counts match the mirror, the canonical
--   v_campaign_metrics `pipe_replies` CTE can be repointed from
--   raw_pipeline_reply_data to this table and the mirror dropped.

CREATE TABLE IF NOT EXISTS raw_instantly_email (
  email_id            VARCHAR PRIMARY KEY,   -- Instantly /emails item id
  campaign_id         VARCHAR,
  workspace_id        VARCHAR NOT NULL,      -- from the key's workspace (organization_id)
  lead_email          VARCHAR,               -- the prospect (item.lead)
  from_address_email  VARCHAR,               -- sender of the received email (the prospect)
  eaccount            VARCHAR,               -- our inbox that received it
  subject             VARCHAR,
  reply_text          VARCHAR,               -- body html or text, whichever present
  step                INTEGER,
  ue_type             INTEGER,               -- 2 = received reply
  thread_id           VARCHAR,
  message_id          VARCHAR,
  reply_timestamp     TIMESTAMPTZ,           -- timestamp_email
  api_response_raw    VARCHAR,               -- JSON of the original item (drill-through)
  _loaded_at          TIMESTAMPTZ NOT NULL,
  _run_id             VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_raw_instantly_email_campaign
  ON raw_instantly_email (campaign_id);
CREATE INDEX IF NOT EXISTS idx_raw_instantly_email_ts
  ON raw_instantly_email (reply_timestamp);

-- =====================================================================
-- PARITY GATE — compare direct-Instantly replies vs the pipeline mirror.
-- Run this before repointing v_campaign_metrics / dropping the mirror.
-- Per campaign: distinct repliers from each source + the delta.
-- =====================================================================
CREATE OR REPLACE VIEW v_reply_source_parity AS
WITH direct AS (
  SELECT campaign_id,
         count(*)                              AS direct_reply_rows,
         count(DISTINCT lower(lead_email))      AS direct_unique_repliers,
         max(reply_timestamp)                   AS direct_max_ts
  FROM raw_instantly_email
  WHERE ue_type = 2
  GROUP BY campaign_id
),
mirror AS (
  SELECT campaign_id,
         count(*)                              AS mirror_reply_rows,
         count(DISTINCT lower(lead_email))      AS mirror_unique_repliers,
         max(reply_timestamp)                   AS mirror_max_ts
  FROM raw_pipeline_reply_data
  GROUP BY campaign_id
)
SELECT
  COALESCE(d.campaign_id, m.campaign_id)               AS campaign_id,
  d.direct_reply_rows,
  m.mirror_reply_rows,
  d.direct_unique_repliers,
  m.mirror_unique_repliers,
  COALESCE(d.direct_unique_repliers, 0)
    - COALESCE(m.mirror_unique_repliers, 0)            AS unique_replier_delta,
  d.direct_max_ts,
  m.mirror_max_ts
FROM direct d
FULL OUTER JOIN mirror m ON m.campaign_id = d.campaign_id;
