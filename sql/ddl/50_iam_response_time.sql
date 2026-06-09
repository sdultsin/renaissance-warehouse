-- core.iam_response_time: one row per prospect reply / IM (Inbox Manager) response pair.
-- Source: main.raw_pipeline_conversation_messages (full thread backfill, both directions).
--   ue_type=2 inbound prospect reply -> next ue_type=3 outbound_manual IM reply in-thread.
-- Replies with no later IM response get iam_responded_at=NULL (response_bucket='no_response').
--
-- NOTE: indexes are created AFTER population (see the INDEXES marker below). Inserting ~800k
-- rows into a table with pre-existing ART indexes throws DuckDB "Invalid argument"; the
-- entity drops+recreates the table, bulk-inserts, then bulk-builds indexes below.
CREATE TABLE IF NOT EXISTS core.iam_response_time (
  id                    TEXT NOT NULL,
  email_id              TEXT NOT NULL,
  campaign_id           TEXT,
  lead_email            TEXT,
  thread_id             TEXT,
  thread_reply_number   INTEGER NOT NULL,
  prospect_replied_at   TIMESTAMPTZ,
  iam_responded_at      TIMESTAMPTZ,
  response_minutes      INTEGER,
  response_bucket       TEXT,
  synced_at             TIMESTAMPTZ DEFAULT now()
);
-- @@INDEXES@@
CREATE INDEX IF NOT EXISTS idx_irt_id ON core.iam_response_time (id);
CREATE INDEX IF NOT EXISTS idx_irt_email_id ON core.iam_response_time (email_id);
CREATE INDEX IF NOT EXISTS idx_irt_lead_email ON core.iam_response_time (lead_email);
CREATE INDEX IF NOT EXISTS idx_irt_campaign ON core.iam_response_time (campaign_id);
CREATE INDEX IF NOT EXISTS idx_irt_responded_at ON core.iam_response_time (iam_responded_at);
