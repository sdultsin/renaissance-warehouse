-- core.iam_response_time: one row per prospect reply / IAM response pair.
-- Backfilled from raw_instantly_email (thread_id) + raw_instantly_sent_email.
-- Pipeline-source replies without thread_ids get iam_responded_at=NULL.
CREATE TABLE IF NOT EXISTS core.iam_response_time (
  id                    TEXT PRIMARY KEY,
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
CREATE INDEX IF NOT EXISTS idx_irt_email_id ON core.iam_response_time (email_id);
CREATE INDEX IF NOT EXISTS idx_irt_lead_email ON core.iam_response_time (lead_email);
CREATE INDEX IF NOT EXISTS idx_irt_campaign ON core.iam_response_time (campaign_id);
CREATE INDEX IF NOT EXISTS idx_irt_responded_at ON core.iam_response_time (iam_responded_at);
