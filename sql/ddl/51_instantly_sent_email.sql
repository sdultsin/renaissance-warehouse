-- raw_instantly_sent_email: IAM manual outbound replies (ue_type=3 from Instantly sent emails).
-- Excludes automated campaign sequence sends (ue_type=1 with step field).
-- Used by core.iam_response_time to compute response latency per prospect reply.
CREATE TABLE IF NOT EXISTS raw_instantly_sent_email (
  email_id           TEXT PRIMARY KEY,
  campaign_id        TEXT,
  workspace_id       TEXT,
  lead_email         TEXT,
  from_address_email TEXT,
  eaccount           TEXT,
  subject            TEXT,
  thread_id          TEXT,
  message_id         TEXT,
  sent_timestamp     TIMESTAMPTZ,
  i_status           INTEGER,
  api_response_raw   TEXT,
  _loaded_at         TIMESTAMPTZ,
  _run_id            TEXT
);
CREATE INDEX IF NOT EXISTS idx_ise_thread_id ON raw_instantly_sent_email (thread_id);
CREATE INDEX IF NOT EXISTS idx_ise_lead_email ON raw_instantly_sent_email (lead_email);
CREATE INDEX IF NOT EXISTS idx_ise_sent_ts ON raw_instantly_sent_email (sent_timestamp);
