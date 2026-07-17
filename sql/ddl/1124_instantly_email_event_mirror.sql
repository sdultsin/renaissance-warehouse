-- @gate: add
-- Intent: mirror the NEW comms-hub webhook-capture table comms.instantly_email_event
--         (comms mig 045, 2026-07-17) into the nightly comms mirror as
--         raw_comms_instantly_email_event. That table is the real-time landing zone for
--         Instantly email-BODY webhooks (email_sent = OUTBOUND body in email_html;
--         reply_received = INBOUND body in reply_text/html) — the push feed that replaces
--         the rate-limited per-lead /emails?lead= drain for keeping outbound thread bodies
--         current. This raw_comms_ table is the warehouse-native FORENSIC snapshot of that
--         feed; entities/instantly_email_webhook_atom.py separately drains it into the
--         canonical thread atom raw_instantly_email_message (collapsing on message_id ==
--         email_id, which VERIFIED 2026-07-17 equals the /emails item id = the atom PK).
-- Depends on: 47
--
-- ADDITIVE ONLY: this file ADDS one raw_comms_* table. Loaded by entities/comms_mirror.py
-- (same postgres_scanner full-refresh REPLACE pattern as 16/47/56/1092/1099):
--   * the raw_* table holds exactly ONE full snapshot (REPLACE-style, warehouse-flags#12) —
--     DELETE all + INSERT fresh, atomic, idempotent. (The SOURCE table is append/upsert-only
--     keyed by email_id, so the snapshot only ever grows.)
--   * _loaded_at = ingestion wall-clock (NOT NULL); _run_id = orchestrator run.
--   * raw_json is jsonb source → CAST to VARCHAR in the mirror SELECT (see comms_mirror
--     _CAST_TO_VARCHAR). No enum/array columns.
--
-- Column names/types match the live source (comms mig 045; verified against the applied
-- table 2026-07-17: bigint identity id; text email_id/event_type/workspace/organization_id/
-- campaign_id/campaign_name/lead_email/eaccount/direction; integer ue_type/step/variant;
-- boolean is_first; text subject/body_text/body_html; timestamptz message_at/received_at;
-- jsonb raw_json).
--
-- ue_type semantics: 1 sequence cold send · 2 prospect reply · 3 manual/AIM reply (derived
-- by the worker — email_sent WITH a step=1, WITHOUT a step=3, reply_received=2; re-derivable
-- from raw_json). workspace = canonical warehouse slug (from the webhook ?ws= param), NOT the
-- Instantly org UUID (that is organization_id).

CREATE TABLE IF NOT EXISTS raw_comms_instantly_email_event (
    id                BIGINT,
    email_id          VARCHAR,      -- physical email id == /emails item id == atom message_id
    event_type        VARCHAR,      -- 'email_sent' | 'reply_received'
    workspace         VARCHAR,      -- canonical warehouse slug (e.g. 'prospects-power')
    organization_id   VARCHAR,      -- Instantly org UUID (provenance)
    campaign_id       VARCHAR,
    campaign_name     VARCHAR,
    lead_email        VARCHAR,      -- lower(trim(email))
    eaccount          VARCHAR,      -- our sending/receiving mailbox
    direction         VARCHAR,      -- 'outbound' | 'inbound'
    ue_type           INTEGER,      -- 1 send · 2 reply · 3 our reply
    step              INTEGER,      -- webhook sequence step index (nullable)
    variant           INTEGER,      -- webhook variant index (nullable)
    is_first          BOOLEAN,      -- webhook is_first flag (nullable)
    subject           VARCHAR,
    body_text         VARCHAR,      -- payload text (or html-derived by the worker)
    body_html         VARCHAR,      -- raw html body
    message_at        TIMESTAMPTZ,  -- payload timestamp/timestamp_email
    raw_json          VARCHAR,      -- full webhook payload (jsonb→varchar); re-derivation
    received_at       TIMESTAMPTZ,  -- when the worker captured it
    _loaded_at        TIMESTAMPTZ NOT NULL,
    _run_id           VARCHAR
);
