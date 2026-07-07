-- @gate: add
-- Depends on 00
-- core.account_first_cold_send: per-inbox FIRST REAL campaign send (= go-live) = MIN(sent timestamp) across
-- the OLD (retired, frozen <=2026-06-23) `main.raw_pipeline_conversation_messages` + the NEW (fresh, daily)
-- `main.raw_instantly_email_message`, WHERE ue_type=1 ("sent from campaign" / cold). This is the REAL go-live
-- moment, unlike account_label.cold_start / v_inbox_overview.go_live which count WARM-UP sends (warm-up traffic
-- inflates actual_sends). Feeds v_inbox_overview.go_live (gated to Active-tagged inboxes only). Full-rebuilt
-- nightly by entities/account_first_cold_send.py; garbage-date floor 2025-01-01 (the old log has bogus 2001 ts).
-- Built 2026-07-07 (MilkBox go-live investigation). No PK (plain rebuild).
CREATE TABLE IF NOT EXISTS core.account_first_cold_send (
    email              VARCHAR NOT NULL,
    first_cold_send_at TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ,
    _run_id            VARCHAR
);
