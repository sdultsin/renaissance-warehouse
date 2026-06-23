-- @gate: add
-- Depends on 113
-- 115_sms_meeting_phone_enriched.sql — enriched SMS-meeting lead email->mobile phones.
-- Chat: classify-all-time-positives (2026-06-23). Full writeup:
--   deliverables/2026-06-22-classify-all-time-positives/REPORT.md + SPEC-sms-phone-enrichment.md
--
-- WHAT: one row per SMS-meeting lead whose mobile was resolved via the email->phone waterfall
--   (LeadMagic, pay-per-valid). Used to close the SMS leg of the meeting->lead join: core.meeting
--   has no phone column, so SMS positives (phone-keyed) can't otherwise be matched to booked meetings.
--   v_positive_lead_alltime (DDL 116) flags SMS positives whose phone ∈ (these ∪ comms-resolved).
--
-- HOW IT PERSISTS (identical pattern to DDL 93 / reply_is_positive_strict): this DDL declares the
--   shape idempotently; entities/sms_meeting_phone_enriched.py re-materializes rows from the
--   out-of-band seed JSONL every nightly (CREATE OR REPLACE, committed==attempted assertion).
--   Seed = seed_data/sms-meeting-phone/enriched.jsonl (gitignored; box/local only). PII (email+phone)
--   stays self-hosted on the droplet. Additive; NOT in required_schema, so it can never block a promote.
CREATE SCHEMA IF NOT EXISTS derived;
CREATE TABLE IF NOT EXISTS derived.sms_meeting_phone_enriched (
    lead_email   VARCHAR,
    phone_e164   VARCHAR,
    source       VARCHAR,
    loaded_at    TIMESTAMPTZ
);
