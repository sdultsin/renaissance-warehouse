-- Spec 16 (BI / Lead-Intent layer), WS-G — core.conversion_event. Version 45.
--
-- ONE unifying row per appointment / booking, across every conversion agent and channel.
-- This is the cross-channel "intent conversion" fact: the moment a lead became a meeting /
-- appointment / application, regardless of who/what produced it (IM closer, warm caller,
-- or — later — the SMS-AIM bot).
--
-- ⚠ EXTENSIBILITY IS A DoD ITEM. The agent / channel / type dimensions are FREE-TEXT
-- VARCHAR (NOT a hard enum / CHECK), so a new conversion agent (e.g. 'sms_aim_v1',
-- 'sms_aim_v2') slots in with NO DDL change — the feeder just inserts the new string.
--   source_channel    : cold_email | sms | wa | …            (how the lead was reached)
--   conversion_agent  : im | warm_caller | sms_aim_v1 | …    (who/what set the appointment)
--   conversion_type   : meeting_booked | appointment_set | application_submitted | …
--
-- Feeders (rebuilt DELETE+INSERT, idempotent — see entities/conversion_event.py):
--   core.meeting  → conversion_agent='im',          conversion_type='meeting_booked'
--   core.call ⋈ core.call_outcome (answered_appt_set)
--                 → conversion_agent='warm_caller',  conversion_type='appointment_set'
--
-- lead_key is the WS-F lead-spine key (core.lead). The lead spine is a SIBLING workstream;
-- if core.lead is absent at run time, lead_key is left NULL and the row still carries
-- lead_email / phone_e164 so the join is a pure backfill (no schema change).
--
-- Additive only. CREATE IF NOT EXISTS. No ALTER/DROP/rename of any pre-existing object.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.conversion_event (
    event_id          VARCHAR PRIMARY KEY,  -- deterministic hash of (agent, source feeder pk)
    lead_key          VARCHAR,              -- WS-F core.lead key; NULL until lead spine exists
    lead_email        VARCHAR,              -- lower/trim; NULL for phone-only (Sendivo) leads
    phone_e164        VARCHAR,              -- the lead phone; NULL for email-only IM meetings
    source_channel    VARCHAR,              -- FREE-TEXT dim: cold_email | sms | wa | …
    conversion_agent  VARCHAR,              -- FREE-TEXT dim: im | warm_caller | sms_aim_v1 | …
    conversion_type   VARCHAR,              -- FREE-TEXT dim: meeting_booked | appointment_set | …
    occurred_at       TIMESTAMPTZ,          -- when the conversion happened (meeting/appt time)
    campaign_id       VARCHAR,              -- nullable; from the feeder's campaign attribution
    warm_caller_id    VARCHAR,              -- nullable; set only for warm_caller events
    resolved_at       TIMESTAMPTZ           -- when this canonical row was last (re)built
);

-- Common access paths: per-agent / per-channel / time rollups.
CREATE INDEX IF NOT EXISTS ix_core_conv_event_agent     ON core.conversion_event (conversion_agent);
CREATE INDEX IF NOT EXISTS ix_core_conv_event_channel   ON core.conversion_event (source_channel);
CREATE INDEX IF NOT EXISTS ix_core_conv_event_occurred  ON core.conversion_event (occurred_at);
CREATE INDEX IF NOT EXISTS ix_core_conv_event_campaign  ON core.conversion_event (campaign_id);
