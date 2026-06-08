-- Spec 16 (BI / Lead-Intent layer), WS-F — the canonical lead spine. Version 44.
--
-- core.lead = ONE canonical row per SIGNAL lead: a lead we have ANY signal on
-- (a reply, a call, a partner disposition, an opportunity, or a meeting). This is
-- NOT the ~27M scraped lead universe — it is only leads that produced a real signal.
--
-- IDENTITY (spec 16 §1 — DETERMINISTIC only, no fuzzy matching):
--   * keyed by EXACT email when an email exists (lower/trim);
--   * keyed by the E.164 PHONE for phone-only Sendivo leads (a call whose lead_email
--     is NULL). Email and phone are the only identity keys — no name/company fuzzing.
--   lead_key = md5( coalesce(lower(trim(email)), phone_e164) ) — a stable surrogate
--   so the same resolved identity always collapses to one row across re-runs and across
--   the multiple source signals it appears in.
--
-- resolution_confidence describes HOW the row was keyed AND whether scraped attrs resolved:
--   'email'     — keyed by email
--   'phone'     — keyed by phone-only (Sendivo, no email)
--   'unmatched' — keyed, but no scraped attrs found in the lead-DB mirror
--   'multi'     — reserved (an identity that the mirror maps to >1 scraped lead)
--   (entities/lead_spine.py sets 'email'/'phone' by key type today; 'unmatched'/'multi'
--    are populated once the lead-mirror join is wired — see the TODO in lead_spine.py.)
--
-- SCHEMA-NAME FLAG FOR PARENT (do not silently fix): core.meeting (sql/ddl/20_meeting.sql
-- + 26_meeting_partner_key.sql) has NO email/phone column — it is Slack-success-channel
-- derived and keyed by meeting_id, with partner/campaign_id/cm only. It therefore
-- contributes NO lead identity to this spine. The union below intentionally omits it. If a
-- future meeting source carries a lead email/phone, add it to the union in lead_spine.py.
--
-- Additive only. No ALTER/DROP/rename of any pre-existing table or view. Idempotent
-- (lead_spine.py does a DELETE+INSERT full rebuild).

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.lead (
    lead_key              VARCHAR PRIMARY KEY,  -- md5(coalesce(lower(email), phone_e164)) — stable surrogate
    email                 VARCHAR,              -- lower/trim'd resolved email; NULL for phone-only
    phone_e164            VARCHAR,              -- E.164 phone; carries identity when email is NULL
    first_name            VARCHAR,              -- scraped attr (lead-DB mirror); NULL until mirror wired
    company               VARCHAR,              -- scraped attr; NULL until mirror wired
    segment               VARCHAR,              -- scraped attr; NULL until mirror wired
    industry              VARCHAR,              -- scraped attr; NULL until mirror wired
    lead_source           VARCHAR,              -- scraped attr (e.g. 'MCA - Isaac'); NULL until mirror wired
    resolution_confidence VARCHAR,              -- 'email' | 'phone' | 'unmatched' | 'multi'
    first_seen_at         TIMESTAMPTZ,          -- when this identity first appeared in any source signal
    resolved_at           TIMESTAMPTZ           -- when this spine row was last (re)built
);

CREATE INDEX IF NOT EXISTS ix_core_lead_email ON core.lead (email);
CREATE INDEX IF NOT EXISTS ix_core_lead_phone ON core.lead (phone_e164);
