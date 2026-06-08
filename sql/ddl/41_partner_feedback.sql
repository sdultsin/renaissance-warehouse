-- Spec 16 (BI / Lead-Intent layer) — partner disposition feedback. Version 41.
--
-- Funding-partner sales-rep feedback on leads we booked/delivered: what happened
-- after hand-off (No show / DNQ / Not interested / LIVE OPPORTUNITY / ...). This is
-- the FIRST object of the BOF/BI layer and has zero dependencies.
--
-- Source = a manual xlsx drop (no live Google Sheet — Sam, 2026-06-08), normalized to
-- seed_data/partner-feedback/partner-disposition-feedback__<period>__lead_detail.csv.
-- ⚠ PII (real lead emails) — the seed dir + *.xlsx are git-ignored; never push.
--
-- raw_partner_lead_feedback  — one row per (lead_email, source_period), typed (not
--                              opaque row_json) so disposition/rep are directly queryable.
-- core.lead_disposition      — canonical: latest disposition per lead, with the raw
--                              disposition string mapped to a tidy disposition_class.
-- v_disposition_funnel       — distribution by disposition_class (the first insight).
--
-- Additive only. No ALTER/DROP of existing tables.

CREATE SCHEMA IF NOT EXISTS core;

-- ── RAW ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_partner_lead_feedback (
    lead_email     VARCHAR,        -- lower-cased; the email join key (may be NULL for phone-only)
    business_name  VARCHAR,        -- INFERRED from email handle / rep notes — pair with id_confidence
    industry       VARCHAR,        -- INFERRED
    id_confidence  VARCHAR,        -- High | Medium | Low (confidence in the business ID)
    rep            VARCHAR,        -- funding-partner sales rep who worked the lead
    disposition    VARCHAR,        -- raw disposition string from the partner sheet
    rep_notes      VARCHAR,        -- free text — gold for intent
    source_period  VARCHAR,        -- e.g. '2026-06-MTD'
    _loaded_at     TIMESTAMPTZ,
    _run_id        VARCHAR,
    PRIMARY KEY (lead_email, source_period)
);

-- ── CANONICAL ────────────────────────────────────────────────────────────────
-- disposition_class enum (tidy roll-up of the raw partner strings):
--   live        — LIVE OPPORTUNITY, Pipeline (soft)
--   no_show     — No show
--   disqualified— DNQ, Not interested
--   reschedule  — Reschedule, Rebooked
--   bad_data    — Bad contact info, Data issue, Unreachable
--   duplicate   — Already in system, Disputed booking
--   cancelled   — Cancelled
--   unknown     — No note / unmapped
CREATE TABLE IF NOT EXISTS core.lead_disposition (
    lead_email        VARCHAR,
    source_period     VARCHAR,
    disposition       VARCHAR,     -- raw string, verbatim
    disposition_class VARCHAR,     -- mapped enum above
    rep               VARCHAR,
    business_name     VARCHAR,
    industry          VARCHAR,
    id_confidence     VARCHAR,
    rep_notes         VARCHAR,
    resolved_at       TIMESTAMPTZ,
    PRIMARY KEY (lead_email, source_period)
);
