-- 64_funding_form.sql  [2026-06-13]  WS-E meetings re-platform onto the Funding-Form sheet.
-- handoffs/2026-06-13-warehouse-audit-residual.md item 1. Applied via apply_ddl_file(version=64).
-- Standard SQL, idempotent (migration-agnostic).
--
-- The Funding-Form Google Sheet (tab 'Data') becomes the source of truth for core.meeting from
-- 2026-06-01 onward: it carries an explicit Channel (Email/SMS/WhatsApp/Call/LinkedIn), Campaign
-- Manager, Campaign Name and lead Email per booking, so meetings attribute DIRECTLY (no fuzzy
-- keyword splitting) and link to leads by email. Pre-Jun-1 Slack-sourced rows are left untouched
-- (the sheet may not be accurate pre-June-1).
--
-- This DDL only stands up the storage + the two new core.meeting columns. The cutover logic lives
-- in entities/meeting.py; the load in entities/sheets_mirror.py (SHEET_TABS registration).

-- ---------------------------------------------------------------------------
-- Raw mirror of the Funding-Form 'Data' tab. Same JSON-array shape as every
-- other raw_sheets_* table (see sql/ddl/17_sheets.sql): one row per sheet row,
-- cells stored as a JSON array string in row_json. REFERENCE DATA — but unlike
-- the other sheets, THIS one is the canonical meetings source >= 2026-06-01
-- (there is no competing Instantly meetings feed; Slack was the prior source).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.raw_sheets_funding_form_data (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,   -- 0-based row position; row 0 is the header
    row_json    VARCHAR,                -- JSON array of the row's cell values (all text)
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- ---------------------------------------------------------------------------
-- core.meeting: two new columns the sheet enables and the prior Slack source could not carry.
--   channel    — explicit booking channel from the sheet (Email/SMS/WhatsApp/Call/LinkedIn).
--                Lets the email funnel count ONLY channel='Email' instead of fuzzy keyword
--                splitting on raw_text (the P2 over-count root cause). NULL for legacy Slack rows.
--   lead_email — the lead the meeting belongs to (lower-cased). Enables lead-grain funnel joins
--                (derived.v_funnel_detail) that the Slack source could never support. NULL for legacy.
-- ---------------------------------------------------------------------------
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS channel    VARCHAR;
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS lead_email VARCHAR;

CREATE INDEX IF NOT EXISTS ix_core_meeting_channel    ON core.meeting (channel);
CREATE INDEX IF NOT EXISTS ix_core_meeting_lead_email ON core.meeting (lead_email);

-- ---------------------------------------------------------------------------
-- Campaign-name normalizer (the "looser" matcher, WS-E analysis). Beyond the
-- ON/OFF/OLD leading-status prefixes the prior Slack matcher stripped, this also
-- strips the trailing variants that block direct attribution: " (copy)", a
-- trailing " - (Name)" CM/copy suffix, and a trailing " RV". Empirically lifts
-- sheet email-meeting attribution 86.2% -> 97.5% (verified 2026-06-13 against the
-- 2,496-campaign universe). Apply to BOTH sides of the match (sheet Campaign Name
-- and raw_pipeline_campaigns.name) so the join is symmetric. Pure scalar / RE2.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MACRO main.norm_campaign_name(x) AS (
  trim(
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(x)), '\s+', ' ', 'g'),  -- collapse whitespace
              '^(on|off|old)\b[\s\-:_|]*', ''),                 -- strip ON/OFF/OLD prefix
            '\s*\(copy\)\s*$', ''),                             -- trailing (copy)
          '\s*-\s*\([^)]*\)\s*$', ''),                          -- trailing " - (Name)"
        '\s*\brv\b\s*$', ''),                                   -- trailing RV
      '\s*\(copy\)\s*$', '')                                    -- second (copy) after RV
  )
);
