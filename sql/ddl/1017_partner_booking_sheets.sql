-- partner_booking_sheets.sql  [2026-06-25]  Pre-IPO partner booking sheets -> core.meeting.
-- deliverables/2026-06-25-partner-booking-sheets-ingest/. Applied via the moderator ship flow.
-- Standard SQL, idempotent (CREATE ... IF NOT EXISTS / CREATE OR REPLACE).
--
-- @gate: add
-- Depends on 64   (raw_sheets_funding_form_data — the shape this mirrors)
-- Depends on 107  (core.meeting WS5 columns offer/program/meeting_date/workspace_* the partner branch fills)
--
-- WHY ---------------------------------------------------------------------------------------------
-- Pre-IPO runs a SEPARATE booking infrastructure from Business Funding. The master Funding-Form sheet
-- (raw_sheets_funding_form_data -> core.meeting source='sheet') is Business-Funding-only — verified
-- 2026-06-25: core.meeting carried ZERO Pre-IPO meetings. Pre-IPO meetings are logged by partner
-- booking desks in their OWN Google Sheets. This stands up the raw mirror for the first two such desks:
--   * Summit Ventures        — SMS Pre-IPO desk (advisor Craig Diana); 89 rows, all channel=SMS.
--   * Collins Investment Ptrs — Pre-IPO desk (advisors Lawrence Michelson / Tsvi Bort); 344 rows,
--                               channel per-row from "Sending Account" (316 SMS / 26 Email / 2 WhatsApp),
--                               + accredited-investor qualification columns (income / net worth / invest).
-- Verified 100% net-new: 0 of (89 Summit + 338 Collins distinct) emails exist in core.meeting (any source).
-- offer='Pre-IPO' for ALL rows (partner-desk level; corroborated by core.v_channel_offer mapping the
-- Summit "Doctors" Sendivo brand funding4doctors_llc -> Pre-IPO, and Collins's explicit Pre-IPO campaigns
-- + investor-qual columns). The normalize/dedup/projection into core.meeting lives in entities/meeting.py
-- (a new source='sheet' partner branch, partner='Summit Ventures'/'Collins Investment Partners',
-- meeting_id namespaced 'summit:'/'collins:' so it never collides with the Funding-Form 'sheet:' rows).
--
-- The CONSUME load (raw_sheets_* fill) is registered in sources/sheets.SHEET_TABS; the PRODUCE step is
-- scripts/stage_partner_booking_sheets.py (Mac; google-sheets token), same split as the Funding-Form sync.

-- ---------------------------------------------------------------------------------------------------
-- 1. Raw mirrors. Same JSON-array shape as every other raw_sheets_* table (one row per sheet row,
--    cells in row_json). REFERENCE DATA — the canonical projection is core.meeting (entities/meeting.py).
-- ---------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.raw_sheets_summit_ventures_leads (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,   -- 0-based row position; row 0 is the header
    row_json    VARCHAR,                -- JSON array of the row's cell values (all text)
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

CREATE TABLE IF NOT EXISTS main.raw_sheets_collins_preipo_leads (
    _sheet_id   VARCHAR     NOT NULL,
    _tab        VARCHAR     NOT NULL,
    row_index   INTEGER     NOT NULL,   -- 0-based row position; row 0 is the header
    row_json    VARCHAR,                -- JSON array of the row's cell values (all text)
    _loaded_at  TIMESTAMPTZ NOT NULL,
    _run_id     VARCHAR
);

-- ---------------------------------------------------------------------------------------------------
-- 2. core.v_preipo_investor_qual — Collins's accredited-investor qualification fields, keyed by the
--    SAME meeting_id entities/meeting.py builds (collins:md5(email|meeting_date|campaign|booked|channel)),
--    so it joins 1:1 to core.meeting for Pre-IPO qualification reporting. A derived VIEW (no copy to keep
--    in sync) over the latest snapshot. Columns 11-14 of the Collins tab carry the qual data.
--    NB: meeting_date here = col A "Date" (the booked-on/business date) — MUST match meeting.py's key
--    construction exactly (channel derived identically) or the join silently misses; covered by the
--    post-apply QA in the deliverable.
-- ---------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_preipo_investor_qual AS
WITH latest AS (
  SELECT row_json FROM main.raw_sheets_collins_preipo_leads
  WHERE _run_id = (SELECT _run_id FROM main.raw_sheets_collins_preipo_leads ORDER BY _loaded_at DESC LIMIT 1)
    AND row_index > 0
),
parsed AS (
  SELECT
    NULLIF(lower(trim(json_extract_string(row_json,'$[3]'))),'')              AS lead_email,
    NULLIF(trim(json_extract_string(row_json,'$[7]')),'')                     AS campaign_name,
    NULLIF(trim(json_extract_string(row_json,'$[8]')),'')                     AS booked_raw,
    trim(json_extract_string(row_json,'$[6]'))                                AS sending_account,
    TRY_STRPTIME(trim(json_extract_string(row_json,'$[0]')),'%m/%d/%Y')::DATE AS meeting_date,
    NULLIF(trim(json_extract_string(row_json,'$[11]')),'')                    AS annual_income,
    NULLIF(trim(json_extract_string(row_json,'$[12]')),'')                    AS annual_income_basis,
    NULLIF(trim(json_extract_string(row_json,'$[13]')),'')                    AS net_worth,
    NULLIF(trim(json_extract_string(row_json,'$[14]')),'')                    AS how_much_to_invest
  FROM latest
)
SELECT
  'collins:' || md5(COALESCE(lead_email,'') || '|' || COALESCE(meeting_date::VARCHAR,'') || '|' ||
                    COALESCE(campaign_name,'') || '|' || COALESCE(booked_raw,'') || '|' ||
                    CASE WHEN lower(sending_account)='sms' THEN 'SMS'
                         WHEN lower(sending_account)='whatsapp' THEN 'WhatsApp'
                         WHEN sending_account='Email' OR sending_account LIKE '%@%' THEN 'Email'
                         ELSE '(unmapped)' END) AS meeting_id,
  lead_email, meeting_date, annual_income, annual_income_basis, net_worth, how_much_to_invest
FROM parsed
WHERE meeting_date IS NOT NULL
  AND (annual_income IS NOT NULL OR net_worth IS NOT NULL OR how_much_to_invest IS NOT NULL);
