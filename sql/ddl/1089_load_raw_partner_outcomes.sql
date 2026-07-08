-- @gate: add
-- Intent: Load BOF partner outcomes data (GBC, GQ, BTC) into main.raw_partner_outcomes
-- Depends on: 1065

INSERT INTO main.raw_partner_outcomes
    (lead_email, partner_key, outcome, outcome_detail, meeting_date, funded_date, amount_funded, commission)
SELECT
    lead_email,
    partner_key,
    outcome,
    outcome_detail,
    TRY_CAST(meeting_date AS DATE),
    TRY_CAST(funded_date AS DATE),
    TRY_CAST(amount_funded AS NUMERIC),
    TRY_CAST(commission AS NUMERIC)
FROM read_csv_auto('C:/Users/User/Documents/PartnersAnalysis/raw_partner_outcomes_load.csv');
