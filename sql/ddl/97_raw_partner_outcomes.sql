-- @gate: add
-- Intent: BOF partner outcome table — unified outcomes for GBC, GoQualifi, and BTC
-- Depends on: none

CREATE TABLE IF NOT EXISTS main.raw_partner_outcomes (
    lead_email        VARCHAR,
    partner_key       VARCHAR,
    outcome           VARCHAR,
    outcome_detail    VARCHAR,
    meeting_date      DATE,
    funded_date       DATE,
    amount_funded     NUMERIC,
    commission        NUMERIC,
    _loaded_at        TIMESTAMP WITH TIME ZONE DEFAULT now()
);
