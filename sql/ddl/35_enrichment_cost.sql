-- Phone-enrichment cost surface (warm-call pipeline).
-- Applied at schema version 35 by scripts/setup_db.py / orchestrator DDL applier.
--
-- WHY A DERIVED VIEW, NOT core.cost_ledger:
--   Per Sam (cost-seed v2, 2026-05-30) core.cost_ledger is DIRECT INFRASTRUCTURE
--   ONLY — the per-lead enrichment providers were explicitly dropped from it ("if we
--   track these we have to track the other 50 platforms too"). Phone-enrichment
--   spend is a measured per-lead consumption cost of the warm-call pipeline, so
--   it lives here in the derived layer instead. THIS is the canonical surface for
--   "what's our phone-enrichment spend" questions — do NOT add these vendors back
--   to cost_ledger.
--
-- SOURCE: comms.phone_enrichment (mirrored -> raw_comms_phone_enrichment) joined to
--   comms.enrichment_vendor_pricing (mirrored -> raw_comms_enrichment_vendor_pricing)
--   for the $/credit rate, and raw_comms_call_opportunity for the opportunity source
--   (instantly | sendivo). All three are full-refresh snapshots, so we pin each to
--   its latest _run_id (same pattern as core.domain's sweep dedup).
--
-- COST MODEL: per-provider $/credit rates are NOT inlined here (vendor identities +
--   rates are out-of-band, not committed to a public repo). They are the single
--   source of truth in comms.enrichment_vendor_pricing (mirrored at runtime), and
--   are additionally bootstrapped from an EXTERNAL, gitignored seed file
--   (seed_data/enrichment_vendor_pricing.csv) so a fresh clone can still dollarize
--   before the live mirror runs. SMS opps arrive WITH a phone -> zero enrichment cost.

CREATE SCHEMA IF NOT EXISTS derived;

-- Raw mirror target for comms.enrichment_vendor_pricing. Also declared in
-- 16_comms_mirror.sql for fresh installs; repeated here (IF NOT EXISTS) so DBs
-- that already applied v16 pick it up — the applier version-gates per file, so an
-- edit to 16 does not re-run on an existing warehouse. See entities/comms_mirror.py.
CREATE TABLE IF NOT EXISTS raw_comms_enrichment_vendor_pricing (
    provider                    VARCHAR,
    usd_per_credit              DOUBLE,
    plan_note                   VARCHAR,
    updated_at                  TIMESTAMPTZ,
    _loaded_at                  TIMESTAMPTZ NOT NULL,
    _run_id                     VARCHAR
);

-- Bootstrap the pricing table from the EXTERNAL, gitignored seed file so a fresh
-- clone (no live mirror yet) can still dollarize. Guarded so a missing seed file is
-- a no-op (the warehouse still builds); the live mirror's _run_id rows always win in
-- the views below (they pin to the latest _run_id, and seed rows carry _run_id=NULL).
-- Idempotent: clear prior seed (NULL _run_id) rows before re-seeding.
DELETE FROM raw_comms_enrichment_vendor_pricing WHERE _run_id IS NULL;
INSERT INTO raw_comms_enrichment_vendor_pricing
  (provider, usd_per_credit, plan_note, updated_at, _loaded_at, _run_id)
SELECT provider, usd_per_credit, plan_note, now(), now(), NULL
FROM read_csv_auto('seed_data/enrichment_vendor_pricing.csv', header=true, nullstr='')
WHERE (SELECT count(*) FROM glob('seed_data/enrichment_vendor_pricing.csv')) > 0;

-- ---------------------------------------------------------------------------
-- derived.enrichment_cost — one dollarized row per enrichment attempt.
-- Grain: enrichment attempt. Roll up by provider / day / opportunity_source.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW derived.enrichment_cost AS
WITH pe AS (
  SELECT * FROM raw_comms_phone_enrichment
  WHERE _run_id = (
    SELECT _run_id FROM raw_comms_phone_enrichment ORDER BY _loaded_at DESC LIMIT 1
  )
),
price AS (
  SELECT * FROM raw_comms_enrichment_vendor_pricing
  WHERE _run_id = (
    SELECT _run_id FROM raw_comms_enrichment_vendor_pricing ORDER BY _loaded_at DESC LIMIT 1
  )
),
opp AS (
  SELECT * FROM raw_comms_call_opportunity
  WHERE _run_id = (
    SELECT _run_id FROM raw_comms_call_opportunity ORDER BY _loaded_at DESC LIMIT 1
  )
)
SELECT
  pe.id                                              AS enrichment_id,
  pe.opportunity_id,
  o.source                                           AS opportunity_source,   -- 'instantly' | 'sendivo'
  pe.provider,
  (pe.mobile_e164 IS NOT NULL)                       AS hit,
  pe.credits_spent,
  COALESCE(price.usd_per_credit, 0.0)                AS usd_per_credit,
  ROUND(pe.credits_spent * COALESCE(price.usd_per_credit, 0.0), 4) AS cost_usd,
  CAST(pe.attempted_at AS DATE)                      AS attempt_date,
  pe.attempted_at
FROM pe
LEFT JOIN price ON price.provider = pe.provider
LEFT JOIN opp o ON o.id = pe.opportunity_id;

-- ---------------------------------------------------------------------------
-- derived.enrichment_cost_daily — provider x day x source spend rollup.
-- The convenience surface for time-series / per-vendor spend questions.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW derived.enrichment_cost_daily AS
SELECT
  attempt_date,
  provider,
  opportunity_source,
  count(*)                          AS attempts,
  count(*) FILTER (WHERE hit)       AS hits,
  sum(credits_spent)                AS credits,
  ROUND(sum(cost_usd), 4)           AS cost_usd
FROM derived.enrichment_cost
GROUP BY 1, 2, 3;

-- ---------------------------------------------------------------------------
-- derived.enrichment_cost_per_lead — headline cost-per-lead by opportunity source.
-- "in_close" lead count comes from the latest call_opportunity snapshot (close_lead_id
-- populated, non-duplicate). cost_per_lead_in_close = total enrichment cost / leads
-- actually delivered to Close. SMS opps = zero cost (arrive with phone).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW derived.enrichment_cost_per_lead AS
WITH opp AS (
  SELECT * FROM raw_comms_call_opportunity
  WHERE _run_id = (
    SELECT _run_id FROM raw_comms_call_opportunity ORDER BY _loaded_at DESC LIMIT 1
  )
),
spend AS (
  SELECT opportunity_source, ROUND(sum(cost_usd), 2) AS total_cost_usd
  FROM derived.enrichment_cost
  GROUP BY 1
),
leads AS (
  SELECT source AS opportunity_source,
         count(*) FILTER (WHERE status <> 'duplicate')                          AS opps,
         count(*) FILTER (WHERE close_lead_id IS NOT NULL AND status <> 'duplicate') AS in_close
  FROM opp
  GROUP BY 1
)
SELECT
  l.opportunity_source,
  l.opps,
  l.in_close,
  COALESCE(s.total_cost_usd, 0.0)                                       AS total_cost_usd,
  ROUND(COALESCE(s.total_cost_usd, 0.0) / NULLIF(l.in_close, 0), 4)     AS cost_per_lead_in_close
FROM leads l
LEFT JOIN spend s ON s.opportunity_source = l.opportunity_source;
