-- Financial data architecture (per specs/13-financial-data-architecture.md).
-- v1 ships: cost_ledger table + cost_lead_acquisition_usd_estimated column on core.campaign.
-- Other inline cost columns land with their Phase 3 entities (sending_account, domain, meeting).

-- =====================================================================
-- core.cost_ledger — the authoritative fact table for every known cost.
-- Mix of actuals (source IN 'stripe' | 'invoice_csv' | 'manual') and
-- estimates (source IN 'estimated' | 'reference_rate'). Same shape for both;
-- the `source` column tells you which.
-- =====================================================================

CREATE TABLE IF NOT EXISTS core.cost_ledger (
  cost_id             VARCHAR PRIMARY KEY,    -- synthetic: vendor:sku:period_start[:attribution_id]
  vendor              VARCHAR NOT NULL,       -- 'instantly' | 'vendorA' | 'vendorE' | 'vendorI' | 'vendorH' | 'vendorC' | 'vendorD' | 'vendorF' | 'anthropic' | 'vendorK' | 'vendorL' | 'vendorM' | 'vendorN' | 'vendorO' | 'mv' | 'supabase' | 'vercel' | 'cloudflare' | 'do_droplet' | 'otd' | 'google' | etc.
  sku                 VARCHAR,                -- vendor item identifier: 'workspace_subscription', '.co_domain_renewal', 'inbox_monthly', 'api_credits'
  cost_unit           VARCHAR NOT NULL,       -- 'workspace' | 'domain' | 'inbox' | 'send' | 'enrichment_lookup' | 'platform' | 'service' | 'one_time'
  unit_count          INTEGER,                -- units this row covers; NULL for fixed-fee
  total_usd           DOUBLE NOT NULL,
  period_start        DATE NOT NULL,
  period_end          DATE NOT NULL,          -- same as period_start for one_time
  amortize_method     VARCHAR,                -- 'monthly' | 'daily' | 'one_time' | 'annual_spread'
  attribution_dim     VARCHAR NOT NULL,       -- 'global' | 'workspace' | 'offer' | 'infra' | 'channel' | 'domain' | 'inbox'
  attribution_id      VARCHAR,                -- workspace_id / offer name / 'OTD' / domain / inbox email; NULL for global
  source              VARCHAR NOT NULL,       -- 'stripe' | 'invoice_csv' | 'manual' | 'estimated' | 'reference_rate'
  source_ref          VARCHAR,                -- Stripe invoice ID, CSV filename, Slack message link, doc path
  notes               VARCHAR,
  _loaded_at          TIMESTAMPTZ NOT NULL,
  _run_id             VARCHAR
);

-- =====================================================================
-- Add cost projection column to core.campaign (the only canonical entity
-- that exists today; the others — sending_account, domain, meeting — get
-- their cost columns when their Phase 3 DDL ships).
-- =====================================================================

ALTER TABLE core.campaign
  ADD COLUMN IF NOT EXISTS cost_lead_acquisition_usd_estimated DOUBLE;
