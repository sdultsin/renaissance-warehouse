-- Cost seed v2 (per Sam 2026-05-30): replaces v1 seed (sql/ddl/14_cost_seed.sql).
--
-- Changes from v1:
--   1. DROP all software / service subscription rows (droplet, supabase, blitzapi, cloudflare,
--      anthropic, leadmagic, findymail, prospeo). Per Sam: "I don't want you adding anything
--      in here that is non-direct infrastructure" — if we track these we have to track the
--      other 50 platforms too. Out of scope for cost ledger.
--   2. ADD ActiveCampaign pilot ($1k/mo @ 1M) + scale ($15k/mo @ 10M) per Sam estimate.
--   3. ADD Warmly full-fleet row ($13k/mo) so kill/scale criteria can be modeled.
--   4. ADD the May 19-20 .co bulk-purchase as the canonical batch-cost example
--      (14,978 domains across Dynadot #9-15 per Domain Tech Sheet; Sam recalls $1.80/domain).

-- Wipe all v1 reference_rate + manual rows so re-seeding picks up the new shape.
-- Anything we've added via Stripe/invoice ingest (Phase 2 later) is preserved by source filter.
DELETE FROM core.cost_ledger WHERE source IN ('reference_rate', 'manual') AND _run_id IS NULL;

INSERT INTO core.cost_ledger
  (cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start, period_end,
   amortize_method, attribution_dim, attribution_id, source, source_ref, notes, _loaded_at, _run_id)
VALUES
  -- ==== DOMAIN RENEWAL RATES (per-unit reference) ====
  ('reference:dynadot:co_renewal:2026-05-30',
   'dynadot', '.co_renewal', 'domain', 1, 9.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   '2026-05-30 Ido call', '.co renewal rate per Ido', now(), NULL),

  ('reference:dynadot:info_renewal:2026-05-30',
   'dynadot', '.info_renewal', 'domain', 1, 9.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   '2026-05-30 Ido call', '.info renewal rate per Ido', now(), NULL),

  ('reference:porkbun:com_renewal:2026-05-30',
   'porkbun', '.com_renewal', 'domain', 1, 12.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   'industry typical', '.com typical renewal — NEEDS ACTUAL Porkbun invoice', now(), NULL),

  -- ==== DOMAIN BULK PURCHASE (canonical batch-cost example) ====
  ('manual:dynadot:co_bulk_acquisition:2026-05-19',
   'dynadot', '.co_acquisition_bulk_sale', 'domain', 14978, 26960.40,
   DATE '2026-05-19', DATE '2026-05-20',
   'one_time', 'batch', 'dynadot_2026-05-19_co_sale_14978', 'manual',
   '2026-05-30 transcript + Domain Tech Sheet Domains(Table) tab',
   'NEEDS SAM CONFIRMATION on $1.80/domain price; 14,978 total across Dynadot #9-15. Individual domains tag to this batch via core.domain.acquisition_batch when Phase 3 ships.',
   now(), NULL),

  -- ==== INBOX RATES (per-unit reference; multiply by inbox count for total burn) ====
  ('reference:otd:inbox_monthly:2026-05-30',
   'otd', 'inbox_monthly', 'inbox', 1, 1.38, DATE '2026-05-30', DATE '2027-12-31',
   'monthly', 'channel', 'otd', 'reference_rate',
   'infra-plan-v11',
   'Derived from $548k/mo / 396,800 inboxes at TARGET scale — NEEDS ACTUAL CURRENT INVOICE',
   now(), NULL),

  ('reference:google:inbox_monthly:2026-05-30',
   'google', 'inbox_monthly', 'inbox', 1, 2.69, DATE '2026-05-30', DATE '2027-12-31',
   'monthly', 'channel', 'google', 'reference_rate',
   'infra-plan-v11',
   'Derived from $305k/mo / 113,600 inboxes at TARGET scale — NEEDS ACTUAL CURRENT INVOICE',
   now(), NULL),

  -- ==== VENDOR PILOT COSTS ====
  ('reference:lucas:inbox_monthly:2026-05-30',
   'lucas', 'inbox_monthly', 'inbox', 1, 0.05, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'outlook_mailin_lucas', 'reference_rate',
   'infra-plan-v11 + 2026-05-30 calc',
   '$5k ongoing / ~99k inboxes (1k domains × 99 acct) — NEEDS LUCAS QUOTE',
   now(), NULL),

  ('reference:tomer:inbox_monthly:2026-05-30',
   'tomer', 'inbox_monthly', 'inbox', 1, 0.065, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'outlook_mailin_tomer', 'reference_rate',
   'infra-plan-v11 + 2026-05-30 calc',
   '$6.4k ongoing / ~99k inboxes (1k domains × 99 acct) — NEEDS TOMER QUOTE',
   now(), NULL),

  ('reference:tucows:mailbox_monthly:2026-05-30',
   'tucows', 'mailbox_monthly', 'inbox', 1, 0.75, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'tucows_whitelabel', 'reference_rate',
   '2026-05-30 Sam calc',
   '$0.75/mailbox/mo — NEEDS TUCOWS QUOTE (Sam: "OTD parity")',
   now(), NULL),

  ('reference:maxify:monthly:2026-05-30',
   'maxify', 'service_monthly', 'service', 1, 4500.0, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'ac_maxify', 'reference_rate',
   'infra-plan-v11',
   '$4.5k/mo for Daniyal/AC warmup service — does NOT include AC platform fees (see activecampaign rows)',
   now(), NULL),

  ('reference:folderly:m1:2026-06-01',
   'folderly', 'platform_m1', 'platform', 1, 14000.0, DATE '2026-06-01', DATE '2026-06-30',
   'monthly', 'channel', 'folderly', 'reference_rate',
   'infra-plan-v11 + Folderly PDF',
   'Month 1 pilot cost (120K/day) — confirm with Folderly closing call',
   now(), NULL),

  ('reference:folderly:target:2026-10-01',
   'folderly', 'platform_target', 'platform', 1, 52000.0, DATE '2026-10-01', DATE '2027-12-31',
   'monthly', 'channel', 'folderly', 'reference_rate',
   'infra-plan-v11',
   'Target month cost at 10M/mo sends — needs Folderly tier confirmation',
   now(), NULL),

  ('reference:warmly:pilot:2026-06-01',
   'warmly', 'pilot_monthly', 'service', 1, 300.0, DATE '2026-06-01', DATE '2026-08-31',
   'monthly', 'channel', 'warmly', 'reference_rate',
   'infra-plan-v11',
   'Warmly pilot — 1k inboxes',
   now(), NULL),

  ('reference:warmly:full:2026-09-01',
   'warmly', 'full_fleet_monthly', 'service', 1, 13000.0, DATE '2026-09-01', DATE '2027-12-31',
   'monthly', 'channel', 'warmly', 'reference_rate',
   'infra-plan-v11',
   'Warmly full fleet (post-pilot if green-lit; ≥20%% KPI lift required per kill criteria)',
   now(), NULL),

  -- ==== ACTIVECAMPAIGN PLATFORM (per Sam 2026-05-30 estimate) ====
  ('reference:activecampaign:pilot_1m:2026-06-01',
   'activecampaign', 'platform_pilot', 'platform', 1, 1000.0, DATE '2026-06-01', DATE '2026-08-31',
   'monthly', 'channel', 'ac_maxify', 'reference_rate',
   '2026-05-30 Sam estimate',
   '~$1k/mo at 1M emails/mo (pilot) — NEEDS ACTUAL AC INVOICE',
   now(), NULL),

  ('reference:activecampaign:scale_10m:2026-09-01',
   'activecampaign', 'platform_scale', 'platform', 1, 15000.0, DATE '2026-09-01', DATE '2027-12-31',
   'monthly', 'channel', 'ac_maxify', 'reference_rate',
   '2026-05-30 Sam estimate',
   '~$15k/mo at 10M emails/mo (scale tier) — NEEDS ACTUAL AC PRICING',
   now(), NULL)
ON CONFLICT (cost_id) DO NOTHING;
