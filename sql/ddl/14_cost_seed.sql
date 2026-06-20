-- Seed core.cost_ledger with reference rates from the infra strategy suite
-- (deliverables/2026-05-30-infra-strategy-suite/ + 2026-05-30 Ido call).
--
-- Scope decision (per Sam 2026-05-30): DIRECT infrastructure costs only.
-- - INCLUDED: domains, inboxes, vendor pilots (Lucas/Tomer/Tucows/Folderly/Maxify/Warmly/AC)
-- - EXCLUDED: software subscriptions (droplet, Supabase, BlitzAPI, Cloudflare, Anthropic,
--             enrichment vendors). Either too small to track row-by-row, or open the door to
--             dozens of platform subscriptions we don't want to enumerate.
--
-- All rows source='reference_rate' or 'manual' — replaced by source IN ('stripe','invoice_csv','manual')
-- once Phase 2 ingest ships. Period extended to 2027-12-31 so they remain queryable until
-- actuals land.
--
-- Idempotent: INSERT ... ON CONFLICT DO NOTHING. Re-applying won't duplicate; updates require
-- DELETE the cost_id then re-INSERT.

INSERT INTO core.cost_ledger
  (cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start, period_end,
   amortize_method, attribution_dim, attribution_id, source, source_ref, notes, _loaded_at, _run_id)
VALUES
  -- ==== DOMAIN RENEWAL RATES (per-unit reference) ====
  ('reference:dynadot:co_renewal:2026-05-30',
   'dynadot', '.co_renewal', 'domain', 1, 9.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   'transcripts/2026-05-30-Ido-x-Sam-...md', '.co renewal rate per Ido', now(), NULL),

  ('reference:dynadot:info_renewal:2026-05-30',
   'dynadot', '.info_renewal', 'domain', 1, 9.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   'transcripts/2026-05-30-Ido-x-Sam-...md', '.info renewal rate per Ido', now(), NULL),

  ('reference:porkbun:com_renewal:2026-05-30',
   'porkbun', '.com_renewal', 'domain', 1, 12.0, DATE '2026-05-30', DATE '2027-12-31',
   'annual_spread', 'global', NULL, 'reference_rate',
   'industry typical — NEEDS ACTUAL', '.com typical renewal — confirm with actual Porkbun invoice', now(), NULL),

  -- ==== DOMAIN BULK PURCHASE (the canonical batch-cost example) ====
  -- Sam recalls $1.80/domain in the May 19-20 .co sale; total across Dynadot #9-15.
  -- Per Domain Tech Sheet (Domains(Table) tab): Dynadot #9-15 hold 14,978 .co domains
  -- (2,142+2,141+2,140+2,139+2,141+2,139+2,136). 286 used per account so far (2,002 used).
  -- This is a BATCH cost: 1 row in ledger, individual domains tag to this batch via
  -- core.domain.acquisition_batch when Phase 3 entity lands.
  ('manual:dynadot:co_bulk_acquisition:2026-05-19',
   'dynadot', '.co_acquisition_bulk_sale', 'domain', 14978, 26960.40,
   DATE '2026-05-19', DATE '2026-05-20',
   'one_time', 'batch', 'dynadot_2026-05-19_co_sale_14978', 'manual',
   'transcripts/2026-05-30 + Domains(Table) sheet',
   'NEEDS SAM CONFIRMATION on $1.80/domain price; total = 14,978 × $1.80 = $26,960.40 (Dynadot #9-15)',
   now(), NULL),

  -- ==== INBOX RATES (per-unit reference; multiply by core.sending_account count for total burn) ====
  ('reference:otd:inbox_monthly:2026-05-30',
   'otd', 'inbox_monthly', 'inbox', 1, 1.38, DATE '2026-05-30', DATE '2027-12-31',
   'monthly', 'channel', 'otd', 'reference_rate',
   'infra-plan-v11 channel table',
   'Derived from $548k/mo / 396,800 inboxes at TARGET scale — NEEDS ACTUAL current invoice',
   now(), NULL),

  ('reference:google:inbox_monthly:2026-05-30',
   'google', 'inbox_monthly', 'inbox', 1, 2.69, DATE '2026-05-30', DATE '2027-12-31',
   'monthly', 'channel', 'google', 'reference_rate',
   'infra-plan-v11 channel table',
   'Derived from $305k/mo / 113,600 inboxes at TARGET scale — NEEDS ACTUAL current invoice',
   now(), NULL),

  -- ==== VENDOR PILOT COSTS (per-inbox or platform fees) ====
  ('reference:lucas:inbox_monthly:2026-05-30',
   'lucas', 'inbox_monthly', 'inbox', 1, 0.05, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'outlook_mailin_lucas', 'reference_rate',
   'infra-plan-v11 + 2026-05-30 calc',
   '$5k ongoing / ~99k inboxes (1k domains × 99 acct) — NEEDS LUCAS QUOTE CONFIRMATION',
   now(), NULL),

  ('reference:tomer:inbox_monthly:2026-05-30',
   'tomer', 'inbox_monthly', 'inbox', 1, 0.065, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'outlook_mailin_tomer', 'reference_rate',
   'infra-plan-v11 + 2026-05-30 calc',
   '$6.4k ongoing / ~99k inboxes (1k domains × 99 acct) — NEEDS TOMER QUOTE CONFIRMATION',
   now(), NULL),

  ('reference:tucows:mailbox_monthly:2026-05-30',
   'tucows', 'mailbox_monthly', 'inbox', 1, 0.75, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'tucows_whitelabel', 'reference_rate',
   '2026-05-30 Sam calc',
   '$0.75/mailbox/mo — NEEDS TUCOWS QUOTE CONFIRMATION (Sam called it "OTD parity")',
   now(), NULL),

  ('reference:maxify:monthly:2026-05-30',
   'maxify', 'service_monthly', 'service', 1, 4500.0, DATE '2026-06-01', DATE '2027-12-31',
   'monthly', 'channel', 'ac_maxify', 'reference_rate',
   'infra-plan-v11',
   '$4.5k/mo recurring for Daniyal/AC warmup service — does NOT include AC platform fees (see activecampaign rows)',
   now(), NULL),

  ('reference:folderly:m1:2026-06-01',
   'folderly', 'platform_m1', 'platform', 1, 14000.0, DATE '2026-06-01', DATE '2026-06-30',
   'monthly', 'channel', 'folderly', 'reference_rate',
   'infra-plan-v11 + Folderly PDF',
   'Month 1 pilot cost (120K/day) — confirm with closing call',
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

  -- ==== ACTIVECAMPAIGN PLATFORM (Sam estimate 2026-05-30) ====
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
