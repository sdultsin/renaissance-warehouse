-- Cost ledger seed loader.
--
-- Reference cost rows are loaded from an EXTERNAL, gitignored seed file rather
-- than being inlined here, so that vendor identities, per-unit rates, and spend
-- figures are not committed to a public repository. The seed file lives at
-- seed_data/cost_seed.csv (gitignored via the repo-wide *.csv + seed_data rules)
-- and is maintained out-of-band on the operator's machine / droplet.
--
-- Columns (header row required):
--   cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start,
--   period_end, amortize_method, attribution_dim, attribution_id, source,
--   source_ref, notes
--
-- Idempotent: ON CONFLICT (cost_id) DO NOTHING. _loaded_at/_run_id are stamped
-- at load time. If the seed file is absent (e.g. a fresh clone without the
-- out-of-band data), no cost rows are seeded and the warehouse still builds.

DELETE FROM core.cost_ledger WHERE source IN ('reference_rate', 'manual') AND _run_id IS NULL;

INSERT INTO core.cost_ledger
  (cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start, period_end,
   amortize_method, attribution_dim, attribution_id, source, source_ref, notes, _loaded_at, _run_id)
SELECT cost_id, vendor, sku, cost_unit, unit_count, total_usd, period_start, period_end,
       amortize_method, attribution_dim, attribution_id, source, source_ref, notes, now(), NULL
FROM read_csv_auto('seed_data/cost_seed.csv', header=true, nullstr='')
WHERE (SELECT count(*) FROM glob('seed_data/cost_seed.csv')) > 0
ON CONFLICT (cost_id) DO NOTHING;
