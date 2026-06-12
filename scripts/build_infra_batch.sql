-- Populate the infra-batch layer (DDL 60) from the compact parquet extracts.
-- Run when a fresh manual export drops (these are point-in-time snapshots, NOT a
-- nightly feed). Idempotent: DELETE + INSERT preserves the DDL-defined schema/PK.
--
--   duckdb /root/core/warehouse.duckdb < scripts/build_infra_batch.sql
--
-- Expects parquet extracts (produced by build/infra-batch/extract_local.sql on the
-- box holding the raw CSVs) at:
SET VARIABLE pq = '/root/core/build/infra-batch';

-- ── 1. core.infra_batch (per Sheet label) ────────────────────────────────────
DELETE FROM core.infra_batch;
INSERT INTO core.infra_batch BY NAME
SELECT *, now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/batch_sheet.parquet');

-- ── 2. cold-start median gap (warmup → cold), from batches with BOTH explicit ──
-- Observed gap is era-dependent (recent ~17d, older ~3-7d). A single median is a
-- coarse fallback; the explicit value always wins and derived rows are flagged.
SET VARIABLE cold_gap = (
    SELECT COALESCE(CAST(median(date_diff('day', warmup_start_date, cold_start_date)) AS INTEGER), 28)
    FROM core.infra_batch
    WHERE warmup_start_date IS NOT NULL AND cold_start_date IS NOT NULL
      AND cold_start_date >= warmup_start_date
);

-- ── 3. core.infra_batch_root (rollup; JOIN target for views) ──────────────────
DELETE FROM core.infra_batch_root;
INSERT INTO core.infra_batch_root BY NAME
WITH canon AS (  -- pick canonical metadata row per root: prefer -R, then latest
    SELECT batch_root, partner, workspace_raw, offer, provider,
           row_number() OVER (PARTITION BY batch_root
               ORDER BY is_reconnect DESC,
                        warmup_start_date DESC NULLS LAST,
                        sip_date DESC NULLS LAST) AS rn
    FROM core.infra_batch
),
agg AS (
    SELECT batch_root,
           min(sip_date)            AS sip_date,
           min(warmup_start_date)   AS warmup_start_date,   -- true biological start
           min(cold_start_date)     AS cold_start_date,
           count(*)                 AS n_sheet_rows,
           bool_or(is_reconnect)    AS has_reconnect,
           sum(n_domains_sheet)     AS n_domains_sheet_sum
    FROM core.infra_batch GROUP BY 1
)
SELECT
    a.batch_root,
    c.partner, c.workspace_raw, c.offer, c.provider,
    a.sip_date, a.warmup_start_date, a.cold_start_date,
    CASE WHEN a.cold_start_date IS NOT NULL THEN a.cold_start_date
         WHEN a.warmup_start_date IS NOT NULL
              THEN a.warmup_start_date + (getvariable('cold_gap') * INTERVAL '1 day')
         ELSE NULL END                                       AS cold_start_resolved,
    CASE WHEN a.cold_start_date IS NOT NULL THEN 'explicit'
         WHEN a.warmup_start_date IS NOT NULL THEN 'derived_median'
         ELSE 'unknown' END                                  AS cold_start_source,
    a.n_sheet_rows, a.has_reconnect, a.n_domains_sheet_sum,
    now() AS _loaded_at
FROM agg a JOIN canon c ON c.batch_root = a.batch_root AND c.rn = 1;

-- ── 4. core.sending_account_batch (bridge, ~2.55M) ────────────────────────────
DELETE FROM core.sending_account_batch;
INSERT INTO core.sending_account_batch BY NAME
SELECT email AS account_email, domain, raw_batch, batch_root, raw_workspace,
       provider_tag, offer, now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/account_batch.parquet');

-- ── 5. core.domain_infra_csv (domain enrichment, ~95k) ────────────────────────
DELETE FROM core.domain_infra_csv;
INSERT INTO core.domain_infra_csv BY NAME
SELECT domain, tld, accounts_per_domain, expiration_date, n_accounts_in_csv,
       now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/domain_purchase.parquet');

-- ── report the resolved gap for the run log ───────────────────────────────────
SELECT 'cold_start_median_gap_days' AS k, getvariable('cold_gap') AS v;
