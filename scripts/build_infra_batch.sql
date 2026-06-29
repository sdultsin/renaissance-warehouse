-- Populate the infra-batch layer (DDL 61) from the compact parquet extracts.
-- Idempotent FULL RECONCILE: DELETE + INSERT makes core.* EQUAL to the parquet,
-- so it is a TRUE MIRROR of the source sheets (adds appear, removes drop — never
-- append/upsert). Preserves the DDL-defined schema/keys.
--
--   duckdb /root/core/warehouse.duckdb < scripts/build_infra_batch.sql
--
-- The parquets are produced by scripts/export_infra_batch.py (re-runnable) and
-- this build is driven WEEKLY by scripts/refresh_infra_batch.sh (cron) — no
-- longer a manual one-off snapshot. rg_tag_1 / rg_tag_2 now flow through the
-- export+INSERT (was a one-time hashed backfill in DDL 1008, which went stale).
--
-- Semantics (Sam-corrected, 2026-06-12):
--   * -R = replacement set (new inboxes/domains). Never merged with its base.
--   * Decimals = one batch split across N workspaces; CSV uses the base label.
--   * Bridge = (email, batch_key) membership; is_current_batch = latest generation.
SET VARIABLE pq = '/root/core/build/infra-batch';

-- ATOMICITY: wrap the whole reconcile (steps 1-6) in ONE transaction. DuckDB
-- auto-commits each statement otherwise, so a DELETE that committed before a
-- failing INSERT (e.g. a parquet column mismatch or a missing file) would leave
-- a core table WIPED. Inside a transaction any failure rolls the whole reconcile
-- back to the prior correct state — "stale-but-correct beats fresh-but-broken".
-- (The run-log summary SELECT is deliberately AFTER the COMMIT so a transient
-- read error in the summary can never fail an already-good load.)
BEGIN TRANSACTION;

-- ── 1. core.infra_batch (per Sheet label) ────────────────────────────────────
DELETE FROM core.infra_batch;
INSERT INTO core.infra_batch BY NAME
SELECT *, now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/batch_sheet.parquet');

-- ── 2. bridge memberships (needed first: key resolution is data-driven) ───────
DELETE FROM core.sending_account_batch;
INSERT INTO core.sending_account_batch BY NAME
SELECT email AS account_email, batch_key, batch_family,
       NULL::BOOLEAN AS is_current_batch,    -- filled in step 5
       domain, raw_workspace, provider_tag, email_tag, offer,
       first_name, last_name, status_csv, n_source_rows,
       rg_tag_1, rg_tag_2,                   -- RG attribution carried from the export (2026-06-28)
       now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/account_batch.parquet');

-- ── 3. observed median warmup→cold gap (explicit-both Sheet labels) ───────────
SET VARIABLE cold_gap = (
    SELECT COALESCE(CAST(median(date_diff('day', warmup_start_date, cold_start_date)) AS INTEGER), 28)
    FROM core.infra_batch
    WHERE warmup_start_date IS NOT NULL AND cold_start_date IS NOT NULL
      AND cold_start_date >= warmup_start_date
);

-- ── 4. core.infra_batch_key (JOIN TARGET; data-driven join_type) ──────────────
DELETE FROM core.infra_batch_key;
INSERT INTO core.infra_batch_key BY NAME
WITH csv_keys AS (
    SELECT DISTINCT batch_key FROM core.sending_account_batch WHERE batch_key IS NOT NULL
),
-- (a) exact: every Sheet label is a key (covers CSV-exact + sheet-only labels)
exact AS (
    SELECT
        ib.batch_label AS batch_key, 'exact' AS join_type,
        ib.batch_label AS sheet_labels, 1 AS n_sheet_labels,
        ib.batch_family, ib.is_replacement,
        (ib.batch_label IN (SELECT batch_key FROM csv_keys)) AS in_csv,
        ib.partner, ib.offer, ib.provider, ib.workspace_raw,
        ib.sip_date, ib.warmup_start_date, ib.cold_start_date,
        ib.billing_day_of_month, ib.n_domains_sheet
    FROM core.infra_batch ib
),
-- (b) CSV keys with no exact Sheet label: split-family if same-generation decimal
--     sub-labels exist (B36→B36.1-.5, B89→B89.1/.2); else no_sheet_row (the only
--     Sheet rows are -R replacements = a DIFFERENT generation, e.g. '1st_batch').
unmatched AS (
    SELECT c.batch_key FROM csv_keys c
    WHERE c.batch_key NOT IN (SELECT batch_label FROM core.infra_batch)
),
fam AS (  -- same-generation (non-replacement) decimal sub-labels per unmatched key
    SELECT u.batch_key,
           string_agg(ib.batch_label, ',' ORDER BY ib.batch_label) AS sheet_labels,
           count(ib.batch_label)        AS n_sheet_labels,
           min(ib.sip_date)             AS sip_date,
           min(ib.warmup_start_date)    AS warmup_start_date,
           min(ib.cold_start_date)      AS cold_start_date,
           min(ib.billing_day_of_month) AS billing_day_of_month,
           sum(ib.n_domains_sheet)      AS n_domains_sheet,
           max(ib.partner)              AS partner,
           max(ib.offer)                AS offer,
           max(ib.provider)             AS provider,
           string_agg(ib.workspace_raw, ' | ' ORDER BY ib.batch_label) AS workspace_raw
    FROM unmatched u
    LEFT JOIN core.infra_batch ib
           ON ib.batch_family = u.batch_key            -- family key == base label for B-codes
          AND NOT ib.is_replacement
          AND ib.batch_label LIKE u.batch_key || '.%'  -- decimal sub-labels only
    GROUP BY u.batch_key
),
resolved_unmatched AS (
    SELECT
        f.batch_key,
        CASE WHEN f.n_sheet_labels > 0 THEN 'split_family' ELSE 'no_sheet_row' END AS join_type,
        f.sheet_labels, f.n_sheet_labels,
        (SELECT any_value(batch_family) FROM core.sending_account_batch s
          WHERE s.batch_key = f.batch_key)             AS batch_family,
        FALSE AS is_replacement,
        TRUE  AS in_csv,
        f.partner, f.offer, f.provider, f.workspace_raw,
        f.sip_date, f.warmup_start_date, f.cold_start_date,
        f.billing_day_of_month, f.n_domains_sheet
    FROM fam f
),
unioned AS (
    SELECT * FROM exact UNION ALL SELECT * FROM resolved_unmatched
)
SELECT *,
    CASE WHEN cold_start_date IS NOT NULL THEN cold_start_date
         WHEN warmup_start_date IS NOT NULL
              THEN warmup_start_date + (getvariable('cold_gap') * INTERVAL '1 day')
         ELSE NULL END AS cold_start_resolved,
    CASE WHEN cold_start_date IS NOT NULL THEN 'explicit'
         WHEN warmup_start_date IS NOT NULL THEN 'derived_median'
         ELSE 'unknown' END AS cold_start_source,
    now() AS _loaded_at
FROM unioned;

-- ── 5. is_current_batch: latest generation per email ──────────────────────────
-- Rank an email's memberships by batch recency: labeled beats unlabeled, then
-- latest warmup/sip date, then numeric batch number (B112 > B59), then key text.
UPDATE core.sending_account_batch b
SET is_current_batch = (r.rn = 1)
FROM (
    SELECT s.account_email, s.batch_key,
           row_number() OVER (
               PARTITION BY account_email
               ORDER BY (s.batch_key IS NULL),                     -- labeled first
                        COALESCE(k.warmup_start_date, k.sip_date) DESC NULLS LAST,
                        TRY_CAST(regexp_extract(upper(COALESCE(s.batch_key,'')), '^B?0*([0-9]+)', 1) AS INTEGER) DESC NULLS LAST,
                        s.batch_key DESC
           ) AS rn
    FROM core.sending_account_batch s
    LEFT JOIN core.infra_batch_key k ON k.batch_key = s.batch_key
) r
WHERE r.account_email = b.account_email
  AND r.batch_key IS NOT DISTINCT FROM b.batch_key;

-- ── 6. core.domain_infra_csv ──────────────────────────────────────────────────
-- NOT owned by export_infra_batch.py (domain-purchase data comes from the domain
-- registry on its own cadence); this build only RE-loads the existing parquet so
-- the table isn't left empty. refresh_infra_batch.sh ASSERTS this file exists
-- before invoking the build, so a missing file is caught with a clear error up
-- front. Should it ever be absent here, the read errors INSIDE the transaction
-- and the whole reconcile rolls back (no partial wipe of any core table).
DELETE FROM core.domain_infra_csv;
INSERT INTO core.domain_infra_csv BY NAME
SELECT domain, tld, accounts_per_domain, expiration_date, n_accounts_in_csv,
       now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/domain_purchase.parquet');

-- Commit the full reconcile atomically (all steps land, or none do).
COMMIT;

-- ── run-log summary ───────────────────────────────────────────────────────────
SELECT 'cold_start_median_gap_days' AS metric, getvariable('cold_gap')::VARCHAR AS value
UNION ALL SELECT 'infra_batch_labels', count(*)::VARCHAR FROM core.infra_batch
UNION ALL SELECT 'batch_keys', count(*)::VARCHAR FROM core.infra_batch_key
UNION ALL SELECT 'keys_by_join_type', (SELECT string_agg(join_type || '=' || n, ', ') FROM (SELECT join_type, count(*) n FROM core.infra_batch_key GROUP BY 1))
UNION ALL SELECT 'memberships', count(*)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'current_memberships', count(*) FILTER (WHERE is_current_batch)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'rg_tag_1_filled', count(*) FILTER (WHERE rg_tag_1 IS NOT NULL)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'rg_tag_2_filled', count(*) FILTER (WHERE rg_tag_2 IS NOT NULL)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'domains', count(*)::VARCHAR FROM core.domain_infra_csv;
