-- Populate the infra-batch layer v2 (DDL 61 + 1072) from the 3-sheet extracts
-- written by scripts/export_infra_batch_v2.py — OFF the 33 unshared
-- "<Workspace> - Email Accounts" sheets forever (TKT-1 §3/§4-B+C, 2026-07-03).
--
--   duckdb /root/core/warehouse.duckdb < scripts/build_infra_batch_v2.sql
--   (driven WEEKLY by scripts/refresh_infra_batch.sh under the writer flock)
--
-- What changes vs v1 (scripts/build_infra_batch.sql, now superseded):
--   * core.rg_tag_dim      — NEW: full-replace load from rg_dim.parquet.
--   * core.sending_account_batch — rebuilt as LIVE ∪ LEGACY:
--       LIVE   = one row per RG-tagged inbox email derived from
--                core.account_tags ⋈ rg_dim (attribution_source =
--                'account_tags_live'; is_cancelled / partner / rg_type filled;
--                batch_key from the Inbox Hub Batch column via rg_dim; other
--                columns enriched from the email's prior current-batch row).
--       LEGACY = the pre-v2 / 06-12 sheet-snapshot rows preserved for emails
--                NOT in the live-derived set (attribution_source =
--                'sheet_snapshot_20260612'; all columns preserved, cancellation
--                columns filled from rg_dim via their rg_tag_1/rg_tag_2).
--                Self-sustaining: each run re-stages legacy rows from the
--                current table BEFORE the delete, so they persist until the
--                email appears in account_tags. is_current_batch is preserved
--                on legacy rows and TRUE on live rows (one row per live email),
--                so the v1 re-rank step is no longer needed.
--   * core.infra_batch     — full replace from batch_sheet_v2.parquet (same
--                            columns as v1; registry parser reused).
--   * core.infra_batch_key + core.domain_infra_csv — rebuilt exactly as v1
--     (steps unchanged) so the join-target and domain layers never go stale
--     relative to the tables they derive from.
--
-- ATOMICITY: ONE transaction; any failure (missing parquet, column mismatch,
-- count-guard error()) rolls the whole reconcile back — stale-but-correct
-- beats fresh-but-broken. Count guards (in-SQL, inside the txn):
--   rg_tag_dim >= 2000 rows        (measured 5,650 on 2026-07-03)
--   rg_tag_dim cancelled > 0       (measured 2,358)
--   live-derived memberships >= 300000 (measured 894,775 on
--                                       warehouse_20260703_043558_874)
--   batch labels >= 100            (measured 181)
--   membership total >= 85% of the prior run's count (step 6c drop guard —
--     mirrors INFRA_BATCH_MAX_DROP_PCT=15 in refresh_infra_batch.sh)
-- Expected post-rebuild total ≈ 2.85M (894,775 live + 1,957,426 legacy rows,
-- measured on warehouse_20260703_043558_874 vs the prior 2,615,590).
SET VARIABLE pq = '/root/core/build/infra-batch';
SET VARIABLE run_id = 'infra_batch_v2_' || strftime(now(), '%Y%m%d%H%M%S');

-- ── 0. Drop the membership unique index BEFORE the transaction ────────────────
-- DuckDB's ART index falsely reports "Duplicate key" when a key deleted earlier
-- in the SAME transaction is re-inserted (hit in the 2026-07-03 rehearsal:
-- legacy row 't.mata@…, B59' re-inserted identical to its deleted self), and a
-- DROP INDEX + CREATE INDEX pair cannot live inside one transaction either
-- ("An index with the name … already exists", verified on DuckDB v1.5.2). So:
-- drop here (auto-commits), enforce uniqueness with the in-txn error() guard in
-- step 6b (bad data can never COMMIT), and physically recreate the index after
-- the COMMIT at the bottom. NOTE: the step-10 recreate covers the SUCCESS path
-- only — on any in-file error the duckdb CLI '.read' bails at the first errored
-- statement (verified v1.5.2; the txn auto-rolls back but nothing after it
-- runs), so refresh_infra_batch.sh recreates the index UNCONDITIONALLY after
-- this invocation, success or failure.
DROP INDEX IF EXISTS core.ux_sab_email_key;  -- index lives in schema core; unqualified DROP silently no-ops

BEGIN TRANSACTION;

-- ── 1. core.rg_tag_dim (full replace from rg_dim.parquet) ─────────────────────
DELETE FROM core.rg_tag_dim;
INSERT INTO core.rg_tag_dim BY NAME
SELECT rg_tag, workspace_name, rg_type, is_cancelled, renewal, partner,
       now() AS _loaded_at, getvariable('run_id') AS _run_id
FROM read_parquet(getvariable('pq') || '/rg_dim.parquet');

-- ── 2. core.infra_batch (full replace from batch_sheet_v2.parquet) ────────────
DELETE FROM core.infra_batch;
INSERT INTO core.infra_batch BY NAME
SELECT *, now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/batch_sheet_v2.parquet');

-- ── 3. Stage LEGACY memberships BEFORE the delete ─────────────────────────────
-- Pre-v2 rows have attribution_source NULL (column added by DDL 1072); rows
-- preserved by a prior v2 run carry 'sheet_snapshot_20260612'. Rows a prior v2
-- run derived live ('account_tags_live') are NOT staged — they are re-derived
-- fresh in step 4.
CREATE TEMP TABLE _legacy AS
SELECT * FROM core.sending_account_batch
WHERE COALESCE(attribution_source, 'sheet_snapshot_20260612') = 'sheet_snapshot_20260612';

-- Enrichment source for live rows = the WHOLE current table (not just legacy):
-- a live row enriched from a sheet row on run 1 (first_name, email_tag, ...)
-- must keep that enrichment on run 2, when the email's row is no longer in
-- _legacy but IS still in the table as last run's 'account_tags_live' row.
CREATE TEMP TABLE _prior_all AS
SELECT account_email, batch_key, batch_family, provider_tag, email_tag, offer,
       first_name, last_name, status_csv, is_current_batch, _loaded_at
FROM core.sending_account_batch;

-- ── 4. Stage LIVE-derived memberships from core.account_tags ⋈ rg_dim ─────────
-- One row per RG-tagged inbox email. rg_tag_1 = the single-form tag ('RG267'),
-- rg_tag_2 = the range-form tag ('RG1000-1009') — v1 column semantics kept.
-- Dim attributes resolve via rg_tag_1 first, rg_tag_2 second. The parquet's
-- enrichment extras (batch / batch_family / email_provider / offer /
-- workspace_name) fill the v1 columns where the prior sheet row had nothing.
CREATE TEMP TABLE _live AS
WITH edges0 AS (
    SELECT lower(email) AS email, workspace_slug, unnest(tags_arr) AS tag
    FROM core.account_tags
),
edges AS (  -- dedupe an email seen in >1 workspace with the same tag
    SELECT email, min(workspace_slug) AS workspace_slug, tag
    FROM edges0
    WHERE regexp_matches(tag, '^RG[0-9]')
    GROUP BY email, tag
),
per_email AS (
    SELECT email,
           min(tag) FILTER (WHERE NOT contains(tag, '-'))  AS rg_tag_1,
           min(tag) FILTER (WHERE contains(tag, '-'))      AS rg_tag_2,
           count(*)                                        AS n_edges,
           min(workspace_slug)                             AS workspace_slug
    FROM edges
    GROUP BY email
),
dim AS (SELECT * FROM read_parquet(getvariable('pq') || '/rg_dim.parquet')),
prior AS (  -- the email's most-current prior row, for name/tag enrichment
    SELECT *, row_number() OVER (
               PARTITION BY account_email
               ORDER BY is_current_batch DESC NULLS LAST, _loaded_at DESC
           ) AS rn
    FROM _prior_all
)
SELECT
    p.email                                          AS account_email,
    COALESCE(d1.batch,        d2.batch,        lg.batch_key)     AS batch_key,
    COALESCE(d1.batch_family, d2.batch_family, lg.batch_family)  AS batch_family,
    TRUE                                             AS is_current_batch,
    split_part(p.email, '@', 2)                      AS domain,
    COALESCE(d1.workspace_name, d2.workspace_name, p.workspace_slug) AS raw_workspace,
    COALESCE(d1.rg_type,        d2.rg_type,        lg.provider_tag)  AS provider_tag,
    COALESCE(d1.email_provider, d2.email_provider, lg.email_tag)     AS email_tag,
    COALESCE(d1.offer,          d2.offer,          lg.offer)         AS offer,
    lg.first_name                                    AS first_name,
    lg.last_name                                     AS last_name,
    lg.status_csv                                    AS status_csv,
    p.n_edges::INTEGER                               AS n_source_rows,
    p.rg_tag_1,
    p.rg_tag_2,
    now()                                            AS _loaded_at,
    COALESCE(d1.is_cancelled, d2.is_cancelled)       AS is_cancelled,
    COALESCE(d1.partner,      d2.partner)            AS partner,
    COALESCE(d1.rg_type,      d2.rg_type)            AS rg_type,
    'account_tags_live'                              AS attribution_source
FROM per_email p
LEFT JOIN dim d1 ON d1.rg_tag = p.rg_tag_1
LEFT JOIN dim d2 ON d2.rg_tag = p.rg_tag_2
LEFT JOIN prior lg ON lg.account_email = p.email AND lg.rn = 1;

-- ── 5. Count guards (any error() aborts + rolls the whole txn back) ───────────
SELECT CASE WHEN (SELECT count(*) FROM core.rg_tag_dim) < 2000
            THEN error('GUARD: rg_tag_dim < 2000 rows — dim export broke, aborting reconcile')
       END;
SELECT CASE WHEN (SELECT count(*) FILTER (WHERE is_cancelled) FROM core.rg_tag_dim) = 0
            THEN error('GUARD: rg_tag_dim has 0 cancelled tags — Cancelled-tab parse broke, aborting')
       END;
SELECT CASE WHEN (SELECT count(*) FROM _live) < 300000
            THEN error('GUARD: live-derived memberships < 300000 — account_tags derivation broke, aborting')
       END;
SELECT CASE WHEN (SELECT count(*) FROM core.infra_batch) < 100
            THEN error('GUARD: infra_batch < 100 labels — registry export broke, aborting')
       END;

-- ── 6. Rebuild core.sending_account_batch = LIVE ∪ LEGACY(anti-join) ──────────
-- Plain DELETE + INSERT (never ON CONFLICT DO UPDATE: DuckDB ART-index upserts
-- have thrown INTERNAL duplicate-key aborts on tables in this repo).
-- The unique index was dropped in step 0 (see there for why); uniqueness is
-- enforced by the step-6b guard inside this transaction and the index is
-- physically recreated after COMMIT.
DELETE FROM core.sending_account_batch;

INSERT INTO core.sending_account_batch BY NAME
SELECT * FROM _live;

INSERT INTO core.sending_account_batch BY NAME
SELECT
    l.account_email, l.batch_key, l.batch_family, l.is_current_batch,
    l.domain, l.raw_workspace, l.provider_tag, l.email_tag, l.offer,
    l.first_name, l.last_name, l.status_csv, l.n_source_rows,
    l.rg_tag_1, l.rg_tag_2,
    now()                                        AS _loaded_at,
    COALESCE(d1.is_cancelled, d2.is_cancelled)   AS is_cancelled,
    COALESCE(d1.partner,      d2.partner)        AS partner,
    COALESCE(d1.rg_type,      d2.rg_type)        AS rg_type,
    'sheet_snapshot_20260612'                    AS attribution_source
FROM _legacy l
LEFT JOIN core.rg_tag_dim d1 ON d1.rg_tag = l.rg_tag_1
LEFT JOIN core.rg_tag_dim d2 ON d2.rg_tag = l.rg_tag_2
WHERE NOT EXISTS (SELECT 1 FROM _live v WHERE v.account_email = l.account_email);

-- ── 6b. Uniqueness guard (replaces the dropped index INSIDE the txn) ──────────
-- NULL batch_key rows are excluded to match ART-index semantics (rows with a
-- NULL in an indexed column are exempt from the unique constraint).
SELECT CASE WHEN (SELECT count(*) FROM core.sending_account_batch WHERE batch_key IS NOT NULL)
         != (SELECT count(DISTINCT (account_email, batch_key))
             FROM core.sending_account_batch WHERE batch_key IS NOT NULL)
            THEN error('GUARD: duplicate (account_email, batch_key) after rebuild — aborting reconcile')
       END;

-- ── 6c. Drop guard INSIDE the txn (restores v1 refuse-to-load semantics) ──────
-- _prior_all (step 3) is a full pre-delete copy of core.sending_account_batch,
-- so its count is the prior-run baseline. A >15% shrink -> error() -> the whole
-- reconcile rolls back and the prior table survives — so neither this script's
-- promote nor the nightly's later serving publish can ever ship the shrunken
-- table. 85 = 100 - 15; keep in sync with INFRA_BATCH_MAX_DROP_PCT=15 in
-- refresh_infra_batch.sh (hardcoded here like the 2000/300000/100 floors —
-- duckdb .read cannot see env; a deliberate larger shrink needs a one-off edit).
SELECT CASE WHEN (SELECT count(*) FROM _prior_all) > 0
             AND (SELECT count(*) FROM core.sending_account_batch) * 100 <
                 (SELECT count(*) FROM _prior_all) * 85
            THEN error('GUARD: membership total dropped >15% vs prior run — likely partial account_tags/source read, aborting reconcile (table left untouched)')
       END;

-- ── 7. observed median warmup→cold gap (unchanged from v1 step 3) ─────────────
SET VARIABLE cold_gap = (
    SELECT COALESCE(CAST(median(date_diff('day', warmup_start_date, cold_start_date)) AS INTEGER), 28)
    FROM core.infra_batch
    WHERE warmup_start_date IS NOT NULL AND cold_start_date IS NOT NULL
      AND cold_start_date >= warmup_start_date
);

-- ── 8. core.infra_batch_key (unchanged from v1 step 4) ────────────────────────
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
--     sub-labels exist (B36→B36.1-.5, B89→B89.1/.2); else no_sheet_row
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

-- ── 9. core.domain_infra_csv (unchanged from v1 step 6) ───────────────────────
-- NOT owned by the v2 exporter (domain-registry cadence); re-loaded so the table
-- isn't left empty. refresh_infra_batch.sh asserts the file exists up front; if
-- absent here the read errors INSIDE the transaction and everything rolls back.
DELETE FROM core.domain_infra_csv;
INSERT INTO core.domain_infra_csv BY NAME
SELECT domain, tld, accounts_per_domain, expiration_date, n_accounts_in_csv,
       now() AS _loaded_at
FROM read_parquet(getvariable('pq') || '/domain_purchase.parquet');

-- Commit the full reconcile atomically (all steps land, or none do).
COMMIT;

-- ── 10. Recreate the membership unique index (dropped in step 0) ──────────────
-- Runs in its own implicit transaction so it never trips the same-txn ART
-- phantom. SUCCESS PATH ONLY: on any in-file error the duckdb CLI '.read'
-- bails at the first errored statement (verified v1.5.2) and never reaches
-- this line — refresh_infra_batch.sh owns the recreate then, via its
-- unconditional post-build CREATE INDEX under the writer flock.
CREATE UNIQUE INDEX IF NOT EXISTS ux_sab_email_key
    ON core.sending_account_batch (account_email, batch_key);

-- ── run-log summary (deliberately AFTER the COMMIT, as in v1) ─────────────────
SELECT 'rg_tag_dim_rows' AS metric, count(*)::VARCHAR AS value FROM core.rg_tag_dim
UNION ALL SELECT 'rg_tag_dim_cancelled', count(*) FILTER (WHERE is_cancelled)::VARCHAR FROM core.rg_tag_dim
UNION ALL SELECT 'infra_batch_labels', count(*)::VARCHAR FROM core.infra_batch
UNION ALL SELECT 'batch_keys', count(*)::VARCHAR FROM core.infra_batch_key
UNION ALL SELECT 'memberships_total', count(*)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'memberships_live', count(*) FILTER (WHERE attribution_source = 'account_tags_live')::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'memberships_legacy', count(*) FILTER (WHERE attribution_source = 'sheet_snapshot_20260612')::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'memberships_cancelled', count(*) FILTER (WHERE is_cancelled)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'memberships_with_partner', count(*) FILTER (WHERE partner IS NOT NULL)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'memberships_with_batch', count(*) FILTER (WHERE batch_key IS NOT NULL)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'rg_tag_1_filled', count(*) FILTER (WHERE rg_tag_1 IS NOT NULL)::VARCHAR FROM core.sending_account_batch
UNION ALL SELECT 'domains', count(*)::VARCHAR FROM core.domain_infra_csv;
