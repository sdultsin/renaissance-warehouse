-- Inbox-Hub stress test — Sam's 10 canonical questions for the infra-batch layer.
-- Run read-only after every export refresh (build_infra_batch.sql):
--   duckdb -readonly /root/core/warehouse.duckdb < scripts/infra_batch_stress_test.sql
-- If any question becomes unanswerable, the layer regressed — fix the structure.
.mode box
.maxwidth 240

SELECT '=== Q1: billing dates for B59, B67, B81 ===' AS q;
SELECT batch_label, billing_raw, billing_day_of_month, partner, provider, offer
FROM core.infra_batch WHERE batch_label IN ('B59','B67','B81') ORDER BY batch_label;

SELECT '=== Q2: accounts sent into production in April (SIP month = 2026-04), breakdown per provider ===' AS q;
-- Account-level SIP is unusable in the export (1,176 junk values) → batch-level SIP × memberships.
SELECT k.provider, count(*) AS accounts_sip_april
FROM core.sending_account_batch b
JOIN core.infra_batch_key k ON k.batch_key = b.batch_key
WHERE k.sip_date >= DATE '2026-04-01' AND k.sip_date < DATE '2026-05-01'
  AND b.is_current_batch
GROUP BY 1 ORDER BY 2 DESC;
-- batch-level companion (which batches those were)
SELECT batch_key, provider, partner, sip_date,
       (SELECT count(*) FROM core.sending_account_batch b WHERE b.batch_key = k.batch_key) accounts
FROM core.infra_batch_key k
WHERE sip_date >= DATE '2026-04-01' AND sip_date < DATE '2026-05-01'
ORDER BY sip_date;

SELECT '=== Q3: Outreach Today — avg days SIP → warmup start ===' AS q;
SELECT count(*) batches_with_both, round(avg(date_diff('day', sip_date, warmup_start_date)),1) avg_days,
       median(date_diff('day', sip_date, warmup_start_date)) median_days,
       min(date_diff('day', sip_date, warmup_start_date)) min_d,
       max(date_diff('day', sip_date, warmup_start_date)) max_d
FROM core.infra_batch
WHERE partner = 'Outreach Today' AND sip_date IS NOT NULL AND warmup_start_date IS NOT NULL;

SELECT '=== Q4: active vs deleted/cancelled accounts (live warehouse truth) ===' AS q;
SELECT status, is_active, count(*) n
FROM core.sending_account GROUP BY 1,2 ORDER BY n DESC;

SELECT '=== Q5: active accounts — domains per TLD ===' AS q;
SELECT COALESCE(dr.tld, regexp_extract(sa.domain,'\.([a-z0-9]+)$',1)) tld,
       count(DISTINCT sa.domain) domains, count(*) active_accounts
FROM core.sending_account sa
LEFT JOIN core.domain_registry dr ON dr.domain = sa.domain
WHERE sa.is_active
GROUP BY 1 ORDER BY domains DESC;

SELECT '=== Q6: active accounts — duplicate first+last names ===' AS q;
WITH active_names AS (
    SELECT b.first_name, b.last_name, b.account_email
    FROM core.sending_account_batch b
    JOIN core.sending_account sa ON lower(sa.account_id) = b.account_email
    WHERE sa.is_active AND b.is_current_batch
      AND b.first_name IS NOT NULL AND b.last_name IS NOT NULL
)
SELECT count(*) FILTER (WHERE cnt > 1) AS duplicate_name_pairs,
       sum(cnt) FILTER (WHERE cnt > 1) AS accounts_sharing_a_name,
       count(*) AS distinct_name_pairs_total
FROM (SELECT first_name, last_name, count(*) cnt FROM active_names GROUP BY 1,2);
-- top duplicated names
WITH active_names AS (
    SELECT b.first_name, b.last_name
    FROM core.sending_account_batch b
    JOIN core.sending_account sa ON lower(sa.account_id) = b.account_email
    WHERE sa.is_active AND b.is_current_batch
      AND b.first_name IS NOT NULL AND b.last_name IS NOT NULL
)
SELECT first_name, last_name, count(*) accounts
FROM active_names GROUP BY 1,2 HAVING count(*) > 1 ORDER BY 3 DESC LIMIT 10;

SELECT '=== Q7: workspace with most daily sending volume right now (last data day) ===' AS q;
WITH last_day AS (SELECT max(date) d FROM core.sending_account_daily WHERE actual_sends > 0)
SELECT d.workspace_slug, sum(d.actual_sends) sends_on_last_day, (SELECT d FROM last_day) data_day
FROM core.sending_account_daily d WHERE d.date = (SELECT d FROM last_day)
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

SELECT '=== Q8: workspace with biggest expected-vs-actual gap (last data day) ===' AS q;
WITH last_day AS (SELECT max(date) d FROM core.sending_account_daily WHERE actual_sends > 0)
SELECT workspace_slug, sum(expected_sends) expected, sum(actual_sends) actual,
       sum(expected_sends) - sum(actual_sends) shortfall,
       round(100.0 * sum(actual_sends) / NULLIF(sum(expected_sends),0), 1) fulfillment_pct
FROM core.sending_account_daily WHERE date = (SELECT d FROM last_day)
GROUP BY 1 ORDER BY shortfall DESC LIMIT 10;

SELECT '=== Q9: per workspace — cancelled/disconnected but NOT yet removed from Instantly ===' AS q;
-- still present in Instantly (is_active=true per account_truth presence) but in
-- connection_error / paused state
SELECT workspace_slug, status, count(*) n
FROM core.sending_account
WHERE is_active AND status IN ('connection_error','paused')
GROUP BY 1,2 ORDER BY n DESC LIMIT 15;

SELECT '=== Q10: average warmup period across all batches (warmup start → cold start, explicit only) ===' AS q;
SELECT count(*) batches_with_both,
       round(avg(date_diff('day', warmup_start_date, cold_start_date)),1) avg_days,
       median(date_diff('day', warmup_start_date, cold_start_date)) median_days
FROM core.infra_batch
WHERE warmup_start_date IS NOT NULL AND cold_start_date IS NOT NULL
  AND cold_start_date >= warmup_start_date;
-- by era (year-quarter of warmup start) — the gap is era-dependent
SELECT strftime(warmup_start_date, '%Y-Q') || CAST((month(warmup_start_date)+2)//3 AS VARCHAR) era,
       count(*) batches, round(avg(date_diff('day', warmup_start_date, cold_start_date)),1) avg_days
FROM core.infra_batch
WHERE warmup_start_date IS NOT NULL AND cold_start_date IS NOT NULL AND cold_start_date >= warmup_start_date
GROUP BY 1 ORDER BY 1;
