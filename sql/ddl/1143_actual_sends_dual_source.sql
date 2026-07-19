-- @gate: add
-- Depends on 1142
-- ============================================================================
-- 1143_actual_sends_dual_source.sql — make core.sending_account_daily.actual_sends
-- DURABLE (dual-source repoint) + the parity view that proves the cutover.
--
-- WHY (task #28 step 2, 2026-07-19): actual_sends was fed EXCLUSIVELY by the
-- account_truth daily CSVs produced by an UN-VERSIONED box-only script on droplet
-- renaissance-worker (/root/Renaissance/deliverables/2026-05-27-instantly-account-truth/
-- account_truth_run.py). The droplet dies ~Jul 25-27 — that feed dies with it, and
-- actual_sends (which drives derived.v_sending_volume_daily true_volume, the
-- account_label Active/Warmup lifecycle, first/last cold-send dates and the §4
-- OTD/Google split) would silently freeze. The repo-versioned, 413-fixed equivalent
-- already lands nightly: main.raw_instantly_account_daily (DDL 1142,
-- entities/instantly_account_daily.py, phase 'replies_late' — same Instantly
-- /accounts/analytics/daily source, emails-chunked).
--
-- THE REPOINT (code in entities/sending_dq.py, NOT here — sending_account_daily is
-- rebuilt from raw every nightly, so no data migration is needed): the canonical
-- rebuild now FULL-OUTER-JOINs the two raw sources per (date, account) and
--   * actual_sends = COALESCE(raw_instantly_account_daily.sent,
--                             raw_account_truth_daily_actuals.actual_sends)
--     — PREFER the repo-versioned feed, FALL BACK to the box CSVs while they last;
--   * capacity columns (daily_limit/expected_sends/delta/fulfillment/
--     active_campaign_count) keep coming from account_truth where it has the row;
--     raw_instantly-only rows get daily_limit/expected_sends filled from
--     core.sending_account (D6 reassert semantics, expected := daily_limit) and esp
--     from the existing vendor/census backfills;
--   * only COMPLETE days (date < current UTC date) are rebuilt — the table stays a
--     D-1 entity even though raw_instantly_account_daily pulls a partial "today".
-- Column list, grain (date, account_id) and semantics are UNCHANGED for consumers.
--
-- WHY FULL OUTER (not a plain date-level switch) — measured live 2026-07-19:
-- values agree exactly where both sources see an account (0 to ±34 sends/day per
-- workspace on settled days), but NEITHER source is a superset:
--   * account_truth-only: accounts purged from Instantly before the 07-18 backfill
--     (renaissance-4 2026-07-16: 6,648 accounts / 100,577 sends);
--   * raw_instantly-only: workspaces the box script's key set misses
--     (tariffs +15,006, warm-leads +19,856 on 2026-07-16).
-- The join keeps both. Once the droplet dies, the CSV side stops producing new
-- dates and the rebuild continues from raw_instantly_account_daily alone — no code
-- change needed at cutover.
--
-- THIS FILE ships the validation surface: core.v_sending_actuals_parity — per-date
-- totals of both raw sources side by side. COMPLETE days only (both sides filtered
-- to date < current UTC date, matching the canonical rebuild's D-1 rule) so the
-- rolling window's partial "today" can never fake a divergence. pct_diff is
-- normalized by GREATEST of the two sides — symmetric, so neither source's
-- coverage gaps dominate the percentage. Acceptance for retiring the account_truth
-- fallback: one clean nightly with pct_diff within ~2% on coverage='both' dates
-- (known benign residue: purge-timing coverage and the koi-and-destroy/funding-4
-- slug alias, which nets to zero fleet-wide; NB the per-date grain nets out
-- per-account offsets by construction — per-account parity was verified manually
-- 2026-07-19 and needs an account-grain join, not this view).
--
-- Reversible: DROP VIEW core.v_sending_actuals_parity; revert entities/sending_dq.py
-- (the nightly rebuild restores the old single-source table from the same raws).
-- ============================================================================

CREATE OR REPLACE VIEW core.v_sending_actuals_parity AS
WITH at_day AS (
    SELECT date,
           count(*)          AS at_rows,
           sum(actual_sends) AS at_sends
    FROM raw_account_truth_daily_actuals
    WHERE date < current_date          -- D-1 grain (the CSVs are already D-1; belt+braces)
    GROUP BY 1
),
iad_day AS (
    SELECT metric_date AS date,
           count(*)    AS iad_rows,
           sum(sent)   AS iad_sends
    FROM main.raw_instantly_account_daily
    WHERE metric_date < current_date   -- exclude the rolling window's PARTIAL today
    GROUP BY 1
)
SELECT
    COALESCE(a.date, i.date)                        AS date,
    a.at_rows,
    a.at_sends,
    i.iad_rows,
    i.iad_sends,
    (COALESCE(i.iad_sends, 0) - COALESCE(a.at_sends, 0))            AS sends_diff,
    -- symmetric: normalized by the LARGER side so neither source's coverage gaps
    -- dominate. NULL unless BOTH sides have the date — a coverage-gap row would
    -- otherwise read as a hard 100.00 and permanently trip any MAX/threshold scan.
    CASE WHEN a.date IS NOT NULL AND i.date IS NOT NULL THEN
        round(100.0 * abs(COALESCE(i.iad_sends, 0) - COALESCE(a.at_sends, 0))
              / nullif(greatest(COALESCE(a.at_sends, 0), COALESCE(i.iad_sends, 0)), 0), 2)
    END                                             AS pct_diff,
    CASE WHEN a.date IS NULL THEN 'instantly_only'
         WHEN i.date IS NULL THEN 'account_truth_only'
         ELSE 'both' END                            AS coverage
FROM at_day a
FULL OUTER JOIN iad_day i ON a.date = i.date;
