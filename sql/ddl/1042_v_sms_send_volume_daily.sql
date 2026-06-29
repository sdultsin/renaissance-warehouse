-- Canonical SMS send-volume by day. Version 1042.
-- @gate: add
-- Depends on 34
--
-- WHY: SMS send VOLUME lives in raw_sendivo_campaign_daily (folded from Sendivo /sms/logs) and
-- reconciles to Grace's daily totals within ~0.1% (06-24=687,481 / 06-25=748,364 / 06-26=1,183,122 /
-- 06-27=758,454; verified 2026-06-29). The recurring confusion is that people read
-- raw_sendivo_blast_daily (the per-BLAST G4 rollup), which Sendivo only populates from 2026-06-26
-- (Larry added blast_id to /sms/logs then) -> it looks "2 days deep". This view is the canonical,
-- discoverable SMS volume surface at the offer-attribution join grain (date x sub_account x
-- campaign), latest run per metric_date, so consumers (e.g. the reporting chat's v_sms_sends_by_offer)
-- join HERE, not the blast table.
--
-- COVERAGE / hard upstream limits (Sendivo-side -- NOT fixable warehouse-side):
--   * Data starts 2026-05-18: Sendivo retains NOTHING earlier -- /sms/logs AND /delivery-metrics
--     both return 0 for all of April / early May (verified 2026-06-29). A pre-05-18 backfill is
--     therefore impossible; the warehouse already holds 100% of what Sendivo keeps.
--   * Per-BLAST breakdown (raw_sendivo_blast_daily) only exists >= 2026-06-26 (blast_id added then).
--   * `messages` = ALL status groups = ATTEMPTED sends -- this is the number that matches Grace.
--     `delivered_messages` is the DELIVERED subset.
-- Freshness/coverage is guarded by scripts/sendivo_sms_logs_watchdog.py (committed vs Sendivo
-- /sms/logs pagination.total per day -> #cc-sam on a MISSING/SHORT day).
CREATE OR REPLACE VIEW main.v_sms_send_volume_daily AS
-- Pick exactly the latest run per metric_date (campaign_daily pulls each day once today, but a
-- re-pull/heal can leave >1 run for a date; keep the most recent so we never double-count).
WITH run_loaded AS (   -- one row per (metric_date, run) with that run's load time
    SELECT metric_date, _run_id, max(_loaded_at) AS run_loaded_at
    FROM main.raw_sendivo_campaign_daily
    GROUP BY metric_date, _run_id
),
latest AS (            -- rank runs within each date; rn=1 = most recent (deterministic via _run_id tiebreak)
    SELECT metric_date, _run_id,
           row_number() OVER (PARTITION BY metric_date
                              ORDER BY run_loaded_at DESC, _run_id DESC) AS rn
    FROM run_loaded
)
SELECT
    c.metric_date,
    c.sub_account_id,
    c.sub_account_name,
    c.campaign_id,
    c.campaign_name,
    sum(c.n_messages)                                             AS messages,
    sum(c.n_messages) FILTER (WHERE c.status_group = 'DELIVERED') AS delivered_messages,
    sum(c.segments)                                              AS segments,
    sum(c.cost_usd)                                             AS cost_usd
FROM main.raw_sendivo_campaign_daily c
JOIN latest l ON c.metric_date = l.metric_date AND c._run_id = l._run_id AND l.rn = 1
GROUP BY 1, 2, 3, 4, 5;
