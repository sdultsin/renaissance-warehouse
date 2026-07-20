-- @gate: add
-- Depends on 1146
-- ============================================================================
-- 1148_domain_rehab_rr_window_floor.sql — Option-B healthy-feed floor on the
--   domain-rehab scorer window (Sam ruling [2026-07-20], memory/decisions.md).
--
-- WHAT: CREATE OR REPLACE of core.v_domain_rr_state (from 1146) with ONE change:
--   the RR window becomes rolling 30d FLOORED at 2026-07-13 —
--     window_start = greatest(CURRENT_DATE - 30, DATE '2026-07-13')
--   Self-heals to a full 30d rolling window by ~Aug 12 with no second change.
--
-- WHY: core.v_domain_reply_daily is only healthy from 2026-07-13. The 07-10..12
--   feed gap inflates human RR ~3-4x if the window spans it
--   (handoffs/2026-07-19-domain-rehab-FOLD-BACK-to-main-chat.md §2.3), which would
--   mis-band domains into rehab/retire on corrupted data. The ≥1,000-send
--   eligibility gate is UNCHANGED — fewer domains score early; coverage grows
--   daily (as of 07-20 the floored-window per-domain max is ~671 sends, so first
--   domains clear the gate ~07-23).
--
-- PARITY: the shadow orchestrator's inline fallback scorer
--   (sync-runner-1:/root/domain-rehab/domain_rehab_orchestrator.py) already
--   carries the identical floor [2026-07-20]; this DDL restores view/inline parity.
--
-- Additive: view replace only; no table/column/row changes.
-- Reversible: re-apply the view block of 1146_domain_rehab.sql.
-- ============================================================================

CREATE OR REPLACE VIEW core.v_domain_rr_state AS
WITH dom_prov AS (   -- total sends per (domain, provider_group) over the window
  SELECT domain, provider_group, SUM(sent) AS sent_sum
  FROM main.raw_instantly_account_daily
  WHERE domain IS NOT NULL
  GROUP BY domain, provider_group
),
dom_infra AS (   -- authoritative infra = provider_group with the MOST SENDS per domain
  SELECT                            -- (send-weighted, not day-count-weighted; honors the send grain)
    domain,
    CASE arg_max(provider_group, sent_sum)
      WHEN 'imap'    THEN 'otd'
      WHEN 'google'  THEN 'reseller'
      WHEN 'outlook' THEN 'milkbox'
      ELSE 'unknown'
    END AS infra
  FROM dom_prov
  GROUP BY domain
),
win AS (   -- Option-B window: rolling 30d FLOORED at 2026-07-13 (healthy feed only)
  SELECT
    domain,
    SUM(sent)          AS sent_30d,
    SUM(human_replies) AS human_30d,
    SUM(auto_replies)  AS auto_30d,
    SUM(human_replies) * 100.0 / NULLIF(SUM(sent), 0) AS human_rr_pct
  FROM core.v_domain_reply_daily
  WHERE date >= greatest(CURRENT_DATE - 30, DATE '2026-07-13')
  GROUP BY domain
)
SELECT
  w.domain,
  COALESCE(di.infra, 'unknown')                         AS infra,
  w.sent_30d,
  w.human_30d,
  ROUND(w.human_rr_pct, 4)                              AS human_rr,
  CASE
    WHEN di.infra = 'milkbox'                                     THEN 'excluded'
    WHEN di.infra IS NULL OR di.infra NOT IN ('otd', 'reseller') THEN 'unknown_infra'
    WHEN w.sent_30d < 1000                                        THEN 'unscored'         -- eligibility gate (UNCHANGED)
    WHEN di.infra = 'otd'      AND w.human_rr_pct < 0.18          THEN 'retire'           -- severity band
    WHEN di.infra = 'otd'      AND w.human_rr_pct < 0.30          THEN 'rehab'
    WHEN di.infra = 'otd'                                         THEN 'good'
    WHEN di.infra = 'reseller' AND w.human_rr_pct < 0.38          THEN 'retire'
    WHEN di.infra = 'reseller' AND w.human_rr_pct < 0.55          THEN 'rehab'
    WHEN di.infra = 'reseller'                                    THEN 'good'
    ELSE 'unknown_infra'                                                                  -- defensive terminal
  END                                                   AS state,
  CURRENT_DATE                                          AS scored_on
FROM win w
LEFT JOIN dom_infra di USING (domain);
