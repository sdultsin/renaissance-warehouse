-- Track G (2026-06-08) — Sending-infrastructure daily capacity + health views.
-- Version 39.
--
-- Built on core.sending_account (canonical inbox, one row per email, rebuilt nightly
-- from the latest account_truth snapshot). That entity already carries the resolved
-- esp / status / daily_limit / workspace_slug / _snapshot_date we need, so these are
-- pure views — no new ingest, no source-DB dependency.
--
-- Infra taxonomy (Sam): Google / OTD / Microsoft-Outlook. esp is 'google'|'otd'|'outlook'.
-- Sendable = status='active' (connected, not paused/connection_error/missing). Sam
-- explicitly wants broken accounts FILTERED OUT of sendable capacity, kept in theoretical.

CREATE OR REPLACE MACRO infra_label(esp) AS
    CASE esp
        WHEN 'google'  THEN 'Google'
        WHEN 'otd'     THEN 'OTD'
        WHEN 'outlook' THEN 'Microsoft-Outlook'
        ELSE 'Unknown'
    END;

-- v_infra_capacity_daily — Σ daily_limit per (snapshot date × infra × workspace),
-- THEORETICAL (all accounts) vs SENDABLE (connected only). One row per snapshot date;
-- becomes a daily series as account_truth snapshots accumulate.
CREATE OR REPLACE VIEW v_infra_capacity_daily AS
SELECT
    _snapshot_date                                                          AS date,
    workspace_slug,
    infra_label(esp)                                                        AS infra,
    count(*)                                                                AS accounts_total,
    count(*) FILTER (WHERE status = 'active')                               AS accounts_sendable,
    count(*) FILTER (WHERE status IN ('paused','connection_error','missing')) AS accounts_blocked,
    COALESCE(sum(daily_limit), 0)                                           AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE status = 'active'), 0)          AS sendable_per_day
FROM core.sending_account
GROUP BY _snapshot_date, workspace_slug, infra_label(esp);

-- v_account_health — disconnected/error/paused counts per workspace × infra (the
-- filtered-out set, so the capacity gap is explainable).
CREATE OR REPLACE VIEW v_account_health AS
SELECT
    workspace_slug,
    infra_label(esp)                                          AS infra,
    count(*)                                                  AS accounts_total,
    count(*) FILTER (WHERE status = 'active')                 AS active,
    count(*) FILTER (WHERE status = 'paused')                 AS paused,
    count(*) FILTER (WHERE status = 'connection_error')       AS connection_error,
    count(*) FILTER (WHERE status = 'missing')                AS missing,
    count(*) FILTER (WHERE status IN ('paused','connection_error','missing')) AS unsendable,
    COALESCE(sum(daily_limit), 0)                             AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE status = 'active'), 0) AS sendable_per_day
FROM core.sending_account
GROUP BY workspace_slug, infra_label(esp);

-- v_accounts_per_domain — account count per domain (+ infra, status mix). Internal
-- analytics grain; never surfaced raw to reports (counts-only rule).
CREATE OR REPLACE VIEW v_accounts_per_domain AS
SELECT
    domain,
    infra_label(esp)                                          AS infra,
    count(*)                                                  AS accounts,
    count(*) FILTER (WHERE status = 'active')                 AS sendable,
    count(*) FILTER (WHERE status IN ('paused','connection_error','missing')) AS unsendable,
    COALESCE(sum(daily_limit), 0)                             AS daily_limit_total
FROM core.sending_account
GROUP BY domain, infra_label(esp);
