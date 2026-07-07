-- 1086_infra_capacity_real_present.sql  [2026-07-07]
-- FIX the phantom over-count in the infra-capacity views (39_infra_capacity.sql).
--
-- TWO bugs, same as everywhere else:
--   (1) NO live filter — they aggregated over ALL 1.79M sending_account rows (incl. 1.37M
--       retired/dead), so accounts_total was wildly inflated.
--   (2) status='active' is the DEAD/phantom value on core.sending_account (live inboxes carry
--       the conn_* vocabulary: conn_active/conn_paused/connection_error/sending_error), so the
--       "sendable"/"active" filters matched only dead rows.
--
-- FIX (per David 2026-07-07): the headline count = the REAL number of inboxes present in
-- Instantly, regardless of connection state = WHERE is_active (399,194 fleet-wide). Column
-- names are UNCHANGED (no consumer breaks); only the WHERE/FILTER predicates are corrected to
-- the live conn_* vocabulary. accounts_total now = present inboxes (all states); the
-- sendable/health sub-columns remain as an accurate breakdown of that real set.
-- @gate: alter
-- Depends on 39
CREATE OR REPLACE VIEW v_infra_capacity_daily AS
SELECT
    _snapshot_date                                                          AS date,
    workspace_slug,
    infra_label(esp)                                                        AS infra,
    count(*)                                                                AS accounts_total,      -- present in Instantly (all states)
    count(*) FILTER (WHERE status = 'conn_active')                          AS accounts_sendable,   -- of those, healthy & sending
    count(*) FILTER (WHERE status IN ('conn_paused','connection_error','sending_error')) AS accounts_blocked,
    COALESCE(sum(daily_limit), 0)                                           AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE status = 'conn_active'), 0)     AS sendable_per_day
FROM core.sending_account
WHERE is_active                                                            -- REAL present fleet only
GROUP BY _snapshot_date, workspace_slug, infra_label(esp);

CREATE OR REPLACE VIEW v_account_health AS
SELECT
    workspace_slug,
    infra_label(esp)                                          AS infra,
    count(*)                                                  AS accounts_total,      -- present (all states)
    count(*) FILTER (WHERE status = 'conn_active')            AS active,
    count(*) FILTER (WHERE status = 'conn_paused')            AS paused,
    count(*) FILTER (WHERE status = 'connection_error')       AS connection_error,
    count(*) FILTER (WHERE status = 'sending_error')          AS missing,
    count(*) FILTER (WHERE status IN ('conn_paused','connection_error','sending_error')) AS unsendable,
    COALESCE(sum(daily_limit), 0)                             AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE status = 'conn_active'), 0) AS sendable_per_day
FROM core.sending_account
WHERE is_active
GROUP BY workspace_slug, infra_label(esp);

CREATE OR REPLACE VIEW v_accounts_per_domain AS
SELECT
    domain,
    infra_label(esp)                                          AS infra,
    count(*)                                                  AS accounts,        -- present (all states)
    count(*) FILTER (WHERE status = 'conn_active')            AS sendable,
    count(*) FILTER (WHERE status IN ('conn_paused','connection_error','sending_error')) AS unsendable,
    COALESCE(sum(daily_limit), 0)                             AS daily_limit_total
FROM core.sending_account
WHERE is_active
GROUP BY domain, infra_label(esp);
