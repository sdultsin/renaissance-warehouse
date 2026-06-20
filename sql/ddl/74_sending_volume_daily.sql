-- Version 74 (2026-06-15) — DAILY true sending volume, per workspace, filterable by INFRA.
-- Sam: "we just need to know day by day" (NO rollups/averages). Built on the existing daily
-- account-state sync core.sending_account_daily (nightly account_truth: daily_limit = send
-- setting, actual_sends = true cold emails sent, esp, active_campaign_count) JOINed to the
-- infra/email-type label core.sending_account_vendor (DDL 72). 100% of SENDING accounts are
-- vendor-labeled, so true_volume-by-infra is ~100% complete (passes 100%-or-wipe).
CREATE OR REPLACE VIEW derived.v_sending_volume_daily AS
SELECT
    d.date,
    d.workspace_slug,
    COALESCE(v.vendor_category, 'Unlabeled')   AS infra,
    d.esp,
    count(*)                                    AS accounts,
    count(*) FILTER (WHERE d.actual_sends > 0)  AS sending_accounts,
    sum(d.daily_limit)                          AS capacity,
    sum(d.expected_sends)                       AS expected_sends,
    sum(d.actual_sends)                         AS true_volume,
    sum(d.active_campaign_count)                AS active_campaigns
FROM core.sending_account_daily d
LEFT JOIN core.sending_account_vendor v ON lower(d.account_id) = v.account_email
GROUP BY 1, 2, 3, 4;

-- org-wide daily volume by infra (workspace collapsed; filter by infra)
CREATE OR REPLACE VIEW derived.v_sending_volume_daily_by_infra AS
SELECT date, infra,
       sum(accounts) accounts, sum(sending_accounts) sending_accounts,
       sum(capacity) capacity, sum(true_volume) true_volume
FROM derived.v_sending_volume_daily GROUP BY 1, 2;
