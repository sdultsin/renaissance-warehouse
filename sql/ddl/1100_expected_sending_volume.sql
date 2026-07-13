-- Version 1100 (2026-07-12) — EXPECTED daily sending volume (the standing "how much SHOULD each
-- workspace send" number), per workspace and per infra.
-- @gate: add
-- Depends on 72 (core.sending_account_vendor). Also reads core tables account_census,
-- account_campaign_live, workspace (built by sync/entities, present before derived views).
--
-- Definition (Sam, 2026-07-12): expected volume = the sum of each account's CONFIGURED sending
-- volume (daily_limit), per infra and per workspace — but counted ONLY over accounts that are
-- actually DEPLOYED, i.e. status=active AND in >=1 active campaign. The in-campaign gate is what
-- makes the config-sum accurate:
--   * the naive "sum ALL accounts" overstates badly (idle/warmup/dead inboxes carry a daily_limit
--     but never send — e.g. Max's workspace balloons to 4.3M vs ~174k real);
--   * the old OTD+Reseller-Active-tag-only sum UNDERstated (missed MilkBox / Google / RG senders —
--     F2 read 467k vs 533k real).
-- Gating on active-campaign membership scopes the sum to exactly the deployed cold-senders.
--
-- Validated 2026-07-12 against live actual sends (derived.v_workspace_send_daily) — reproduces
-- reality within a few % on every desk: F2 527,085 vs 532,657 actual; F3 507,885 vs 506,282;
-- F1 393,150 vs 390,777; Max's 185,280 vs 174,355 (was 2.5M under the preset method).
--
-- Sources: core.account_census (per-account daily_limit + status; daily snapshot),
-- core.account_campaign_live (LIVE campaign membership; == Instantly in_campaign), used instead of
-- core.sending_account_daily.active_campaign_count which is unreliable (reads ~9k in-campaign for a
-- desk that truly has ~35k). Infra label from core.sending_account_vendor (DDL 72); friendly
-- workspace name from core.workspace. Point-in-time (latest census_date) because campaign
-- membership is live-only.

CREATE OR REPLACE VIEW derived.v_expected_sending_volume_by_infra AS
WITH latest AS (SELECT max(census_date) AS cd FROM core.account_census),
deployed AS (
    SELECT c.census_date,
           c.email,
           c.workspace_slug,
           c.daily_limit,
           COALESCE(v.vendor_category, 'Unlabeled') AS infra
    FROM core.account_census c
    JOIN latest ON c.census_date = latest.cd
    JOIN core.account_campaign_live l
      ON l.account_email = c.email
     AND l.workspace_slug = c.workspace_slug
     AND l.n_active_campaigns > 0
    LEFT JOIN core.sending_account_vendor v ON lower(c.email) = v.account_email
    WHERE c.status = 1
)
SELECT
    d.census_date,
    d.workspace_slug,
    w.name                       AS workspace_name,
    d.infra,
    count(*)                     AS inboxes,
    sum(d.daily_limit)::BIGINT   AS expected_daily_volume
FROM deployed d
LEFT JOIN core.workspace w ON w.slug = d.workspace_slug
GROUP BY 1, 2, 3, 4;

-- per-workspace rollup (infra collapsed)
CREATE OR REPLACE VIEW derived.v_expected_sending_volume AS
SELECT census_date, workspace_slug, workspace_name,
       sum(inboxes)                       AS inboxes,
       sum(expected_daily_volume)::BIGINT AS expected_daily_volume
FROM derived.v_expected_sending_volume_by_infra
GROUP BY 1, 2, 3;
