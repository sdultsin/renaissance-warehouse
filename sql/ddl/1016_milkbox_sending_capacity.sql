-- @gate: add
-- core.v_milkbox_sending_capacity — planned cold-send capacity for the MilkBox inbox batch.
-- Applied at schema version 1016.
--
-- WHAT: per (workspace, density tier) — the configured cold-email send capacity of the MilkBox
-- inboxes. MilkBox provisioned ~1,000 domains across Funding 1/2/4 with 25 / 49 / 75 inboxes PER
-- DOMAIN (the "MilkBox 25/49/75" tiers). The configured per-inbox cold-send rate is INVERSE to
-- density so every domain lands at ~constant ~200 emails/day:
--     25 inboxes/domain -> 8 cold emails/inbox/day   (25 x 8  = 200/domain)
--     49 inboxes/domain -> 4 cold emails/inbox/day   (49 x 4  = 196/domain)
--     75 inboxes/domain -> 3 cold emails/inbox/day   (75 x 3  = 225/domain)
-- (rates per David, 2026-06-25). This view applies those rates to the ACTUAL per-domain inbox
-- counts in core.account_registry to give the real daily cold-email capacity — replacing the wrong
-- flat ~15/inbox estimate (which over-counted ~4x).
--
-- GRAIN / SCOPE: FULL PROVISIONED capacity = ALL MilkBox inboxes at full ramp, NOT current live
-- volume. For WHICH inboxes are warmed/active on a given date (the warmup->active timeline) use
-- core.v_warmup_golive_schedule / v_warmup_golive_daily; a freshly-activated inbox also ramps up to
-- its rate over ~1-2 weeks. Tier is DERIVED from each domain's inbox count (count(*) per domain),
-- because David's "MilkBox Active/Warmup 25/49/75" tags were not yet synced into the census at build
-- time; the derived counts match the tag tiers exactly. A domain whose inbox count is not one of
-- 25/49/75 yields a NULL rate + NULL capacity (fail-visible, never a silent 0) — none exist today.

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_milkbox_sending_capacity AS
WITH dom AS (
  SELECT workspace_label, domain, count(*) AS inboxes_per_domain
  FROM core.account_registry
  WHERE vendor = 'MilkBox'
  GROUP BY 1, 2
)
SELECT
  workspace_label                                   AS workspace,
  inboxes_per_domain                                AS tier,                       -- 25 | 49 | 75 inboxes/domain
  CASE inboxes_per_domain WHEN 25 THEN 8 WHEN 49 THEN 4 WHEN 75 THEN 3 END
                                                    AS cold_emails_per_inbox_day,  -- NULL if tier unexpected
  count(*)                                          AS domains,
  count(*) * inboxes_per_domain                     AS inboxes,
  count(*) * inboxes_per_domain
    * CASE inboxes_per_domain WHEN 25 THEN 8 WHEN 49 THEN 4 WHEN 75 THEN 3 END
                                                    AS daily_email_capacity        -- inboxes x per-inbox rate
FROM dom
GROUP BY 1, 2
ORDER BY 1, 2;
