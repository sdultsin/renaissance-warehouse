-- @gate: add
-- Depends on 105
-- 108_ws6_sending_truth_cube.sql  [2026-06-21]  WS6 portal-data-rebuild — sending-truth cube.
-- Applied via apply_ddl_file(version=108). Idempotent CREATE OR REPLACE views only — no table writes.
-- Live MAX(core.schema_version)=104 this session (103=WS1 normalization, 104=account_census). 105/106/107 are
--   the WS3/WS4/WS5 slots that land BEFORE this file in the reconciled batch; 108 is the intended WS6 slot.
--   Re-check `SELECT max(version) FROM core.schema_version` immediately before apply (the moderator does this and
--   bumps the whole remaining block by any delta if the nightly moved the floor — C1/C7). A taken/duplicate
--   version PK-dedupes => the WHOLE migration silently no-ops, so the free-slot check is MANDATORY.
--
-- PURPOSE: point-in-time Sending-Truth cube. Replaces the inline re-classification of the inflated
--   main.raw_account_truth_daily_actuals inside scripts/sending_truth_dashboard_data.py so:
--   (1) fulfillment > 100% is IMPOSSIBLE — numerator & denominator measured over the SAME census-active
--       account-set per date, AND sent is capped at expected PER ACCOUNT (sent_capped). The generator + app.js
--       consume sent_capped_eligible/_assigned so the RENDERED pill is bounded too (B1).
--   (2) the Outlook phantom (1.11M dead accounts, ~44 real sends) is excluded — membership comes from
--       core.account_census (live set); the dl=1 warm-only Outlook pool is trimmed by the dl>=2 floor (B/C).
--   (3) capacity is point-in-time per census_date from the TRUE census daily_limit (bimodal 5/15, NOT flat-15).
--   (4) the cube max date is clamped to LEAST(max census, max daily) so the headline date has actuals (B2).
--
-- ============================================================================================================
-- WS6-PREP RENUMBER + VALIDATION NOTES [2026-06-21] (this staged file, vs the source design ws6-sending-truth-v2.md):
--   * RENUMBERED 107 -> 108 (source assumed live MAX=102; live MAX is 104, WS1+WS2 already deployed).
--   * CRITICAL FIX — census STATUS axis is INTEGER, the LABEL is the string. The source DDL compared
--     `census_status NOT IN ('paused','connection_error',...)` against core.account_census.status which is an
--     INTEGER (1/-1/-3/2). That predicate would NEVER match a string -> is_eligible would be TRUE for ALL
--     statuses (connection_error/sending_error/paused all leak into "eligible"). FIXED here to read
--     status_label (VARCHAR: 'active'/'connection_error'/'sending_error'/'paused' — VERIFIED live enum).
--   * status/warmup label-collision: this DDL carries BOTH disambiguated axes through (census_status_label =
--     connection axis; census_warmup_label = warmup axis) so a downstream consumer can never confuse them.
--     The authoritative disambiguation (U2 relabel) lands in the WS3 block (v105); WS6 reads status_label
--     directly here so it is correct even if that view isn't present yet.
--   * census daily_limit is DOUBLE; cast to the cube as-is (capacity is a SUM of DOUBLE -> fine).
--   * No object-name collisions: v_sending_truth_bounds / _account_pit / _pit do not exist live (VERIFIED).
-- ============================================================================================================
--
-- COLUMN CONTRACT (frozen, CONTRACT C2) — bind to these EXACT names (ALL VERIFIED present in live schema):
--   core.sending_account: infra_provider, esp, lifecycle_state, retired_at, is_active, first_cold_send_at,
--                         first_seen_at, last_seen_at, email, workspace_slug, daily_limit.   [VERIFIED]
--   core.account_census : census_date, email, workspace_slug, provider_code, status, status_label,
--                         warmup_status, warmup_status_label, daily_limit.                   [VERIFIED]
--   core.sending_account_daily: date, account_id(=lower email), daily_limit, expected_sends,
--                         actual_sends, active_campaign_count, workspace_slug.               [VERIFIED]
--   vendor side-table   : core.sending_account_vendor(account_email, vendor_category).       [VERIFIED]
--
-- DEPENDS ON (must land first, in this order, in the reconciled batch):
--   104 WS2 core.account_census   — DEPLOYED + POPULATED (314,887 rows, census_date 2026-06-21). [LIVE]
--   105 WS3 core.sending_account  — REBUILT to census-active set + census-derived daily_limit + lifecycle.
--       *** NOT YET DEPLOYED. WS6 MUST gate AFTER 105. Reading TODAY's pre-WS3 inflated sending_account
--       (1.36M rows / 0 retired) would re-inflate; the census INNER-membership protects the count but
--       lifecycle_state/first_cold_send_at are not yet populated until WS3/WS4. ***
--   106 WS4 account_label taxonomy — NOTE: WS4 writes title-case 'Active'/'Warmup' to the SEPARATE table
--       core.account_label.lifecycle, NOT to core.sending_account.lifecycle_state. The cube reads
--       sending_account.lifecycle_state, which is LOWERCASE live ('active'/'paused'/'retired'/'warming'/'warmed'
--       — VERIFIED 2026-06-21) and WS3 carries forward as-is (COALESCE(h.lifecycle_state,'unknown')). The
--       eligibility predicate below therefore uses case-insensitive lower(lifecycle_state)='active' (R1 resolved).

-- effective_max_date = LEAST(max census_date, max daily.date). All account-grain rows clamp to <= this so the
-- newest census date with no daily actuals can never become the headline (B2). NOTE [WS6-PREP]: today census
-- carries ONLY 2026-06-21 and daily maxes at 2026-06-20 -> effective_max_date=06-20, but census has NO 06-20
-- row, so the cube is EMPTY today. This is the snapshot-timing/census-depth trap fixed in the WS3 block (U2);
-- WS6 inherits a healthy multi-date census from WS3. See REVIEW.md F1.
CREATE OR REPLACE VIEW core.v_sending_truth_bounds AS
SELECT LEAST(
         (SELECT max(census_date) FROM core.account_census),
         (SELECT max(date)        FROM core.sending_account_daily)
       ) AS effective_max_date;

-- v_sending_truth_account_pit — one row per (census_date, email) over the CENSUS-ACTIVE set only,
-- carrying point-in-time daily_limit/expected/actual + MX infra (infra_provider) + vendor + eligibility.
CREATE OR REPLACE VIEW core.v_sending_truth_account_pit AS
WITH bounds AS (SELECT effective_max_date FROM core.v_sending_truth_bounds),
census AS (
  SELECT census_date AS date, lower(email) AS email, workspace_slug,
         provider_code,
         status_label   AS census_status_label,   -- CONNECTION axis (string; FIX: was INTEGER status)
         warmup_status_label AS census_warmup_label, -- WARMUP axis (string; carried through, disambiguated)
         daily_limit    AS census_daily_limit       -- TRUE per-account cap (bimodal 5/15) — DOUBLE
  FROM core.account_census
  WHERE census_date <= (SELECT effective_max_date FROM bounds)   -- B2 clamp
),
sa AS (
  -- WS3 rebuilt account dim. C2 EXACT names: infra_provider / lifecycle_state / is_active / first_cold_send_at.
  SELECT lower(email) AS email, infra_provider, esp, lifecycle_state,
         is_active AS sa_is_active, first_cold_send_at, first_seen_at, retired_at,
         workspace_slug AS sa_workspace_slug
  FROM core.sending_account
),
ven AS (
  SELECT lower(account_email) AS email, max(vendor_category) AS vendor_category
  FROM core.sending_account_vendor GROUP BY 1                    -- vendor side-table (C2)
),
day AS (
  SELECT date, lower(account_id) AS email, workspace_slug AS day_workspace_slug,
         daily_limit, expected_sends, actual_sends,
         coalesce(active_campaign_count, 0) AS active_campaign_count
  FROM core.sending_account_daily
)
SELECT
  c.date,
  c.email,
  coalesce(c.workspace_slug, sa.sa_workspace_slug, d.day_workspace_slug) AS workspace_slug,
  -- infra CLASS = MX-based infra_provider (WS3); OTD is the residual default per the MX waterfall.
  coalesce(sa.infra_provider, 'OTD')                                     AS infra,
  coalesce(ven.vendor_category, '(untagged)')                           AS vendor,
  coalesce(sa.lifecycle_state, 'Unknown')                              AS lifecycle,
  -- point-in-time capacity for this account on this date: TRUE census daily_limit (the live cap);
  -- fall back to the daily-fact limit only if the census didn't carry one.
  coalesce(c.census_daily_limit, d.daily_limit, 0)                      AS daily_limit,
  coalesce(d.expected_sends, c.census_daily_limit, 0)                   AS expected_sends,
  greatest(coalesce(d.actual_sends, 0), 0)                              AS actual_sends,
  d.active_campaign_count,
  c.census_status_label,
  c.census_warmup_label,
  sa.first_cold_send_at,
  -- COLD-ELIGIBLE (C6): census-active CONNECTION status AND cold-capable lifecycle AND daily_limit>=2.
  -- The daily_limit>=2 floor EXCLUDES the dl=1 warm-only Outlook pool (59,501 accounts live). Membership
  -- from the census kills the departed/phantom 1.11M. lifecycle 'active' = cold-capable.
  -- FIX: compares status_label (string), defaulting 'active' if census missing the row.
  -- CASE FIX [WS6-PREP 2026-06-21]: core.sending_account.lifecycle_state is LOWERCASE live ('active'
  --   713,640 / 'paused' / 'retired' / 'warming' / 'warmed' — VERIFIED). WS3's generator carries it forward as
  --   COALESCE(h.lifecycle_state,'unknown') (no title-case), and WS4 writes title-case 'Active' to the SEPARATE
  --   core.account_label.lifecycle table, NOT here. Testing ='Active' join-missed every row -> eligible
  --   collapsed (166K, A-CAP FAIL). lower(lifecycle_state)='active' restores the census-active band (1.66M live).
  (coalesce(c.census_status_label, 'active') = 'active'
     AND lower(coalesce(sa.lifecycle_state, 'active')) = 'active'
     AND coalesce(c.census_daily_limit, d.daily_limit, 0) >= 2)         AS is_eligible,
  (coalesce(c.census_status_label, 'active') = 'active'
     AND lower(coalesce(sa.lifecycle_state, 'active')) = 'active'
     AND coalesce(c.census_daily_limit, d.daily_limit, 0) >= 2
     AND coalesce(d.active_campaign_count, 0) > 0)                      AS is_campaign_assigned_eligible
FROM census c
LEFT JOIN sa  ON sa.email  = c.email
LEFT JOIN ven ON ven.email = c.email
LEFT JOIN day d ON d.email = c.email AND d.date = c.date;

-- v_sending_truth_pit — the cube grain the generator consumes. One row per
-- (date, workspace_slug, infra, vendor, lifecycle), with capped fulfillment that CANNOT exceed 1.
CREATE OR REPLACE VIEW core.v_sending_truth_pit AS
SELECT
  date, workspace_slug, infra, vendor, lifecycle,
  count(*)                                                           AS account_count,
  count(*) FILTER (WHERE is_eligible)                               AS eligible_account_count,
  coalesce(sum(expected_sends), 0)                                  AS configured_capacity,
  coalesce(sum(expected_sends) FILTER (WHERE NOT is_eligible), 0)   AS excluded_capacity,
  coalesce(sum(expected_sends) FILTER (WHERE is_eligible), 0)       AS eligible_capacity,
  coalesce(sum(expected_sends) FILTER (WHERE is_campaign_assigned_eligible), 0)
                                                                     AS campaign_assigned_capacity,
  coalesce(sum(actual_sends), 0)                                    AS actual_sends,
  -- account-level cap: sent can never exceed that account's own expected (kills per-account overshoot, B1).
  coalesce(sum(least(greatest(actual_sends,0), greatest(expected_sends,0))), 0)
                                                                     AS sent_capped,
  -- capped sent restricted to the eligible set (this is the numerator the bounded pill divides).
  coalesce(sum(least(greatest(actual_sends,0), greatest(expected_sends,0)))
             FILTER (WHERE is_eligible), 0)                         AS sent_capped_eligible,
  coalesce(sum(least(greatest(actual_sends,0), greatest(expected_sends,0)))
             FILTER (WHERE is_campaign_assigned_eligible), 0)       AS sent_capped_assigned,
  coalesce(sum(CASE WHEN is_eligible THEN greatest(expected_sends - actual_sends, 0)
                    ELSE 0 END), 0)                                 AS missing_volume,
  -- headline metric, bounded to [0,1] BY CONSTRUCTION (capped numerator / eligible denominator).
  CASE WHEN coalesce(sum(expected_sends) FILTER (WHERE is_eligible), 0) > 0
       THEN coalesce(sum(least(greatest(actual_sends,0), greatest(expected_sends,0)))
                       FILTER (WHERE is_eligible), 0)
            / sum(expected_sends) FILTER (WHERE is_eligible)
       ELSE NULL END                                                AS fulfillment,
  count(*) FILTER (WHERE actual_sends = 0 AND expected_sends > 0)   AS zero_send_accounts,
  count(*) FILTER (WHERE is_eligible AND coalesce(active_campaign_count,0) = 0)
                                                                     AS no_campaign_accounts,
  count(*) FILTER (WHERE NOT is_eligible)                           AS bad_status_accounts,
  count(*) FILTER (WHERE coalesce(daily_limit,0) = 0)               AS zero_limit_accounts
FROM core.v_sending_truth_account_pit
GROUP BY 1,2,3,4,5;
