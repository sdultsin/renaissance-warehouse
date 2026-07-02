-- @gate: data-backfill
-- Depends on 106, 79
-- One-shot LIVE heal of core.account_label lifecycle to the capacity-gated rule.
--
-- DEPENDENCY NOTE (core.account_registry): the MilkBox subquery references core.account_registry, which is
-- created by DDL 79 (CREATE TABLE IF NOT EXISTS core.account_registry). Since 79 < 1014, setup_db applies it
-- before this statement, so the table is GUARANTEED to exist when this runs — empty on a fresh rebuild (the
-- subquery then matches nothing and the MilkBox guard no-ops), populated live. The entity guards the join
-- with has_reg only for partial/test DBs; in the canonical apply path the table always exists. Even with an
-- empty registry the result is correct: the daily_limit>0 capacity gate alone moves every current (cap=0)
-- MilkBox warmer to Warmup; the registry guard only adds the date-window hold for any future MilkBox inbox
-- that carries a cold limit mid-warmup.
--
-- BACKGROUND. core.account_label.lifecycle was "Active = ever-cold-sent (any
-- core.sending_account_daily row with actual_sends>0), else Warmup" (DDL 106 /
-- entities/account_label.py). A one-time warmup-ramp blip records a cold send and
-- permanently flips a still-warming inbox to "Active" — which is why ~44.7k MilkBox
-- Outlook accounts (daily_limit=0, still warming) showed as Active on the
-- Sending-Volume-Truth dashboard. Verified live 2026-06-24.
--
-- FIX (binary D1 preserved). Active now requires BOTH ever-cold-sent AND a live cold
-- daily_limit>0; MilkBox is additionally held in Warmup for a 2-week warmup window
-- (warmup_start + 14d; Darcy/Sam 2026-06-24), identified via the batch sheet
-- core.account_registry vendor='MilkBox'. Net effect validated live before ship:
--   OTD     Active 162,650 / cap 2,437,450  -> UNCHANGED
--   Google  Active  30,888 / cap   458,212  -> UNCHANGED
--   Outlook Active  44,685 / cap    18,756  -> 1,443 / cap 18,756 (43,242 cap=0 warmers -> Warmup)
-- i.e. zero real cold capacity moves; only the cap=0 warmup-ramp mislabels are corrected.
--
-- This statement is the live-now twin of entities/account_label.py's new lifecycle
-- (kept logically identical so a live-healed row equals a nightly-rebuilt row). Applied
-- via apply-now it heals the currently-served snapshot in minutes; the nightly rebuild
-- then keeps it correct from the entity.
--
-- PREDICATE IDENTITY (cold_start ≡ ever-cold-sent) — proving live heal == nightly rebuild:
-- core.account_label.cold_start is the MATERIALIZED first-actual-cold-send date. The entity builds it as
--     cold AS (SELECT lower(account_id) AS email, MIN(date) AS cold_start, ...
--              FROM core.sending_account_daily WHERE actual_sends > 0 GROUP BY 1)
-- and writes cold.cold_start into this column. So `cold_start IS NOT NULL` is EXACTLY "exists a
-- core.sending_account_daily row with actual_sends>0 for this account" — the same population the entity's
-- lifecycle CASE tests via `cold.email IS NOT NULL`. cold_start is NOT a scheduled/warmup-ramp start (that
-- is timestamp_warmup_start, used here only for the MilkBox 2-week window). Hence this UPDATE and the
-- nightly rebuild label IDENTICAL rows — no flip-back on the next nightly. (Column verified present on
-- core.account_label 2026-06-24: cold_start, timestamp_warmup_start, daily_limit, warmup_status all exist;
-- no CHECK constraint pins lifecycle_basis, so the added basis enum values are safe.)
--
-- reason_uncertain ELSE preserves the row's existing value: it applies ONLY to non-cold rows
-- (cold_start NULL, not MilkBox-warming), whose reason_uncertain branches in the entity are UNCHANGED by
-- this fix — so the preserved value already equals what the nightly rebuild writes (no stale/inconsistent text).
--
-- PARTITION TARGET = max(census_date) over core.account_label (NOT over core.account_census). This is
-- deliberate and is the correct heal target: account_label's max partition IS the one currently materialized
-- and served to the dashboard, so it is exactly what we must heal now. In steady state it equals
-- max(census_date FROM core.account_census) (the partition the nightly entity rebuilds). If account_label
-- transiently LAGS census (census promoted, label not yet rebuilt), anchoring on max(account_census) would
-- target a partition that has NO account_label rows yet -> the UPDATE would no-op and the served bug would
-- persist; anchoring on account_label's own max heals the served snapshot regardless. The next nightly then
-- rebuilds its partition with the identical entity logic, so no flip-back.
--
-- Idempotent + safe on rebuild: setup_db.py applies DDLs BEFORE entities populate, so on
-- a fresh rebuild this UPDATE runs against an empty/partial partition (no-op) and the
-- entity repopulates with the same logic. Re-running only ever recomputes the same labels
-- from the row's own columns -> never double-applies. Non-destructive: rewrites only the
-- lifecycle / lifecycle_confidence / lifecycle_basis / reason_uncertain label columns of
-- the latest census partition; no rows added or removed, no capacity touched.
UPDATE core.account_label AS al
SET
  lifecycle = CASE
      WHEN lower(al.email) IN (SELECT lower(email) FROM core.account_registry WHERE vendor = 'MilkBox' AND email IS NOT NULL)
           AND al.timestamp_warmup_start IS NOT NULL
           AND al.census_date < CAST(al.timestamp_warmup_start AS DATE) + 14
        THEN 'Warmup'
      WHEN al.cold_start IS NOT NULL AND COALESCE(al.daily_limit, 0) > 0 THEN 'Active'
      ELSE 'Warmup' END,
  lifecycle_confidence = CASE
      WHEN lower(al.email) IN (SELECT lower(email) FROM core.account_registry WHERE vendor = 'MilkBox' AND email IS NOT NULL)
           AND al.timestamp_warmup_start IS NOT NULL
           AND al.census_date < CAST(al.timestamp_warmup_start AS DATE) + 14
        THEN 'confident'
      WHEN al.cold_start IS NOT NULL AND COALESCE(al.daily_limit, 0) > 0 THEN 'confident'
      ELSE 'uncertain' END,
  lifecycle_basis = CASE
      WHEN lower(al.email) IN (SELECT lower(email) FROM core.account_registry WHERE vendor = 'MilkBox' AND email IS NOT NULL)
           AND al.timestamp_warmup_start IS NOT NULL
           AND al.census_date < CAST(al.timestamp_warmup_start AS DATE) + 14
        THEN 'milkbox_2wk_warmup'
      WHEN al.cold_start IS NOT NULL AND COALESCE(al.daily_limit, 0) > 0 THEN 'cold_send_history'
      WHEN al.cold_start IS NOT NULL THEN 'cold_history_no_live_capacity'
      WHEN al.daily_limit > 0 AND al.warmup_status IN (1, 0) THEN 'capacity_only_no_cold'
      WHEN al.warmup_status = -1 AND al.daily_limit > 0 THEN 'warmup_banned_dl_pos_no_cold'
      WHEN al.timestamp_warmup_start IS NULL THEN 'no_warmup_start_no_cold'
      ELSE 'unclassified_no_cold' END,
  reason_uncertain = CASE
      WHEN lower(al.email) IN (SELECT lower(email) FROM core.account_registry WHERE vendor = 'MilkBox' AND email IS NOT NULL)
           AND al.timestamp_warmup_start IS NOT NULL
           AND al.census_date < CAST(al.timestamp_warmup_start AS DATE) + 14
        THEN NULL
      WHEN al.cold_start IS NOT NULL AND COALESCE(al.daily_limit, 0) > 0 THEN NULL
      WHEN al.cold_start IS NOT NULL THEN 'cold_history_but_no_live_daily_limit'
      ELSE al.reason_uncertain END
WHERE al.census_date = (SELECT max(census_date) FROM core.account_label);
