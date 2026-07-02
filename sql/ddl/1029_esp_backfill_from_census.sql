-- @gate: data-backfill
-- Depends on 95
-- DDL version 1029.
-- LIVE-NOW twin of entities/sending_dq.py D3b ("ESP BACKFILL pass 2, from the census").
--
-- core.sending_account_daily.esp is derived from the account-truth CSV's infra_type
-- (Google/Outlook/OTD -> google/outlook/otd, else NULL). Two backfills then run on the
-- rebuilt table: D3 projects esp from core.sending_account_vendor (DDL 72; recovers ~91%
-- of esp-NULL sending rows). The residual (~5-8k sends/day) are accounts that sent but are
-- ALSO absent from the vendor table -> stay NULL. Measured 2026-06-27: 603 accounts /
-- 8,139 sends on 2026-06-25, 100% present in core.account_label (DDL 95, the phantom-free
-- MX-infra census) as Active OTD, 0 in core.sending_account_vendor.
--
-- account_label IS the canonical infra resolver the Sending-Truth lens
-- (scripts/sending_truth_dashboard_data.py) and the June-26 report's Section-4 join already
-- trust, so this projects its infra onto esp for whatever the vendor pass left NULL. It
-- closes the esp-NULL-with-sends hole to ~0 (100%-or-flag) so esp-GROUPED reads (e.g.
-- derived.v_sending_volume_daily) match the account_label-join truth — killing the exact
-- "by-infra split disagrees with Grace" class of bug.
--
-- Idempotent + safe on rebuild: setup_db.py applies DDLs BEFORE entities populate the
-- table, so on a fresh rebuild this UPDATE runs against an empty core.sending_account_daily
-- (no-op); the entity then repopulates and its own D3b pass does the work. Applied via
-- apply-now it heals the currently-served snapshot in minutes. Only ever touches esp IS NULL
-- rows, so re-running never re-writes a resolved esp (no double-apply). Source is deduped to
-- one infra per email via arg_max(infra, census_date) so the pick is deterministic.
-- Kept byte-consistent with entities/sending_dq.py::run_sending_dq D3b.
-- CASE arms must stay in lockstep with the inner `WHERE infra IN (...)` list below.
-- The ELSE sad.esp is defence-in-depth: if a future edit ever adds an infra value to that
-- WHERE-IN without a matching CASE arm, this is a no-op (keeps the existing value) rather
-- than silently overwriting esp with NULL.
UPDATE core.sending_account_daily AS sad
SET esp = CASE lab.infra
              WHEN 'OTD'     THEN 'otd'
              WHEN 'Google'  THEN 'google'
              WHEN 'Outlook' THEN 'outlook'
              ELSE sad.esp
          END
FROM (
    SELECT lower(email) AS email,
           arg_max(infra, census_date) AS infra
    FROM core.account_label
    WHERE infra IN ('OTD', 'Google', 'Outlook')
    GROUP BY lower(email)
) AS lab
WHERE sad.esp IS NULL
  AND lab.email = lower(sad.account_id);
