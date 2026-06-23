-- @gate: data-backfill
-- Depends on 31
-- DDL version 1003 (claimed via moderator next-version; earlier hand-picked 96/114 were taken).
-- One-shot LIVE heal of the upstream account-truth generator's "-999 zeroing".
--
-- The account-truth daily generator stamps any sender it cannot match to its
-- (stale/incomplete) account_inventory snapshot as account_status=-999 /
-- infra_type='Missing Current Inventory' / provider_code=0 / daily_limit=0 /
-- expected_sends=0. That silently reports 0 cold capacity for real, active
-- sending accounts (measured 2026-06-23: ~360k/day of fleet cold capacity hidden;
-- F2 renaissance-5 Google = 3,307 active accts shown as 0 while sending ~45k/day).
--
-- core.sending_account already carries the correct ESP + daily_limit for these.
-- This statement reasserts capacity from that source of truth for every -999 row
-- whose email resolves to an ACTIVE account with a resolved ESP and daily_limit>0.
-- It is the live-now twin of entities/sending_dq.py::_reassert_capacity_from_core
-- (which heals the raw table on every nightly rebuild). Applied via apply-now this
-- heals the currently-served snapshot in minutes.
--
-- Idempotent + safe on rebuild: setup_db.py applies DDLs BEFORE entities populate
-- the table, so on a fresh rebuild this UPDATE runs against an empty table (no-op);
-- the entity then repopulates + reasserts. It only ever touches account_status=-999
-- rows, so re-running never double-applies. Conservative: leaves retired /
-- esp-unresolved / warming (daily_limit<=0) rows untouched -> no capacity overcount.
UPDATE raw_account_truth_daily_actuals AS f
SET infra_type = CASE sa.esp
        WHEN 'google' THEN 'Google'
        WHEN 'outlook' THEN 'Outlook'
        WHEN 'otd' THEN 'OTD'
        ELSE f.infra_type END,
    provider_code = CASE sa.esp
        WHEN 'otd' THEN 1
        WHEN 'google' THEN 2
        WHEN 'outlook' THEN 3
        ELSE f.provider_code END,
    account_status = 1,
    account_status_label = 'Active (reasserted from core.sending_account)',
    daily_limit = sa.daily_limit,
    expected_sends = sa.daily_limit,
    delta = sa.daily_limit - COALESCE(f.actual_sends, 0),
    fulfillment = CASE WHEN sa.daily_limit > 0
        THEN COALESCE(f.actual_sends, 0)::DOUBLE / sa.daily_limit END,
    warning_flags = NULLIF(TRIM(BOTH ';' FROM
        COALESCE(f.warning_flags, '') || ';reasserted_from_core'), '')
FROM core.sending_account sa
WHERE f.account_status = -999
  AND LOWER(sa.email) = LOWER(f.email)
  AND sa.workspace_slug = f.workspace_slug
  AND sa.is_active
  AND sa.esp IS NOT NULL
  AND sa.daily_limit > 0;
