-- DDL 59 (2026-06-12): Add has_errors to core.sending_account + capacity view.
--
-- "Has Errors" = accounts with a negative Instantly status code that are NOT
-- Missing Current Inventory:
--   status = -1  Connection Error
--   status = -2  Soft Bounce Error
--   status = -3  Sending Error
-- (status = -999 is 'Missing Current Inventory' — retired/phantom, is_active=false)
--
-- has_errors is derived entirely from raw_account_truth_accounts.status (INTEGER),
-- which is already in _SRC_COLS — no generator or mirror changes needed.
--
-- Also fixes the lifecycle_state mapping for -2/-3 (previously fell through to
-- 'warming'; correct is 'paused').
--
-- Adds derived.v_sending_account_capacity: per-workspace active/errored/total_active
-- matching the Instantly/CM dashboard "Total Active Accounts (Active − Has Errors)".

ALTER TABLE raw_account_truth_accounts
    ADD COLUMN IF NOT EXISTS has_errors BOOLEAN;

ALTER TABLE core.sending_account
    ADD COLUMN IF NOT EXISTS has_errors BOOLEAN;

CREATE SCHEMA IF NOT EXISTS derived;

-- v_sending_account_capacity — per-workspace capacity counters matching the
-- Instantly dashboard display (Active, Has Errors, Total Active = Active − Has Errors).
-- One row per workspace_slug over the current (most-recent) snapshot.
CREATE OR REPLACE VIEW derived.v_sending_account_capacity AS
SELECT
    workspace_slug,
    count(*) FILTER (WHERE is_active)                               AS active,
    count(*) FILTER (WHERE COALESCE(has_errors, FALSE))             AS errored,
    count(*) FILTER (WHERE is_active AND NOT COALESCE(has_errors, FALSE)) AS total_active,
    COALESCE(sum(daily_limit) FILTER (
        WHERE is_active AND NOT COALESCE(has_errors, FALSE)
    ), 0)                                                           AS sendable_daily_limit
FROM core.sending_account
GROUP BY workspace_slug
ORDER BY workspace_slug;
