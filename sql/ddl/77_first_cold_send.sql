-- 77_first_cold_send.sql  [2026-06-16 infra-data-truth / C3]
-- first_cold_send_at on core.sending_account: the date an account first sent a COLD email (earliest
-- date with actual_sends>0 in core.sending_account_daily). Distinct from warmup_started_at — the
-- distinction Sam cares about for honest per-cohort deliverability-over-account-age analysis was
-- previously unmodeled. Populated by backfill_warmup_coldstart.py. Additive, idempotent.
ALTER TABLE core.sending_account ADD COLUMN IF NOT EXISTS first_cold_send_at TIMESTAMP WITH TIME ZONE;
