-- @gate: add
-- Depends on 1141
-- ============================================================================
-- 1142_instantly_account_daily.sql — warehouse-native per-account daily analytics
-- (sent + human/auto reply split) + per-domain reply-rate rollup.
--
-- WHY: Instantly's GET /accounts/analytics/daily whole-workspace pull began 413-ing
-- ("Payload Too Large — add an emails filter or request a smaller date range") once
-- workspaces crossed ~a few hundred accounts. That single break silently zeroed THREE
-- per-account "actuals" surfaces at the 2026-07-09→07-10 boundary:
--   * DP-v2 public.infra_account_daily_metrics  (frozen 07-09; lane being DROPPED)
--   * core.sending_account_daily.actual_sends    (0 since 07-10 — repaired out-of-band
--       by chunking the account-truth job's same /accounts/analytics/daily pull)
--   * per-account human/auto reply split          (never had a warehouse-native home)
-- See deliverables/2026-07-18-peraccount-reply-metrics-answer.md +
--     deliverables/2026-07-18-account-daily-sync/.
--
-- ROOT FIX (in entities/instantly_account_daily.py, NOT here): the 413 is account-COUNT
-- driven, not date-range driven. The sync pages the per-account pull by the `emails`
-- filter (batches of <=100 accounts/request — verified live 2026-07-18: whole-workspace
-- renaissance-1 @ 13,607 accounts 413s; emails-filtered batches of 200 return 200, 500
-- 413s). A single filtered request may span the whole rolling window (one row per
-- account per active day).
--
-- GRAIN: one row per (account_email, metric_date). `unique_replies` = HUMAN replies
-- (per-lead dedup); `unique_replies_automatic` = AUTO replies — Instantly's own
-- authoritative split, the SAME columns core.campaign_daily.replies_human/_auto are
-- built from at campaign grain (sibling /campaigns/analytics/daily). This mirrors the
-- schema of the dead main.raw_pipeline_infra_account_daily_metrics so downstream is
-- drop-in.
--
-- provider_group is BEST-EFFORT (google|outlook|imap|other) mapped from the Instantly
-- account provider_code at pull time — NOT the dead pipeline's OTD-aware business
-- taxonomy ('google_otd'); consumers needing true vendor should join
-- core.sending_account_vendor. domain = split_part(account_email,'@',2).
--
-- LOAD: entities/instantly_account_daily.py (phase 'replies_late'; upsert ON CONFLICT
-- (account_email, metric_date)). Rolling WAREHOUSE_ACCOUNT_DAILY_DAYS window nightly;
-- one-off backfill via `python -m entities.instantly_account_daily --start D --end D`.
--
-- Reversible: DROP VIEW core.v_domain_reply_daily; DROP TABLE main.raw_instantly_account_daily.
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.raw_instantly_account_daily (
  account_email             VARCHAR NOT NULL,
  metric_date               DATE    NOT NULL,
  workspace_slug            VARCHAR,
  domain                    VARCHAR,
  provider_group            VARCHAR,          -- best-effort google|outlook|imap|other (provider_code)
  sent                      BIGINT,
  bounced                   BIGINT,
  contacted                 BIGINT,
  new_leads_contacted       BIGINT,
  opened                    BIGINT,
  unique_opened             BIGINT,
  replies                   BIGINT,
  unique_replies            BIGINT,           -- HUMAN replies (per-lead dedup)
  replies_automatic         BIGINT,
  unique_replies_automatic  BIGINT,           -- AUTO replies (per-lead dedup)
  clicks                    BIGINT,
  unique_clicks             BIGINT,
  api_synced_at             TIMESTAMPTZ,       -- when this row was pulled from Instantly
  _loaded_at                TIMESTAMPTZ DEFAULT now(),
  _run_id                   VARCHAR,
  PRIMARY KEY (account_email, metric_date)
);

-- Per-domain human/auto reply-rate rollup (the deliverable's ask). Re-grains the
-- workspace-grain main.v_deliv_human_auto_reply_daily from workspace to domain, and
-- reads the native per-account table instead of the pipeline mirror. Uses the unique_
-- columns (per-lead dedup), matching the campaign-grain convention.
-- SEMANTICS: human_replies/auto_replies are SUMS of per-account, per-lead-deduped
-- unique_* counts. At domain grain that is an UPPER BOUND — a lead that replied to two
-- accounts in the same domain is counted twice (dedup is per-account, not per-domain).
-- This is intentional and matches both v_deliv_human_auto_reply_daily and the dead
-- pipeline it replaces; a true per-domain unique count would need a lead-grain reply
-- source. Domain-only grain merges a domain across workspaces (the per-domain ask).
CREATE OR REPLACE VIEW core.v_domain_reply_daily AS
SELECT
    metric_date                                                     AS date,
    domain,
    sum(sent)                                                       AS sent,
    sum(unique_replies)                                             AS human_replies,
    sum(unique_replies_automatic)                                   AS auto_replies,
    (sum(unique_replies) + sum(unique_replies_automatic))           AS total_replies,
    (100.0 * sum(unique_replies))           / nullif(sum(sent), 0)  AS human_reply_rate_pct,
    (100.0 * sum(unique_replies_automatic)) / nullif(sum(sent), 0)  AS auto_reply_rate_pct
FROM main.raw_instantly_account_daily
WHERE domain IS NOT NULL
GROUP BY metric_date, domain;
