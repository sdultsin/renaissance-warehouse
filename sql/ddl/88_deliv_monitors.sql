-- =====================================================================
-- DELIVERABILITY SAFE-MONITORS (D2)  — Version 84
-- =====================================================================
-- Built by `deliv-monitors` (bus) per the deliverability/Samuel deep-dive D2 spec:
--   handoffs/2026-06-17-deliverability-samuel-deepdive.md  (Renaissance memory repo)
--   handoffs/2026-06-16-deliverability-cluster-closeout.md (GAP CARRIED — DoD#4 never built)
--
-- WHY: deliverability is the north star. Two leading-indicator monitors that
-- nobody surfaces today, both diagnostic of SOFT-FOLDERING (placement decay):
--
--   1. REPLY-LAG MONITOR  (send -> first-reply latency, RECENT-WINDOW trend).
--      If our mail starts soft-foldering, prospects see it later and reply later,
--      so the send->first-reply latency DRIFTS UP before bounce/RR fully collapse.
--      This is DISTINCT from core.sla_reply_time (DDL 69), which measures OUR
--      response speed to a prospect (a CM handling SLA). This monitor measures the
--      PROSPECT's send->reply lag — a deliverability signal, not a handling one.
--      RECENT-WINDOW by construction: percentiles recomputed per reply_date from
--      the response-level base (never average daily percentiles); the Slack post
--      compares the last-7d window vs the prior-7d window so a recent rise shows.
--
--   2. HUMAN-vs-AUTO REPLY TILE  (daily human-reply rate vs auto/invalid-reply rate).
--      Per the Samuel doc this is "the single most diagnostic metric for
--      soft-foldering" and is on no dashboard today: when mail folders to spam,
--      AUTO/invalid replies (OOO, bounces-as-replies, autoresponders) hold up while
--      HUMAN replies fall, so the auto:human ratio climbs. HUMAN = total - auto, per
--      [[reference_warehouse_reply_and_tag_truth_20260614]]: Instantly NATIVE is the
--      sole truth — unique_replies = human, unique_replies_automatic = auto. We do
--      NOT use core.reply.is_auto_reply (broken heuristic) or any LLM intent.
--
-- SOURCES (canonical, per deliverables/warehouse-query-prompt.md):
--   * Human/auto/total replies + sends: raw_pipeline_campaign_daily_metrics
--     (the COMPLETE daily fact — never core.campaign_daily, which has the delisting
--     bug; never SUM(unique_*) as an absolute across a window for opps, but
--     unique_replies / unique_replies_automatic ARE additive day-counts and that is
--     how the canonical "Human replies"/"Auto replies" metrics are defined).
--   * Send->reply lag: main.raw_pipeline_conversation_messages — ue_type=1 = our
--     automated campaign sends, ue_type=2 = prospect inbound replies. (Same source
--     family DDL 69 uses; verified ue_type distribution + thread pairing 2026-06-18.)
--
-- GRAIN / SCOPE:
--   * Headline grain = ORG-WIDE (all live workspaces). Deliverability degradation is
--     a FLEET signal; blending is fine here (unlike the per-pool placement gap, which
--     a blend hides — that is a separate monitor). We ALSO expose a Funding-scoped cut
--     (the 5 CM slugs) for comparability, via the workspace_slug column + a flag view.
--   * All dates are UTC calendar dates (TZ = Etc/UTC), matching DDL 69.
--
-- PERCENTILE RULE (same as DDL 69): the daily snapshot is the trend source;
--   percentiles CANNOT be averaged across days. Multi-day windows recompute from the
--   response-level base via the rollup view below.
--
-- All objects are CREATE OR REPLACE / IF NOT EXISTS — idempotent across re-runs.
-- The apply path is core.db.apply_ddl_file(version=84, ...). setup_db parses
-- 84 from the filename prefix; no manual schema_version INSERT here.

CREATE SCHEMA IF NOT EXISTS core;

-- =====================================================================
-- MONITOR 1 — REPLY-LAG (send -> first-reply latency)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1a. RESPONSE-LEVEL BASE: one row per (thread, first prospect reply), carrying
--     the send->first-reply latency. The "send" is the LAST of our automated sends
--     (ue_type=1) at or before the prospect's FIRST inbound reply (ue_type=2) in
--     that thread — i.e. the message the prospect was replying to. Built fresh each
--     night (cheap: ~hundreds of k threads). reply_date = the prospect-reply UTC day
--     (the SLA day — we want "how fast did THIS day's replies come back").
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.deliv_reply_lag (
  thread_id                TEXT NOT NULL,
  workspace_slug           TEXT,            -- durable workspace key (the source slug)
  first_reply_ts           TIMESTAMPTZ,     -- prospect's first inbound reply (UTC)
  send_ts                  TIMESTAMPTZ,     -- our last send at/before that reply (UTC)
  lag_minutes              BIGINT,          -- first_reply_ts - send_ts, minutes (>= 0)
  reply_date               DATE,            -- first_reply_ts::DATE (UTC) — the trend day
  _built_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id                  VARCHAR,
  PRIMARY KEY (thread_id)
);
CREATE INDEX IF NOT EXISTS ix_deliv_lag_date ON core.deliv_reply_lag (reply_date);
CREATE INDEX IF NOT EXISTS ix_deliv_lag_ws   ON core.deliv_reply_lag (workspace_slug);

-- ---------------------------------------------------------------------
-- 1b. DAILY SNAPSHOT (the trend source). Per workspace per reply_date: count +
--     median/p25/p75/p90 of lag_minutes. Percentiles snapshotted daily (cannot be
--     averaged). The build re-snapshots a trailing window (late-arriving replies
--     change recent days). ORG totals = recompute from the base via 1d (below).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.deliv_reply_lag_daily (
  reply_date        DATE    NOT NULL,
  workspace_slug    TEXT    NOT NULL,
  n_replies         BIGINT  NOT NULL,
  median_lag_min    DOUBLE,
  p25_lag_min       DOUBLE,
  p75_lag_min       DOUBLE,
  p90_lag_min       DOUBLE,
  _snapshot_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id           VARCHAR,
  PRIMARY KEY (reply_date, workspace_slug)
);
CREATE INDEX IF NOT EXISTS ix_deliv_lag_daily_date ON core.deliv_reply_lag_daily (reply_date);

-- 1c. ORG-WIDE daily reply-lag trend, recomputed from the response-level base (correct
--     percentiles across all workspaces). This is the HEADLINE the Slack monitor reads.
CREATE OR REPLACE VIEW v_deliv_reply_lag_daily_org AS
SELECT
  reply_date,
  count(*)                                  AS n_replies,
  median(lag_minutes)                       AS median_lag_min,
  quantile_cont(lag_minutes, 0.25)          AS p25_lag_min,
  quantile_cont(lag_minutes, 0.75)          AS p75_lag_min,
  quantile_cont(lag_minutes, 0.90)          AS p90_lag_min,
  100.0 * count(*) FILTER (WHERE lag_minutes > 360) / NULLIF(count(*), 0) AS pct_over_6h
FROM core.deliv_reply_lag
WHERE lag_minutes IS NOT NULL AND reply_date IS NOT NULL
GROUP BY reply_date;

-- 1d. ROLLUP — recompute the lag distribution over an ARBITRARY window from the
--     response-level base (never averages daily percentiles). Org grain + a
--     Funding-scoped flag, so the Slack post can do last-7d vs prior-7d cleanly.
CREATE OR REPLACE MACRO deliv_reply_lag_rollup(start_date, end_date) AS TABLE
  SELECT
    ANY_VALUE(CAST(start_date AS DATE))          AS period_start,
    ANY_VALUE(CAST(end_date   AS DATE))          AS period_end,
    count(*)                                     AS n_replies,
    median(lag_minutes)                          AS median_lag_min,
    quantile_cont(lag_minutes, 0.25)             AS p25_lag_min,
    quantile_cont(lag_minutes, 0.75)             AS p75_lag_min,
    quantile_cont(lag_minutes, 0.90)             AS p90_lag_min,
    100.0 * count(*) FILTER (WHERE lag_minutes > 360) / NULLIF(count(*), 0) AS pct_over_6h
  FROM core.deliv_reply_lag
  WHERE lag_minutes IS NOT NULL
    AND reply_date >= CAST(start_date AS DATE)
    AND reply_date <= CAST(end_date   AS DATE);

-- =====================================================================
-- MONITOR 2 — DAILY HUMAN-vs-AUTO REPLY TILE
-- =====================================================================
-- Instantly-native truth ONLY (reply-truth reference): human = unique_replies,
-- auto = unique_replies_automatic, total = human + auto. Rates are over SENDS that
-- day (the canonical reply-rate denominator). The auto:human RATIO is the headline
-- soft-foldering signal (climbs when human replies fold to spam but autoresponders
-- still land). One row per (date, workspace_slug); org = re-aggregate.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_deliv_human_auto_reply_daily AS
SELECT
  date                                                          AS reply_date,
  workspace_id                                                  AS workspace_slug,
  SUM(sent)                                                     AS sent,
  SUM(unique_replies)                                           AS human_replies,
  SUM(unique_replies_automatic)                                 AS auto_replies,
  SUM(unique_replies) + SUM(unique_replies_automatic)           AS total_replies,
  100.0 * SUM(unique_replies)           / NULLIF(SUM(sent), 0)  AS human_reply_rate_pct,
  100.0 * SUM(unique_replies_automatic) / NULLIF(SUM(sent), 0)  AS auto_reply_rate_pct,
  100.0 * (SUM(unique_replies) + SUM(unique_replies_automatic))
                                        / NULLIF(SUM(sent), 0)  AS total_reply_rate_pct,
  -- the diagnostic: auto replies per human reply. > 1.0 = auto outpaces human =
  -- classic soft-foldering signature (the doc: "HUMAN reply rate < AUTO every week
  -- since April"). NULL-safe.
  1.0 * SUM(unique_replies_automatic) / NULLIF(SUM(unique_replies), 0) AS auto_to_human_ratio
FROM raw_pipeline_campaign_daily_metrics
WHERE workspace_id IS NOT NULL
GROUP BY date, workspace_id;

-- ORG-WIDE daily human-vs-auto tile (the headline the Slack monitor reads).
CREATE OR REPLACE VIEW v_deliv_human_auto_reply_daily_org AS
SELECT
  date                                                          AS reply_date,
  SUM(sent)                                                     AS sent,
  SUM(unique_replies)                                           AS human_replies,
  SUM(unique_replies_automatic)                                 AS auto_replies,
  SUM(unique_replies) + SUM(unique_replies_automatic)           AS total_replies,
  100.0 * SUM(unique_replies)           / NULLIF(SUM(sent), 0)  AS human_reply_rate_pct,
  100.0 * SUM(unique_replies_automatic) / NULLIF(SUM(sent), 0)  AS auto_reply_rate_pct,
  100.0 * (SUM(unique_replies) + SUM(unique_replies_automatic))
                                        / NULLIF(SUM(sent), 0)  AS total_reply_rate_pct,
  1.0 * SUM(unique_replies_automatic) / NULLIF(SUM(unique_replies), 0) AS auto_to_human_ratio
FROM raw_pipeline_campaign_daily_metrics
GROUP BY date;

-- Funding-scoped cut (the 5 CM slugs) — comparable view for the CM lens.
CREATE OR REPLACE VIEW v_deliv_human_auto_reply_daily_funding AS
SELECT
  date                                                          AS reply_date,
  SUM(sent)                                                     AS sent,
  SUM(unique_replies)                                           AS human_replies,
  SUM(unique_replies_automatic)                                 AS auto_replies,
  100.0 * SUM(unique_replies)           / NULLIF(SUM(sent), 0)  AS human_reply_rate_pct,
  100.0 * SUM(unique_replies_automatic) / NULLIF(SUM(sent), 0)  AS auto_reply_rate_pct,
  1.0 * SUM(unique_replies_automatic) / NULLIF(SUM(unique_replies), 0) AS auto_to_human_ratio
FROM raw_pipeline_campaign_daily_metrics
WHERE workspace_id IN ('renaissance-4','renaissance-5','prospects-power','koi-and-destroy','renaissance-2')
GROUP BY date;

-- ---------------------------------------------------------------------
-- REGISTRY: apply via core.db.apply_ddl_file(version=84,...).
-- Filename prefix 84 is parsed by setup_db. No manual schema_version INSERT.
-- ---------------------------------------------------------------------
