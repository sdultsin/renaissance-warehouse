-- =====================================================================
-- SLA REPLY-TIME METRIC  (Version 1070 — canonical business-minute clock)
-- =====================================================================
-- @gate: alter-type
-- Depends on 69
--
-- WHY THIS SUPERSEDES DDL 69 (DR-7, 2026-07-03):
--   DDL 69 built core.sla_reply_time from core.iam_response_time as a per-reply
--   (all-seq) fact measured in RAW WALL-CLOCK UTC minutes. That is NOT the metric
--   the daily report ships. The report's §6 (render_daily.py, PR #151, validated to
--   the decimal against the handoff reference — deliverables/2026-07-02-sla-scrutiny/
--   FINDINGS.md) is a DIFFERENT, spec-true clock, locked with Grace 2026-06-30:
--     * FIRST prospect reply only (seq_in_thread = 1 per thread) — load-bearing:
--       FINDINGS §1 shows counting every reply halves R1's median (28->9) and ~2x n.
--     * BUSINESS MINUTES accrued ONLY inside 12:00-20:00 ET Mon-Fri (IMs log in 11am
--       ET; first hour = catch-up; no nights/weekends). DST-correct via zoneinfo.
--     * Each thread buckets on the ET date its SLA clock OPENS (clock_open_date), not
--       the raw UTC arrival date — an off-window/weekend arrival opens next window.
--   Until now that clamp lived ONLY in render_daily.py, so a warehouse consumer and
--   the report could not share one definition. This migrates the clamp into the
--   canonical fact so §6 reads `seq_in_thread=1` here and every consumer agrees.
--
-- SOURCE / GRAIN (DR-7, reconciled 2026-07-03 against the validated §6 numbers):
--   Base = core.email_message (ue_type 2 = inbound prospect reply, ue_type 3 = our
--   outbound reply) — the EXACT source §6 validated on. First-reply pairing:
--     inbound.seq = row_number() OVER (PARTITION BY thread_id, workspace_id
--                                      ORDER BY message_at, message_id); keep seq=1.
--     our reply   = min(ue_type=3 message_at) matched on thread_id AND workspace_id
--                   with message_at > the first prospect reply. Unanswered rows are
--                   KEPT (our_reply_ts / biz_latency_minutes NULL) so answer-rate is
--                   derivable; the daily snapshot + rollups aggregate only answered.
--   GRAIN = THREAD (thread_id, workspace_id), NOT lead_email. This is the reconciled
--   choice: the validated reference IS thread-grain, so §6 reproduces its numbers with
--   ZERO delta reading seq_in_thread=1 here. The thread-vs-lead_email dedupe delta
--   (a lead with 2 threads counts twice) was MEASURED — R1 0.3-0.4%, Leo up to 4.5%,
--   Warm ~9% of first-replies (FINDINGS §4; reproduced 2026-07-03). It is exposed as
--   an OPTIONAL lead-grain rollup view (#5) for consumers who want lead-level dedup;
--   it does not change the thread-grain fact §6 reads.
--   `workspace_slug` = the source workspace_id (the durable SLUG). LEFT-join core.workspace
--   for the display name (some slugs are deleted/renamed workspaces not in the live dim).
--   biz_latency_minutes + clock_open_date are computed in build_sla_reply_time.py by the
--   IDENTICAL _biz_minutes / _clock_open_date functions render_daily.py §6 uses — so the
--   materialized values are digit-for-digit what §6 computed inline (by construction).
--
-- BLENDED IM+AIM — RESOLVED (2026-07-06; supersedes the FINDINGS §3 / PR #175 caveat): the
--   source DOES carry a per-message human/AI split — the /emails item's `ai_agent_id`
--   (non-null on AI-authored messages), surfaced as core.email_message.ai_agent_id / is_aim
--   by DDL 1081. The answering ue_type=3 message's flag is joined onto every fact row as
--   our_reply_ai_agent_id / our_reply_is_aim, so consumers can split the SLA clock
--   human-vs-AIM instead of reading a blended median.

CREATE SCHEMA IF NOT EXISTS core;

-- Staged table, redefined (DDL 69 -> 1070). Consumer audit (2026-07-03, clears DR-7's
-- "no live consumer" for the DROP):
--   * render/orchestrator/nightly: DO NOT read it (render_daily.py §6 only STARTS reading it
--     with this change).
--   * scripts/build_deliv_reply_lag.py: COMMENT-only reference (builds a different table,
--     core.deliv_reply_lag) — not a consumer.
--   * scripts/apply_gap_dimensions_20260614.py: a ONE-TIME backfill script (not in nightly/
--     orchestrator/cron). It SELECTs `response_latency_minutes` — PRESERVED here as an alias
--     of raw_latency_minutes — so it still resolves; and it calls build_sla_reply_time.main(),
--     which now builds this new schema. It does not depend on all-seq rows or lead_email grain.
-- Hence the DROP is non-destructive. The nightly build DROP+CREATE+INSERTs it every run anyway.
DROP TABLE IF EXISTS core.sla_reply_time;

-- ---------------------------------------------------------------------
-- 1. RESPONSE-LEVEL FACT — first-reply (seq=1) business-minute SLA clock, thread-grain.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sla_reply_time (
  response_id              TEXT NOT NULL,    -- stable key: thread_id || '|' || workspace_slug (one first-reply pair per thread+ws)
  thread_id                TEXT,
  workspace_slug           TEXT,             -- durable workspace key (= source workspace_id slug)
  campaign_id              TEXT,             -- best-effort; often unmatched in core.campaign
  lead_email               TEXT,             -- carried for the optional lead-grain rollup (#5)
  seq_in_thread            INTEGER NOT NULL, -- = 1 (first prospect reply in the thread). §6 reads seq_in_thread=1.
  prospect_msg_ts          TIMESTAMPTZ,      -- first prospect reply message_at (UTC)
  our_reply_ts             TIMESTAMPTZ,      -- min ue_type=3 message_at after the first prospect reply (UTC); NULL if unanswered
  our_reply_ai_agent_id    TEXT,             -- ai_agent_id of THE answering message (core.email_message, DDL 1081); NULL = human answer or unanswered
  our_reply_is_aim         BOOLEAN,          -- TRUE = AIM (AI-authored) answer · FALSE = human answer · NULL = unanswered (three-state by design)
  biz_latency_minutes      DOUBLE,           -- §6 business-minute clock (12-20 ET Mon-Fri) prospect->our reply; NULL if unanswered
  raw_latency_minutes      DOUBLE,           -- raw wall-clock minutes our_reply_ts - prospect_msg_ts (reference); NULL if unanswered
  response_latency_minutes DOUBLE,           -- back-compat alias of raw_latency_minutes (DDL-69 consumers); NULL if unanswered
  clock_open_date          DATE,             -- ET date the SLA clock OPENS (the report bucket day); set even if unanswered
  reply_date               DATE,             -- prospect_msg_ts::DATE (UTC) — kept for debug/back-compat
  _built_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id                  VARCHAR
);
-- @@INDEXES@@
CREATE INDEX IF NOT EXISTS ix_sla_rt_clockopen ON core.sla_reply_time (clock_open_date);
CREATE INDEX IF NOT EXISTS ix_sla_rt_ws        ON core.sla_reply_time (workspace_slug);
CREATE INDEX IF NOT EXISTS ix_sla_rt_thread    ON core.sla_reply_time (thread_id);

-- ---------------------------------------------------------------------
-- 2. DAILY SNAPSHOT (the TREND source) — bucketed on CLOCK_OPEN_DATE, business-minute stats.
--    Percentiles can't be averaged across days -> snapshot them daily; recompute spans via #4.
--    Rebuilt FULL-HISTORY from the fact each nightly run (the fact is cheap + fully rebuilt, so a
--    full re-snapshot is uniformly clock_open_date-bucketed — no accumulation gap, no cutover
--    discontinuity). reply_date (UTC) is intentionally NOT carried here; clock_open_date (ET) is the
--    spec-true SLA bucket. No consumer reads this table today (§6 recomputes weekly from the fact).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS core.sla_reply_time_daily;
CREATE TABLE IF NOT EXISTS core.sla_reply_time_daily (
  clock_open_date    DATE    NOT NULL,       -- the ET SLA bucket day
  workspace_slug     TEXT    NOT NULL,
  n_responses        BIGINT  NOT NULL,       -- answered first-reply pairs clock-opening that day
  avg_latency_min    DOUBLE,                 -- business minutes
  median_latency_min DOUBLE,
  q25_latency_min    DOUBLE,
  q50_latency_min    DOUBLE,
  q75_latency_min    DOUBLE,
  _snapshot_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id            VARCHAR,
  PRIMARY KEY (clock_open_date, workspace_slug)
);
CREATE INDEX IF NOT EXISTS ix_sla_daily_clockopen ON core.sla_reply_time_daily (clock_open_date);

-- ---------------------------------------------------------------------
-- 3. READ VIEW over the daily snapshot (adds soft-delete-safe workspace name).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sla_reply_time_daily AS
SELECT
  d.clock_open_date,
  d.workspace_slug,
  COALESCE(w.name, d.workspace_slug) AS workspace_name,
  (w.workspace_id IS NULL)           AS workspace_orphaned,
  d.n_responses,
  d.avg_latency_min,
  d.median_latency_min,
  d.q25_latency_min,
  d.q50_latency_min,
  d.q75_latency_min,
  d._snapshot_at
FROM core.sla_reply_time_daily d
LEFT JOIN core.workspace w ON w.slug = d.workspace_slug;

-- ---------------------------------------------------------------------
-- 4. ROLLUP MACRO + convenience view — recompute business-minute stats over an ARBITRARY
--    clock-open date range FROM THE RESPONSE-LEVEL ROWS (correct percentiles; never averages
--    daily ones). This is the weekly/monthly/custom-span source of truth, and exactly what
--    §6's trailing-7d weekly recomputes.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MACRO sla_reply_time_rollup(start_date, end_date) AS TABLE
  SELECT
    workspace_slug,
    ANY_VALUE(CAST(start_date AS DATE))               AS period_start,
    ANY_VALUE(CAST(end_date   AS DATE))               AS period_end,
    count(*)                                          AS n_responses,
    avg(biz_latency_minutes)                          AS avg_latency_min,
    median(biz_latency_minutes)                       AS median_latency_min,
    quantile_cont(biz_latency_minutes, 0.25)          AS q25_latency_min,
    quantile_cont(biz_latency_minutes, 0.50)          AS q50_latency_min,
    quantile_cont(biz_latency_minutes, 0.75)          AS q75_latency_min
  FROM core.sla_reply_time
  WHERE seq_in_thread = 1
    AND biz_latency_minutes IS NOT NULL
    AND clock_open_date >= CAST(start_date AS DATE)
    AND clock_open_date <= CAST(end_date   AS DATE)
  GROUP BY workspace_slug;

CREATE OR REPLACE VIEW v_sla_reply_time_rollup_period AS
WITH base AS (
  SELECT workspace_slug, clock_open_date, biz_latency_minutes
  FROM core.sla_reply_time
  WHERE seq_in_thread = 1 AND biz_latency_minutes IS NOT NULL
),
windows AS (
  SELECT 'last_7d'  AS period, CURRENT_DATE - INTERVAL 6 DAY  AS lo, CURRENT_DATE AS hi
  UNION ALL SELECT 'last_30d', CURRENT_DATE - INTERVAL 29 DAY, CURRENT_DATE
  UNION ALL SELECT 'mtd',      DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE
  UNION ALL SELECT 'qtd',      DATE_TRUNC('quarter', CURRENT_DATE), CURRENT_DATE
)
SELECT
  win.period,
  CAST(win.lo AS DATE)                              AS period_start,
  CAST(win.hi AS DATE)                              AS period_end,
  b.workspace_slug,
  count(*)                                          AS n_responses,
  avg(b.biz_latency_minutes)                        AS avg_latency_min,
  median(b.biz_latency_minutes)                     AS median_latency_min,
  quantile_cont(b.biz_latency_minutes, 0.25)        AS q25_latency_min,
  quantile_cont(b.biz_latency_minutes, 0.50)        AS q50_latency_min,
  quantile_cont(b.biz_latency_minutes, 0.75)        AS q75_latency_min
FROM base b
JOIN windows win
  ON b.clock_open_date >= CAST(win.lo AS DATE)
 AND b.clock_open_date <= CAST(win.hi AS DATE)
GROUP BY win.period, win.lo, win.hi, b.workspace_slug;

-- ---------------------------------------------------------------------
-- 5. OPTIONAL LEAD-GRAIN ROLLUP — dedup a lead's multiple threads to its EARLIEST first
--    reply per (workspace, lead_email). Provided for consumers who want lead-level dedup;
--    §6 does NOT use it. Delta vs thread-grain measured (FINDINGS §4): R1 ~0.3-0.4%,
--    Leo up to 4.5%, Warm ~9% of first-replies. NULL-email rows fall back to thread-grain.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sla_reply_time_lead_grain AS
WITH firsts AS (
  SELECT
    workspace_slug,
    COALESCE(NULLIF(lower(lead_email), ''), 'thr:' || thread_id) AS lead_key,
    clock_open_date, biz_latency_minutes, prospect_msg_ts,
    row_number() OVER (
      PARTITION BY workspace_slug,
                   COALESCE(NULLIF(lower(lead_email), ''), 'thr:' || thread_id)
      ORDER BY prospect_msg_ts, thread_id
    ) AS lead_rank
  FROM core.sla_reply_time
  WHERE seq_in_thread = 1 AND biz_latency_minutes IS NOT NULL
)
SELECT workspace_slug, lead_key, clock_open_date, biz_latency_minutes
FROM firsts
WHERE lead_rank = 1;

-- ---------------------------------------------------------------------
-- REGISTRY: applied via core.db.apply_ddl_file(version=1070). setup_db parses 1070 from
-- the filename prefix. No manual schema_version INSERT here.
-- ---------------------------------------------------------------------
