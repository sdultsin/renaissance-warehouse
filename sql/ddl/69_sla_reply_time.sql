-- =====================================================================
-- SLA REPLY-TIME METRIC  (Version 69 — APPLIED 2026-06-14)
-- =====================================================================
-- RENUMBERED from placeholder 68 -> 69 at apply time. Sequence in the post-cutover
-- idle writer window: 66=SMS, 67=workspace soft-delete, 68=workspace fact-driven
-- views, 69=this SLA reply-time, 70=portal gap dims. Live schema_version was 65
-- immediately before this window.
--   apply via: apply_ddl_file(conn, <this file>, version=69)
--
-- WHAT THIS BUILDS
--   1. core.sla_reply_time           — response-level fact (one row per response-pair),
--                                       WORKSPACE-AWARE. This is the queryable base that
--                                       weekly/monthly/custom rollups RECOMPUTE percentiles
--                                       from (you cannot average percentiles across days).
--   2. core.sla_reply_time_daily     — daily SNAPSHOT: per workspace per day, count + avg +
--                                       median + q25 + q50 + q75 of response_latency_minutes.
--                                       This is the TREND source (percentiles snapshotted daily).
--   3. v_sla_reply_time_daily        — convenience read view over the snapshot (joins the
--                                       soft-deleted-safe workspace name).
--   4. fn-style ROLLUP via a MACRO + view: recompute stats over an arbitrary date range
--      from the response-level rows (NOT from daily percentiles).
--
-- SEMANTICS (confirmed by Sam):
--   * Track EVERY prospect-message -> our-response latency within a thread, repeating per
--     back-and-forth. A thread may have 1, 2, or 10 such pairs -> ROWS, one per pair
--     (seq_in_thread = 1..N), NOT wide response_time_1..N columns.
--   * AIM + IM COMBINED. We cannot split AI vs human (no such flag in the source). Both
--     land in the source as ue_type=3 outbound_manual replies -> already combined here.
--
-- SOURCE / GRAIN VERIFICATION (2026-06-14, read-only):
--   Base = core.iam_response_time (DDL 50) — ALREADY one row per prospect reply (the
--   response-pair grain). 834,590 rows; 135,250 with an IM response (16%); 699,340
--   unanswered (response_bucket='no_response'). thread_reply_number = seq_in_thread.
--   GAP #1: it has NO workspace column. FIX: carry workspace_id (= the SLUG) from the
--           source main.raw_pipeline_conversation_messages, joined on m.id = irt.email_id
--           (verified: 834,590/834,590 join). Do NOT attribute via core.campaign
--           (only 8.5% of responded rows join — campaigns get deleted; the slug persists).
--   GAP #2: workspace_id in the source is the SLUG (e.g. 'koi-and-destroy'), NOT the UUID.
--           Join to core.workspace on slug, LEFT (some slugs are deleted/renamed workspaces
--           not in the 16 live rows: warm-leads, the-dyad, renaissance-6/7/8, outlook-2,
--           erc-2). Keep the slug as the durable workspace key; name is enrichment only.
--   Timezone: source + base are TIMESTAMPTZ; DB TimeZone = Etc/UTC. All dates below are
--           UTC calendar dates (prospect_replied_at::DATE). Note for the dashboard.

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------
-- 1. RESPONSE-LEVEL FACT (workspace-aware). Queryable base for rollups.
--    Built fresh each night from core.iam_response_time + the source workspace_id.
--    Only RESPONDED pairs (iam_responded_at NOT NULL) carry a latency; we keep the
--    no-response rows too (latency NULL) so coverage/answer-rate is derivable, but the
--    daily snapshot and rollups aggregate only WHERE response_latency_minutes IS NOT NULL.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sla_reply_time (
  response_id              TEXT NOT NULL,    -- = core.iam_response_time.id (email_id||'_'||seq)
  thread_id                TEXT,
  workspace_slug           TEXT,             -- the durable workspace key (source slug)
  campaign_id              TEXT,             -- best-effort; often unmatched in core.campaign
  lead_email               TEXT,
  seq_in_thread            INTEGER NOT NULL, -- = thread_reply_number (1 = first reply in thread)
  prospect_msg_ts          TIMESTAMPTZ,      -- prospect_replied_at (UTC)
  our_reply_ts             TIMESTAMPTZ,      -- iam_responded_at (UTC); NULL if no response yet
  response_latency_minutes INTEGER,          -- our_reply_ts - prospect_msg_ts, in minutes; NULL if unanswered
  reply_date               DATE,             -- prospect_msg_ts::DATE (UTC) — the SLA day
  _built_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id                  VARCHAR
);
-- @@INDEXES@@
CREATE INDEX IF NOT EXISTS ix_sla_rt_date   ON core.sla_reply_time (reply_date);
CREATE INDEX IF NOT EXISTS ix_sla_rt_ws     ON core.sla_reply_time (workspace_slug);
CREATE INDEX IF NOT EXISTS ix_sla_rt_thread ON core.sla_reply_time (thread_id);

-- ---------------------------------------------------------------------
-- 2. DAILY SNAPSHOT (the TREND source). Per workspace per day.
--    Percentiles MUST be snapshotted daily — you cannot average percentiles across days.
--    This table is APPEND/UPSERT keyed (reply_date, workspace_slug); a nightly rebuild of
--    the trailing window re-derives recent days (late-arriving IM responses change them).
--    For weekly/monthly/custom spans DO NOT average these percentile columns — use the
--    rollup macro/view (#4) which recomputes from core.sla_reply_time.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.sla_reply_time_daily (
  reply_date        DATE    NOT NULL,
  workspace_slug    TEXT    NOT NULL,
  n_responses       BIGINT  NOT NULL,   -- count of ANSWERED response-pairs that day
  avg_latency_min   DOUBLE,
  median_latency_min DOUBLE,            -- q50 (p50)
  q25_latency_min   DOUBLE,             -- p25
  q50_latency_min   DOUBLE,             -- p50 (== median; kept distinct per Sam's column list)
  q75_latency_min   DOUBLE,             -- p75
  _snapshot_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id           VARCHAR,
  PRIMARY KEY (reply_date, workspace_slug)
);
CREATE INDEX IF NOT EXISTS ix_sla_daily_date ON core.sla_reply_time_daily (reply_date);

-- ---------------------------------------------------------------------
-- 3. READ VIEW over the daily snapshot (adds soft-delete-safe workspace name).
--    Window-correct by construction: a deleted workspace appears only on the days it
--    actually has snapshot rows (ties into the workspace-deletion lifecycle work).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sla_reply_time_daily AS
SELECT
  d.reply_date,
  d.workspace_slug,
  COALESCE(w.name, d.workspace_slug) AS workspace_name,
  (w.workspace_id IS NULL)           AS workspace_orphaned,  -- TRUE = deleted/renamed, not in live dim
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
-- 4. ROLLUP — recompute stats per workspace over an ARBITRARY date range
--    FROM THE RESPONSE-LEVEL ROWS (correct percentiles; never averages daily ones).
--
--    DuckDB has no parameterised SQL "function" object, so the rollup is delivered two
--    ways; use whichever the serving layer prefers:
--
--    (a) MACRO (table macro) — call like a function with a date range:
--          SELECT * FROM sla_reply_time_rollup('2026-06-01', '2026-06-14');
--        Recomputes count/avg/median/q25/q50/q75 over [start_date, end_date] inclusive,
--        per workspace, straight from core.sla_reply_time. THIS is the weekly/monthly/
--        custom-span source of truth.
--
--    (b) v_sla_reply_time_rollup_period — a convenience view exposing common windows
--        (7d / 30d / MTD / QTD) so a dashboard can read pre-shaped buckets without
--        passing parameters. Also recomputed from the response-level rows.
-- ---------------------------------------------------------------------
CREATE OR REPLACE MACRO sla_reply_time_rollup(start_date, end_date) AS TABLE
  SELECT
    workspace_slug,
    -- The macro params are scalar (constant per call) but must be aggregated or
    -- grouped alongside the GROUP BY; ANY_VALUE keeps them out of the GROUP BY.
    ANY_VALUE(CAST(start_date AS DATE))                   AS period_start,
    ANY_VALUE(CAST(end_date   AS DATE))                   AS period_end,
    count(*)                                              AS n_responses,
    avg(response_latency_minutes)                         AS avg_latency_min,
    median(response_latency_minutes)                      AS median_latency_min,
    quantile_cont(response_latency_minutes, 0.25)         AS q25_latency_min,
    quantile_cont(response_latency_minutes, 0.50)         AS q50_latency_min,
    quantile_cont(response_latency_minutes, 0.75)         AS q75_latency_min
  FROM core.sla_reply_time
  WHERE response_latency_minutes IS NOT NULL
    AND reply_date >= CAST(start_date AS DATE)
    AND reply_date <= CAST(end_date   AS DATE)
  GROUP BY workspace_slug;

-- Convenience: standard windows in one view (each recomputed from response-level rows).
-- 'today' is evaluated at query time (CURRENT_DATE, UTC).
CREATE OR REPLACE VIEW v_sla_reply_time_rollup_period AS
WITH base AS (
  SELECT workspace_slug, reply_date, response_latency_minutes
  FROM core.sla_reply_time
  WHERE response_latency_minutes IS NOT NULL
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
  avg(b.response_latency_minutes)                   AS avg_latency_min,
  median(b.response_latency_minutes)                AS median_latency_min,
  quantile_cont(b.response_latency_minutes, 0.25)   AS q25_latency_min,
  quantile_cont(b.response_latency_minutes, 0.50)   AS q50_latency_min,
  quantile_cont(b.response_latency_minutes, 0.75)   AS q75_latency_min
FROM base b
JOIN windows win
  ON b.reply_date >= CAST(win.lo AS DATE)
 AND b.reply_date <= CAST(win.hi AS DATE)
GROUP BY win.period, win.lo, win.hi, b.workspace_slug;

-- ---------------------------------------------------------------------
-- REGISTRY: the apply path is core.db.apply_ddl_file(version=69,...).
-- Filename = 69_sla_reply_time.sql; setup_db parses 69 from the prefix.
-- No manual INSERT here — db.apply_ddl_file records core.schema_version itself.
-- ---------------------------------------------------------------------
