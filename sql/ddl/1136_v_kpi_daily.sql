-- @gate: add
-- Depends on 1105
-- Depends on 1110
-- Depends on 1135
-- ============================================================================
-- 1136_v_kpi_daily.sql — the CANONICAL day × workspace KPI layer for the
-- booking-site KPIs tab (R36: KPI tab = LLM labels only, backfill all sound
-- history, warehouse is canonical) + its measured coverage/soundness view.
--
-- TWO OBJECTS:
--   core.v_kpi_coverage_daily — DAY grain: measured coverage inputs (native
--     analytics marks vs recovered event-basis marks vs labeled leads) + the
--     per-metric soundness verdicts. "Compute a coverage table, don't
--     hand-wave" — the ratio columns ARE the evidence for the boundary.
--   core.v_kpi_daily — DAY × WORKSPACE: sent / replies (1105 MAX-stitch
--     restatement), LLM-label counts + append-only event cohorts (1110),
--     meetings (v_meeting_truth by meeting day), opp→meeting inputs, with the
--     day-grain soundness flags joined on.
--
-- SOUNDNESS BOUNDARIES (measured 2026-07-17, this lane):
--   * labels_sound_floor = 2026-05-15. Two independent grounds:
--     (1) standing invariant — Instantly NATIVE positive marks are positive
--         truth ≥ 2026-05-15 only (the labeled universe IS the positive-marked
--         slice, R18); (2) measured mark-recovery: event-basis recovered marks
--         vs native analytics interested ≈ 0 before 2026-03-26 (webhooks
--         start), 0.64 in the Apr-13 outage week, ≥0.88 and multi-source
--         redundant from May (MOF intake 05-28, ledger 06-12). Pre-boundary
--         label columns are NULL in consumers — 100%-or-wipe, never partial.
--   * sends/replies sound from 2024-01-15 (1105 API restatement floor; every
--     calendar day 2024-01-15→now has a stitched row, 914/914 measured).
--     Deleted-workspace absence = zero-COVERAGE, not zero sends (1105 header).
--   * meetings sound from 2024-01-01 (portal im_bookings SoT + BTC backfill).
--   * completed_day encodes R11 (completed sending days only): day is complete
--     when it is before today-ET. Computed ICU-free as now()-5h (EST offset;
--     during EDT a day flips 'complete' up to 1h late — never early).
--
-- LABEL SEMANTICS = the conversion feed's, verbatim (append-only cohorts per
-- Sam's 07-17 cohort ruling): opp_cohort/pos_cohort count DISTINCT leads with
-- an opportunity (resp. opp-or-engagement) label EVENT whose reply falls on
-- the day, across ALL labeler versions (ever-labeled never decrements);
-- labeled/opp/eng/conf/ni are day-grain CURRENT-state (latest labeled_at per
-- workspace×lead×day; deterministic tie-break labeled_at/message_ts/ref_id).
-- Gate classes (auto/bot/labeler_error) excluded from every stat.
-- opp_met = post-reply meeting join (meeting_date >= reply day).
--
-- REVIEWER-SETTLED SEMANTICS (two-key round 1, 2026-07-17 — explicit design
-- choices, not oversights):
--   * labels_sound/labels_state in v_kpi_daily are PER-WORKSPACE: the row gate
--     uses that workspace's OWN label watermark (MAX labeled reply-day), never
--     a global day watermark — a lagging workspace can never be marked sound by
--     other workspaces' progress. v_kpi_coverage_daily stays DAY-grain EVIDENCE
--     (aggregate ratios + global watermark) and is deliberately NOT joined into
--     the fact rows (also removes any fan-out surface).
--   * opp_met attributes a meeting to EVERY opp-reply day on/before it (per-day
--     cohort semantics; NOT additive across days — summing opp_met over a range
--     double-counts multi-day repliers by design; range-level conversion must
--     recompute at lead grain or use the feed/tab range logic).
--   * settling days (within D+2 of the workspace watermark = the rolling
--     re-sweep window) ARE labels_sound=TRUE — the escrow-direct feed's
--     slice-coverage gate decides their DISPLAY; labels_state exposes the
--     distinction for consumers that want to exclude them.
--   * completed_day uses the fixed EST offset (now()-5h): never marks a day
--     complete early; during EDT it flips up to 1h late. Accepted (ICU-free).
--
-- NOTE (freshness): this view reads main.raw_reply_label_event (nightly escrow
-- load) — canonical, promote-cadence. The booking feed stays ESCROW-DIRECT for
-- label freshness (same SQL shapes over the parquets); nothing user-facing
-- waits on a promote. Equivalence is by construction: the entity loads the
-- same escrow rows the feed reads.
--
-- Reversible: DROP VIEW core.v_kpi_daily; DROP VIEW core.v_kpi_coverage_daily.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_kpi_coverage_daily AS
WITH nat AS (
  SELECT date AS day,
         CAST(SUM(sent_stitched) AS BIGINT)           AS native_sent,
         CAST(SUM(replies_human_stitched) AS BIGINT)  AS native_replies_human,
         CAST(SUM(replies_auto_stitched) AS BIGINT)   AS native_replies_auto,
         CAST(SUM(opps_stitched) AS BIGINT)           AS native_opps
  FROM core.v_sends_truth_daily
  GROUP BY 1
),
rec AS (  -- recovered Instantly positive marks, EVENT basis (the per-lead surfaces
          -- the all-time pool was built from; snapshot-basis sources have no true
          -- mark dates and are deliberately absent — that unknowability is itself
          -- why pre-boundary days are unsound)
  SELECT d AS day, COUNT(DISTINCT le) AS recovered_marks_event
  FROM (
    SELECT lower(lead_email) AS le, CAST(opened_at AS DATE) AS d
    FROM core.opportunity
    WHERE "source" = 'instantly' AND lead_email IS NOT NULL
    UNION
    SELECT lower(lead_email), CAST(status_changed_at AS DATE)
    FROM main.raw_comms_instantly_lead_state_event
    WHERE observed_status >= 1 AND lead_email IS NOT NULL
    UNION
    SELECT lower(lead_email), CAST(event_timestamp AS DATE)
    FROM main.raw_pipeline_lead_events
    WHERE event_type IN ('lead_interested', 'lead_closed') AND lead_email IS NOT NULL
  )
  GROUP BY 1
),
lab AS (
  SELECT CAST(message_ts AS DATE) AS day,
         COUNT(DISTINCT lower(lead_email)) AS labeled_leads
  FROM main.raw_reply_label_event
  WHERE lower(CAST(label AS VARCHAR)) IN
        ('opportunity', 'engagement', 'confused', 'not_interested', 'not interested')
  GROUP BY 1
),
wm AS (
  SELECT MAX(CAST(message_ts AS DATE)) AS wm_day
  FROM main.raw_reply_label_event
  WHERE lower(CAST(label AS VARCHAR)) IN
        ('opportunity', 'engagement', 'confused', 'not_interested', 'not interested')
),
spine AS (
  SELECT day FROM nat
  UNION SELECT day FROM rec
  UNION SELECT day FROM lab
)
SELECT
  s.day,
  nat.native_sent, nat.native_replies_human, nat.native_replies_auto, nat.native_opps,
  rec.recovered_marks_event,
  ROUND(rec.recovered_marks_event * 1.0 / NULLIF(nat.native_opps, 0), 3) AS mark_recovery_ratio,
  lab.labeled_leads,
  wm.wm_day AS labels_watermark_day,
  (s.day >= DATE '2024-01-15')                                   AS sends_sound,
  (s.day >= DATE '2024-01-01')                                   AS meetings_sound,
  -- DAY-GRAIN aggregate verdicts — EVIDENCE ONLY (global watermark); the
  -- row-level gate consumers must use is v_kpi_daily's PER-WORKSPACE flags
  CASE WHEN s.day <  DATE '2026-05-15'                THEN 'pre_boundary'
       WHEN s.day >  wm.wm_day                        THEN 'unlabeled'
       WHEN s.day >  wm.wm_day - INTERVAL 2 DAY       THEN 'settling'   -- D+2 rolling re-sweep window
       ELSE 'sound' END                                          AS labels_state,
  (s.day >= DATE '2026-05-15' AND s.day <= wm.wm_day)            AS labels_sound,
  (s.day < CAST(now() - INTERVAL 5 HOUR AS DATE))                AS completed_day
FROM spine s
LEFT JOIN nat ON nat.day = s.day
LEFT JOIN rec ON rec.day = s.day
LEFT JOIN lab ON lab.day = s.day
CROSS JOIN wm;

CREATE OR REPLACE VIEW core.v_kpi_daily AS
WITH nat AS (
  SELECT workspace_slug, date AS day,
         sent_stitched            AS sent,
         replies_human_stitched   AS replies_human,
         replies_auto_stitched    AS replies_auto,
         opps_stitched            AS native_opps    -- Instantly-mark side column, NEVER a KPI number (R18)
  FROM core.v_sends_truth_daily
),
real_ev AS (
  SELECT workspace_slug, lower(lead_email) AS le, CAST(message_ts AS DATE) AS day,
         lower(CAST(label AS VARCHAR)) AS label, labeled_at, message_ts, message_ref_id
  FROM main.raw_reply_label_event
  WHERE lower(CAST(label AS VARCHAR)) IN
        ('opportunity', 'engagement', 'confused', 'not_interested', 'not interested')
),
cur AS (  -- day-grain CURRENT state: latest labeled_at per workspace×lead×day
          -- (tie-break fully deterministic: same-batch equal labeled_at resolved
          --  by latest message then ref id — matches the v4 feed)
  SELECT workspace_slug, day,
         COUNT(*) AS labeled,
         SUM(CASE WHEN label = 'opportunity' THEN 1 ELSE 0 END) AS opp,
         SUM(CASE WHEN label = 'engagement'  THEN 1 ELSE 0 END) AS eng,
         SUM(CASE WHEN label = 'confused'    THEN 1 ELSE 0 END) AS conf,
         SUM(CASE WHEN label IN ('not_interested', 'not interested') THEN 1 ELSE 0 END) AS ni
  FROM (
    SELECT workspace_slug, le, day, label,
           row_number() OVER (PARTITION BY workspace_slug, le, day
                              ORDER BY labeled_at DESC, message_ts DESC, message_ref_id DESC) AS rn
    FROM real_ev
  )
  WHERE rn = 1
  GROUP BY 1, 2
),
ws_wm AS (  -- PER-WORKSPACE label watermark (the row-level soundness gate)
  SELECT workspace_slug, MAX(day) AS wm_day FROM real_ev GROUP BY 1
),
coh AS (  -- append-only event cohorts (ever-labeled, never decrements)
  SELECT workspace_slug, day,
         COUNT(DISTINCT CASE WHEN label = 'opportunity' THEN le END)                  AS opp_cohort,
         COUNT(DISTINCT CASE WHEN label IN ('opportunity', 'engagement') THEN le END) AS pos_cohort
  FROM real_ev
  GROUP BY 1, 2
),
ml AS (
  SELECT DISTINCT lower(lead_email) AS le, meeting_date
  FROM core.v_meeting_truth
  WHERE channel_norm = 'Email' AND is_ours
    AND lead_email IS NOT NULL AND meeting_date IS NOT NULL
),
om AS (  -- post-reply conversion inputs: of the day's opp cohort, who booked on/after
  SELECT oc.workspace_slug, oc.day,
         COUNT(DISTINCT oc.le) AS opp_leads,
         COUNT(DISTINCT CASE WHEN ml.le IS NOT NULL THEN oc.le END) AS opp_met
  FROM (SELECT DISTINCT workspace_slug, le, day FROM real_ev WHERE label = 'opportunity') oc
  LEFT JOIN ml ON ml.le = oc.le AND ml.meeting_date >= oc.day
  GROUP BY 1, 2
),
mt AS (  -- meetings by MEETING day (ws-attributed; 1135 dim backstop)
  SELECT workspace_slug, meeting_date AS day, COUNT(*) AS meetings_booked
  FROM core.v_meeting_truth
  WHERE channel_norm = 'Email' AND is_ours AND meeting_date IS NOT NULL
    AND workspace_slug IS NOT NULL AND workspace_slug <> ''
  GROUP BY 1, 2
),
spine AS (
  SELECT DISTINCT day, workspace_slug FROM (
    SELECT day, workspace_slug FROM nat
    UNION SELECT day, workspace_slug FROM coh
    UNION SELECT day, workspace_slug FROM mt
  )
  WHERE workspace_slug IS NOT NULL AND workspace_slug <> ''
)
SELECT
  s.day, s.workspace_slug,
  w.name AS workspace_name,
  nat.sent, nat.replies_human, nat.replies_auto, nat.native_opps,
  cur.labeled, cur.opp, cur.eng, cur.conf, cur.ni,
  coh.opp_cohort, coh.pos_cohort,
  om.opp_leads, om.opp_met,
  mt.meetings_booked,
  (s.day >= DATE '2024-01-15')                                    AS sends_sound,
  -- PER-WORKSPACE label gate (reviewer-settled): this workspace's own watermark
  (s.day >= DATE '2026-05-15' AND s.day <= ww.wm_day)             AS labels_sound,
  CASE WHEN s.day <  DATE '2026-05-15'                     THEN 'pre_boundary'
       WHEN ww.wm_day IS NULL OR s.day > ww.wm_day         THEN 'unlabeled'
       WHEN s.day >  ww.wm_day - INTERVAL 2 DAY            THEN 'settling'  -- D+2 re-sweep window
       ELSE 'sound' END                                           AS labels_state,
  (s.day >= DATE '2024-01-01')                                    AS meetings_sound,
  (s.day <  CAST(now() - INTERVAL 5 HOUR AS DATE))                AS completed_day
FROM spine s
LEFT JOIN nat   ON nat.day = s.day AND nat.workspace_slug = s.workspace_slug
LEFT JOIN cur   ON cur.day = s.day AND cur.workspace_slug = s.workspace_slug
LEFT JOIN coh   ON coh.day = s.day AND coh.workspace_slug = s.workspace_slug
LEFT JOIN om    ON om.day  = s.day AND om.workspace_slug  = s.workspace_slug
LEFT JOIN mt    ON mt.day  = s.day AND mt.workspace_slug  = s.workspace_slug
LEFT JOIN ws_wm ww ON ww.workspace_slug = s.workspace_slug
LEFT JOIN core.workspace w ON w.slug = s.workspace_slug;
