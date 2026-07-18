-- @gate: add
-- Depends on 1136
-- ============================================================================
-- 1138_v_kpi_weekly_monthly.sql — MOF-19: period rollups of the canonical KPI
-- layer + engaged→booked / confused→booked conversion surface (R38 extension).
--
-- THREE OBJECTS (all layered on the DDL-1136 day×workspace KPI layer):
--   core.v_kpi_daily (REPLACED, additive — 1125 precedent: original column order
--     preserved EXACTLY, new columns APPENDED):
--       eng_cohort / conf_cohort — append-only event cohorts for engagement
--         resp. confused, same convention as opp_cohort/pos_cohort (DISTINCT
--         leads with a label EVENT whose reply falls on the day, across ALL
--         labeler versions; ever-labeled never decrements).
--       opp_to_booked / eng_to_booked / conf_to_booked — day-grain simple
--         conversion ratios = meetings_booked (by MEETING day) ÷ that day's
--         cohort. R38 lag-mismatch convention (Sam simple-ratio ruling
--         2026-07-17): same-window simple division, cross-period lag accepted
--         ("it will be every day", steady-state washes out), no cold-start
--         correction, >100% possible by design — NO ≤100% assertion. NULL
--         unless the day is labels_sound (per-workspace gate).
--   core.v_kpi_weekly  — ISO week (Mon–Sun) × workspace rollup.
--   core.v_kpi_monthly — calendar month × workspace rollup.
--
-- PERIOD SOUNDNESS (the 100%-or-wipe rule at period grain — NEVER blend unsound
-- days into a rollup):
--   * labels: a period is label-sound ONLY if EVERY calendar day of the FULL
--     period passes the per-workspace day gate (day >= 2026-05-15 AND day <= the
--     workspace's OWN label watermark) — equivalently period_start >= 2026-05-15
--     AND period_end <= wm_day. An in-progress / partially-labeled / boundary-
--     straddling period ships NULL in every label-derived column (the tab's
--     '—'); the days_labels_sound coverage column still shows how many of its
--     days are individually sound. The watermark is recovered from the daily
--     layer itself (MAX day WHERE labels_sound, per workspace) — identical to
--     1136's ws_wm by construction.
--   * sends/replies: native columns are NULL unless the WHOLE period starts
--     on/after the 1105 restatement floor (2024-01-15) — kills the partial
--     Jan-2024 month; no ISO week is affected (2024-01-15 is a Monday).
--   * meetings: sound from 2024-01-01 (flag emitted; every period in range
--     passes). meetings_booked is by MEETING day (v_meeting_truth semantics).
--   * period_complete mirrors 1136 completed_day (fixed EST offset now()-5h,
--     ICU-free): TRUE when every day of the period is a completed sending day.
--   * labels_state_period: pre_boundary | straddles_boundary | incomplete |
--     settling (period end within D+2 of the watermark — counts may still grow
--     via re-sweeps) | sound. Settling periods ARE labels_sound_period=TRUE
--     (reviewer-settled 1136 convention: the display gate is the consumer's).
--
-- CONVERSION CONVENTION (R38, replicated at every grain — the KPIs-tab math):
-- X→booked = ALL meetings in the period ÷ the period's labeled X count, where
-- the X count = SUM of the DAY-grain append-only cohorts over the period
-- (exactly what the tab sums — a lead with label events on two days counts
-- twice, accepted). The lead-grain opp_leads/opp_met surface is deliberately
-- NOT summed here (per the 1136 header: NOT additive across days; range-level
-- lead-grain conversion must recompute at lead grain). These are ANALYSIS
-- surfaces, not targets — no secondary IM targets derive from eng/conf
-- conversions.
--
-- Reversible: DROP VIEW core.v_kpi_weekly; DROP VIEW core.v_kpi_monthly;
-- re-apply 1136 to restore the prior core.v_kpi_daily shape.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. core.v_kpi_daily — 1136 body VERBATIM + appended cohorts/ratios
-- ─────────────────────────────────────────────────────────────────────────────
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
         COUNT(DISTINCT CASE WHEN label IN ('opportunity', 'engagement') THEN le END) AS pos_cohort,
         COUNT(DISTINCT CASE WHEN label = 'engagement'  THEN le END)                  AS eng_cohort,
         COUNT(DISTINCT CASE WHEN label = 'confused'    THEN le END)                  AS conf_cohort
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
  (s.day <  CAST(now() - INTERVAL 5 HOUR AS DATE))                AS completed_day,
  -- ── 1138 appended columns (consumer safety: everything above is 1136 verbatim) ──
  coh.eng_cohort, coh.conf_cohort,
  -- day-grain R38 simple conversion ratios (meetings ÷ cohort; labels-sound days
  -- only; lag-mismatch accepted, >100% possible by design — no assertion)
  CASE WHEN s.day >= DATE '2026-05-15' AND s.day <= ww.wm_day
       THEN ROUND(COALESCE(mt.meetings_booked, 0) * 1.0 / NULLIF(coh.opp_cohort, 0), 4)
  END                                                             AS opp_to_booked,
  CASE WHEN s.day >= DATE '2026-05-15' AND s.day <= ww.wm_day
       THEN ROUND(COALESCE(mt.meetings_booked, 0) * 1.0 / NULLIF(coh.eng_cohort, 0), 4)
  END                                                             AS eng_to_booked,
  CASE WHEN s.day >= DATE '2026-05-15' AND s.day <= ww.wm_day
       THEN ROUND(COALESCE(mt.meetings_booked, 0) * 1.0 / NULLIF(coh.conf_cohort, 0), 4)
  END                                                             AS conf_to_booked
FROM spine s
LEFT JOIN nat   ON nat.day = s.day AND nat.workspace_slug = s.workspace_slug
LEFT JOIN cur   ON cur.day = s.day AND cur.workspace_slug = s.workspace_slug
LEFT JOIN coh   ON coh.day = s.day AND coh.workspace_slug = s.workspace_slug
LEFT JOIN om    ON om.day  = s.day AND om.workspace_slug  = s.workspace_slug
LEFT JOIN mt    ON mt.day  = s.day AND mt.workspace_slug  = s.workspace_slug
LEFT JOIN ws_wm ww ON ww.workspace_slug = s.workspace_slug
LEFT JOIN core.workspace w ON w.slug = s.workspace_slug;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. core.v_kpi_weekly — ISO week (Mon–Sun) × workspace
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_kpi_weekly AS
WITH wm AS (  -- per-workspace label watermark, recovered from the daily gate
  SELECT workspace_slug, MAX(CASE WHEN labels_sound THEN day END) AS wm_day
  FROM core.v_kpi_daily
  GROUP BY 1
),
agg AS (
  SELECT CAST(date_trunc('week', day) AS DATE) AS period_start,
         workspace_slug,
         MAX(workspace_name)                    AS workspace_name,
         COUNT(*)                               AS days_with_rows,
         COUNT(*) FILTER (WHERE completed_day)  AS days_completed,
         COUNT(*) FILTER (WHERE sends_sound)    AS days_sends_sound,
         COUNT(*) FILTER (WHERE labels_sound)   AS days_labels_sound,
         SUM(COALESCE(sent, 0))                 AS sent,
         SUM(COALESCE(replies_human, 0))        AS replies_human,
         SUM(COALESCE(replies_auto, 0))         AS replies_auto,
         SUM(COALESCE(native_opps, 0))          AS native_opps,
         SUM(COALESCE(labeled, 0))              AS labeled,
         SUM(COALESCE(opp, 0))                  AS opp,
         SUM(COALESCE(eng, 0))                  AS eng,
         SUM(COALESCE(conf, 0))                 AS conf,
         SUM(COALESCE(ni, 0))                   AS ni,
         SUM(COALESCE(opp_cohort, 0))           AS opp_cohort,
         SUM(COALESCE(pos_cohort, 0))           AS pos_cohort,
         SUM(COALESCE(eng_cohort, 0))           AS eng_cohort,
         SUM(COALESCE(conf_cohort, 0))          AS conf_cohort,
         SUM(COALESCE(meetings_booked, 0))      AS meetings_booked
  FROM core.v_kpi_daily
  GROUP BY 1, 2
),
flags AS (
  SELECT a.*, w.wm_day,
         (a.period_start + 6)                                           AS period_end,
         (a.period_start >= DATE '2024-01-15')                          AS sends_sound_period,
         (a.period_start >= DATE '2024-01-01')                          AS meetings_sound_period,
         (a.period_start >= DATE '2026-05-15'
          AND a.period_start + 6 <= w.wm_day)                           AS labels_sound_period,
         (a.period_start + 6 < CAST(now() - INTERVAL 5 HOUR AS DATE))   AS period_complete
  FROM agg a
  LEFT JOIN wm w ON w.workspace_slug = a.workspace_slug
)
SELECT
  period_start                                                          AS week_start,
  period_end                                                            AS week_end,
  isoyear(period_start)                                                 AS iso_year,
  weekofyear(period_start)                                              AS iso_week,
  workspace_slug, workspace_name,
  7                                                                     AS days_in_period,
  days_with_rows, days_completed, days_sends_sound, days_labels_sound,
  -- native columns: whole-period sends soundness or NULL (100%-or-wipe)
  CASE WHEN sends_sound_period THEN sent          END                   AS sent,
  CASE WHEN sends_sound_period THEN replies_human END                   AS replies_human,
  CASE WHEN sends_sound_period THEN replies_auto  END                   AS replies_auto,
  CASE WHEN sends_sound_period THEN native_opps   END                   AS native_opps,  -- side column, NEVER a KPI (R18)
  -- label columns: ALL-days-sound gate or NULL (100%-or-wipe at period grain)
  CASE WHEN labels_sound_period THEN labeled     END                    AS labeled,
  CASE WHEN labels_sound_period THEN opp         END                    AS opp,
  CASE WHEN labels_sound_period THEN eng         END                    AS eng,
  CASE WHEN labels_sound_period THEN conf        END                    AS conf,
  CASE WHEN labels_sound_period THEN ni          END                    AS ni,
  CASE WHEN labels_sound_period THEN opp_cohort  END                    AS opp_cohort,
  CASE WHEN labels_sound_period THEN pos_cohort  END                    AS pos_cohort,
  CASE WHEN labels_sound_period THEN eng_cohort  END                    AS eng_cohort,
  CASE WHEN labels_sound_period THEN conf_cohort END                    AS conf_cohort,
  meetings_booked,
  -- derived ratios (R38 simple period-grain convention; NULL when gated)
  CASE WHEN sends_sound_period
       THEN ROUND(sent * 1.0 / NULLIF(meetings_booked, 0), 1) END       AS kpi_emails_per_meeting,
  CASE WHEN sends_sound_period
       THEN ROUND(replies_human * 1.0 / NULLIF(sent, 0), 6) END         AS human_rr,
  CASE WHEN labels_sound_period
       THEN ROUND(pos_cohort * 1.0 / NULLIF(replies_human, 0), 6) END   AS positive_rr,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(opp_cohort, 0), 4) END AS opp_to_booked,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(eng_cohort, 0), 4) END AS eng_to_booked,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(conf_cohort, 0), 4) END AS conf_to_booked,
  sends_sound_period, meetings_sound_period, labels_sound_period,
  CASE WHEN period_end   <  DATE '2026-05-15'                THEN 'pre_boundary'
       WHEN period_start <  DATE '2026-05-15'                THEN 'straddles_boundary'
       WHEN wm_day IS NULL OR period_end > wm_day            THEN 'incomplete'
       WHEN period_end   >  wm_day - INTERVAL 2 DAY          THEN 'settling'  -- D+2 re-sweep window
       ELSE 'sound' END                                                 AS labels_state_period,
  period_complete
FROM flags;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. core.v_kpi_monthly — calendar month × workspace
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_kpi_monthly AS
WITH wm AS (  -- per-workspace label watermark, recovered from the daily gate
  SELECT workspace_slug, MAX(CASE WHEN labels_sound THEN day END) AS wm_day
  FROM core.v_kpi_daily
  GROUP BY 1
),
agg AS (
  SELECT CAST(date_trunc('month', day) AS DATE) AS period_start,
         workspace_slug,
         MAX(workspace_name)                    AS workspace_name,
         COUNT(*)                               AS days_with_rows,
         COUNT(*) FILTER (WHERE completed_day)  AS days_completed,
         COUNT(*) FILTER (WHERE sends_sound)    AS days_sends_sound,
         COUNT(*) FILTER (WHERE labels_sound)   AS days_labels_sound,
         SUM(COALESCE(sent, 0))                 AS sent,
         SUM(COALESCE(replies_human, 0))        AS replies_human,
         SUM(COALESCE(replies_auto, 0))         AS replies_auto,
         SUM(COALESCE(native_opps, 0))          AS native_opps,
         SUM(COALESCE(labeled, 0))              AS labeled,
         SUM(COALESCE(opp, 0))                  AS opp,
         SUM(COALESCE(eng, 0))                  AS eng,
         SUM(COALESCE(conf, 0))                 AS conf,
         SUM(COALESCE(ni, 0))                   AS ni,
         SUM(COALESCE(opp_cohort, 0))           AS opp_cohort,
         SUM(COALESCE(pos_cohort, 0))           AS pos_cohort,
         SUM(COALESCE(eng_cohort, 0))           AS eng_cohort,
         SUM(COALESCE(conf_cohort, 0))          AS conf_cohort,
         SUM(COALESCE(meetings_booked, 0))      AS meetings_booked
  FROM core.v_kpi_daily
  GROUP BY 1, 2
),
flags AS (
  SELECT a.*, w.wm_day,
         (CAST(a.period_start + INTERVAL 1 MONTH AS DATE) - 1)          AS period_end,
         (a.period_start >= DATE '2024-01-15')                          AS sends_sound_period,
         (a.period_start >= DATE '2024-01-01')                          AS meetings_sound_period,
         (a.period_start >= DATE '2026-05-15'
          AND CAST(a.period_start + INTERVAL 1 MONTH AS DATE) - 1 <= w.wm_day)
                                                                        AS labels_sound_period,
         (CAST(a.period_start + INTERVAL 1 MONTH AS DATE) - 1
            < CAST(now() - INTERVAL 5 HOUR AS DATE))                    AS period_complete
  FROM agg a
  LEFT JOIN wm w ON w.workspace_slug = a.workspace_slug
)
SELECT
  period_start                                                          AS month_start,
  period_end                                                            AS month_end,
  year(period_start)                                                    AS year,
  month(period_start)                                                   AS month,
  workspace_slug, workspace_name,
  date_diff('day', period_start, CAST(period_start + INTERVAL 1 MONTH AS DATE))
                                                                        AS days_in_period,
  days_with_rows, days_completed, days_sends_sound, days_labels_sound,
  -- native columns: whole-period sends soundness or NULL (100%-or-wipe —
  -- Jan-2024 starts before the 2024-01-15 restatement floor and ships NULL)
  CASE WHEN sends_sound_period THEN sent          END                   AS sent,
  CASE WHEN sends_sound_period THEN replies_human END                   AS replies_human,
  CASE WHEN sends_sound_period THEN replies_auto  END                   AS replies_auto,
  CASE WHEN sends_sound_period THEN native_opps   END                   AS native_opps,  -- side column, NEVER a KPI (R18)
  -- label columns: ALL-days-sound gate or NULL (May-2026 straddles the
  -- 2026-05-15 boundary and ships NULL; first label-sound month = 2026-06)
  CASE WHEN labels_sound_period THEN labeled     END                    AS labeled,
  CASE WHEN labels_sound_period THEN opp         END                    AS opp,
  CASE WHEN labels_sound_period THEN eng         END                    AS eng,
  CASE WHEN labels_sound_period THEN conf        END                    AS conf,
  CASE WHEN labels_sound_period THEN ni          END                    AS ni,
  CASE WHEN labels_sound_period THEN opp_cohort  END                    AS opp_cohort,
  CASE WHEN labels_sound_period THEN pos_cohort  END                    AS pos_cohort,
  CASE WHEN labels_sound_period THEN eng_cohort  END                    AS eng_cohort,
  CASE WHEN labels_sound_period THEN conf_cohort END                    AS conf_cohort,
  meetings_booked,
  -- derived ratios (R38 simple period-grain convention; NULL when gated)
  CASE WHEN sends_sound_period
       THEN ROUND(sent * 1.0 / NULLIF(meetings_booked, 0), 1) END       AS kpi_emails_per_meeting,
  CASE WHEN sends_sound_period
       THEN ROUND(replies_human * 1.0 / NULLIF(sent, 0), 6) END         AS human_rr,
  CASE WHEN labels_sound_period
       THEN ROUND(pos_cohort * 1.0 / NULLIF(replies_human, 0), 6) END   AS positive_rr,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(opp_cohort, 0), 4) END AS opp_to_booked,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(eng_cohort, 0), 4) END AS eng_to_booked,
  CASE WHEN labels_sound_period
       THEN ROUND(meetings_booked * 1.0 / NULLIF(conf_cohort, 0), 4) END AS conf_to_booked,
  sends_sound_period, meetings_sound_period, labels_sound_period,
  CASE WHEN period_end   <  DATE '2026-05-15'                THEN 'pre_boundary'
       WHEN period_start <  DATE '2026-05-15'                THEN 'straddles_boundary'
       WHEN wm_day IS NULL OR period_end > wm_day            THEN 'incomplete'
       WHEN period_end   >  wm_day - INTERVAL 2 DAY          THEN 'settling'  -- D+2 re-sweep window
       ELSE 'sound' END                                                 AS labels_state_period,
  period_complete
FROM flags;
