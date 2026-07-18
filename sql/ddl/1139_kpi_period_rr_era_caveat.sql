-- @gate: add
-- Depends on 1138
-- ============================================================================
-- 1139_kpi_period_rr_era_caveat.sql — RR-comparability caveat on the period
-- rollups (funnel-diagnosis finding 2026-07-18, additive only).
--
-- WHY: Instantly's reply classification FLIPPED on 2026-07-02 (~4-6k replies/day
-- moved auto→human, same day, ALL workspaces, totals flat) — human-RR is NOT
-- comparable across that boundary (post-Jul-2 human RR ≈ ×2.2 the pre-flip basis
-- for the same engagement). Cross-era comparisons must use TOTAL replies / send
-- (and labeled-positive / send), never raw human RR. This applies to
-- replies_human / human_rr at EVERY grain (core.v_kpi_daily's raw counts too);
-- see memory/reference_reply_rr_not_comparable_jul2_20260718.md and
-- deliverables/2026-07-18-funnel-diagnosis/REPORT.md.
--
-- WHAT (CREATE OR REPLACE core.v_kpi_weekly + core.v_kpi_monthly, 1138 bodies
-- VERBATIM with two columns APPENDED — no existing column changes, no metric
-- redesign):
--   rr_era   — 'pre_jul02_reclass' | 'post_jul02_reclass' |
--              'straddles_jul02_reclass': whether the period sits entirely
--              before / after / across the 2026-07-02 reclassification flip.
--              Never compare human_rr across different rr_era values.
--   total_rr — (replies_human + replies_auto) / sent (sends-gated) — the
--              era-COMPARABLE engagement rate prescribed by the diagnosis.
--
-- Reversible: re-apply 1138 to restore the prior weekly/monthly shapes.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

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
  period_complete,
  -- ── 1139 appended: RR-era comparability caveat (Instantly reclass flip 2026-07-02) ──
  CASE WHEN period_end   <  DATE '2026-07-02' THEN 'pre_jul02_reclass'
       WHEN period_start >= DATE '2026-07-02' THEN 'post_jul02_reclass'
       ELSE 'straddles_jul02_reclass' END                               AS rr_era,
  CASE WHEN sends_sound_period
       THEN ROUND((replies_human + replies_auto) * 1.0 / NULLIF(sent, 0), 6)
  END                                                                   AS total_rr
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
  period_complete,
  -- ── 1139 appended: RR-era comparability caveat (Instantly reclass flip 2026-07-02) ──
  CASE WHEN period_end   <  DATE '2026-07-02' THEN 'pre_jul02_reclass'
       WHEN period_start >= DATE '2026-07-02' THEN 'post_jul02_reclass'
       ELSE 'straddles_jul02_reclass' END                               AS rr_era,
  CASE WHEN sends_sound_period
       THEN ROUND((replies_human + replies_auto) * 1.0 / NULLIF(sent, 0), 6)
  END                                                                   AS total_rr
FROM flags;
