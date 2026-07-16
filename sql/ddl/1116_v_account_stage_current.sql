-- 1116_v_account_stage_current.sql  [2026-07-16] current-stint view for the Activation Pipeline
-- @gate: add
--
-- PURPOSE
-- The Activation Pipeline's Rehab rule needs "when did THIS inbox enter its CURRENT rehab stint?".
-- DDL 1115's core.v_account_stage_entry answers "first EVER seen in this stage" — correct for the
-- analytical question (when did inboxes ramp up) but WRONG as a flip trigger: an inbox on its
-- SECOND rehab stint would read as months old and flip straight back out, getting zero rehab.
--
-- This view answers the operational question instead: for each inbox, its CURRENT stage and when
-- that stint started. One row per (email, workspace_uuid).
--
-- TWO SAFETY PROPERTIES, both deliberate:
--
-- (1) GAP-TOLERANT. A stint breaks only when we OBSERVE the inbox in a DIFFERENT stage — never
--     because a calendar day is missing. Keying islands on consecutive dates would turn a single
--     missed nightly into a false stint boundary, restarting the clock and releasing the inbox
--     EARLY — the dangerous direction. The islands here are cut on observation sequence
--     (seq_all - seq_stage), so a skipped night is invisible to the stint.
--
-- (2) OPEN STINTS ONLY. A stint is returned only if the inbox's LATEST observation is that stage
--     (the JOIN on current_stage + last_observed_on). Consequence, and the point: if an inbox left
--     rehab and RE-ENTERED after the last snapshot, this view still reports the older stage, so the
--     pipeline finds NO rehab row, computes t0=0, and skips it — the inbox stays in rehab one more
--     day until the snapshot catches up. It can never hand back a stale stint that would flip an
--     inbox out on day zero of a fresh stint.
--
-- Reads only core.account_tags_daily (DDL 1115). Additive: no table altered, no data mutated, no
-- consumer repointed. Revert = DROP this view. v_account_stage_entry is UNCHANGED and still serves
-- the analytical question.

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_account_stage_current AS
WITH obs AS (
    SELECT email, workspace_uuid, workspace_slug, snapshot_date, stage,
           ROW_NUMBER() OVER (PARTITION BY email, workspace_uuid ORDER BY snapshot_date)        AS seq_all,
           ROW_NUMBER() OVER (PARTITION BY email, workspace_uuid, stage ORDER BY snapshot_date) AS seq_stage
    FROM core.account_tags_daily
    WHERE stage IS NOT NULL
),
stints AS (
    -- (seq_all - seq_stage) is constant across a run of consecutive OBSERVATIONS of one stage.
    SELECT email, workspace_uuid,
           ANY_VALUE(workspace_slug) AS workspace_slug,
           stage,
           MIN(snapshot_date) AS stint_started_on,
           MAX(snapshot_date) AS stint_last_seen_on,
           COUNT(*)           AS days_observed
    FROM obs
    GROUP BY email, workspace_uuid, stage, (seq_all - seq_stage)
),
latest AS (
    SELECT email, workspace_uuid, stage AS current_stage, snapshot_date AS last_observed_on
    FROM (
        SELECT email, workspace_uuid, stage, snapshot_date,
               ROW_NUMBER() OVER (PARTITION BY email, workspace_uuid ORDER BY snapshot_date DESC) AS rn
        FROM core.account_tags_daily
        WHERE stage IS NOT NULL
    )
    WHERE rn = 1
)
SELECT s.email,
       s.workspace_uuid,
       s.workspace_slug,
       s.stage,
       s.stint_started_on,
       s.stint_last_seen_on,
       s.days_observed,
       -- TRUE = the inbox was already in this stage on our very first snapshot, so its true entry
       -- date predates the history and is unknowable. Consumers that time a stage MUST exclude
       -- these. (For Rehab this is moot in practice: zero inboxes carried a Rehab tag when the
       -- history started on 2026-07-16, so every rehab stint is observed from its true start.)
       (s.stint_started_on = (SELECT MIN(snapshot_date) FROM core.account_tags_daily)) AS is_left_censored
FROM stints s
JOIN latest l
  ON  l.email            = s.email
  AND l.workspace_uuid   = s.workspace_uuid
  AND l.current_stage    = s.stage
  AND l.last_observed_on = s.stint_last_seen_on;
