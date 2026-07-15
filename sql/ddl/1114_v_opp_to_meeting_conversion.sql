-- @gate: add
-- Depends on 1106
-- Depends on 1112
-- ============================================================================
-- 1114_v_opp_to_meeting_conversion.sql — core.v_opp_to_meeting_conversion: TRUE
-- opp→meeting conversion — cohort opportunities (ever_opportunity, never decrements)
-- joined to meeting truth by lead identity.
--
-- MEETING SIDE (join resolved against the LIVE core.v_meeting_truth, DDL 1106):
--   lead_email  = lower(mt.lead_email)  (era-1 portal rows are already lowercased;
--                 era-2 core.meeting rows may be mixed case)
--   meeting_ts  = COALESCE(CAST(mt.meeting_date AS TIMESTAMPTZ), mt.posted_at)
--                 (meeting_date = the business/booking day; posted_at fallback)
--   ALL channels count as conversion (a labeled email opp that books through any
--   channel converted); v_meeting_truth is one row per deduped meeting
--   (email/phone+day, never row id).
--
-- HONESTY: meetings without a recoverable lead_email CANNOT join here — they are
-- counted by the meetings lane, never invented here (companion query at bottom).
-- A meeting counts as converted only ON/AFTER the lead's first_opportunity_ts minus a
-- 1-day grace (same-day booking-before-label-sync skew).
-- Slack-era portal rebuild rows carry lead_email (that is the point of DDL 1106), so
-- pre-June meetings DO join; core.meeting slack rows never could (lead_email NULL).
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_opp_to_meeting_conversion AS
WITH opp_cohort AS (
    SELECT workspace_slug, lead_email, first_opportunity_ts, first_opportunity_campaign_id,
           ever_opt_out, current_label
    FROM core.v_lead_label_cohort
    WHERE ever_opportunity
),
meetings AS (
    SELECT lower(mt.lead_email) AS lead_email,
           COALESCE(CAST(mt.meeting_date AS TIMESTAMPTZ), mt.posted_at) AS meeting_ts
    FROM core.v_meeting_truth mt
    WHERE mt.lead_email IS NOT NULL AND mt.lead_email <> ''
),
joined AS (
    SELECT
        o.workspace_slug,
        o.first_opportunity_campaign_id AS campaign_id,
        o.lead_email,
        o.first_opportunity_ts,
        min(m.meeting_ts) AS first_meeting_ts
    FROM opp_cohort o
    LEFT JOIN meetings m
      ON m.lead_email = o.lead_email
     AND m.meeting_ts >= o.first_opportunity_ts - INTERVAL 1 DAY
    GROUP BY 1, 2, 3, 4
)
SELECT
    workspace_slug,
    campaign_id,
    count(*)                                          AS cohort_opportunities,
    count(first_meeting_ts)                           AS opps_with_meeting,
    round(count(first_meeting_ts) * 1.0 / nullif(count(*), 0), 4)
                                                      AS opp_to_meeting_rate,
    median(date_diff('day', first_opportunity_ts, first_meeting_ts))
        FILTER (first_meeting_ts IS NOT NULL)         AS median_days_opp_to_meeting,
    min(first_opportunity_ts)                         AS cohort_first_opp_ts,
    max(first_opportunity_ts)                         AS cohort_last_opp_ts
FROM joined
GROUP BY 1, 2;

-- Companion honesty number for consumers (meetings this view can never see):
--   SELECT count(*) AS meetings_unjoinable FROM core.v_meeting_truth
--   WHERE lead_email IS NULL OR lead_email = '';
