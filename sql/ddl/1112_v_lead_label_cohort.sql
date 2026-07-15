-- @gate: add
-- Depends on 1110
-- Depends on 1111
-- ============================================================================
-- 1112_v_lead_label_cohort.sql — core.v_lead_label_cohort: per-lead cohort view.
-- ever_opportunity NEVER decrements (charter §4) — a lead that was ever an opportunity
-- stays in the opp cohort after later not_interested / opt-out events (case-47
-- semantics). This is what makes TRUE opp→meeting conversion computable and kills the
-- Instantly decrementing-count problem (the 19-vs-20 mystery).
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_lead_label_cohort AS
WITH real_labels AS (
    SELECT *
    FROM main.raw_reply_label_event
    WHERE label IN ('opportunity', 'engagement', 'confused', 'not_interested')
),
per_lead AS (
    SELECT
        workspace_slug,
        lead_email,
        bool_or(label = 'opportunity')                                    AS ever_opportunity,
        min(CASE WHEN label = 'opportunity' THEN message_ts END)          AS first_opportunity_ts,
        max(CASE WHEN label = 'opportunity' THEN message_ts END)          AS last_opportunity_ts,
        min(CASE WHEN label = 'opportunity' THEN campaign_id END)         AS first_opportunity_campaign_id,
        bool_or(label = 'engagement')                                     AS ever_engagement,
        bool_or(opt_out)                                                  AS ever_opt_out,
        min(message_ts)                                                   AS first_labeled_message_ts,
        max(message_ts)                                                   AS last_labeled_message_ts,
        count(*)                                                          AS n_label_events,
        count(DISTINCT labeler_version)                                   AS n_labeler_versions
    FROM real_labels
    GROUP BY 1, 2
)
SELECT
    p.*,
    c.current_label,
    c.current_opt_out
FROM per_lead p
LEFT JOIN core.v_reply_label_current c USING (workspace_slug, lead_email);
