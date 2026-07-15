-- @gate: add
-- Depends on 1110
-- ============================================================================
-- 1113_v_label_weekly.sql — core.v_label_weekly: weekly label rollup, calendar weeks
-- Mon–Sun by event (message) date (charter §5 time axis), campaign grain WITH workspace
-- (aggregate to workspace by summing).
--
-- HONESTY COLUMNS (charter data-honesty rule): autos/bots are COLLECTED but never in
-- label stats; share_low_confidence + share_refute_disagree expose where the labeler is
-- unsure; deterministic_share = how much of the week never touched the LLM;
-- labeler_versions marks mixed-version weeks.
-- CONSUMER CAVEAT (carry everywhere): counts are THREAD-END-STATE events anchored to
-- their latest inbound message — a week's counts firm up as the daily increment appends
-- newer events; the in-flight backfill is RECENT-FIRST, so older weeks fill in last.
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_label_weekly AS
WITH events AS (
    SELECT
        CAST(date_trunc('week', message_ts) AS DATE) AS week_start,   -- DuckDB weeks start Monday
        workspace_slug,
        campaign_id,
        label,
        opt_out,
        confidence,
        refute_fired,
        refute_agree,
        deterministic_gate,
        flag_human,
        labeler_version,
        lead_email
    FROM main.raw_reply_label_event
    WHERE message_ts IS NOT NULL
      AND label <> 'labeler_error'
)
SELECT
    week_start,
    workspace_slug,
    campaign_id,
    -- label stats (gate classes excluded)
    count(*) FILTER (WHERE label = 'opportunity')                 AS opportunity,
    count(*) FILTER (WHERE label = 'engagement')                  AS engagement,
    count(*) FILTER (WHERE label = 'confused')                    AS confused,
    count(*) FILTER (WHERE label = 'not_interested')              AS not_interested,
    count(*) FILTER (WHERE label IN ('opportunity','engagement','confused','not_interested'))
                                                                  AS labeled_threads,
    -- DISTINCT inside CASE (not DISTINCT+FILTER, which some engine versions reject at
    -- query time): count(DISTINCT CASE ...) ignores the NULL arm — identical semantics.
    count(DISTINCT CASE WHEN label IN ('opportunity','engagement','confused','not_interested')
                        THEN lead_email END)                      AS labeled_leads,
    count(*) FILTER (WHERE opt_out)                               AS opt_out_events,
    -- collected separately, never in label stats (charter §4)
    count(*) FILTER (WHERE label = 'auto')                        AS auto_collected,
    count(*) FILTER (WHERE label = 'bot')                         AS bot_collected,
    -- honesty columns
    round(count(*) FILTER (WHERE confidence < 70 AND label IN ('opportunity','engagement','confused','not_interested'))
          * 1.0 / nullif(count(*) FILTER (WHERE label IN ('opportunity','engagement','confused','not_interested')), 0), 4)
                                                                  AS share_low_confidence,
    round(count(*) FILTER (WHERE refute_fired AND refute_agree = FALSE)
          * 1.0 / nullif(count(*) FILTER (WHERE refute_fired), 0), 4)
                                                                  AS share_refute_disagree,
    round(count(*) FILTER (WHERE deterministic_gate IS NOT NULL) * 1.0 / nullif(count(*), 0), 4)
                                                                  AS deterministic_share,
    count(*) FILTER (WHERE flag_human)                            AS flagged_for_human,
    list_sort(list(DISTINCT labeler_version))                     AS labeler_versions
FROM events
GROUP BY 1, 2, 3;
