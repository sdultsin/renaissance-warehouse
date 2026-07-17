-- @gate: add
-- Depends on 1110
-- Depends on 1124
-- ============================================================================
-- 1125_v_reply_label_current_meeting_booked.sql — R34a: meeting_booked becomes a
-- TERMINAL-POSITIVE state in the CURRENT-state view.
--
-- PRECEDENCE (designed per R34a + booked-exclusivity semantics): a lead with ANY
-- meeting_booked event (matched by lead_email, ANY workspace — ever-booked =
-- permanent booked-meeting DNC) shows current_label='meeting_booked' in
-- core.v_reply_label_current, REGARDLESS of event order — booked is terminal, a
-- later reply never un-books. The underlying reply-derived label stays visible as
-- current_reply_label. Cohort semantics (v_lead_label_cohort.ever_opportunity)
-- are UNCHANGED — ever-opportunity never decrements; only CURRENT state demotes.
--
-- CONSUMER SAFETY:
--   * Column order of the original view (1111) is preserved exactly; new columns
--     are APPENDED (current_reply_label, meeting_booked, first/last_meeting_booked_ts,
--     n_meeting_booked).
--   * core.v_reply_label_current_replyonly = the 1111 definition VERBATIM (reply-label
--     truth, no meeting overlay) for consumers that must stay reply-label-based
--     (the portal conversion feed pins to it; the booking-site KPIs feed reads
--     escrow parquets and never touches these views).
--   * v_lead_label_cohort (1112) keeps reading this view for its current_label
--     passthrough — a booked lead now correctly shows meeting_booked there too;
--     its ever_* columns come from raw events only and cannot change. VERIFIED
--     against 1112's shipped definition (submitted alongside): its per_lead CTE is
--         SELECT workspace_slug, lead_email,
--                bool_or(label = 'opportunity') AS ever_opportunity, …
--         FROM real_labels  -- = main.raw_reply_label_event WHERE label IN (4)
--         GROUP BY 1, 2
--     i.e. ever_* aggregates the RAW event table directly; the ONLY columns 1112
--     takes from this view are the current_label/current_opt_out passthrough
--     (LEFT JOIN core.v_reply_label_current c USING (workspace_slug, lead_email)).
--     ever_opportunity therefore CANNOT decrement from this overlay.
--   * The new 'meeting_booked' value appears ONLY in current_label. Known
--     current_label consumers audited 2026-07-17: conversion_dashboard_data.py
--     (pinned to replyonly in this change-set), conversion_booking_feed.py
--     (escrow-direct, never reads these views), kpi_dashboard_data.py (comment
--     only), /root/mof/opps_upload/build_upload.py (one-off, filters
--     current_label='opportunity' — booked leads correctly LEAVE its base; its own
--     booked-DNC filter previously removed them anyway). Enum-widening is the
--     R34a intent: booked "opportunities" must stop counting as live pipeline.
--
-- Reversible: re-apply 1111 (CREATE OR REPLACE back) + DROP the replyonly view.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- The 1111 semantics, verbatim, under a pinned name (reply-label-only current state).
CREATE OR REPLACE VIEW core.v_reply_label_current_replyonly AS
WITH real_labels AS (
    SELECT *
    FROM main.raw_reply_label_event
    WHERE label IN ('opportunity', 'engagement', 'confused', 'not_interested')
),
ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY workspace_slug, lead_email
               ORDER BY message_ts DESC, labeled_at DESC, labeler_version DESC
           ) AS rn
    FROM real_labels
)
SELECT
    workspace_slug,
    lead_email,
    label            AS current_label,
    opt_out          AS current_opt_out,
    confidence,
    campaign_id      AS current_campaign_id,
    message_ts       AS current_label_message_ts,
    labeled_at,
    labeler_version,
    prompt_hash,
    flag_human,
    trick_class,
    evidence,
    rationale
FROM ranked
WHERE rn = 1;

-- The current-state view every status consumer reads: reply-label current state
-- OVERLAID with the meeting_booked terminal state (R34a).
CREATE OR REPLACE VIEW core.v_reply_label_current AS
WITH booked AS (
    SELECT
        lower(lead_email)  AS lead_email,
        min(event_ts)      AS first_meeting_booked_ts,
        max(event_ts)      AS last_meeting_booked_ts,
        count(*)           AS n_meeting_booked
    FROM main.raw_lead_status_event
    WHERE event_type = 'meeting_booked'
      AND lead_email IS NOT NULL AND lead_email <> ''
    GROUP BY 1
)
SELECT
    r.workspace_slug,
    r.lead_email,
    CASE WHEN b.lead_email IS NOT NULL THEN 'meeting_booked' ELSE r.current_label END AS current_label,
    r.current_opt_out,
    r.confidence,
    r.current_campaign_id,
    r.current_label_message_ts,
    r.labeled_at,
    r.labeler_version,
    r.prompt_hash,
    r.flag_human,
    r.trick_class,
    r.evidence,
    r.rationale,
    r.current_label                    AS current_reply_label,
    (b.lead_email IS NOT NULL)         AS meeting_booked,
    b.first_meeting_booked_ts,
    b.last_meeting_booked_ts,
    b.n_meeting_booked
FROM core.v_reply_label_current_replyonly r
LEFT JOIN booked b ON b.lead_email = lower(r.lead_email);
