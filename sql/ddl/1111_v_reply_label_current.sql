-- @gate: add
-- Depends on 1110
-- ============================================================================
-- 1111_v_reply_label_current.sql — core.v_reply_label_current: current-state label per
-- lead, DERIVED from the append-only event stream (the stream is the truth, charter §4).
--
-- Latest REAL label per (workspace_slug, lead_email): newest anchoring message wins;
-- ties by labeled_at, then labeler_version (lexicographic on semver-shaped strings is
-- fine at this cadence; swap for a version-rank map if versions ever go double-digit).
-- Gate classes ('auto'/'bot') and 'labeler_error' are EXCLUDED — autos never touch
-- label stats (charter §4).
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_reply_label_current AS
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
