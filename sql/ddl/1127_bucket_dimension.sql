-- @gate: add
-- Depends on 1110
-- ============================================================================
-- 1127_bucket_dimension.sql — R24 retarget/dialer BUCKET dimension, CRM n=1
-- semantics: the bucket lives on the positive-reply EVENT, not the lead
-- ("a lead can be day-6 in one thread, day-1 in another"); the lead-grain
-- current bucket derives from the MOST-RECENT positive reply. Plain SQL only.
--
-- DEFINITIONS (Sam R24/R30):
--   positive reply  = a labeled reply whose message-grain CURRENT label is
--                     opportunity or engagement (the message's latest labeler
--                     verdict wins — an old-version 'opportunity' overruled by a
--                     newer relabel does not count as positive).
--   bucket          = 'fresh'    when 0-5 days have passed since that reply
--                     'day5plus' when >5 days.
--   Buckets are TIME-DEPENDENT (evaluated against current_date at query time —
--   an event ages from fresh to day5plus by itself; that is the point).
--
-- v_positive_reply_event_bucket — event grain (one row per positive reply message).
-- v_lead_bucket_current         — lead grain (email), from the most-recent positive
--                                 reply across workspaces; leads with no positive
--                                 reply simply have no row (honest absence, no
--                                 fallback bucket).
--
-- Reversible: DROP VIEW ×2.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_positive_reply_event_bucket AS
WITH message_current AS (
    -- one row per labeled inbound message = its latest labeler verdict
    SELECT *,
           row_number() OVER (
               PARTITION BY message_ref_table, message_ref_id
               ORDER BY labeled_at DESC, labeler_version DESC
           ) AS rn
    FROM main.raw_reply_label_event
    WHERE label IN ('opportunity', 'engagement', 'confused', 'not_interested')
      AND message_ts IS NOT NULL
)
SELECT
    event_id,
    workspace_slug,
    lower(lead_email)                                            AS lead_email,
    campaign_id,
    label,
    message_ts,
    date_diff('day', CAST(message_ts AS DATE), current_date)     AS days_since_reply,
    CASE WHEN date_diff('day', CAST(message_ts AS DATE), current_date) <= 5
         THEN 'fresh' ELSE 'day5plus' END                        AS bucket,
    labeler_version,
    labeled_at
FROM message_current
WHERE rn = 1
  AND label IN ('opportunity', 'engagement');

CREATE OR REPLACE VIEW core.v_lead_bucket_current AS
SELECT
    lead_email,
    bucket,
    days_since_reply,
    message_ts        AS last_positive_at,
    label             AS last_positive_label,
    workspace_slug    AS last_positive_workspace,
    campaign_id       AS last_positive_campaign_id
FROM core.v_positive_reply_event_bucket
QUALIFY row_number() OVER (PARTITION BY lead_email ORDER BY message_ts DESC, labeled_at DESC) = 1;
