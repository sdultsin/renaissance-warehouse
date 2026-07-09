-- 1090: v_workspace_send_daily — recover workspace attribution from campaign_id (robust fix).
--
-- WHY: the raw fact `raw_pipeline_campaign_daily_metrics` stopped populating `workspace_id` — it is
-- NULL on EVERY row (back to at least 2026-06-29). The view keyed workspace off f.workspace_id, so it
-- collapsed every row to workspace_id='(unknown)' / workspace_name=NULL. That broke the Data Hub
-- Sending tab (filters workspace_name IS NOT NULL -> 0 rows) and every per-workspace send number
-- (surfaced by David 2026-07-09). The fleet total was still correct (07-08 = 2,638,116).
--
-- FIX: `campaign_id` IS still populated on the fact, and core.campaign maps campaign_id -> workspace_id
-- 1:1 (992 rows / 992 distinct campaign_id, max dupe = 1 -> NO fan-out / no double-count). So resolve
-- workspace as COALESCE(fact.workspace_id, campaign.workspace_id). This is robust: it stays correct
-- whether or not the upstream loader ever restores workspace_id, so it can't silently break like this
-- again. Verified against the live warehouse: 2026-07-08 recovers the full per-workspace split
-- (Funding 1-6 + Max's + Renaissance 1 + Warm leads) summing to the fleet total 2,638,116; ~2.9% falls
-- to '(unknown)' from deleted/new campaigns not yet in core.campaign.
--
-- Columns are UNCHANGED (only the workspace-resolution logic is augmented). LEFT JOINs throughout, so
-- no row is ever dropped. READ-ONLY on core.campaign / core.workspace / the raw fact.

CREATE OR REPLACE VIEW derived.v_workspace_send_daily AS
WITH fw AS (
    SELECT m.*, COALESCE(m.workspace_id, c.workspace_id) AS _ws_id
    FROM raw_pipeline_campaign_daily_metrics m
    LEFT JOIN core.campaign c ON c.campaign_id = m.campaign_id
)
SELECT
    fw.date,
    COALESCE(fw._ws_id, '(unknown)')                            AS workspace_id,
    -- name: live/soft-deleted dimension, then the campaign-recovered id, then the fact's own name/id.
    COALESCE(w.name, fw.workspace_name, fw._ws_id)              AS workspace_name,
    CASE
        WHEN w.deleted_at IS NOT NULL
        THEN COALESCE(w.name, fw.workspace_name, fw._ws_id)
             || ' (deleted ' || CAST(w.deleted_at AS DATE) || ')'
        ELSE COALESCE(w.name, fw.workspace_name, fw._ws_id)
    END                                                         AS workspace_label,
    (w.deleted_at IS NOT NULL)                                  AS workspace_deleted,
    w.deleted_at,
    w.last_active_date,
    SUM(fw.sent)                                                AS sent,
    SUM(fw.unique_opportunities)                                AS unique_opportunities,
    SUM(fw.unique_replies)                                      AS unique_replies,
    SUM(fw.unique_replies_automatic)                            AS unique_replies_automatic
FROM fw
LEFT JOIN core.workspace w ON w.workspace_id = fw._ws_id
GROUP BY 1, 2, 3, 4, 5, 6, 7;
