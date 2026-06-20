-- Version 68 (2026-06-14) — canonical fact-driven, deletion-aware workspace
-- send views. APPLIED 2026-06-14 in the post-cutover idle writer window.
-- RENUMBERED 67->68 at apply time (66=SMS took 66, so soft-delete became 67, these
-- views 68). Depends on 67_workspace_soft_delete.sql (deleted_at / last_active_date).
--
-- SCHEMA-VERSION NOTE: live schema_version was 65 before this window. apply_ddl_file
-- treats version as a PRIMARY KEY and SILENTLY no-ops if the version row exists.
--   apply via: apply_ddl_file(conn, <this file>, version=68)
--
-- These are the canonical "send volume per workspace over a time window" surfaces
-- that handle deleted workspaces correctly BY CONSTRUCTION:
--   * the row set is driven by the FACT (a workspace appears iff it sent in-window),
--   * the dimension is LEFT JOINed ONLY to upgrade the display name + attach the
--     "(deleted YYYY-MM-DD)" label,
--   * the name never goes NULL (fact carries denormalized workspace_name; dim soft-
--     deletes the row as a second safety net),
--   * NO is_active gate, NO INNER JOIN to the live dimension.
-- Migration-agnostic standard SQL.

-- --------------------------------------------------------------------------------
-- v_workspace_send_daily — the base grain: (workspace, date) sends, ALL workspaces
-- incl. deleted/orphaned, over the 90d send-fact window. Group/filter THIS for any
-- window (MTD / last-7 / last-30) and deletion lifecycle is automatic.
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW derived.v_workspace_send_daily AS
SELECT
    f.date,
    COALESCE(f.workspace_id, '(unknown)')                       AS workspace_id,
    -- name: prefer the live/soft-deleted dimension, fall back to the fact's
    -- send-time name, finally the id. Never NULL for a real workspace_id.
    COALESCE(w.name, f.workspace_name, f.workspace_id)          AS workspace_name,
    -- UI label that flags deletion in-line.
    CASE
        WHEN w.deleted_at IS NOT NULL
        THEN COALESCE(w.name, f.workspace_name, f.workspace_id)
             || ' (deleted ' || CAST(w.deleted_at AS DATE) || ')'
        ELSE COALESCE(w.name, f.workspace_name, f.workspace_id)
    END                                                          AS workspace_label,
    (w.deleted_at IS NOT NULL)                                  AS workspace_deleted,
    w.deleted_at,
    w.last_active_date,
    SUM(f.sent)                                                 AS sent,
    SUM(f.unique_opportunities)                                 AS unique_opportunities, -- TREND only (overcounts)
    SUM(f.unique_replies)                                       AS unique_replies,
    SUM(f.unique_replies_automatic)                             AS unique_replies_automatic
FROM raw_pipeline_campaign_daily_metrics f
LEFT JOIN core.workspace w
       ON w.workspace_id = f.workspace_id          -- LEFT JOIN: never gate inclusion on the dimension
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- --------------------------------------------------------------------------------
-- v_workspace_send_mtd — convenience MTD rollup. A deleted workspace appears here
-- iff it has fact rows since the 1st of the current month.
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW derived.v_workspace_send_mtd AS
SELECT
    workspace_id,
    any_value(workspace_label)        AS workspace_label,
    bool_or(workspace_deleted)        AS workspace_deleted,
    max(last_active_date)             AS last_active_date,
    SUM(sent)                         AS sent_mtd,
    SUM(unique_replies)               AS replies_mtd,
    SUM(unique_opportunities)         AS opps_mtd_trend   -- TREND only
FROM derived.v_workspace_send_daily
WHERE date >= date_trunc('month', current_date)
GROUP BY workspace_id
ORDER BY sent_mtd DESC;

-- Usage notes (for the report / dashboard wiring):
--   * Any window: SELECT ... FROM derived.v_workspace_send_daily
--                 WHERE date BETWEEN :start AND :end GROUP BY workspace_id;
--   * 90d horizon: a long-deleted workspace whose last send predates (today-90d)
--     has aged out of the send-fact pull (freeze-on-delete keeps rows only until
--     they age out). last_active_date still tells you it existed and when it stopped.
--   * unique_opportunities is a WINDOWED TREND (overcounts the deduped cumulative);
--     never assert it equals v_campaign_metrics. (warehouse-query-prompt.md §Opps.)
