-- Version 67 (2026-06-14) — workspace soft-delete lifecycle columns.
-- APPLIED 2026-06-14 in the post-cutover idle writer window.
-- RENUMBERED 66->67 at apply time: 66 was taken by 66_sms_failure_assets.sql
-- (applied immediately before this in the same window). Sequence in this window:
-- 66=SMS, 67=soft-delete columns (this file), 68=fact-driven views (depend on these
-- columns), 69=SLA reply-time, 70=portal gap dims (meeting advisor/IM + instantly_credit).
--
-- SCHEMA-VERSION NOTE: live schema_version was 65 immediately before this window
-- (DDL 63/64/65 = 63_audit_fixes, 64_funding_form, 65_meetings_channel_aware).
-- core/db.py::apply_ddl_file treats `version` as a PRIMARY KEY and SILENTLY no-ops
-- (returns False, runs no SQL) if that version row already exists.
--   apply via: apply_ddl_file(conn, <this file>, version=67)
--
-- Adds explicit soft-delete lifecycle to core.workspace so:
--   (a) a deleted Instantly workspace KEEPS its dimension row (already true — the
--       builder flips is_active=FALSE and never deletes — see entities/workspace.py),
--   (b) consumers can LABEL it "Outlook 3 (deleted 2026-06-14)" and
--   (c) the orphan-inventory query is cheap.
--
-- Distinction from existing columns:
--   is_active    — "appeared in the LAST workspace ingest run". NOISY as a deletion
--                  signal: a 1-run API-key 401 also flips it false (nightly tolerates
--                  "dead workspaces 401/402"). Kept for back-compat.
--   last_seen_at — last SYNC time the workspace resolved (API key still worked).
--   deleted_at        (NEW) — STICKY. Stamped only once a workspace is GENUINELY
--                  gone (see backfill predicate below — NOT on a single is_active
--                  flip). NULL = live. Canonical "is gone" predicate.
--   last_active_date  (NEW) — the last DAY the workspace has a SEND FACT row
--                  (MAX(date) over raw_pipeline_campaign_daily_metrics for this
--                  workspace_id). Lifecycle truth for UI labels + orphan inventory;
--                  a different clock from last_seen_at.
--
-- Migration-agnostic standard SQL (must port off single-file DuckDB unchanged).

ALTER TABLE core.workspace ADD COLUMN IF NOT EXISTS deleted_at       TIMESTAMPTZ;
ALTER TABLE core.workspace ADD COLUMN IF NOT EXISTS last_active_date DATE;
-- Counter column (added with the other ALTERs; the nightly workspace.py patch bumps it).
ALTER TABLE core.workspace ADD COLUMN IF NOT EXISTS consecutive_missing_runs INTEGER DEFAULT 0;

-- Helpful index for orphan/inventory scans.
-- IMPORTANT (DuckDB): CREATE INDEX must precede the UPDATEs below. apply_ddl_file wraps
-- this whole file in ONE transaction, and DuckDB raises "Cannot create index with
-- outstanding updates" if an index is created in a txn that already has pending UPDATEs
-- on the same table. Creating it here (right after ADD COLUMN, before any UPDATE) is
-- correct and the index is populated regardless of statement order.
CREATE INDEX IF NOT EXISTS ix_core_workspace_deleted_at ON core.workspace (deleted_at);

-- One-time backfill (idempotent) -------------------------------------------------
-- 1) last_active_date := MAX(send-fact date) per workspace_id, from the COMPLETE
--    daily fact. Workspaces with no fact rows (never sent, or aged past the 90d
--    window) stay NULL.
UPDATE core.workspace AS w
SET last_active_date = f.max_date
FROM (
    SELECT workspace_id, MAX(date) AS max_date
    FROM raw_pipeline_campaign_daily_metrics
    WHERE workspace_id IS NOT NULL
    GROUP BY workspace_id
) AS f
WHERE w.workspace_id = f.workspace_id
  AND (w.last_active_date IS NULL OR w.last_active_date < f.max_date);

-- 2) deleted_at := stamp ONLY workspaces that are GENUINELY gone, not merely
--    is_active=FALSE. is_active is noisy: a single API-key 401 flips it for one run
--    (the nightly explicitly tolerates "dead workspaces 401/402"), which would
--    mislabel a live workspace as deleted. So gate on a STRONGER signal — the
--    workspace must be both inactive AND have genuinely stopped sending:
--      * missing from the send fact for > N days (last send older than today-N), OR
--      * last_seen_at (last successful sync) older than today-N,
--    with N = 14 days. This tolerates a transient 401 (which clears on the next
--    good run, long before 14 days elapse). Stamp "when it stopped" with the best
--    available proxy: last_active_date, else last_seen_at, else resolved_at.
--    Sticky thereafter.
UPDATE core.workspace
SET deleted_at = COALESCE(
        deleted_at,
        CAST(last_active_date AS TIMESTAMPTZ),
        last_seen_at,
        resolved_at)
WHERE is_active = FALSE
  AND deleted_at IS NULL
  AND (
        (last_active_date IS NOT NULL AND last_active_date < current_date - INTERVAL '14 days')
     OR (last_active_date IS NULL AND last_seen_at < current_date - INTERVAL '14 days')
  );

-- 2b) The two KNOWN genuine deletions — stamp explicitly so the report names them
--     even if the >14d predicate hasn't elapsed yet (Outlook 3 was deleted today).
--     R3 = 2aa14704-dd1d-4ca2-a527-bcc4cadf5af2. Outlook 3 is matched by name since
--     its id is not pinned here (cross-check 04_inventory_queries.sql Q3 for the id).
UPDATE core.workspace
SET deleted_at = COALESCE(deleted_at, CAST(last_active_date AS TIMESTAMPTZ), last_seen_at, resolved_at)
WHERE deleted_at IS NULL
  AND (
        workspace_id = '2aa14704-dd1d-4ca2-a527-bcc4cadf5af2'         -- R3
     OR lower(name) LIKE '%outlook 3%'                                -- Outlook 3
  );

-- ================================================================================
-- entities/workspace.py PATCH (applied in the same writer window — code, not SQL).
--
-- INSERTION POINT — CRITICAL: insert the new UPDATEs BETWEEN the "flip is_active"
-- UPDATE and the `DROP TABLE IF EXISTS _run_latest_ws` line, i.e. BEFORE the DROP.
-- The new UPDATEs query the _run_latest_ws TEMP TABLE, so they MUST run while it
-- still exists. In current workspace.py the relevant region is:
--
--     169    # Flip is_active for workspaces we did NOT see this run.
--     170    ctx.db.execute(
--     171        """
--     172        UPDATE core.workspace
--     173        SET is_active = FALSE,
--     174            resolved_at = ?
--     175        WHERE workspace_id NOT IN (SELECT workspace_id FROM _run_latest_ws)
--     176        """,
--     177        [now],
--     178    )
--     179
--  >> INSERT THE THREE NEW UPDATEs HERE (lines below) <<
--     180
--     180    ctx.db.execute("DROP TABLE IF EXISTS _run_latest_ws")
--
-- (Putting them after the DROP would hit a missing _run_latest_ws and error.)
--
-- Insert, BEFORE the DROP:
--
--   -- Stamp deleted_at ONLY after N consecutive missing runs (avoid flap-thrash on
--   -- a flapping API key). A single 401 must NOT stamp a live workspace as deleted.
--   -- Requires a small counter column tracked across runs (add via this DDL if not
--   -- present): core.workspace.consecutive_missing_runs INTEGER DEFAULT 0.
--   N_MISSING_RUNS = 14   # ~14 nightly runs ≈ 14 days before we call it deleted
--
--   # bump the missing-run counter for anything absent this run; reset it for
--   # anything present this run.
--   ctx.db.execute(
--       """
--       UPDATE core.workspace
--       SET consecutive_missing_runs = consecutive_missing_runs + 1
--       WHERE workspace_id NOT IN (SELECT workspace_id FROM _run_latest_ws)
--       """
--   )
--   ctx.db.execute(
--       """
--       UPDATE core.workspace
--       SET consecutive_missing_runs = 0
--       WHERE workspace_id IN (SELECT workspace_id FROM _run_latest_ws)
--       """
--   )
--   # stamp deleted_at once the workspace has been missing for >= N consecutive runs.
--   ctx.db.execute(
--       """
--       UPDATE core.workspace
--       SET deleted_at = COALESCE(deleted_at, ?)
--       WHERE workspace_id NOT IN (SELECT workspace_id FROM _run_latest_ws)
--         AND deleted_at IS NULL
--         AND consecutive_missing_runs >= ?
--       """,
--       [now, N_MISSING_RUNS],
--   )
--   # Un-delete + reset counter if a workspace reappears (API key restored).
--   ctx.db.execute(
--       """
--       UPDATE core.workspace
--       SET deleted_at = NULL, consecutive_missing_runs = 0
--       WHERE workspace_id IN (SELECT workspace_id FROM _run_latest_ws)
--         AND deleted_at IS NOT NULL
--       """
--   )
--   # Refresh last_active_date from the send fact every run (robust lifecycle clock,
--   # independent of the missing-run counter).
--   ctx.db.execute(
--       """
--       UPDATE core.workspace AS w
--       SET last_active_date = f.max_date
--       FROM (SELECT workspace_id, MAX(date) AS max_date
--             FROM raw_pipeline_campaign_daily_metrics
--             WHERE workspace_id IS NOT NULL GROUP BY workspace_id) AS f
--       WHERE w.workspace_id = f.workspace_id
--         AND (w.last_active_date IS NULL OR w.last_active_date < f.max_date)
--       """
--   )
--
-- (Counter column consecutive_missing_runs is added with the other ALTERs at the top of
--  this file so the patch has it on first run.)
--
-- NOTE: deleted_at is intentionally LAGGING (N consecutive missing runs) so transient
-- 401 flapping never mislabels a live workspace. last_active_date (fact-derived) is the
-- robust lifecycle clock regardless and updates every run. For the two KNOWN genuine
-- deletions (R3 2aa14704-…, Outlook 3) deleted_at is stamped explicitly in 2b above.
-- ================================================================================
