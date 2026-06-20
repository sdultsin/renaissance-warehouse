-- Version 70 (2026-06-14) — portal consolidation gap dimensions.
-- APPLIED 2026-06-14 in the post-cutover idle writer window.
-- Sequence in this window: 66=SMS, 67=workspace soft-delete, 68=workspace fact-driven
-- views, 69=SLA reply-time, 70=this (portal gap dims). Live schema_version was 65
-- immediately before this window.
--   apply via: apply_ddl_file(conn, <this file>, version=70)
--
-- Adds the 3 VERIFIED portal gap dimensions (deliverables/2026-06-14-portal-consolidation/
-- GENERATOR-NOTES.md §4 "Known GAPS"):
--   (1) ADVISOR        -> core.meeting.advisor          (from Funding-Form row_json[5], >=2026-06-01)
--   (2) INBOX-MANAGER  -> core.meeting.inbox_manager    (from Funding-Form row_json[15], first->full normalized)
--   (3) INSTANTLY CREDITS -> core.instantly_credit table (per-workspace OUTREACH lead-list quota,
--                            via scripts/portal_credits.py; "The Eagles" Free-Trial junk row excluded)
--
-- The COLUMN backfill for (1)+(2) is executed by entities/meeting.py (the canonical projection
-- now selects advisor + inbox_manager from the sheet). The TABLE load for (3) is executed by
-- scripts/load_instantly_credit.py (reads portal_credits.py JSON, upserts keyed (date, workspace)).
-- Migration-agnostic standard SQL (must port off single-file DuckDB unchanged).

-- ============================================================================
-- (1) + (2)  core.meeting gap columns (sheet-era only; NULL for slack-era rows).
--     advisor       — RAW sheet value, format "<PARTNER_PREFIX>: <Full Name>" (e.g. "BTC: Jett Lurvey").
--     advisor_name  — the name portion only (after the ": ").
--     advisor_partner — the partner the prefix maps to (BTC->Big Think Capital, GQ->GoQualifi, ...).
--     inbox_manager — the Funding-Form Inbox Manager, first-name-only values resolved to full names.
-- ============================================================================
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS advisor          VARCHAR;
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS advisor_name     VARCHAR;
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS advisor_partner  VARCHAR;
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS inbox_manager    VARCHAR;

-- ============================================================================
-- (3)  core.instantly_credit — per-workspace Instantly OUTREACH lead-list quota.
--      Mirrors the portal's "Accounts > Lead Credits" tab columns
--      (date, workspace, used, lim, remaining, pct_used). Keyed (date, workspace) so the
--      nightly puller UPSERTs one snapshot per day (trend over time). The org-wide shared
--      credit pool is a SINGLE figure, stored once per day in core.instantly_credit_pool.
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.instantly_credit (
    snapshot_date  DATE    NOT NULL,
    workspace      VARCHAR NOT NULL,   -- organization_name the API returns (drift-proof vs renames)
    env_key        VARCHAR,            -- INSTANTLY_KEY_* the row came from (provenance)
    used           BIGINT,             -- current_lead_count (leads loaded in this workspace)
    lim            BIGINT,             -- total_lead_limit (this workspace's lead cap)
    remaining      BIGINT,             -- lim - used
    pct_used       INTEGER,            -- round(100*used/lim)
    plan           VARCHAR,            -- outreach plan_name
    _loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id        VARCHAR,
    PRIMARY KEY (snapshot_date, workspace)
);
CREATE INDEX IF NOT EXISTS ix_instantly_credit_date ON core.instantly_credit (snapshot_date);

-- Org-wide shared credit pool (NOT per workspace) — one row per day.
CREATE TABLE IF NOT EXISTS core.instantly_credit_pool (
    snapshot_date     DATE NOT NULL PRIMARY KEY,
    organization      VARCHAR,
    plan              VARCHAR,
    total_credits     BIGINT,
    available_credits BIGINT,
    used_credits      BIGINT,
    pct_used          INTEGER,
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    _run_id           VARCHAR
);

-- Convenience read view: latest snapshot per workspace (what the portal tile reads).
CREATE OR REPLACE VIEW core.v_instantly_credit_latest AS
SELECT c.*
FROM core.instantly_credit c
JOIN (SELECT workspace, max(snapshot_date) AS d FROM core.instantly_credit GROUP BY workspace) m
  ON m.workspace = c.workspace AND m.d = c.snapshot_date;
