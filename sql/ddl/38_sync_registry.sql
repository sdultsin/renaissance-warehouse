-- Track E (2026-06-08) — Freshness instrumentation + QA backbone.
-- Version 37. (2026-06-11: + biz_sla_days data-recency SLA / is_data_stale —
-- a sync can SUCCEED while pulling nothing new; watch the data's own date too.)
--
-- core.sync_registry  — one row per physical feed (raw_* table or key core/derived
--                       decision table). Holds the cadence POLICY (expected_cadence,
--                       sla_hours, freshness_column, send-sensitivity) plus the
--                       refreshed STATE (last_synced_at, row_count, last_row_delta).
--                       Seeded + refreshed by scripts/refresh_sync_registry.py, which
--                       auto-discovers every raw_ table (so no feed can silently lack a
--                       registry row) and applies the cadence policy map.
--
-- v_warehouse_freshness — the at-a-glance staleness view the E4 QA job reads. One row
--                       per registry feed with hours_since_sync / is_stale / is_empty
--                       + days_since_biz / is_data_stale (data-recency SLA).
--
-- Cadence -> sla_hours policy (stored per-row so the view is pure SQL):
--   daily    -> 36h   (a daily feed must advance within 36h or it's stale)
--   weekly   -> 192h  (8 days)
--   periodic -> 192h  (weekly-ish refreshes: DNS sweep, registrar, CF, sheets)
--   once     -> NULL  (launched variant copy/spintax; never goes stale once loaded)
--   retired  -> NULL  (intentionally lapsed; never alerts)
--
-- biz_sla_days — data-recency SLA on last_biz_date (max(biz_date_column)). NULL = no
--   check. Distinct from sla_hours: sla_hours watches "did the sync RUN", biz_sla_days
--   watches "is the DATA recent" — a successful-but-empty sync trips only the latter.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.sync_registry (
    name              VARCHAR PRIMARY KEY,   -- physical name: 'raw_*' bare, or 'core.x'/'derived.x'
    table_schema      VARCHAR NOT NULL,      -- 'main' | 'core' | 'derived'
    source            VARCHAR,               -- logical source system (instantly, pipeline_supabase, account_truth, dns, registrar, cloudflare, sheets, comms, sendivo, d1_cc, ...)
    owner_phase       VARCHAR,               -- orchestrator phase that loads it
    expected_cadence  VARCHAR NOT NULL,      -- daily | weekly | periodic | once | retired
    sla_hours         INTEGER,               -- staleness threshold in hours (NULL = never alerts)
    freshness_column  VARCHAR,               -- column used to derive last_synced_at (NULL if none found)
    biz_date_column   VARCHAR,               -- business-date column, if any (nullable)
    biz_sla_days      INTEGER,               -- data-recency SLA in days on last_biz_date (NULL = no check)
    is_send_sensitive BOOLEAN DEFAULT FALSE, -- TRUE => row_delta=0 on a send-day is an alert
    status            VARCHAR DEFAULT 'active', -- active | retired | empty

    -- refreshed state (written each run by refresh_sync_registry.py):
    last_synced_at    TIMESTAMPTZ,           -- max(freshness_column)
    last_biz_date     DATE,                  -- max(biz_date_column)
    row_count         BIGINT,
    last_row_delta    BIGINT,                -- row_count - prev_row_count (this refresh)
    prev_row_count    BIGINT,
    last_checked_at   TIMESTAMPTZ,
    notes             VARCHAR
);

-- Upgrade path for registries created before biz_sla_days existed.
ALTER TABLE core.sync_registry ADD COLUMN IF NOT EXISTS biz_sla_days INTEGER;

-- v_warehouse_freshness — lives in the default (main) schema so it reads as
-- `SELECT ... FROM v_warehouse_freshness` (matches v_campaign_metrics convention).
CREATE OR REPLACE VIEW v_warehouse_freshness AS
SELECT
    name,
    table_schema,
    source,
    owner_phase,
    expected_cadence,
    status,
    is_send_sensitive,
    sla_hours,
    biz_sla_days,
    last_synced_at,
    last_biz_date,
    row_count,
    last_row_delta,
    last_checked_at,
    CASE WHEN last_synced_at IS NULL THEN NULL
         ELSE date_diff('hour', last_synced_at, now()) END        AS hours_since_sync,
    CASE WHEN last_biz_date IS NULL THEN NULL
         ELSE date_diff('day', last_biz_date, current_date) END   AS days_since_biz,
    CASE
        WHEN status = 'retired'
          OR expected_cadence IN ('once', 'retired')              THEN FALSE
        WHEN sla_hours IS NULL                                    THEN FALSE
        WHEN last_synced_at IS NULL                               THEN TRUE
        WHEN last_synced_at < now() - to_hours(sla_hours)         THEN TRUE
        ELSE FALSE
    END                                                           AS is_stale,
    CASE
        WHEN status = 'retired'
          OR expected_cadence IN ('once', 'retired')              THEN FALSE
        WHEN biz_sla_days IS NULL                                 THEN FALSE
        WHEN last_biz_date IS NULL                                THEN TRUE
        WHEN last_biz_date < current_date - biz_sla_days          THEN TRUE
        ELSE FALSE
    END                                                           AS is_data_stale,
    (COALESCE(row_count, 0) = 0)                                  AS is_empty
FROM core.sync_registry;
