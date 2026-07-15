-- @gate: add
-- ============================================================================
-- 1101_mof_instantly_history_tables.sql — landing tables for the ONE-TIME Instantly
-- day-grain analytics history escrow (fetched 2026-07-15 from the Instantly API,
-- GET /campaigns/analytics/daily + /steps + /campaigns list, back to workspace creation,
-- INCLUDING campaigns deleted from Instantly).
--
-- WHY (coverage-map Defect A / kpi-corrected-weekly.md): raw_pipeline_campaign_daily_metrics'
-- frozen region (≤ ~2026-05, last synced 2026-05-12) is missing up to ~89% of true sends in
-- Feb→mid-Mar (weekly correction factors 4.5–8.7×); the API serves day-grain truth much
-- further back. NEITHER source is a superset (Defect E: the API retroactively loses whole
-- days — e.g. campaign 3a4f57f4-2114 on 2026-05-07: API=0 vs warehouse=25,197) — hence the
-- MAX-stitch views in DDL 1105, and hence these rows land in SEPARATE frozen tables, never
-- mixed into the live nightly sync surfaces (raw_instantly_workspace_analytics_daily /
-- raw_instantly_campaign_analytics_daily), whose rolling-window freshness semantics stay clean.
--
-- DATA / LOAD PATH (escrow relocation): the normalized escrow parquet lives OUT-OF-BAND at
-- <repo>/seed_data/mof_bi_20260715/ (seed_data/ is gitignored BY DESIGN — "data never in
-- the repo", public-repo rule; untracked files survive the hourly `git reset --hard` guard).
-- Copy it onto the droplet checkout at ship time (ship notes step); the durable master copy
-- is the parent-repo deliverable dir
-- (Renaissance/deliverables/2026-07-14-cold-email-bi/warehouse-seed/mof_bi_20260715/).
-- Once the first load lands, THE WAREHOUSE TABLES THEMSELVES are the durable home — they
-- ride the nightly publish and the MotherDuck migration; the droplet's original escrow
-- (/root/mof/instantly_backfill/, box deleted ~2026-07-25) is then only provenance.
-- Loading is done by entities/mof_bi_history.py (idempotent INSERT … ON CONFLICT DO
-- NOTHING; first nightly run = the initial backfill, variant_copy precedent). NOT loaded
-- here: the PR gate rejects SQL referencing filesystem paths (1089 precedent), and a
-- read_parquet seed inside DDL is the exact failure class that killed the nightly for
-- 4 days (DDL 92 post-mortem) — the entity degrades a missing seed file to a logged
-- warning instead.
--
-- APPEND-ONLY / FROZEN semantics: rows are a point-in-time API capture (fetched_at
-- 2026-07-15); they are never updated or deleted. Deleted campaigns freeze at this capture;
-- live campaigns keep refreshing via the existing nightly sync surfaces (stitched in 1105).
--
-- EXPECTED LOAD (validation, from the committed parquet):
--   raw_instantly_ws_daily_history        2,829 rows · 10 workspaces · sum(sent)=192,412,822
--     (parquet holds 2,837 incl. 8 identical-value Jan-1 fetch-boundary duplicates)
--   raw_instantly_campaign_daily_history 20,664 rows · 1,711 campaigns
--   raw_instantly_campaign_steps_history 16,183 rows · 1,680 campaigns · sum(sent)=100,985,165
--     (parquet holds 19,059; 2,876 null-step/variant reply-residue rows excluded at load —
--      sent=0, sum(replies)=10,078, unattributable to a variant; retained in the parquet)
--   raw_instantly_campaign_dim_history     288 rows · all source='list'
--
-- COVERAGE HONESTY (state in every consumer):
--   * API side exists ONLY for live-key workspaces (koi-and-destroy back to 2024-01-15,
--     prospects-power 2025-09-08+, renaissance-1/2/4/5, the-eagles ≤2026-05-23,
--     the-gatekeepers, warm-leads, tariffs). DELETED workspaces (renaissance-3/6/7, the-dyad,
--     equinox, outlook-1/2/3, automated-applications, erc-1/2, section-125-1/2) have NO API
--     side — their history lives only in raw_pipeline_campaign_daily_metrics (a known floor).
--   * prospects-power campaign-grain is PARTIAL (31 campaigns / 3.56M sent vs 25.7M ws-grain
--     — daily-cap-throttled fetch); its ws-grain is complete. Campaign-grain consumers must
--     not read prospects-power history as complete.
--
-- Reversible: DROP the four tables (raw escrow also survives as
-- seed_data/mof_bi_20260715/instantly_backfill_raw_20260715.tar.gz — raw JSON provenance).
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.raw_instantly_ws_daily_history (
  workspace_slug            VARCHAR NOT NULL,
  date                      DATE    NOT NULL,
  sent                      BIGINT,
  contacted                 BIGINT,
  new_leads_contacted       BIGINT,
  opened                    BIGINT,
  unique_opened             BIGINT,
  clicks                    BIGINT,
  unique_clicks             BIGINT,
  replies                   BIGINT,
  unique_replies            BIGINT,
  replies_automatic         BIGINT,
  unique_replies_automatic  BIGINT,
  opportunities             BIGINT,
  unique_opportunities      BIGINT,
  fetched_at                DATE,                -- API pull date (escrow provenance)
  _source                   VARCHAR DEFAULT 'mof_backfill_20260715',
  _loaded_at                TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (workspace_slug, date)
);

CREATE TABLE IF NOT EXISTS main.raw_instantly_campaign_daily_history (
  campaign_id               VARCHAR NOT NULL,
  date                      DATE    NOT NULL,
  workspace_slug            VARCHAR NOT NULL,
  sent                      BIGINT,
  contacted                 BIGINT,
  new_leads_contacted       BIGINT,
  opened                    BIGINT,
  unique_opened             BIGINT,
  clicks                    BIGINT,
  unique_clicks             BIGINT,
  replies                   BIGINT,
  unique_replies            BIGINT,
  replies_automatic         BIGINT,
  unique_replies_automatic  BIGINT,
  opportunities             BIGINT,
  unique_opportunities      BIGINT,
  fetched_at                DATE,
  _source                   VARCHAR DEFAULT 'mof_backfill_20260715',
  _loaded_at                TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (campaign_id, date)
);

-- Step/variant-grain analytics capture (per campaign LIFETIME as of fetched_at — NOT day
-- grain). Feeds core.v_campaign_variant_history (DDL 1108): deleted campaigns freeze at
-- this capture; live campaigns keep refreshing via raw_pipeline_campaign_data nightly.
CREATE TABLE IF NOT EXISTS main.raw_instantly_campaign_steps_history (
  campaign_id               VARCHAR NOT NULL,
  step                      VARCHAR NOT NULL,
  variant                   VARCHAR NOT NULL,
  workspace_slug            VARCHAR NOT NULL,
  sent                      BIGINT,
  opened                    BIGINT,
  unique_opened             BIGINT,
  clicks                    BIGINT,
  unique_clicks             BIGINT,
  replies                   BIGINT,
  unique_replies            BIGINT,
  replies_automatic         BIGINT,
  unique_replies_automatic  BIGINT,
  fetched_at                DATE    NOT NULL,   -- capture date; part of PK so a future
  _source                   VARCHAR DEFAULT 'mof_backfill_20260715',
  _loaded_at                TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (campaign_id, step, variant, fetched_at)  -- re-capture appends, never overwrites
);

-- Durable identity for campaigns recovered by the backfill (names for campaigns since
-- deleted from Instantly). Complements raw_instantly_campaign_dim (live census, Jun+ era)
-- and core.campaign_dim_patch (DDL 1104); coalesced in core.v_campaign_dim_unified.
CREATE TABLE IF NOT EXISTS main.raw_instantly_campaign_dim_history (
  campaign_id        VARCHAR PRIMARY KEY,
  workspace_slug     VARCHAR,
  name               VARCHAR,
  status             INTEGER,           -- Instantly numeric status at capture
  timestamp_created  TIMESTAMPTZ,
  source             VARCHAR,           -- 'list' | 'meta_rescue'
  fetched_at         DATE,
  _source            VARCHAR DEFAULT 'mof_backfill_20260715',
  _loaded_at         TIMESTAMPTZ DEFAULT now()
);
