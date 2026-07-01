-- Daily-report centralization (PROVENANCE-MAP §2, 2026-07-01). Version 1061.
--
-- The daily report LIVE-pulls three of its sections at render time (§1 email
-- sent/opps per workspace, §1b per-campaign infra fan-out, §2 SMS sent from the
-- Sendivo billing report). This DDL adds the nightly warehouse mirrors for those
-- pulls so the numbers exist in the warehouse with freshness bookkeeping.
-- ADDITIVE ONLY: the renderer keeps live-pulling until a separate flip decision;
-- nothing existing is modified or repointed.
--
-- WHY NEW TABLES (not raw_pipeline_campaign_daily_metrics): the pipeline daily
-- fact NULL-buckets 140-270k sends/day (provenance map §1) and is mid-retirement.
-- WHY NOT core.campaign_daily: that is a nightly FULL-HISTORY rebuild (Track H)
-- with its own lifecycle; these mirrors are day-scoped incremental upserts that
-- exactly reproduce the renderer's live calls, so a later flip is a straight swap.
--
-- Grain + semantics (validated live 2026-07-01 against the June-30 report):
--   * workspace daily : GET /campaigns/analytics/daily (no campaign_id), one row
--     per (workspace, day). Σ sent over the 8 report-roster workspaces for
--     2026-06-30 == 2,433,955 == the §1 report total.
--   * campaign daily  : GET /campaigns/analytics/daily?campaign_id=… fan-out,
--     one row per (campaign, day). `unique_replies` is HUMAN (excludes auto);
--     `unique_replies_automatic` is the separate auto count (NOT a subset).
--     Σ campaign sent < workspace sent by the subsequence-send gap (§1b's
--     documented honest residual) — do NOT expect exact equality.
--   * tag defs        : raw tag id -> label. Campaign rows carry the RAW
--     email_tag_list ids + labels. Deliberately NO tag->tier/infra mapping here:
--     the taxonomy is a pending business definition; store raw dimensions only.
--   * sync status     : one row per (run, workspace) — the 100%-or-flagged rule.
--     A workspace that fails to ingest is VISIBLY recorded, never silently zero.
--   * sendivo billing : GET /billing/report(start=end=day), one row per
--     (day, sub_account). sms_fee_qty is the §2 "SMS sent" truth
--     (billing_report.sms_fees.quantity — NOT delivery_metrics); the fee columns
--     double as the SMS cost/day feed. Ren3 (14603) 2026-06-30 == 94,796.

-- =====================================================================
-- §1 workspace-grain daily analytics (per report-roster workspace)
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_instantly_workspace_analytics_daily (
  workspace_slug             VARCHAR NOT NULL,   -- canonical slug (== Instantly key slug == core.workspace.slug)
  date                       DATE    NOT NULL,
  sent                       BIGINT,
  contacted                  BIGINT,
  new_leads_contacted        BIGINT,
  opened                     BIGINT,
  unique_opened              BIGINT,
  clicks                     BIGINT,
  unique_clicks              BIGINT,
  replies                    BIGINT,             -- human (excludes automatic)
  unique_replies             BIGINT,
  replies_automatic          BIGINT,
  unique_replies_automatic   BIGINT,
  opportunities              BIGINT,             -- == §1 report "Opps"
  unique_opportunities       BIGINT,
  api_response_raw           VARCHAR,            -- the day's raw JSON object
  _loaded_at                 TIMESTAMPTZ NOT NULL,
  _run_id                    VARCHAR NOT NULL,
  PRIMARY KEY (workspace_slug, date)
);

-- =====================================================================
-- §1b campaign-grain daily analytics (per campaign per day, w/ raw tags)
-- =====================================================================
-- campaign_id is the ONLY stable campaign key (names get renamed — e.g. the
-- bounce guard's 'BOUNCED ' prefix); campaign_name here is a convenience copy
-- of the latest name at load time. Durable identity lives in
-- raw_instantly_campaign_dim below.
CREATE TABLE IF NOT EXISTS raw_instantly_campaign_analytics_daily (
  campaign_id                VARCHAR NOT NULL,
  date                       DATE    NOT NULL,
  workspace_slug             VARCHAR NOT NULL,
  campaign_name              VARCHAR,            -- latest name at load (UNSTABLE; key on campaign_id)
  campaign_status            INTEGER,            -- Instantly v2 status code (1 active, 2 paused, ...)
  sent                       BIGINT,
  contacted                  BIGINT,
  new_leads_contacted        BIGINT,
  opened                     BIGINT,
  unique_opened              BIGINT,
  clicks                     BIGINT,
  unique_clicks              BIGINT,
  replies                    BIGINT,             -- human (excludes automatic)
  unique_replies             BIGINT,             -- == §1b HumanRR numerator
  replies_automatic          BIGINT,
  unique_replies_automatic   BIGINT,
  opportunities              BIGINT,
  unique_opportunities       BIGINT,             -- == §1b →opp numerator
  tag_ids                    VARCHAR,            -- JSON array: raw email_tag_list UUIDs
  tag_labels                 VARCHAR,            -- JSON array: resolved raw labels (NO tier mapping — pending business def)
  _loaded_at                 TIMESTAMPTZ NOT NULL,
  _run_id                    VARCHAR NOT NULL,
  PRIMARY KEY (campaign_id, date)
);

-- =====================================================================
-- DURABLE campaign dimension registry (append/upsert, NEVER wipe-and-reload)
-- =====================================================================
-- Campaign NAMES are UNSTABLE: the bounce guard renames campaigns by prefixing
-- 'BOUNCED ' (campaign_id unchanged), and other renames happen too. campaign_id
-- is the ONLY stable key. This table keeps one durable row per campaign ever
-- seen: latest name/status/tags AND the first-seen name, surviving renames and
-- nightly reloads (unlike the campaign→account mapping, which is wiped nightly —
-- 809 June campaigns → only 135 still resolvable). The queued campaign-scoreboard
-- registry (handoffs/2026-07-01-campaign-scoreboard-standing-view.md) is expected
-- to seed from here.
CREATE TABLE IF NOT EXISTS raw_instantly_campaign_dim (
  campaign_id       VARCHAR PRIMARY KEY,
  workspace_slug    VARCHAR NOT NULL,
  campaign_name     VARCHAR,            -- LATEST name (mutable; renames overwrite)
  campaign_status   INTEGER,            -- latest status code
  first_seen_name   VARCHAR,            -- name at first ingest (immutable)
  first_seen_at     TIMESTAMPTZ,        -- when this ingest first saw the campaign (immutable)
  last_seen_at      TIMESTAMPTZ,        -- last run that saw the campaign in /campaigns
  tag_ids           VARCHAR,            -- JSON array: latest raw email_tag_list UUIDs
  tag_labels        VARCHAR,            -- JSON array: latest resolved raw labels (NO tier mapping)
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR NOT NULL
);

-- =====================================================================
-- Raw tag catalog (only ids actually referenced by campaigns; label cache
-- + fallback when the per-id fetch fails on a fragile night)
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_instantly_tag_def (
  tag_id            VARCHAR PRIMARY KEY,
  label             VARCHAR,
  organization_id   VARCHAR,
  timestamp_created VARCHAR,
  timestamp_updated VARCHAR,
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR NOT NULL
);

-- =====================================================================
-- Per-run per-workspace ingest status (100%-or-flagged; append-only, tiny)
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_instantly_analytics_sync_status (
  _run_id             VARCHAR NOT NULL,
  workspace_slug      VARCHAR NOT NULL,
  status              VARCHAR NOT NULL,   -- 'ok' | 'failed'
  error               VARCHAR,            -- NULL when ok
  window_start        DATE,
  window_end          DATE,
  ws_day_rows         BIGINT,             -- workspace-grain rows upserted
  campaign_day_rows   BIGINT,             -- campaign-grain rows upserted
  campaigns_total     BIGINT,             -- campaigns fanned out
  campaigns_failed    BIGINT,             -- per-campaign fetches that failed (partial ws)
  tags_unresolved     BIGINT,             -- referenced tag ids with no resolvable label
  _loaded_at          TIMESTAMPTZ NOT NULL
);

-- =====================================================================
-- §2 SMS sent + $ : Sendivo billing report at DAY grain per sub-account
-- (existing raw_sendivo_billing is month-window grain; untouched)
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_sendivo_billing_daily (
  metric_date          DATE   NOT NULL,
  sub_account_id       BIGINT NOT NULL,
  location_id          VARCHAR,
  total_spend          DOUBLE,
  sms_fee_qty          BIGINT,            -- == §2 "SMS Sent" truth (sms_fees.quantity)
  sms_fee_usd          DOUBLE,
  carrier_fee_qty      BIGINT,
  carrier_fee_usd      DOUBLE,
  campaign_setup_usd   DOUBLE,
  campaign_renewal_usd DOUBLE,
  brand_fee_usd        DOUBLE,
  phone_setup_usd      DOUBLE,
  phone_renewal_usd    DOUBLE,
  raw_json             VARCHAR,
  _loaded_at           TIMESTAMPTZ NOT NULL,
  _run_id              VARCHAR NOT NULL,
  PRIMARY KEY (metric_date, sub_account_id)
);
