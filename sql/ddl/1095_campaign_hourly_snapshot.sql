-- @gate: add
-- Depends on 32
-- Append-only HOURLY snapshot of campaign-grain cumulative analytics. Version 1095.
--
-- WHY THIS EXISTS (2026-07-10, Ben's send-time ask):
--   "Reply rate by SEND hour" is not computable from anything we store today —
--   Instantly only exposes DAILY grain, and nothing records at what hour emails
--   actually went out. This table makes send-hour analysis possible GOING
--   FORWARD by snapshotting each campaign's CUMULATIVE counters once per hour
--   (a small hourly cron pulls GET /campaigns/analytics per workspace — the
--   same endpoint/fields as raw_instantly_campaign_analytics v32 and the DAILY
--   snapshot v1066). Sends-per-hour is then the successive DIFF of
--   emails_sent_count between consecutive snapshots of the same campaign.
--
--   This is the hourly sibling of v1066 (raw_instantly_campaign_analytics_snapshot,
--   daily grain): same source, same cumulative-counter differencing logic, finer
--   time grain. ~1.1k campaigns x 24 ticks/day ≈ 25k small numeric rows/day —
--   negligible; keep everything (no retention trimming).
--
--   GAP TOLERANCE: a missed tick is fine. Cumulative differencing over the
--   surviving ticks is still exact; a gap simply lumps that window's activity
--   onto the next snapshot's delta (same design point as v1066).
--
--   Populated by scripts/instantly_hourly_snapshot.py (standalone hourly cron
--   under with_warehouse_lock.sh — NOT an orchestrator phase; the nightly is
--   too coarse for this purpose). Deliberately NOT in sync_registry (gate
--   review 1095, option chosen): the table is gap-tolerant by design and the
--   cron wrapper's consecutive-failure Slack alert is the freshness guard —
--   registry-level staleness alarms would false-positive on tolerated ticks.
--
-- GRAIN: one row per (snapshot_hour, workspace_id, campaign_id). snapshot_hour
--   is the hour-truncated UTC tick; snapshot_ts is the exact pull time (use it
--   for per-hour RATE math when a tick fires late). UPSERT on the PK so a
--   same-hour re-run overwrites rather than duplicates. workspace_id is in the
--   PK (and in the view's PARTITION BY) deliberately: Instantly campaign_ids
--   are globally unique today, but if one ever recurred across workspaces a
--   campaign-only key would silently overwrite rows and diff counters across
--   workspace boundaries (gate review 1095) — workspace-scoping costs nothing
--   and removes that failure class.

CREATE TABLE IF NOT EXISTS raw_instantly_campaign_hourly_snapshot (
  snapshot_hour             TIMESTAMPTZ NOT NULL,  -- date_trunc('hour', pull time, UTC)
  campaign_id               VARCHAR     NOT NULL,
  workspace_id              VARCHAR     NOT NULL,
  workspace_slug            VARCHAR,               -- INSTANTLY_KEY_<SLUG> that pulled it
  snapshot_ts               TIMESTAMPTZ NOT NULL,  -- exact pull time
  campaign_name             VARCHAR,
  campaign_status           INTEGER,
  -- cumulative counters as returned by GET /campaigns/analytics (UI-matching):
  leads_count               BIGINT,
  contacted_count           BIGINT,
  new_leads_contacted_count BIGINT,
  emails_sent_count         BIGINT,   -- cumulative == UI "sent"; hourly diff = sends that hour
  reply_count               BIGINT,
  reply_count_unique        BIGINT,   -- cumulative == UI "replied"
  reply_count_automatic     BIGINT,
  bounced_count             BIGINT,
  unsubscribed_count        BIGINT,
  completed_count           BIGINT,
  total_opportunities       BIGINT,   -- cumulative == UI "opportunities"
  total_opportunity_value   BIGINT,
  link_click_count          BIGINT,
  open_count                BIGINT,
  _loaded_at                TIMESTAMPTZ NOT NULL,
  _run_id                   VARCHAR     NOT NULL,
  PRIMARY KEY (snapshot_hour, workspace_id, campaign_id)
);

-- Convenience view: per-hour deltas via LAG over consecutive snapshots.
-- Same two design points as v1066's v_campaign_opps_daily:
--  * first-ever snapshot of a campaign has NULL deltas (no prior row — do not
--    count the pre-capture backlog as "sent this hour");
--  * across a MISSED tick the resume row's delta lumps the whole gap onto one
--    row — hours_since_prev exposes that, so per-hour analysis can filter to
--    hours_since_prev = 1 (or rate-normalize by the actual elapsed time).
-- GREATEST(delta, 0) floors Instantly's occasional downward revisions (v1066
-- design point); counter_revised_down makes a floored revision VISIBLE so
-- hourly analysis can distinguish "no activity" from "counter revised down"
-- (exclude those rows, or reconstruct exactly from the raw cumulative).
CREATE OR REPLACE VIEW v_campaign_sends_hourly AS
WITH s AS (
  SELECT
    snapshot_hour, snapshot_ts, campaign_id, campaign_name, workspace_id, workspace_slug,
    campaign_status, emails_sent_count, reply_count_unique, total_opportunities,
    LAG(snapshot_hour)        OVER w AS prev_hour,
    LAG(snapshot_ts)          OVER w AS prev_ts,
    LAG(emails_sent_count)    OVER w AS prev_sent,
    LAG(reply_count_unique)   OVER w AS prev_replies,
    LAG(total_opportunities)  OVER w AS prev_opps
  FROM raw_instantly_campaign_hourly_snapshot
  WINDOW w AS (PARTITION BY workspace_id, campaign_id ORDER BY snapshot_hour)
)
SELECT
  snapshot_hour, snapshot_ts, campaign_id, campaign_name, workspace_id, workspace_slug,
  campaign_status,
  emails_sent_count, reply_count_unique, total_opportunities,
  CASE WHEN prev_sent    IS NULL THEN NULL ELSE GREATEST(emails_sent_count   - prev_sent,    0) END AS sends_delta,
  CASE WHEN prev_replies IS NULL THEN NULL ELSE GREATEST(reply_count_unique  - prev_replies, 0) END AS replies_delta,
  CASE WHEN prev_opps    IS NULL THEN NULL ELSE GREATEST(total_opportunities - prev_opps,    0) END AS opps_delta,
  CASE WHEN prev_hour IS NULL THEN NULL
       ELSE CAST(date_diff('hour', prev_hour, snapshot_hour) AS INTEGER) END AS hours_since_prev,
  (prev_sent    IS NOT NULL AND emails_sent_count   < prev_sent)
    OR (prev_replies IS NOT NULL AND reply_count_unique  < prev_replies)
    OR (prev_opps    IS NOT NULL AND total_opportunities < prev_opps) AS counter_revised_down
FROM s;
