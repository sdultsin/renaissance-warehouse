-- @gate: add
-- Depends on 32
-- Append-only daily snapshot of campaign-grain cumulative analytics. Version 1066.
--
-- WHY THIS EXISTS (2026-07-03):
--   raw_instantly_campaign_analytics (v32) is UPSERTed one-row-per-campaign
--   (latest snapshot only). Two consequences make accurate WINDOWED opportunity
--   counts impossible from what we store today:
--     1. When a campaign is DELETED from Instantly it stops refreshing; its last
--        cumulative value is frozen and we can never recover a per-window count
--        for it (the analytics endpoint only returns LIVE campaigns).
--     2. The daily fact table's unique_opportunities double-counts across days
--        (a lead in "opportunity" state is counted every day) — summing a month
--        of days overstates the true distinct count (verified: Instantly-Short
--        June summed daily = 534 vs the true windowed 367).
--
--   This table appends ONE dated row per campaign per nightly run, capturing the
--   CUMULATIVE counters (total_opportunities, emails_sent_count, ...) while the
--   campaign is still alive. Any window is then reconstructed by DIFFERENCING the
--   cumulative across snapshot dates — deletion-proof and never double-counted.
--
--   AUTHORITATIVE (exact, gap-tolerant) windowed reconstruction:
--       opps_in_[start,end] = last(total_opportunities on/before end)
--                           - last(total_opportunities on/before start-1)
--   This endpoint-difference is exact even across missed nightly runs and even
--   through downward revisions. Prefer it for any reconciliation / deal report.
--
--   Populated by entities/campaign_analytics_snapshot.py in the `instantly`
--   phase, immediately AFTER campaign_analytics (so the source table is fresh).
--
-- GRAIN: one row per (snapshot_date, campaign_id). UPSERT on that PK, so a
--   same-day re-run overwrites rather than duplicates.

CREATE TABLE IF NOT EXISTS raw_instantly_campaign_analytics_snapshot (
  snapshot_date            DATE        NOT NULL,
  campaign_id              VARCHAR     NOT NULL,
  workspace_id             VARCHAR     NOT NULL,
  campaign_name            VARCHAR,
  campaign_status          INTEGER,
  emails_sent_count        BIGINT,     -- cumulative, == UI "sent"
  reply_count_unique       BIGINT,     -- cumulative, == UI "replied"
  total_opportunities      BIGINT,     -- cumulative, == UI "opportunities"
  total_opportunity_value  BIGINT,
  _loaded_at               TIMESTAMPTZ NOT NULL,
  _run_id                  VARCHAR     NOT NULL,
  PRIMARY KEY (snapshot_date, campaign_id)
);

-- Per-day NEW opportunities/sends: the delta of the cumulative vs the campaign's
-- PREVIOUS snapshot. These deltas are additive, so SUM() over any date window
-- gives an accurate, deduplicated, deletion-proof count of opportunities OPENED
-- within that window:
--     SELECT campaign_name, SUM(new_opportunities)
--     FROM v_campaign_opps_daily
--     WHERE snapshot_date BETWEEN '2026-08-01' AND '2026-08-31' GROUP BY 1;
--
-- Two deliberate design points (both reviewed 2026-07-03):
--  * BASELINE DAY: a campaign's first-ever snapshot has no prior row, so
--    new_opportunities is NULL there (NOT the whole pre-capture backlog). This
--    prevents a window that spans capture-start from silently counting historical
--    opps as "new in window". Consequence: SUM(new_opportunities) all-time will
--    UNDERCOUNT by each campaign's capture-start backlog — that is intended; the
--    view answers "new WITHIN the window since capture began", and the exact
--    cumulative is always available via total_opportunities differencing above.
--  * GAP TOLERANCE: new_opportunities is a per-day series only where snapshots are
--    contiguous. If a nightly is missed, the resume day's delta lumps the gap's
--    growth onto one day — SUM over a window is still correct, but do NOT read a
--    single snapshot_date's new_opportunities as a true single-day figure.
--    GREATEST(delta,0) floors Instantly's occasional downward revisions; for exact
--    reconciliation use the endpoint-difference formula above, not the view SUM.
CREATE OR REPLACE VIEW v_campaign_opps_daily AS
WITH s AS (
  SELECT
    snapshot_date, campaign_id, campaign_name, workspace_id,
    total_opportunities, emails_sent_count,
    LAG(total_opportunities) OVER (PARTITION BY campaign_id ORDER BY snapshot_date) AS prev_opps,
    LAG(emails_sent_count)   OVER (PARTITION BY campaign_id ORDER BY snapshot_date) AS prev_sent
  FROM raw_instantly_campaign_analytics_snapshot
)
SELECT
  snapshot_date, campaign_id, campaign_name, workspace_id,
  total_opportunities,
  CASE WHEN prev_opps IS NULL THEN NULL
       ELSE GREATEST(total_opportunities - prev_opps, 0) END AS new_opportunities,
  emails_sent_count,
  CASE WHEN prev_sent IS NULL THEN NULL
       ELSE GREATEST(emails_sent_count - prev_sent, 0) END AS new_emails_sent
FROM s;
