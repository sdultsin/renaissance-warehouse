#!/usr/bin/env bash
# Meetings-only refresh. Cron target (UTC, droplet):
#   0 7 * * * /root/renaissance-warehouse/scripts/meetings_refresh.sh >> /root/renaissance-warehouse/logs/meetings_refresh.log 2>&1
#
# Closes the meetings cadence race (2026-06-11): the Slack scraper lands the previous
# day's meetings in Pipeline-Supabase at ~06:05 UTC and the matcher cron runs 07:30,
# but the warehouse nightly mirrors at 03:30 — 2.5h BEFORE the scraper — so meetings
# were always ~2 days stale even when everything worked. This pulls just the meetings
# mirror + rebuilds core.meeting + re-applies the meetings column of campaign_daily
# after the upstream data for "yesterday" actually exists.
#
# Single-writer discipline: every write is wrapped in flock on the warehouse write
# lock with a bounded wait; if another writer holds it past the wait we skip (exit 0)
# rather than queue into someone's long job. Never scheduled inside 03:30-05:45 UTC.

set -euo pipefail

cd /root/renaissance-warehouse
PY=.venv/bin/python
LOCK=/root/core/warehouse.write.lock

echo "=== meetings_refresh @ $(date -u +%FT%TZ) ==="

# 1. Mirror just meetings_booked_raw from Pipeline-Supabase (watermark + 2d overlap).
#    The phase registers a single 'all' ingest, so table selection goes through the
#    PIPELINE_MIRROR_ONLY env filter, not --ingest.
flock -w 300 "$LOCK" -c "PIPELINE_MIRROR_ONLY=meetings_booked_raw $PY -m core.orchestrator --phase pipeline_mirror" \
    || { echo "SKIP: writer lock busy or mirror failed"; exit 0; }

# 2. Rebuild core.meeting (idempotent full rebuild).
flock -w 300 "$LOCK" -c "$PY -m core.orchestrator --phase canonical --ingest meeting" \
    || { echo "SKIP: writer lock busy (meeting)"; exit 0; }

# 3. Re-apply the meetings + bounces columns of core.campaign_daily in place.
#    (A full build_campaign_daily.py re-fetches the Instantly API for every campaign —
#    unnecessary here; the nightly does that. We only refresh the join columns.)
flock -w 300 "$LOCK" -c "duckdb /root/core/warehouse.duckdb \"
UPDATE core.campaign_daily d SET meetings_booked = COALESCE(m.n, 0)
FROM (
  SELECT campaign_id, CAST(posted_at AS DATE) AS date, count(*) n
  FROM core.meeting WHERE campaign_id IS NOT NULL GROUP BY 1, 2
) m
WHERE m.campaign_id = d.campaign_id AND m.date = d.date;
\"" || { echo "SKIP: writer lock busy (campaign_daily)"; exit 0; }

echo "=== meetings_refresh done @ $(date -u +%FT%TZ) ==="
