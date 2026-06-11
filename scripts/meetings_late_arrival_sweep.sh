#!/usr/bin/env bash
# Weekly meetings late-arrival sweep. Cron target (UTC, droplet):
#   15 8 * * 0 /root/renaissance-warehouse/scripts/meetings_late_arrival_sweep.sh >> /root/renaissance-warehouse/logs/meetings_late_arrival_sweep.log 2>&1
#
# Companion to meetings_refresh.sh (2026-06-11 hygiene Task 1): the watermark mirror
# structurally skips upstream rows that arrive late with an old posted_at, so the gap
# re-accumulates (~1%/quarter) without this. Pulls the missing rows by anti-join, then
# rebuilds core.meeting + the campaign_daily meetings column.
#
# Single-writer discipline: every write wrapped in flock with bounded wait; skip
# (exit 0) if a long writer holds it. Never scheduled inside 03:30-05:45 UTC.

set -euo pipefail

cd /root/renaissance-warehouse
PY=.venv/bin/python
LOCK=/root/core/warehouse.write.lock

echo "=== meetings_late_arrival_sweep @ $(date -u +%FT%TZ) ==="

# 1. Anti-join pull of late-arrived rows (no watermark).
flock -w 300 "$LOCK" -c "$PY scripts/meetings_late_arrival_sweep.py" \
    || { echo "SKIP: writer lock busy or sweep failed"; exit 0; }

# 2. Rebuild core.meeting (idempotent full rebuild).
flock -w 300 "$LOCK" -c "$PY -m core.orchestrator --phase canonical --ingest meeting" \
    || { echo "SKIP: writer lock busy (meeting)"; exit 0; }

# 3. Re-apply the meetings column of core.campaign_daily in place (same as
#    meetings_refresh.sh step 3).
flock -w 300 "$LOCK" -c "duckdb /root/core/warehouse.duckdb \"
UPDATE core.campaign_daily d SET meetings_booked = COALESCE(m.n, 0)
FROM (
  SELECT campaign_id, CAST(posted_at AS DATE) AS date, count(*) n
  FROM core.meeting WHERE campaign_id IS NOT NULL GROUP BY 1, 2
) m
WHERE m.campaign_id = d.campaign_id AND m.date = d.date;
\"" || { echo "SKIP: writer lock busy (campaign_daily)"; exit 0; }

echo "=== meetings_late_arrival_sweep done @ $(date -u +%FT%TZ) ==="
