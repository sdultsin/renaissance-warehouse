#!/usr/bin/env bash
# instantly_hourly_snapshot.sh — cron entrypoint for the hourly campaign-counter snapshot.
#
# Runs scripts/instantly_hourly_snapshot.py (DDL 1095) under the single-writer
# flock. Failure-only alerting: a lock-skip (exit 75) is a tolerated missed tick
# (the nightly legitimately holds the writer ~05:30Z; cumulative diffs tolerate
# gaps) and is NOT counted; any other non-zero exit increments a consecutive-
# failure counter and pages #cc-sam (via scripts/alert_slack.py) at 3 in a row
# (~3h broken), then once more every 24 (daily reminder, no healthy pings).
#
# Cron (UTC):  52 * * * *  /root/renaissance-warehouse/scripts/instantly_hourly_snapshot.sh \
#                >> /root/renaissance-warehouse/logs/instantly_hourly_snapshot.log 2>&1
set -uo pipefail

WH="${WAREHOUSE_REPO:-/root/renaissance-warehouse}"
STATE="$WH/logs/instantly_hourly_snapshot.failcount"
mkdir -p "$WH/logs"

echo "==== instantly hourly snapshot $(date -u +%FT%TZ) ===="

WAREHOUSE_LOCK_WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-540}" \
  bash "$WH/scripts/with_warehouse_lock.sh" \
  "$WH/.venv/bin/python" "$WH/scripts/instantly_hourly_snapshot.py"
rc=$?

if [ "$rc" -eq 0 ]; then
  rm -f "$STATE"
  exit 0
fi

if [ "$rc" -eq 75 ]; then
  # writer lock busy (nightly / another writer) — tolerated missed tick.
  echo "lock busy — skipping this tick (rc=75, not counted as failure)"
  exit 0
fi

fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"
echo "FAILED rc=$rc (consecutive failures: $fails)"

if [ "$fails" -eq 3 ] || [ $(( fails % 24 )) -eq 0 ]; then
  "$WH/.venv/bin/python" "$WH/scripts/alert_slack.py" \
    ":rotating_light: instantly_hourly_snapshot has failed $fails consecutive hourly ticks (last rc=$rc) — send-hour snapshot data (raw_instantly_campaign_hourly_snapshot) is not landing. Log: $WH/logs/instantly_hourly_snapshot.log" \
    || true
fi

exit "$rc"
