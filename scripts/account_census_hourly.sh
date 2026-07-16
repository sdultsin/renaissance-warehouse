#!/usr/bin/env bash
# Hourly account_census refresh — keeps fleet STATUS in the warehouse at most ~1h stale.
#
# WHY [2026-07-16, David: "It would be good if the data in the hub would only be one hour old"]
# ---------------------------------------------------------------------------------------------
# The live /accounts poller already writes a parquet EVERY HOUR (run_hourly.sh, cron 7 * * * *),
# but `account_census` only promoted ONE of them per day — whichever was latest when the nightly
# ran (~06:30 UTC). So the whole day's status came from a single morning photograph.
#
# On 2026-07-16 that photograph was taken mid bulk-preset-run: the 06:09 poll caught 24,544 inboxes
# paused (they stayed paused ~05:09->13:09 UTC, then all resumed at 14:09). The Hub therefore showed
# Section 125 as 0 warming / 3,008 paused on a 14,932-inbox workspace for the whole day, when every
# one of them was warming. The data was never fabricated — it was a real state, 9 hours stale.
#
# This ticks the SAME promote hourly, so the census always reflects the most recent poll. It is the
# identical entity the nightly runs (idempotent: transactional DELETE+INSERT for today's census_date
# only, then a last-good carry-forward for any workspace Instantly failed to serve) — so running it
# more often can only make it fresher, never structurally different.
#
# Deliberately runs ONLY --phase account_census. It does NOT run account_status_history: that entity
# is append-only and records CHANGES vs the last observation, so it stays on its nightly cadence and
# cannot be disturbed by the census moving underneath it during the day.
#
# NOT a full answer to "the Hub is 1h old": the Hub reads the PROMOTED serving snapshot (~122GB copy,
# ~16 min, a couple of times a day), so the Hub is still (promote age + <=1h) stale. This removes the
# staleness at SOURCE; the serving path is a separate piece of work.
#
# Modelled 1:1 on instantly_hourly_snapshot.sh (the house pattern for an hourly warehouse writer):
# take the warehouse write lock, treat "lock busy" as a clean skip (rc=75, NOT a failure — another
# writer owning the DB is normal), count consecutive real failures, and alert Slack rather than fail
# silently. Never fights a live writer: the whole point of the lock wait.
set -uo pipefail
WH="${WAREHOUSE_REPO:-/root/renaissance-warehouse}"
STATE="$WH/logs/account_census_hourly.failcount"
mkdir -p "$WH/logs"
echo "==== account_census hourly $(date -u +%FT%TZ) ===="

# 540s: same wait the hourly instantly snapshot uses. Long enough to sit out a normal writer, short
# enough that a stuck writer never stacks two of these ticks on top of each other.
WAREHOUSE_LOCK_WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-540}" \
  bash "$WH/scripts/with_warehouse_lock.sh" \
  "$WH/.venv/bin/python" -m core.orchestrator --phase account_census
rc=$?

if [ "$rc" -eq 0 ]; then
  rm -f "$STATE"
  exit 0
fi
if [ "$rc" -eq 75 ]; then
  echo "lock busy — skipping this tick (rc=75, not counted as failure)"
  exit 0
fi
fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"
echo "FAILED rc=$rc (consecutive failures: $fails)"
if [ "$fails" -eq 3 ] || [ $(( fails % 24 )) -eq 0 ]; then
  "$WH/.venv/bin/python" "$WH/scripts/alert_slack.py" \
    ":rotating_light: account_census_hourly has failed $fails consecutive hourly ticks (last rc=$rc) — fleet STATUS in core.account_census is going stale, so the Data Hub's paused/disconnected/warming counts will drift from live Instantly. Log: $WH/logs/account_census_hourly.log" \
    || true
fi
exit 0
