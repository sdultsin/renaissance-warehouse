#!/usr/bin/env bash
# nightly_success_watchdog.sh — verifies the warehouse NIGHTLY actually COMMITTED, and
# detects warehouse-writer lock STARVATION. Alerts #cc-sam (tag Sam) on failure, gated.
#
# Monitors the OUTCOME, not "the cron fired":
#   1. core.sync_run has a FULL nightly (phase_count >= MIN_PHASES, status='success',
#      phase_failed_count=0) that STARTED after the most recent scheduled 03:30Z nightly.
#   2. core.campaign_daily max(date) is recent (>= today-2 UTC) — the nightly's headline fact.
#   3. the serving snapshot file (/opt/duckdb/warehouse_current.duckdb) mtime is < SNAPSHOT_STALE_HRS.
#   4. lock STARVATION: the warehouse-writer flock has not been HELD continuously longer than
#      LOCK_HELD_MAX_HRS (a stuck/runaway writer starving the nightly).
# All reads are READ-ONLY against the serving snapshot — safe to run anytime, never opens the
# live writer.
#
# Gating (mirrors /root/monitors/portal_feed_freshness_watchdog.sh): alert on the 2nd consecutive
# unhealthy check (rides out a single transient blip / a nightly still finishing), one alert per
# outage keyed by reason, plus a single RECOVERED ping. State in $STATE.
#
# Cron (UTC) — after the 06:30Z snapshot publish; re-checks catch a late-finishing nightly:
#   15 7  * * * /root/renaissance-warehouse/scripts/nightly_success_watchdog.sh >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1
#   0  9  * * * /root/renaissance-warehouse/scripts/nightly_success_watchdog.sh >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1
set -u

DUCKDB="${DUCKDB:-/usr/local/bin/duckdb}"
SNAPSHOT="${WAREHOUSE_SNAPSHOT:-/opt/duckdb/warehouse_current.duckdb}"
LOCK_FILE="${WAREHOUSE_LOCK_FILE:-/root/core/warehouse.write.lock}"
ENV_FILE="${MONITOR_ENV:-/root/monitors/.env}"
STATE="${STATE:-/tmp/nightly-success-watchdog.state}"
CH="${SLACK_CHANNEL:-C0AR0EA21C1}"          # #cc-sam
MENTION="${SLACK_MENTION:-<@U0AM2CQHW9E>}"  # Sam
MIN_PHASES="${MIN_PHASES:-20}"              # full nightly is 30 phases; >=20 = a real full run
SNAPSHOT_STALE_HRS="${SNAPSHOT_STALE_HRS:-28}"   # 06:30 publish + grace
LOCK_HELD_MAX_HRS="${LOCK_HELD_MAX_HRS:-4}"      # a legit nightly+heal fits well under this
NOW_TS="$(date -u +%FT%TZ)"

post_slack() {
  local msg="$1" TOKEN
  TOKEN="$(grep -E '^CC_SLACK_BOT_TOKEN=' "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'"'"')"
  [ -z "$TOKEN" ] && { echo "WARN no CC_SLACK_BOT_TOKEN in $ENV_FILE — cannot alert" >&2; return 0; }
  curl -s -X POST https://slack.com/api/chat.postMessage \
    -H "Authorization: Bearer ${TOKEN}" \
    -H 'Content-Type: application/json; charset=utf-8' \
    --data "$(python3 -c 'import json,sys;print(json.dumps({"channel":sys.argv[1],"text":sys.argv[2]}))' "$CH" "$msg")" \
    >/dev/null 2>&1
}

REASONS=""      # accumulates failure reasons (newline-separated)
add_reason() { REASONS="${REASONS:+$REASONS$'\n'}- $1"; }

# --- Check 1+2: nightly committed + fact freshness (single read-only query) -------------------
# Most recent scheduled nightly start = today 03:30Z if now >= 03:30Z else yesterday 03:30Z.
SCHED_EPOCH="$(date -u -d 'today 03:30' +%s 2>/dev/null)"
[ "$(date -u +%s)" -lt "$SCHED_EPOCH" ] && SCHED_EPOCH="$(date -u -d 'yesterday 03:30' +%s)"
SCHED_ISO="$(date -u -d "@$SCHED_EPOCH" +%FT%TZ)"

SQL_OUT="$(
  "$DUCKDB" -readonly -noheader -list "$SNAPSHOT" "
    SELECT
      (SELECT count(*) FROM core.sync_run
         WHERE status='success' AND phase_count >= $MIN_PHASES AND phase_failed_count = 0
           AND started_at >= TIMESTAMPTZ '$SCHED_ISO') AS fresh_full_nightly,
      (SELECT coalesce(max(started_at)::varchar,'none') FROM core.sync_run
         WHERE status='success' AND phase_count >= $MIN_PHASES) AS last_full_nightly,
      (SELECT coalesce(max(date)::varchar,'none') FROM core.campaign_daily) AS cd_max_date,
      (SELECT CASE WHEN max(date) >= (current_date - INTERVAL 2 DAY) THEN 1 ELSE 0 END
         FROM core.campaign_daily) AS cd_fresh
  " 2>&1
)"
RC=$?
if [ "$RC" -ne 0 ]; then
  add_reason "Cannot read serving snapshot $SNAPSHOT (duckdb rc=$RC): ${SQL_OUT//$'\n'/ }"
  FRESH_FULL=0; LAST_FULL="?"; CD_MAX="?"; CD_FRESH=0
else
  FRESH_FULL="$(echo "$SQL_OUT" | cut -d'|' -f1)"
  LAST_FULL="$(echo "$SQL_OUT"  | cut -d'|' -f2)"
  CD_MAX="$(echo "$SQL_OUT"     | cut -d'|' -f3)"
  CD_FRESH="$(echo "$SQL_OUT"   | cut -d'|' -f4)"
  [ "${FRESH_FULL:-0}" -ge 1 ] 2>/dev/null || add_reason "No FULL nightly committed since ${SCHED_ISO} (last full nightly: ${LAST_FULL}). The 03:30Z nightly did not finish/commit."
  [ "${CD_FRESH:-0}" = "1" ] || add_reason "core.campaign_daily is stale — max date=${CD_MAX} (expected >= $(date -u -d 'yesterday' +%F))."
fi

# --- Check 3: serving snapshot file freshness -------------------------------------------------
if [ -e "$SNAPSHOT" ]; then
  SNAP_MTIME="$(stat -c %Y "$SNAPSHOT" 2>/dev/null || stat -f %m "$SNAPSHOT" 2>/dev/null)"
  if [ -n "$SNAP_MTIME" ]; then
    SNAP_AGE_H=$(( ( $(date -u +%s) - SNAP_MTIME ) / 3600 ))
    [ "$SNAP_AGE_H" -le "$SNAPSHOT_STALE_HRS" ] || add_reason "Serving snapshot is ${SNAP_AGE_H}h old (> ${SNAPSHOT_STALE_HRS}h) — snapshot-publisher (06:30Z) did not promote a fresh DB."
  fi
else
  add_reason "Serving snapshot $SNAPSHOT missing."
fi

# --- Check 4: lock starvation (warehouse-writer held continuously too long) -------------------
if [ -e "$LOCK_FILE" ] && command -v flock >/dev/null 2>&1; then
  # Open the lock file READ-only (`<`) for the probe so we do NOT truncate it (a `>` open would
  # reset its mtime to now and defeat the held-duration check below). flock works on a read fd.
  if ! ( exec 207<"$LOCK_FILE"; flock -n 207 ) 2>/dev/null; then
    # lock is HELD right now — how long has it been held? (lock-file mtime is refreshed on claim)
    LK_MTIME="$(stat -c %Y "$LOCK_FILE" 2>/dev/null || stat -f %m "$LOCK_FILE" 2>/dev/null)"
    if [ -n "$LK_MTIME" ]; then
      LK_AGE_H=$(( ( $(date -u +%s) - LK_MTIME ) / 3600 ))
      HOLDER="$(cat "$LOCK_FILE" 2>/dev/null | tr '\n' ' ' )"
      if [ "$LK_AGE_H" -ge "$LOCK_HELD_MAX_HRS" ]; then
        add_reason "warehouse-writer lock HELD continuously ~${LK_AGE_H}h (> ${LOCK_HELD_MAX_HRS}h) — likely a stuck/runaway writer starving the nightly. Holder: ${HOLDER:-unknown}. Inspect: fuser -v ${LOCK_FILE}."
      fi
    fi
  fi
fi

# --- gating + alert ---------------------------------------------------------------------------
FAILS="$(cat "$STATE" 2>/dev/null || echo 0)"
if [ -z "$REASONS" ]; then
  if [ "${FAILS:-0}" -ge 2 ] 2>/dev/null; then
    post_slack ":white_check_mark: *Warehouse nightly RECOVERED* — a full nightly has committed (last full: ${LAST_FULL}; campaign_daily max=${CD_MAX}; snapshot fresh). ${NOW_TS}"
  fi
  echo 0 > "$STATE"
  echo "[$NOW_TS] OK  fresh_full=$FRESH_FULL last_full=$LAST_FULL cd_max=$CD_MAX"
else
  FAILS=$(( FAILS + 1 ))
  echo "$FAILS" > "$STATE"
  echo "[$NOW_TS] UNHEALTHY (consecutive=$FAILS):"; printf '%s\n' "$REASONS"
  if [ "$FAILS" -eq 2 ]; then
    post_slack "$(printf '%s :rotating_light: *Warehouse NIGHTLY did not commit* (%s)\n%s\n\nThe 03:30Z nightly likely failed or is starved on the warehouse-writer lock. Check on renaissance-worker: `tail -80 /root/renaissance-warehouse/logs/nightly.log`, `duckdb -readonly /opt/duckdb/warehouse_current.duckdb \"SELECT * FROM core.sync_run ORDER BY started_at DESC LIMIT 5\"`, `fuser -v /root/core/warehouse.write.lock`. Owner: warehouse-ops.' "$MENTION" "$NOW_TS" "$REASONS")"
  fi
fi
