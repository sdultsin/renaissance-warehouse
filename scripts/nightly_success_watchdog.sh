#!/usr/bin/env bash
# nightly_success_watchdog.sh — verifies the warehouse NIGHTLY actually COMMITTED the DATA it is
# supposed to produce, and detects warehouse-writer lock STARVATION. Alerts #cc-sam (tag Sam) on
# failure, gated. Read-only against the serving snapshot — safe to run anytime, never opens the
# live writer.
#
# DESIGN — monitor the OUTCOME (the data that has to land), not a proxy that can read green while a
# table rots. The 06-18 false-confidence failure this script was hardened against: it reported `OK`
# while core.campaign_daily was already 2 days stale (cd_max=2026-06-16), because the old check used
# a hardcoded `max(date) >= current_date - 2 DAY` window that TOLERATES 2-day-old data. And the old
# "a full sync_run committed" check (1) is a PROXY: on 2026-06-20 sync_run showed status=success
# phase_count=39, yet every core/raw table was still pinned at 2026-06-16 — the process finished but
# produced no fresh data. So the headline signal here is per-table FRESHNESS, driven by the warehouse's
# own SLA registry, with hard floors that cannot be loosened by a mis-declared registry row:
#
#   1. FRESHNESS SWEEP (the deliverable). For every status='active' row in core.sync_registry, compare
#      its declared SLA to current state: last_synced_at must be within sla_hours (+ FRESH_GRACE_HRS),
#      and last_biz_date must lag <= biz_sla_days (+ BIZ_GRACE_DAYS). Future/negative biz lags are
#      ignored (a bad biz_date can't false-alarm). This auto-covers core.campaign_daily, raw_cc_*, the
#      mirror, core.sync_registry's tracked outputs, etc., each at its OWN cadence. The headline facts
#      ALSO get a HARD FLOOR queried directly off the table, independent of the registry, so a stale
#      headline ALWAYS fires even if its registry row is wrong/missing: core.campaign_daily on
#      CALENDAR-DAY lag (CRIT_STALE_DAYS — today/yesterday UTC, not epoch hours, so a healthy
#      yesterday-grained fact doesn't false-alarm on a post-midnight run) and core.sync_registry's own
#      last_synced_at on hours (CRIT_STALE_HRS) — a frozen registry = a frozen nightly. All reads are
#      FAIL-LOUD: an unreadable signal becomes a reason, never a silent "OK".
#   2. NIGHTLY RAN: core.sync_run has a FULL nightly (phase_count >= MIN_PHASES, status='success',
#      phase_failed_count=0) STARTED after the most recent scheduled 03:30Z nightly. (Secondary/
#      corroborating signal only — check 1 is the real deliverable test.)
#   3. serving snapshot file (/opt/duckdb/warehouse_current.duckdb) mtime < SNAPSHOT_STALE_HRS.
#   4. lock STARVATION: the warehouse-writer flock not HELD continuously > LOCK_HELD_MAX_HRS.
#   5. campaign_data D1 PUBLISH freshness (CC read-model OUTCOME): campaign_data_publish_meta.published_at
#      in CC's D1 < READMODEL_STALE_HRS old. SKIPPED (never alerts) if D1 creds are absent.
#
# Gating: alert on the 2nd consecutive unhealthy check (rides out a single transient blip / a nightly
# still finishing), one alert per outage (deduped by the set of reasons), plus a single RECOVERED ping.
# State in $STATE. A healthy nightly NEVER pings.
#
# Cron (UTC) — after the 06:30Z snapshot publish; re-checks catch a late-finishing nightly. These two
# lines live in the warehouse-ops block of root's crontab and MUST be preserved by any crontab edit:
#   15 7  * * * /root/renaissance-warehouse/scripts/nightly_success_watchdog.sh >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1
#   0  9  * * * /root/renaissance-warehouse/scripts/nightly_success_watchdog.sh >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1
set -u

DUCKDB="${DUCKDB:-/usr/local/bin/duckdb}"
SNAPSHOT="${WAREHOUSE_SNAPSHOT:-/opt/duckdb/warehouse_current.duckdb}"
LOCK_FILE="${WAREHOUSE_LOCK_FILE:-/root/core/warehouse.write.lock}"
ENV_FILE="${MONITOR_ENV:-/root/monitors/.env}"
STATE="${STATE:-/tmp/nightly-success-watchdog.state}"
REASONS_STATE="${REASONS_STATE:-/tmp/nightly-success-watchdog.reasons}"  # last alerted reason-set (dedup key)
HEARTBEAT="${HEARTBEAT:-/tmp/nightly-success-watchdog.heartbeat}"        # proves the watchdog itself ran
CH="${SLACK_CHANNEL:-C0AR0EA21C1}"          # #cc-sam
MENTION="${SLACK_MENTION:-<@U0AM2CQHW9E>}"  # Sam
MIN_PHASES="${MIN_PHASES:-20}"              # full nightly is ~30-39 phases; >=20 = a real full run
SNAPSHOT_STALE_HRS="${SNAPSHOT_STALE_HRS:-28}"   # 06:30 publish + grace
LOCK_HELD_MAX_HRS="${LOCK_HELD_MAX_HRS:-4}"      # a legit nightly+heal fits well under this
READMODEL_STALE_HRS="${READMODEL_STALE_HRS:-26}" # CC read-model SLA (matches CC self-audit RED ceiling)
FRESH_GRACE_HRS="${FRESH_GRACE_HRS:-6}"          # grace added to each table's declared sla_hours
BIZ_GRACE_DAYS="${BIZ_GRACE_DAYS:-1}"            # grace added to each table's declared biz_sla_days
CRIT_STALE_HRS="${CRIT_STALE_HRS:-30}"           # HARD FLOOR (hours) for timestamp-tracked headline tables
CRIT_STALE_DAYS="${CRIT_STALE_DAYS:-1}"          # HARD FLOOR (calendar days) for DATE-grained facts (campaign_daily):
                                                 # max(date) must be today or yesterday (UTC). Day-lag, not epoch math,
                                                 # so a healthy "yesterday" fact never false-alarms on a post-midnight run.
# Headline tables that must ALWAYS be fresh after a nightly, floored independent of the registry.
# (sync_registry itself is floored too — a frozen registry means the nightly stopped advancing.)
CRITICAL_TABLES="${CRITICAL_TABLES:-core.campaign_daily main.raw_cc_daily_snapshots core.sync_registry}"
NOW_TS="$(date -u +%FT%TZ)"

# Heartbeat first thing: even if a check below dies, this file proves the cron fired this run.
echo "$NOW_TS" > "$HEARTBEAT" 2>/dev/null || true

# --- Cron self-heal -----------------------------------------------------------------------------
# ROOT CAUSE of the 06-18→06-20 silent gap: another chat re-wrote root's crontab (it is edited by
# many agents) and dropped the two watchdog lines, so the cron simply stopped firing while every
# OTHER job kept running. Defend against a recurrence: on each run, re-assert our own two cron lines
# if missing. This self-corrects the common single-line-dropped case (the lines run at 07:15 AND
# 09:00, so as long as at least one survives to execute, it restores the other). Idempotent, backs
# up the crontab before any change, and is a no-op when both lines are present. Disable with
# SELF_HEAL_CRON=0. Only meaningful as root with crontab available.
# Detection AND re-install both use the fixed CANONICAL installed path (not readlink -f "$0"): the
# installed cron lines and the docs use this literal path, so matching on the resolved path would miss
# them under a symlinked/worktree checkout and APPEND a duplicate every run. Self-heal is therefore a
# no-op unless invoked from the canonical path (SELF_HEAL_CRON=0 to disable; CANON_PATH override).
SELF_HEAL_CRON="${SELF_HEAL_CRON:-1}"
CANON_PATH="${CANON_PATH:-/root/renaissance-warehouse/scripts/nightly_success_watchdog.sh}"
CRON_07="15 7 * * * ${CANON_PATH} >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1"
CRON_09="0 9 * * * ${CANON_PATH} >> /root/renaissance-warehouse/logs/nightly_success_watchdog.log 2>&1"
if [ "$SELF_HEAL_CRON" = "1" ] && [ "$0" = "$CANON_PATH" ] && command -v crontab >/dev/null 2>&1; then
  CUR_CRON="$(crontab -l 2>/dev/null)"
  if [ -n "$CUR_CRON" ]; then  # never act on an empty/unreadable crontab (would risk clobbering)
    NEED_07=1; NEED_09=1
    printf '%s\n' "$CUR_CRON" | grep -Fq "${CANON_PATH}" && {
      printf '%s\n' "$CUR_CRON" | grep -E '^[[:space:]]*15[[:space:]]+7[[:space:]]' | grep -Fq "${CANON_PATH}" && NEED_07=0
      printf '%s\n' "$CUR_CRON" | grep -E '^[[:space:]]*0[[:space:]]+9[[:space:]]'  | grep -Fq "${CANON_PATH}" && NEED_09=0
    }
    if [ "$NEED_07" = 1 ] || [ "$NEED_09" = 1 ]; then
      cp -p /var/spool/cron/crontabs/root "/root/crontab.bak.selfheal.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
      {
        printf '%s\n' "$CUR_CRON"
        echo "# nightly-success watchdog (self-healed $(date -u +%FT%TZ) — do not drop these 2 lines)"
        [ "$NEED_07" = 1 ] && echo "$CRON_07"
        [ "$NEED_09" = 1 ] && echo "$CRON_09"
      } | crontab - 2>/dev/null \
        && echo "[$NOW_TS] SELF-HEAL: re-added missing watchdog cron line(s) (07:15=$([ $NEED_07 = 1 ] && echo re-added || echo ok), 09:00=$([ $NEED_09 = 1 ] && echo re-added || echo ok))" \
        || echo "[$NOW_TS] WARN: self-heal crontab write failed"
    fi
  fi
fi

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

# --- Check 1: OUTCOME — per-table freshness sweep (the deliverable) ----------------------------
# One read-only query returns: the registry-SLA violations, the hard-floor violations for the
# critical headline tables, and the headline campaign_daily diagnostics for the log/alert.
# `last_synced_age_hrs` uses last_synced_at; `biz_lag_days` uses last_biz_date (skipped when future).
# 1a. Registry SLA sweep — COUNT violations + capture the worst-offender sample. A whole-nightly
# failure stales dozens of tables at once; collapsing to "N of M active tables stale" + a short
# sample keeps the alert actionable instead of a 60-line wall. Each table is judged at its OWN
# declared cadence (sla_hours / biz_sla_days). Future/negative biz lags are ignored.
# All freshness queries pin TimeZone='UTC' so current_date / now() match the script's UTC framing
# (DuckDB's session TZ is otherwise ambient). A NULL last_synced_at on an active table WITH an SLA is
# itself a violation (never loaded), not a free pass — coalesced to a huge age so it always counts.
SLA_PRED="sla_hours IS NOT NULL AND (last_synced_at IS NULL OR (epoch(CAST(now() AS TIMESTAMPTZ)) - epoch(last_synced_at))/3600.0 > sla_hours + $FRESH_GRACE_HRS)"
BIZ_PRED="biz_sla_days IS NOT NULL AND last_biz_date IS NOT NULL AND (current_date - last_biz_date) >= 0 AND (current_date - last_biz_date) > biz_sla_days + $BIZ_GRACE_DAYS"
TOT_LINE="$(
  "$DUCKDB" -readonly -noheader -list -separator '|' "$SNAPSHOT" "
    SET TimeZone='UTC';
    WITH reg AS (SELECT * FROM core.sync_registry WHERE status = 'active')
    SELECT (SELECT count(*) FROM reg) AS active_n,
           (SELECT count(*) FROM reg WHERE $SLA_PRED) AS sla_n,
           (SELECT count(*) FROM reg WHERE $BIZ_PRED) AS biz_n;
  " 2>&1
)"
RC_FRESH=$?
SLA_N=0; BIZ_N=0; ACTIVE_N=0
# Fail LOUD on a read error OR on a success-but-unparseable result (non-numeric counts) — never let an
# unreadable registry coerce to "0 stale" and report green (the false-confidence anti-pattern).
if [ "$RC_FRESH" -ne 0 ] || ! printf '%s' "$TOT_LINE" | grep -Eq '^[0-9]+\|[0-9]+\|[0-9]+$'; then
  add_reason "Cannot read sync_registry freshness from $SNAPSHOT (duckdb rc=$RC_FRESH): ${TOT_LINE//$'\n'/ }"
else
  ACTIVE_N="$(echo "$TOT_LINE" | cut -d'|' -f1)"
  SLA_N="$(echo "$TOT_LINE"    | cut -d'|' -f2)"
  BIZ_N="$(echo "$TOT_LINE"    | cut -d'|' -f3)"
  if [ "$SLA_N" -gt 0 ]; then
    SAMPLE="$("$DUCKDB" -readonly -noheader -list -separator ' ' "$SNAPSHOT" "
      SET TimeZone='UTC';
      SELECT name FROM core.sync_registry
      WHERE status='active' AND ($SLA_PRED)
      ORDER BY (epoch(CAST(now() AS TIMESTAMPTZ)) - epoch(coalesce(last_synced_at, TIMESTAMPTZ '1970-01-01'))) DESC
      LIMIT 6;" 2>/dev/null | tr '\n' ' ')"
    SLA_TABLES="$SAMPLE"
    add_reason "STALE LOADS: ${SLA_N} of ${ACTIVE_N} active tables past their load SLA — the nightly is not producing fresh data. Worst: ${SAMPLE}"
  fi
  if [ "$BIZ_N" -gt 0 ]; then
    SAMPLE_B="$("$DUCKDB" -readonly -noheader -list -separator ' ' "$SNAPSHOT" "
      SET TimeZone='UTC';
      SELECT name FROM core.sync_registry
      WHERE status='active' AND ($BIZ_PRED)
      ORDER BY (current_date - last_biz_date) DESC LIMIT 6;" 2>/dev/null | tr '\n' ' ')"
    add_reason "STALE BUSINESS-DATES: ${BIZ_N} of ${ACTIVE_N} active tables missing their newest day(s). Worst: ${SAMPLE_B}"
  fi
fi

# 1b. HARD FLOOR on the headline facts — queried DIRECTLY against the table, so a missing /
# mis-declared / mis-prefixed registry row can never mask a stale headline table. campaign_daily is
# the exact 06-18 false-confidence case. Judged on CALENDAR-DAY lag (max(date) must be today or
# yesterday in UTC), NOT epoch-of-midnight hours — a healthy "yesterday" fact on a 07:15Z run is ~31h
# "old" by epoch math and would false-alarm; day-lag does not. Reads are FAIL-LOUD: a read error
# becomes a reason, never a silent pass. Output: "<max>|<stale 0/1>".
CD_OUT="$("$DUCKDB" -readonly -noheader -list -separator '|' "$SNAPSHOT" "
  SET TimeZone='UTC';
  SELECT coalesce(max(date)::varchar,'none'),
         CASE WHEN max(date) IS NULL OR (current_date - max(date)) > $CRIT_STALE_DAYS THEN 1 ELSE 0 END
  FROM core.campaign_daily" 2>&1)"
RC_CD=$?
CD_MAX="?"
if [ "$RC_CD" -ne 0 ] || ! printf '%s' "$CD_OUT" | grep -Eq '\|[01]$'; then
  add_reason "Cannot read core.campaign_daily from $SNAPSHOT (duckdb rc=$RC_CD): ${CD_OUT//$'\n'/ } — headline-fact freshness UNVERIFIABLE."
else
  CD_MAX="$(echo "$CD_OUT" | cut -d'|' -f1)"
  CD_STALE="$(echo "$CD_OUT" | cut -d'|' -f2)"
  if [ "$CD_STALE" = "1" ]; then
    # Only suppress if check 1a's STALE-LOADS sample already named campaign_daily exactly.
    case " ${SLA_TABLES:-} " in
      *" core.campaign_daily "*) : ;;
      *) add_reason "STALE (hard floor): core.campaign_daily max(date)=${CD_MAX} is more than ${CRIT_STALE_DAYS}d behind today (UTC) — the headline fact did not advance." ;;
    esac
  fi
fi
# sync_registry self-freshness: a frozen registry = a frozen nightly. Fail-loud read; fires even when
# the 1a sweep counted 0 (e.g. registry readable but every row mis-statused) — a distinct safety net.
REG_OUT="$("$DUCKDB" -readonly -noheader -list "$SNAPSHOT" "
  SET TimeZone='UTC';
  SELECT CASE WHEN max(last_synced_at) IS NULL OR (epoch(CAST(now() AS TIMESTAMPTZ)) - epoch(max(last_synced_at)))/3600.0 > $CRIT_STALE_HRS
              THEN 1 ELSE 0 END FROM core.sync_registry WHERE status='active'" 2>&1)"
RC_REG=$?
if [ "$RC_REG" -ne 0 ] || ! printf '%s' "$REG_OUT" | grep -Eq '^[01]$'; then
  add_reason "Cannot read core.sync_registry self-freshness from $SNAPSHOT (duckdb rc=$RC_REG): ${REG_OUT//$'\n'/ }."
elif [ "$REG_OUT" = "1" ] && [ "${SLA_N:-0}" = "0" ]; then
  add_reason "STALE (hard floor): core.sync_registry has not advanced any active table's last_synced_at in > ${CRIT_STALE_HRS}h — the nightly is not committing."
fi

# --- Check 2: nightly actually ran (corroborating proxy) --------------------------------------
SCHED_EPOCH="$(date -u -d 'today 03:30' +%s 2>/dev/null)"
[ "$(date -u +%s)" -lt "$SCHED_EPOCH" ] && SCHED_EPOCH="$(date -u -d 'yesterday 03:30' +%s)"
SCHED_ISO="$(date -u -d "@$SCHED_EPOCH" +%FT%TZ)"
RUN_OUT="$(
  "$DUCKDB" -readonly -noheader -list -separator '|' "$SNAPSHOT" "
    SELECT
      (SELECT count(*) FROM core.sync_run
         WHERE status='success' AND phase_count >= $MIN_PHASES AND phase_failed_count = 0
           AND started_at >= TIMESTAMPTZ '$SCHED_ISO') AS fresh_full_nightly,
      (SELECT coalesce(max(started_at)::varchar,'none') FROM core.sync_run
         WHERE status='success' AND phase_count >= $MIN_PHASES) AS last_full_nightly
  " 2>&1
)"
if [ $? -ne 0 ]; then
  FRESH_FULL=0; LAST_FULL="?"
else
  FRESH_FULL="$(echo "$RUN_OUT" | cut -d'|' -f1)"
  LAST_FULL="$(echo "$RUN_OUT"  | cut -d'|' -f2)"
  [ "${FRESH_FULL:-0}" -ge 1 ] 2>/dev/null || add_reason "No FULL nightly committed since ${SCHED_ISO} (last full nightly: ${LAST_FULL}). The 03:30Z nightly did not finish/commit."
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

# --- Check 5: campaign_data D1 publish freshness (CC read-model OUTCOME) -----------------------
# Read the single-row campaign_data_publish_meta.published_at out of CC's D1 via the
# Cloudflare D1 HTTP API and alert if it is older than READMODEL_STALE_HRS. This is the
# publisher's SUCCESS signal — the thing that was SILENTLY stale on 06-17. SKIP (no alert)
# if creds/tools are missing so a misconfigured monitor box never false-alarms.
WH_ENV="${WH_ENV:-/root/renaissance-warehouse/.env}"
_readkey() {  # $1=key — first non-empty hit across ENV_FILE then WH_ENV
  local v
  v="$(grep -E "^$1=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'"'"')"
  [ -z "$v" ] && v="$(grep -E "^$1=" "$WH_ENV" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"'"'"'"')"
  printf '%s' "$v"
}
CF_ACCT="$(_readkey CLOUDFLARE_RG_ACCOUNT_ID)"
CC_D1_ID="$(_readkey CC_D1_DATABASE_ID)"
[ -z "$CC_D1_ID" ] && CC_D1_ID="25a32aa3-9d95-42a3-9e9e-8cd3a9e3f3eb"
CC_D1_TOK="$(_readkey CC_D1_API_TOKEN)"
if [ -n "$CF_ACCT" ] && [ -n "$CC_D1_ID" ] && [ -n "$CC_D1_TOK" ] && command -v curl >/dev/null 2>&1; then
  D1_RESP="$(curl -s -m 30 -X POST \
    "https://api.cloudflare.com/client/v4/accounts/${CF_ACCT}/d1/database/${CC_D1_ID}/query" \
    -H "Authorization: Bearer ${CC_D1_TOK}" \
    -H 'Content-Type: application/json' \
    --data '{"sql":"SELECT published_at FROM campaign_data_publish_meta WHERE id = 1"}' 2>/dev/null)"
  PUB_AGE_H="$(python3 - "$D1_RESP" <<'PY' 2>/dev/null
import json, sys
from datetime import datetime, timezone
try:
    out = json.loads(sys.argv[1])
    if not out.get("success"):
        print("ERR"); raise SystemExit
    rows = (out.get("result") or [{}])[0].get("results") or []
    if not rows:
        print("NOROW"); raise SystemExit
    ts = rows[0]["published_at"]
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    print(f"{age:.1f}")
except SystemExit:
    pass
except Exception:
    print("ERR")
PY
)"
  if [ "$PUB_AGE_H" = "NOROW" ]; then
    add_reason "campaign_data D1 publish meta row MISSING (publish_campaign_data_d1.py never wrote campaign_data_publish_meta) — CC read-model freshness unverifiable."
  elif [ "$PUB_AGE_H" = "ERR" ] || [ -z "$PUB_AGE_H" ]; then
    echo "[$NOW_TS] NOTE: could not read campaign_data publish meta from D1 (skipping check 5; not alerting)"
  else
    if awk "BEGIN{exit !($PUB_AGE_H > $READMODEL_STALE_HRS)}"; then
      add_reason "campaign_data D1 read-model is ${PUB_AGE_H}h stale (> ${READMODEL_STALE_HRS}h) — publish_campaign_data_d1.py did not publish a fresh snapshot; Campaign Control is reading a stale campaign set (the 06-17 failure mode)."
    fi
  fi
else
  echo "[$NOW_TS] NOTE: D1 creds/curl unavailable — skipping check 5 (campaign_data publish freshness)"
fi

# --- gating + alert ---------------------------------------------------------------------------
# POSITIVE-HEALTH GUARD: "no reasons" only counts as healthy if we actually managed to READ the
# headline signals. If every freshness read silently returned empty (unreadable DB) we must NOT flip
# to OK/RECOVERED — that is the original silent-OK failure. Require that we parsed an active-table
# count AND a campaign_daily max. (A real read failure already added a reason above; this is a final
# belt-and-suspenders so an unreadable warehouse can never masquerade as healthy.)
if [ -z "$REASONS" ] && { [ "${ACTIVE_N:-0}" -le 0 ] 2>/dev/null || [ "$CD_MAX" = "?" ]; }; then
  add_reason "Health UNVERIFIABLE — could not read active-table count (${ACTIVE_N:-?}) or campaign_daily max (${CD_MAX}); refusing to report OK."
fi

# Dedup key = the STABLE shape of the outage: category labels + counts + table names, with only the
# volatile per-run AGE magnitudes (e.g. 100.6h, 213.2h, 5d) stripped. Keeping the "N of M" counts and
# table names means a WORSENING outage (3->56 stale, or a different table) re-fires; only the same
# outage with drifting age numbers is deduped.
REASON_KEY="$(printf '%s\n' "$REASONS" | sed -E 's/[0-9]+(\.[0-9]+)?[hd]\b//g' | sort | md5sum | cut -d' ' -f1)"
LAST_KEY="$(cat "$REASONS_STATE" 2>/dev/null || echo '')"
FAILS="$(cat "$STATE" 2>/dev/null || echo 0)"

if [ -z "$REASONS" ]; then
  if [ "${FAILS:-0}" -ge 2 ] 2>/dev/null; then
    post_slack ":white_check_mark: *Warehouse nightly RECOVERED* — all tracked tables are within SLA again (campaign_daily max=${CD_MAX}; last full nightly: ${LAST_FULL}; serving snapshot fresh). ${NOW_TS}"
  fi
  echo 0 > "$STATE"
  : > "$REASONS_STATE"
  echo "[$NOW_TS] OK  fresh_full=${FRESH_FULL:-?} last_full=${LAST_FULL:-?} cd_max=${CD_MAX} active_tables=${ACTIVE_N:-?}"
else
  FAILS=$(( FAILS + 1 ))
  echo "$FAILS" > "$STATE"
  echo "[$NOW_TS] UNHEALTHY (consecutive=$FAILS):"; printf '%s\n' "$REASONS"
  # Fire on the 2nd consecutive unhealthy check, AND re-fire if the failure SET changed since the
  # last alert (a new/different problem). Never re-fire the identical outage.
  if { [ "$FAILS" -ge 2 ] && [ "$REASON_KEY" != "$LAST_KEY" ]; }; then
    post_slack "$(printf '%s :rotating_light: *Warehouse nightly DID NOT PRODUCE FRESH DATA* (%s)\n%s\n\ncampaign_daily max=%s. Triage on renaissance-worker: tail -80 /root/renaissance-warehouse/logs/nightly.log ; duckdb -readonly /opt/duckdb/warehouse_current.duckdb to inspect core.sync_registry (oldest last_synced_at among active) ; fuser -v /root/core/warehouse.write.lock . Owner: warehouse-ops.' "$MENTION" "$NOW_TS" "$REASONS" "$CD_MAX")"
    echo "$REASON_KEY" > "$REASONS_STATE"
  fi
fi
