#!/usr/bin/env bash
# daily_report_sync.sh — the 10 PM-ET Daily RevOps Report SLA sync (+ 12:30 AM-ET re-lock).
#
# Lands every field the "Daily RevOps Report" sheet (June-26 tab = spec) is built from in the
# warehouse and renders a fresh dated tab — fully SERVER-SIDE on the box (no laptop dependence).
# Decoupled from the heavy 03:30Z nightly per handoffs/2026-06-29-daily-report-10pm-sync-BUILD.md.
#
# Modes:
#   evening (default)  full report path: stage funding form -> pipeline_mirror, sheets,
#                      sendivo(mirror+inbound), iskra, close, canonical(meeting) -> promote -> render.
#   relock             cheap final re-lock at ~12:30 AM ET: stage funding form -> sheets ->
#                      canonical(meeting) -> promote -> re-render (locks ~99%-final bookings).
#
# Cron (UTC, droplet):
#   0  1 * * *  /root/renaissance-warehouse/scripts/daily_report_sync.sh evening >> .../logs/daily_report_sync.log 2>&1   # 9 PM ET
#   30 4 * * *  /root/renaissance-warehouse/scripts/daily_report_sync.sh relock  >> .../logs/daily_report_sync.log 2>&1   # 12:30 AM ET
#
# Each orchestrator --phase self-serializes on the warehouse writer lock (core/db.py in-proc
# acquire-or-wait), so this never clobbers another writer. The heavy nightly is scheduled AFTER
# the re-lock (05:00Z) so the light SLA path always wins the lock first.
#
# Manual / pilot:  daily_report_sync.sh evening 2026-06-29   (explicit REPORT_DATE override = arg 2)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"
mkdir -p logs

MODE="${1:-evening}"
# REPORT_DATE = the ET day the tab is for.
#   evening cron fires 01:00Z (= 9 PM ET, same ET day)        -> today ET
#   relock  cron fires 04:30Z (= 12:30 AM ET, the day ended)  -> yesterday ET
if [[ -n "${2:-}" ]]; then
    REPORT_DATE="$2"
elif [[ "$MODE" == "relock" ]]; then
    REPORT_DATE="$(TZ=America/New_York date -d 'yesterday' +%F)"
else
    REPORT_DATE="$(TZ=America/New_York date +%F)"
fi
TAB="$(date -d "$REPORT_DATE" +'%b %-d')"   # e.g. "Jun 29" (matches Jun 22-26 / Jun MTD naming)
# All callers (cron / watchdog self-heal / manual) own the redirect to logs/daily_report_sync.log,
# so the script writes to stdout/stderr only — no internal tee (which would double every line).
PROMOTE_LOG="$REPO_DIR/logs/daily_report_promote.log"   # detached publisher gets its own file
READER_TOK="$(awk -F'\t' '$2=="cc-service-reader"{print $1}' /opt/duckdb/allowed_tokens.txt)"
WH_API="https://renaissance-droplet.tailae5c80.ts.net/query"
WLOCK="${WAREHOUSE_WRITE_LOCK_PATH:-/root/core/warehouse.write.lock}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
alert(){ .venv/bin/python scripts/alert_slack.py "$1" >/dev/null 2>&1 || true; }
snap_id(){ curl -s -m 20 -X POST "$WH_API" -H "Authorization: Bearer $READER_TOK" \
    -H 'Content-Type: application/json' -d '{"sql":"SELECT 1"}' 2>/dev/null \
    | grep -o '"snapshot_id":"[^"]*"'; }

log "================ daily_report_sync MODE=$MODE REPORT_DATE=$REPORT_DATE TAB='$TAB' ================"

source .venv/bin/activate 2>/dev/null || true
PY=".venv/bin/python"
export WAREHOUSE_PULL_REPLIES=1
export GOOGLE_TOKEN="/root/.config/mcp-google-sheets/token.json"

run_phase(){  # run_phase <label> <orchestrator args...>  — fail-aware, never aborts the run
    local label="$1"; shift
    log "phase: $label ($*)"
    $PY -m core.orchestrator "$@"
    local rc=$?
    [[ $rc -ne 0 ]] && log "WARN phase '$label' rc=$rc (continuing; orchestrator returns 1 on PARTIAL)"
    return 0
}

# ---- 1) pre-stage the Funding-Form bookings sheet (box-local) ----
log "staging funding form ..."
if ! $PY scripts/stage_funding_form.py; then
    log "WARN funding-form stage failed (consumer will use prior snapshot)"
fi

# ---- 1b) writer-lock PRE-PROBE — resilience against a long/stuck warehouse writer ----
# The report-path phases below each acquire-or-WAIT on the warehouse writer lock. If the heavy
# nightly (or any writer) is stuck holding it, an unguarded run would HANG here indefinitely (the
# antithesis of "auto-produces nightly with zero hand-holding"). Probe the lock non-blocking: if a
# writer holds it, SKIP the phases + promote and render straight from the existing serving snapshot +
# the LIVE APIs — §1 (Instantly), §2 (sendivo), §4-Actual are promote-independent, so the report
# still produces (meetings/§6 are then as-of the last promote). Normal evening runs (writer idle at
# 9 PM ET, nightly is later) take the full path.
SKIP_WAREHOUSE=0
if flock -n "$WLOCK" -c true 2>/dev/null; then
    log "writer lock free — running full warehouse-refresh path"
else
    SKIP_WAREHOUSE=1
    log "WARN warehouse writer lock HELD (long/stuck writer) — SKIPPING phases + promote; rendering from the existing snapshot + live APIs"
    alert ":warning: *daily_report_sync ($MODE)* — warehouse writer busy at run time; report rendered from the existing snapshot + live APIs (email/SMS/§4-actual current; meetings/§6 as of the last promote). If this persists, the nightly writer is likely stuck."
fi

# ---- 2) report-path phases (each self-locks the warehouse writer) ----
# NOTE [2026-06-30]: core.meeting is built from im_bookings for Funding >=2026-06-29 (PR #115).
if [[ "$SKIP_WAREHOUSE" == "0" && "$MODE" == "relock" ]]; then
    run_phase sheets            --phase sheets
    run_phase im_bookings       --phase im_bookings
    # billing_daily on the relock too [2026-07-02, PR #161 follow-up]: at 12:30 AM ET the report day
    # is CLOSED, so this locks §2 'Cost $ (actual)' with the final, stable billing row. Without it the
    # day's cost cell would stay '—' FOREVER: the 05:30Z heavy nightly (the only other billing_daily
    # runner) fires AFTER this final relock render, and get_sms_wa's tripwire rightly dashes any
    # same-day row whose sms_fee_qty lags live billing >5%.
    run_phase sendivo_billing   --phase sendivo --ingest billing_daily
    run_phase canonical_meeting --phase canonical --ingest meeting
elif [[ "$SKIP_WAREHOUSE" == "0" ]]; then
    $PY scripts/setup_db.py || log "WARN setup_db failed (continuing)"
    run_phase pipeline_mirror   --phase pipeline_mirror
    run_phase sheets            --phase sheets
    run_phase im_bookings       --phase im_bookings
    run_phase sendivo_mirror    --phase sendivo --ingest mirror
    run_phase sendivo_inbound   --phase sendivo --ingest inbound
    # refresh the day-grain billing rows (§2 'Cost $ (actual)' feed) so the 10PM-ET render's same-day
    # cost is as-of ~9PM ET (still tripwire-guarded if late sends diverge >5%; the relock locks final $)
    run_phase sendivo_billing   --phase sendivo --ingest billing_daily
    run_phase iskra             --phase iskra
    run_phase close             --phase close
    run_phase canonical_meeting --phase canonical --ingest meeting
fi

# ---- 3) promote serving snapshot (DETACHED — a foreground SSH-wrapped publisher drops on
#         broken-pipe; nohup + poll the read-API snapshot_id is the robust pattern) ----
# Skipped when the writer was busy (nothing new staged to promote; the publisher's own writer-lock
# guard would benign-abort anyway). The render then uses the existing snapshot + live APIs.
if [[ "$SKIP_WAREHOUSE" != "0" ]]; then
    log "skipping promote (writer busy / phases skipped) — render uses existing snapshot + live APIs"
else
PREV_SNAP="$(snap_id)"
log "promoting serving snapshot (detached -> $PROMOTE_LOG); prev=$PREV_SNAP  (full ~161GB copy, ~20-25 min)"
nohup /opt/duckdb/venv/bin/python /opt/duckdb/bin/publisher.py --reason "daily_report_$MODE" >>"$PROMOTE_LOG" 2>&1 &
PUB_PID=$!
# The publisher does a faithful ~161GB byte-copy (copy_s ~1200-1530s observed), so the render MUST
# wait for it to finish, else it reads the OLD snapshot (§1 shows 0s). Poll up to ~40 min for the
# snapshot_id to change; also break early if the publisher PID exits (failed/finished).
NEW_SNAP="$PREV_SNAP"
for _ in $(seq 1 240); do          # up to ~40 min (10s each)
    sleep 10
    CUR="$(snap_id)"
    if [[ -n "$CUR" && "$CUR" != "$PREV_SNAP" ]]; then NEW_SNAP="$CUR"; break; fi
    if ! kill -0 "$PUB_PID" 2>/dev/null; then     # publisher exited
        CUR="$(snap_id)"; [[ -n "$CUR" && "$CUR" != "$PREV_SNAP" ]] && NEW_SNAP="$CUR"
        break
    fi
done
if [[ "$NEW_SNAP" == "$PREV_SNAP" ]]; then
    log "WARN promote: snapshot_id unchanged after polling ($PREV_SNAP) — render may show stale data; see $PROMOTE_LOG"
    alert ":warning: daily_report_sync ($MODE) — promote did not produce a new snapshot for $REPORT_DATE; tab may be stale. Box: $PROMOTE_LOG"
else
    log "promoted: $NEW_SNAP"
fi
fi   # end: skip-promote-when-writer-busy guard

# ---- 4) render the day's tab (reads the freshly promoted serving snapshot; ALWAYS runs) ----
log "rendering tab '$TAB' for $REPORT_DATE ..."
if $PY scripts/render_daily.py "$REPORT_DATE" "$TAB"; then
    log "================ daily_report_sync DONE MODE=$MODE tab='$TAB' ================"
else
    rc=$?
    log "ERROR render failed rc=$rc"
    alert ":rotating_light: *daily_report_sync ($MODE)* — render FAILED for $REPORT_DATE (rc=$rc). Tab '$TAB' may be stale. Box log: $REPO_DIR/$LOG"
    exit 1
fi
