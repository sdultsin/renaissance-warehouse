#!/usr/bin/env bash
# daily_report_v2.sh — the Daily RevOps Report v2 production render (D-1-FINAL, warehouse-only).
#
# Renders YESTERDAY-ET's tab ONCE, gated on DATA FRESHNESS (render_daily_v2.py self-gates: exit 3 =
# gate not ready yet -> a later cron tick retries; exit 0 = rendered). A per-date DONE marker makes
# every post-success tick a no-op, so the tab is written exactly once per day (idempotent re-runs are
# safe, but we avoid needless rewrites / clobbering a manual annotation).
#
# Cron (UTC, droplet) — ticks across the window the 05:30Z nightly promote lands in; first ready tick
# renders, the rest skip on the marker; a still-not-ready gate past the deadline alerts #cc-sam:
#   */20 6-14 * * *  /root/renaissance-warehouse/scripts/daily_report_v2.sh >> .../logs/daily_report_v2.log 2>&1
#
# Manual / pilot:  daily_report_v2.sh 2026-07-08        (explicit date; ignores + refreshes the marker)
#                  daily_report_v2.sh 2026-07-08 --shadow   (write the '<tab> ·v2' shadow tab)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"; mkdir -p logs
PY="$REPO_DIR/.venv/bin/python"
export GOOGLE_SA_KEY="${GOOGLE_SA_KEY:-/root/.config/gcp-sa/droplet-sheets-sync.json}"  # [2026-07-14 creds-rebuild] SA key (old OAuth token destroyed)

EXTRA=""
if [[ "${1:-}" == "--shadow" ]]; then EXTRA="--shadow"; shift; fi
if [[ -n "${1:-}" && "${1:-}" != --* ]]; then
    REPORT_DATE="$1"; MANUAL=1
else
    REPORT_DATE="$(TZ=America/New_York date -d 'yesterday' +%F)"; MANUAL=0
fi
[[ "${2:-}" == "--shadow" ]] && EXTRA="--shadow"
MARKER="$REPO_DIR/logs/daily_v2_done_${REPORT_DATE}"
HOUR_UTC="$(date -u +%H)"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
alert(){ [[ -x "$PY" && -f "$SCRIPT_DIR/alert_slack.py" ]] && "$PY" "$SCRIPT_DIR/alert_slack.py" "$1" >/dev/null 2>&1 || true; }

log "==== daily_report_v2 REPORT_DATE=$REPORT_DATE manual=$MANUAL extra='$EXTRA' ===="
# Auto path: skip once the day is already rendered (marker). Manual/shadow runs always render.
if [[ "$MANUAL" == "0" && -z "$EXTRA" && -f "$MARKER" ]]; then
    log "already rendered $REPORT_DATE (marker present) — skipping"
    exit 0
fi

DAILY_V2_ALERT_ON_SKIP=0 "$PY" scripts/render_daily_v2.py "$REPORT_DATE" $EXTRA
RC=$?
if [[ $RC -eq 0 ]]; then
    [[ -z "$EXTRA" ]] && touch "$MARKER"
    log "OK rendered $REPORT_DATE (rc=0)"
    # keep only the last ~10 markers
    ls -1t "$REPO_DIR"/logs/daily_v2_done_* 2>/dev/null | tail -n +11 | xargs -r rm -f
elif [[ $RC -eq 3 ]]; then
    # gate not ready. Silent while the nightly is plausibly still running; alert if it's late.
    if (( 10#$HOUR_UTC >= 13 )) && [[ "$MANUAL" == "0" ]]; then
        log "GATE STILL NOT READY for $REPORT_DATE at ${HOUR_UTC}:00Z (nightly late/failed?) — alerting"
        alert ":warning: *daily-report v2* — the freshness gate is STILL not ready for $REPORT_DATE at ${HOUR_UTC}:00Z. The 05:30Z nightly promote is late or failed, so the ${REPORT_DATE} tab has not rendered. Check logs/nightly.log + logs/daily_report_v2.log."
    else
        log "gate not ready for $REPORT_DATE (nightly still running); will retry next tick"
    fi
    exit 0
else
    log "RENDER FAILED for $REPORT_DATE (rc=$RC) — alerting"
    alert ":rotating_light: *daily-report v2* — render of $REPORT_DATE FAILED (rc=$RC). See logs/daily_report_v2.log."
    exit "$RC"
fi
