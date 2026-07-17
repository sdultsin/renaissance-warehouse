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
#   backfill           post-NIGHTLY re-render of YESTERDAY-ET's tab [2026-07-02]. Both renders above
#                      fire BEFORE the 05:30Z heavy nightly that lands the D-1 sources (sendivo
#                      log-level delivered/fail -> §2 Fail%, core.sending_account_daily -> §4 Actual
#                      OTD/Google split, core.email_message -> §6), so without this pass those columns
#                      keep their evening state FOREVER. Render-ONLY (no ingest phases, no promote):
#                      the nightly already rebuilt + promoted; we just re-read the fresh snapshot.
#                      GUARD: skips + Slack-warns unless today's nightly COMPLETED (exit 0/1 in
#                      logs/<utc-date>.log, or the orchestrator's own "ended (status=success|
#                      partial)" line as fallback — the exit= line is missing whenever a post-
#                      orchestrator step dies under set -e) AND the serving snapshot was promoted
#                      AFTER it started (after it ENDED, on the fallback path) — never silently
#                      re-render the same stale data as if it were fresh.
#                      BACKFILL_GUARD_ONLY=1 evaluates the guard and exits (0=GO / 3=SKIP) without
#                      rendering or alerting — the dry guard check.
#
# Cron (UTC, droplet):
#   0  1 * * *  /root/renaissance-warehouse/scripts/daily_report_sync.sh evening >> .../logs/daily_report_sync.log 2>&1   # 9 PM ET
#   30 4 * * *  /root/renaissance-warehouse/scripts/daily_report_sync.sh relock  >> .../logs/daily_report_sync.log 2>&1   # 12:30 AM ET
#   45 12 * * * /root/renaissance-warehouse/scripts/daily_report_sync.sh backfill >> .../logs/daily_report_sync.log 2>&1  # post-nightly D-1 re-render (nightly 05:30Z + ~2-4h expected post-compaction; guard skips if late)
#   (each wrapped in `/usr/bin/flock -n /tmp/daily_report_sync.lock` — see scripts/install_daily_report_backfill_cron.sh)
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
#   evening  cron fires 01:00Z (= 9 PM ET, same ET day)        -> today ET
#   relock   cron fires 04:30Z (= 12:30 AM ET, the day ended)  -> yesterday ET
#   backfill cron fires ~12:45Z (= ~8:45 AM ET, post-nightly)  -> yesterday ET
if [[ -n "${2:-}" ]]; then
    REPORT_DATE="$2"
elif [[ "$MODE" == "relock" || "$MODE" == "backfill" ]]; then
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

# ---- 0) backfill GUARD [2026-07-02] — render only from a genuinely post-nightly snapshot ----
# Two facts must BOTH hold, else re-rendering would just repaint the same stale evening-state data
# with a fresher-looking timestamp (worse than skipping):
#   (a) today's heavy nightly (05:30Z cron -> logs/<utc-date>.log) COMPLETED with exit 0 (clean) or
#       1 (partial: tables rebuilt, only peripheral ingests failed — same threshold the nightly
#       itself uses to publish dashboards + promote). No start marker / no exit line (still running,
#       e.g. the 10.6h Jul-1 run, or died hard like the exit-line-less Jul-1 log) / exit>=2 -> SKIP.
#   (b) the SERVING snapshot the renderer reads was promoted AFTER that nightly STARTED. The
#       snapshot_id embeds its build UTC timestamp (warehouse_YYYYMMDD_HHMMSS_mmm.duckdb), and the
#       evening/relock promotes (01:00Z/04:30Z) all predate a 05:30Z start, so ts >= nightly-start
#       can only be the nightly's own promote-at-completion, the 06:30Z fallback timer, or a later
#       manual promote. (Accepted edge: a 06:30Z fallback promote of a mid-build DB while the
#       completion-promote failed — that failure already fires its own :warning: alert path in
#       nightly.sh, and the publisher server-side re-validates before swapping.)
# Skip = log + Slack-warn + exit 0 (the tab keeps its relock state; re-run manually post-nightly:
# `daily_report_sync.sh backfill`). Idempotent: the render rewrites the whole tab, so re-runs are safe.
if [[ "$MODE" == "backfill" ]]; then
    NIGHTLY_LOG="$REPO_DIR/logs/$(date -u +%F).log"
    NSTART="$(grep -o '^=== nightly @ [0-9T:Z-]*' "$NIGHTLY_LOG" 2>/dev/null | tail -1 | grep -o '[0-9][0-9-]*T[0-9:]*Z')"
    NEXIT="$(grep -o '^exit=[0-9]*' "$NIGHTLY_LOG" 2>/dev/null | tail -1 | cut -d= -f2)"
    skip_backfill(){
        log "SKIP backfill: $1"
        if [[ "${BACKFILL_GUARD_ONLY:-0}" == "1" ]]; then
            log "guard-only evaluation (BACKFILL_GUARD_ONLY=1): verdict=SKIP — no Slack alert, no render"
            exit 3
        fi
        alert ":warning: *daily_report_sync (backfill)* — SKIPPED the post-nightly re-render of $REPORT_DATE (tab '$TAB'): $1. §2 Fail% / §4 OTD-Google split / §6 stay at their evening state until a manual \`scripts/daily_report_sync.sh backfill\` after the nightly lands."
        exit 0
    }
    [[ -n "$NSTART" ]] || skip_backfill "no nightly start marker in $NIGHTLY_LOG (nightly did not run today?)"
    # Completion detection [2026-07-02 fix]: the `exit=N` line is nightly.sh's LAST line, written only
    # if every post-orchestrator step survives — a step dying under `set -euo pipefail` (the
    # warehouse_qa breach-exit bug, fixed alongside this) killed the script BEFORE it on BOTH Jul-1
    # and Jul-2, so the guard skipped even though the nightly HAD completed (orchestrator
    # "ended (status=success)" 10:12:20Z, QA wrapped 11:23Z). FALLBACK: accept the orchestrator's own
    # completion line, which IS reliably tee'd into the per-date log:
    #   "HH:MM:SS INFO core.orchestrator: Run <ts>-<id> ended (status=success|partial, failed_ingests=N)"
    # status=success ≙ exit 0, status=partial ≙ exit 1 (core/orchestrator.py:269-274 — the identical
    # GO threshold: tables rebuilt, only peripheral ingests failed). Any other/absent status -> SKIP
    # (fail-safe unchanged). On this fallback path the snapshot comparison below uses the ENDED time
    # (stricter than start: the promote must postdate actual completion, not just launch).
    NENDED_TS=""
    if [[ -z "$NEXIT" ]]; then
        ENDED_LINE="$(grep 'core\.orchestrator: Run .* ended (status=' "$NIGHTLY_LOG" 2>/dev/null | tail -1)"
        NSTATUS="$(echo "$ENDED_LINE" | grep -o 'ended (status=[a-z]*' | cut -d= -f2)"
        case "$NSTATUS" in
            success) NEXIT=0 ;;
            partial) NEXIT=1 ;;
        esac
        if [[ -n "$NEXIT" ]]; then
            ENDED_HMS="$(echo "$ENDED_LINE" | grep -o '^[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}')"
            [[ -n "$ENDED_HMS" ]] && NENDED_TS="$(date -u +%Y%m%d)$(echo "$ENDED_HMS" | tr -d ':')"
            log "backfill guard: no exit= line, using orchestrator completion fallback (status=$NSTATUS -> exit $NEXIT, ended ${ENDED_HMS:-unknown}Z)"
        fi
    fi
    [[ -n "$NEXIT" ]] || skip_backfill "nightly started $NSTART but has neither an exit line nor an orchestrator 'ended (status=success|partial)' line yet (still running, or died hard)"
    [[ "$NEXIT" == "0" || "$NEXIT" == "1" ]] || skip_backfill "nightly exited $NEXIT (hard fail — D-1 sources were NOT rebuilt)"
    SNAP="$(snap_id | grep -o 'warehouse_[0-9]\{8\}_[0-9]\{6\}')"
    [[ -n "$SNAP" ]] || skip_backfill "could not read a snapshot_id from the query API (read API down?)"
    SNAP_TS="${SNAP#warehouse_}"; SNAP_TS="${SNAP_TS/_/}"            # warehouse_YYYYMMDD_HHMMSS -> YYYYMMDDHHMMSS
    NSTART_TS="$(echo "$NSTART" | tr -dc '0-9')"                     # 2026-07-02T05:30:02Z -> 20260702053002
    # On the orchestrator-fallback path compare against the ENDED time when parseable (stricter);
    # otherwise keep the original start-time comparison.
    CMP_TS="$NSTART_TS"; CMP_WHAT="today's nightly start $NSTART"
    if [[ -n "$NENDED_TS" ]]; then
        CMP_TS="$NENDED_TS"; CMP_WHAT="today's orchestrator completion ${ENDED_HMS}Z"
    fi
    if ! [[ "$SNAP_TS" =~ ^[0-9]{14}$ && "$CMP_TS" =~ ^[0-9]{14}$ ]]; then
        skip_backfill "unparseable snapshot/nightly timestamps (snap='$SNAP', start='$NSTART', ended='$NENDED_TS')"
    fi
    if (( 10#$SNAP_TS < 10#$CMP_TS )); then
        skip_backfill "serving snapshot $SNAP predates $CMP_WHAT — the post-nightly promote has not happened"
    fi
    log "backfill guard OK: nightly start=$NSTART exit=$NEXIT snapshot=$SNAP — re-rendering $REPORT_DATE from the promoted snapshot"
    if [[ "${BACKFILL_GUARD_ONLY:-0}" == "1" ]]; then
        log "guard-only evaluation (BACKFILL_GUARD_ONLY=1): verdict=GO — exiting before render"
        exit 0
    fi
fi

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

# ---- 1) pre-stage the Funding-Form bookings sheet (box-local; NOT in backfill — render-only) ----
if [[ "$MODE" == "backfill" ]]; then
    SKIP_WAREHOUSE=1   # render-only BY DESIGN: no staging, no phases, no promote (guard above ensured
                       # the nightly already rebuilt + promoted everything the render reads)
else
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
if [[ "$MODE" == "backfill" ]]; then
    log "skipping promote (backfill is render-only — the nightly already promoted the snapshot this render reads)"
elif [[ "$SKIP_WAREHOUSE" != "0" ]]; then
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
for _ in $(seq 1 450); do          # up to ~75 min (10s each) [2026-07-16: publisher lock-hold+wait]
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

# ---- 4) render (reads the freshly promoted serving snapshot; ALWAYS runs) ----
# backfill re-renders D-1..D-3 [2026-07-03, SMS/WA funnel doctrine]: D-1 heals §2 Delivered/Fail +
# the blast-reconcile split + §4 account-grain split + §6 (nightly D-1 sources); D-2/D-3 heal §2
# Opps — the Qwen classifier lands in serving ~D+2 morning (09:00Z incremental -> seed -> next
# nightly), so a D-1-only pass would leave Opps '—' forever. Renders are idempotent full-tab
# rewrites. An explicit date arg (manual/pilot) keeps the single-day behavior.
RENDER_DATES=("$REPORT_DATE")
if [[ "$MODE" == "backfill" && -z "${2:-}" ]]; then
    RENDER_DATES+=("$(TZ=America/New_York date -d '2 days ago' +%F)" "$(TZ=America/New_York date -d '3 days ago' +%F)")
fi
RENDER_FAILED=0
for RD in "${RENDER_DATES[@]}"; do
    TB="$(date -d "$RD" +'%b %-d')"
    log "rendering tab '$TB' for $RD ..."
    if $PY scripts/render_daily.py "$RD" "$TB"; then
        log "rendered tab '$TB' OK"
    else
        rc=$?
        RENDER_FAILED=1
        log "ERROR render failed rc=$rc for $RD"
        alert ":rotating_light: *daily_report_sync ($MODE)* — render FAILED for $RD (rc=$rc). Tab '$TB' may be stale. Box log: $REPO_DIR/logs/daily_report_sync.log"
    fi
done
if [[ "$RENDER_FAILED" == "0" ]]; then
    log "================ daily_report_sync DONE MODE=$MODE tab='$TAB' ================"
else
    exit 1
fi
