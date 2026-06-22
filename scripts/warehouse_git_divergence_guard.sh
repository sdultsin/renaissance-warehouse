#!/usr/bin/env bash
# Warehouse git divergence guard — detects re-drift of the running box vs origin/main.
# Root cause this prevents: box edits never pushed -> origin silently falls behind (the 2026-06-20 reconcile).
#
# Runs as a box cron at :05 each hour (offset from 03:15 delta-sync / 03:30 nightly / 06:35 b2b load).
# Phase 1 = ALERT-ONLY (no auto-pull). Alert-on-REAL-drift, DEDUPED (never per-run spam).
# Durable channel = Slack via scripts/alert_slack.py (the same channel the nightly alerts to);
# the agent-bus is a local-machine construct and does not exist on the box.
set -uo pipefail

REPO=/root/renaissance-warehouse
STATE=/root/.warehouse-git-drift-state          # last-alerted "signature\nepoch"
DAILY=86400                                      # re-alert same drift at most once/day
PY="$REPO/.venv/bin/python"; [ -x "$PY" ] || PY=python3
SCHEMA_DRIFT_PY="$REPO/scripts/schema_db_repo_drift.py"   # DB-schema-ahead-of-repo check (v96 blind spot)
SNAPSHOT="${WAREHOUSE_CURRENT_DUCKDB:-/opt/duckdb/warehouse_current.duckdb}"

cd "$REPO" || exit 0
mkdir -p "$REPO/logs" 2>/dev/null || true        # ensure the drift-helper stderr redirect target exists
git fetch origin --quiet 2>/dev/null || exit 0   # transient network fail -> stay SILENT (no false alarm)

AHEAD=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)    # committed-but-unpushed box commits
BEHIND=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)   # origin commits the box hasn't pulled
# Uncommitted working-tree drift (the hole AHEAD>0 misses — e.g. the dirty meeting.py / ~15-file tree).
# git status --porcelain already omits gitignored paths; we additionally drop known runtime-generated noise.
DIRTY=$(git status --porcelain 2>/dev/null \
        | grep -vE '(^|[[:space:]])(logs/|data/|.*\.duckdb|.*\.duckdb\.wal|.*\.bak[-.]|\.env($|\.)|.*\.log$|.*\.beat$|.*\.parquet$|.*\.tmp$)' \
        | grep -cE '.' || true)

# DB-vs-repo schema drift (the v96 blind spot AHEAD/DIRTY can't see): a moderator apply-now applied a
# DDL to the live warehouse (core.schema_version) that was never committed to sql/ddl/ at origin/main.
# Helper is read-only + fail-SILENT on its own errors (prints DBDRIFT=0) so it can never false-alarm.
DBDRIFT=0; DBDRIFT_VERS=""
if [ -f "$SCHEMA_DRIFT_PY" ] && [ -e "$SNAPSHOT" ]; then
  DBOUT="$(WAREHOUSE_REPO_ROOT="$REPO" WAREHOUSE_CURRENT_DUCKDB="$SNAPSHOT" SCHEMA_REPO_REF=origin/main \
           "$PY" "$SCHEMA_DRIFT_PY" 2>>"$REPO/logs/git_drift_guard.log")"
  DBDRIFT="$(printf '%s\n' "$DBOUT" | sed -n 's/^DBDRIFT=\([0-9][0-9]*\).*/\1/p' | tail -1)"
  DBDRIFT_VERS="$(printf '%s\n' "$DBOUT" | sed -n 's/^DBDRIFT=[0-9][0-9]* VERSIONS=\(.*\)$/\1/p' | tail -1)"
  case "$DBDRIFT" in ''|*[!0-9]*) DBDRIFT=0 ;; esac
fi

REASONS=""
[ "${AHEAD:-0}"  -gt 0 ] && REASONS="${REASONS}AHEAD=${AHEAD}(unpushed box commits) "
[ "${BEHIND:-0}" -gt 5 ] && REASONS="${REASONS}BEHIND=${BEHIND}(box not pulling) "
[ "${DIRTY:-0}"  -gt 0 ] && REASONS="${REASONS}DIRTY=${DIRTY}(uncommitted box edits) "
[ "${DBDRIFT:-0}" -gt 0 ] && REASONS="${REASONS}DBDRIFT=${DBDRIFT}(schema applied-not-in-repo: ${DBDRIFT_VERS}) "

# Healthy -> clear state so the NEXT real drift alerts fresh; stay silent.
if [ -z "$REASONS" ]; then rm -f "$STATE"; exit 0; fi

# Drift present -> DEDUP. Signature changes whenever the drift picture changes (worsens/shifts).
SIG="A${AHEAD}|B${BEHIND}|D${DIRTY}|S${DBDRIFT}"
NOW=$(date +%s)
PREV_SIG=""; PREV_TS=0
if [ -f "$STATE" ]; then PREV_SIG=$(sed -n 1p "$STATE" 2>/dev/null); PREV_TS=$(sed -n 2p "$STATE" 2>/dev/null); fi
# Same drift signature, already alerted, still inside the daily cooldown -> SILENT (no re-spam).
if [ "$SIG" = "$PREV_SIG" ] && [ $((NOW - ${PREV_TS:-0})) -lt $DAILY ]; then exit 0; fi

printf '%s\n%s\n' "$SIG" "$NOW" > "$STATE"
MSG=":rotating_light: Warehouse DRIFT (box/DB vs origin/main): ${REASONS}| origin=$(git rev-parse --short origin/main) box=$(git rev-parse --short HEAD). Going-forward rule = edit -> PR to origin -> box pulls; the box must not carry unpushed commits or a dirty tree, and a moderator apply-now must not skip the commit (DBDRIFT = schema in the live DB that was never committed). Runbook: GIT-SYNC-DISCIPLINE.md (repo root)"
"$PY" "$REPO/scripts/alert_slack.py" "$MSG" 2>/dev/null || true
