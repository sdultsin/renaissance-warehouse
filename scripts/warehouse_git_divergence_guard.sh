#!/usr/bin/env bash
# Warehouse git divergence guard — detects re-drift of the running box vs origin/main.
# Root cause this prevents: box edits never pushed -> origin silently falls behind (the 2026-06-20 reconcile).
#
# Runs as a box cron at :05 each hour (offset from 03:15 delta-sync / 03:30 nightly / 06:35 b2b load).
# Durable channel = Slack via scripts/alert_slack.py (the same channel the nightly alerts to);
# the agent-bus is a local-machine construct and does not exist on the box.
#
# [2026-06-29] Phase 2 = AUTO-HEAL. When *git* drift is found (AHEAD/BEHIND/DIRTY — a stray branch,
# unpushed commit, or dirty tree), the guard snaps the main checkout back to clean origin/main, but
# preserves ALL local work to a PUSHED rescue branch FIRST and only resets if that push SUCCEEDS
# (preserve-then-reset; logic unit-tested in isolation 2026-06-29, 13/13). Safety rails:
#   * kill switch — `touch /root/.warehouse-autoheal-off` reverts to alert-only, no code change.
#   * yields to a running writer job (warehouse.write.lock) so it never resets code mid-rebuild.
#   * only touches the main checkout ($REPO); /root/wt-* worktrees are never touched.
#   * DB-schema drift (DBDRIFT) is NOT git-fixable -> it always just alerts, never auto-heals.
set -uo pipefail

REPO=/root/renaissance-warehouse
STATE=/root/.warehouse-git-drift-state          # last-alerted "signature\nepoch"
DAILY=86400                                      # re-alert same drift at most once/day
PY="$REPO/.venv/bin/python"; [ -x "$PY" ] || PY=python3
SCHEMA_DRIFT_PY="$REPO/scripts/schema_db_repo_drift.py"   # DB-schema-ahead-of-repo check (v96 blind spot)
SNAPSHOT="${WAREHOUSE_CURRENT_DUCKDB:-/opt/duckdb/warehouse_current.duckdb}"
AUTOHEAL_OFF=/root/.warehouse-autoheal-off       # kill switch: if present, auto-heal is disabled
WRITE_LOCK=/root/core/warehouse.write.lock       # nightly/apply writer flock — yield while held

# auto_heal_to_main: safely snap the CURRENT git repo (cwd) back to clean origin/main.
# Preserves EVERYTHING (commits + dirty tree, tracked AND untracked) onto a PUSHED rescue branch
# FIRST, and only performs the destructive reset if that push SUCCEEDED. If preservation fails,
# it ABORTS and leaves the work intact (committed locally on the rescue branch) WITHOUT resetting.
# Echoes a status token (+ rescue branch on heal). Returns 0 on heal/clean, 1 on abort/failure.
auto_heal_to_main() {
  git fetch origin --quiet >/dev/null 2>&1 || { echo "ABORTED_NO_FETCH"; return 1; }
  local cur ahead behind dirty ts rescue need_rescue=0
  cur=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
  ahead=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)
  behind=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)
  dirty=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')

  if [ "$cur" = "main" ] && [ "$ahead" -eq 0 ] && [ "$behind" -eq 0 ] && [ "$dirty" -eq 0 ]; then
    echo "ALREADY_CLEAN"; return 0
  fi

  [ "$ahead" -gt 0 ] && need_rescue=1
  [ "$dirty" -gt 0 ] && need_rescue=1
  [ "$cur" != "main" ] && need_rescue=1

  if [ "$need_rescue" -eq 1 ]; then
    ts=$(date +%Y%m%dT%H%M%S)
    rescue="box-rescue/${cur}-${ts}"
    git checkout -b "$rescue" >/dev/null 2>&1 || { echo "ABORTED_BRANCH"; return 1; }
    git add -A >/dev/null 2>&1 || true
    git commit -m "box auto-rescue ${ts} (pre-heal snapshot)" --no-verify >/dev/null 2>&1 || true
    # The destructive step below is GATED on this push succeeding. No preservation -> no reset.
    if ! git push origin "$rescue" >/dev/null 2>&1; then
      echo "ABORTED_PRESERVE_FAILED ${rescue}"; return 1
    fi
  fi

  git checkout -B main origin/main >/dev/null 2>&1 || { echo "FAILED_CHECKOUT"; return 1; }
  git reset --hard origin/main >/dev/null 2>&1
  [ "$need_rescue" -eq 1 ] && git clean -fd >/dev/null 2>&1   # only AFTER work is rescued+pushed

  cur=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
  ahead=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 1)
  behind=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 1)
  dirty=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
  if [ "$cur" = "main" ] && [ "$ahead" -eq 0 ] && [ "$behind" -eq 0 ] && [ "$dirty" -eq 0 ]; then
    echo "HEALED ${rescue:-}"; return 0
  fi
  echo "FAILED_VERIFY"; return 1
}

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

# --- Phase 2: AUTO-HEAL git drift before alerting (DBDRIFT is not git-fixable, so it falls through) ---
GIT_DRIFT=0
{ [ "${AHEAD:-0}" -gt 0 ] || [ "${BEHIND:-0}" -gt 5 ] || [ "${DIRTY:-0}" -gt 0 ]; } && GIT_DRIFT=1
HEAL_NOTE=""
if [ "$GIT_DRIFT" -eq 1 ] && [ ! -e "$AUTOHEAL_OFF" ]; then
  if command -v flock >/dev/null 2>&1 && [ -e "$WRITE_LOCK" ] && ! flock -n "$WRITE_LOCK" -c true 2>/dev/null; then
    HEAL_NOTE="(auto-heal deferred — writer job in progress) "          # try again next hour
  else
    HEAL_RESULT="$(auto_heal_to_main 2>/dev/null || true)"
    case "$HEAL_RESULT" in
      HEALED*)
        rm -f "$STATE"
        MSG=":broom: Warehouse AUTO-HEALED git drift -> clean origin/main (was: ${REASONS}). Prior box work preserved on ${HEAL_RESULT#HEALED }. origin=$(git rev-parse --short origin/main 2>/dev/null) box=$(git rev-parse --short HEAD 2>/dev/null). Runbook: GIT-SYNC-DISCIPLINE.md"
        "$PY" "$REPO/scripts/alert_slack.py" "$MSG" 2>/dev/null || true
        exit 0
        ;;
      ALREADY_CLEAN) ;;                                                  # nothing to do; fall through
      *) HEAL_NOTE="(auto-heal could not complete: ${HEAL_RESULT:-unknown} — manual reconcile needed) " ;;
    esac
  fi
fi

# Drift remains (DBDRIFT-only, auto-heal disabled/deferred, or aborted) -> DEDUP + alert.
# Signature changes whenever the drift picture changes (worsens/shifts).
SIG="A${AHEAD}|B${BEHIND}|D${DIRTY}|S${DBDRIFT}"
NOW=$(date +%s)
PREV_SIG=""; PREV_TS=0
if [ -f "$STATE" ]; then PREV_SIG=$(sed -n 1p "$STATE" 2>/dev/null); PREV_TS=$(sed -n 2p "$STATE" 2>/dev/null); fi
# Same drift signature, already alerted, still inside the daily cooldown -> SILENT (no re-spam).
if [ "$SIG" = "$PREV_SIG" ] && [ $((NOW - ${PREV_TS:-0})) -lt $DAILY ]; then exit 0; fi

printf '%s\n%s\n' "$SIG" "$NOW" > "$STATE"
MSG=":rotating_light: Warehouse DRIFT (box/DB vs origin/main): ${HEAL_NOTE}${REASONS}| origin=$(git rev-parse --short origin/main) box=$(git rev-parse --short HEAD). Going-forward rule = edit -> PR to origin -> box pulls; the box must not carry unpushed commits or a dirty tree, and a moderator apply-now must not skip the commit (DBDRIFT = schema in the live DB that was never committed). Runbook: GIT-SYNC-DISCIPLINE.md (repo root)"
"$PY" "$REPO/scripts/alert_slack.py" "$MSG" 2>/dev/null || true
