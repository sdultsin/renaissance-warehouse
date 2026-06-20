#!/usr/bin/env bash
# Step 2a resumable full-history backfill of raw_instantly_email.
# - Forces per-workspace full pull (WAREHOUSE_REPLIES_FULL_BACKFILL=1) so all 16
#   live workspaces get complete history despite the GLOBAL watermark.
# - Idempotent upserts on email_id => every pass is safe & resumable.
# - Single warehouse writer; never runs inside 03:20-05:55 UTC (padded writer window).
# - Loops passes until row count stops advancing (delta < THRESHOLD) or MAX_PASSES.
set -uo pipefail

REPO_DIR=/root/renaissance-warehouse
DB=/root/core/warehouse.duckdb
LOGDIR=$REPO_DIR/logs
RUNLOG=$LOGDIR/replies_backfill_run.log
LOCK=/tmp/replies_backfill.lock
THRESHOLD=25
MAX_PASSES=12

mkdir -p "$LOGDIR"
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "$(date -u +%FT%TZ) another backfill instance holds the lock; exiting" >> "$RUNLOG"
  exit 0
fi

log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$RUNLOG"; }
count_rows(){ duckdb -readonly "$DB" -noheader -list "SELECT count(*) FROM raw_instantly_email" 2>/dev/null | tr -dc '0-9'; }

# True only if a REAL orchestrator python process (not this script, not pgrep) runs.
orchestrator_running(){
  pgrep -af "python.* -m core.orchestrator" | grep -vq "replies_backfill"
}
in_writer_window(){
  local now; now=$(date -u +%H%M); now=$((10#$now))
  [ "$now" -ge 320 ] && [ "$now" -le 555 ]
}
wait_clear(){
  while in_writer_window; do log "writer window 03:20-05:55 UTC — sleep 300s"; sleep 300; done
  while orchestrator_running; do
    log "another orchestrator (nightly) running — wait 120s"; sleep 120
    while in_writer_window; do sleep 300; done
  done
}

cd "$REPO_DIR" || { log "cannot cd $REPO_DIR"; exit 1; }
source .venv/bin/activate
export WAREHOUSE_PULL_REPLIES=1
export WAREHOUSE_REPLIES_FULL_BACKFILL=1

log "=== BACKFILL START (THRESHOLD=$THRESHOLD MAX_PASSES=$MAX_PASSES) ==="
prev=$(count_rows); prev=${prev:-0}
log "starting row count = $prev"

for pass in $(seq 1 $MAX_PASSES); do
  wait_clear
  PASSLOG=$LOGDIR/replies_backfill_pass${pass}.log
  log "--- PASS $pass starting (log: $PASSLOG) ---"
  timeout 5400 python -m core.orchestrator --phase instantly --ingest instantly_replies > "$PASSLOG" 2>&1
  rc=$?
  cur=$(count_rows); cur=${cur:-$prev}
  delta=$((cur - prev))
  log "--- PASS $pass done rc=$rc rows=$cur delta=$delta ---"
  tail -3 "$PASSLOG" | sed 's/^/    /' >> "$RUNLOG"
  if [ "$delta" -lt "$THRESHOLD" ] && [ "$pass" -gt 1 ]; then
    log "delta $delta < THRESHOLD after pass $pass — watermark stopped advancing; STOPPING"
    prev=$cur; break
  fi
  prev=$cur
done
log "=== BACKFILL COMPLETE final_rows=$prev ==="
