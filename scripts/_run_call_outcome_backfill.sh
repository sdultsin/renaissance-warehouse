#!/bin/bash
# W1i Fix-1b backfill + conversion-event surface — waits for the nightly writer lock to
# release, runs the qwen transcript->outcome classification (populates core.call_outcome_llm),
# then rebuilds core.conversion_event so the warm-caller call bookings surface in the
# conversion fact + fill core.warm_caller.appt_set_calls. Single-writer-safe (runs only when
# the nightly is done). Detached (nohup) so it survives the launching SSH session. Idempotent.
set -u
LOG=/root/renaissance-warehouse/logs/classify_call_outcomes.log
LOCK=/root/core/warehouse.write.lock
cd /root/renaissance-warehouse
echo "$(date -u +%FT%TZ) WATCHER start — waiting for nightly writer lock release" >> "$LOG"
while true; do
  if [ -f "$LOCK" ]; then
    PID=$(grep -oE "pid=[0-9]+" "$LOCK" | cut -d= -f2)
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
      echo "$(date -u +%FT%TZ) writer lock held by pid=$PID — waiting 60s" >> "$LOG"; sleep 60; continue
    fi
  fi
  if pgrep -f "core.orchestrator" >/dev/null 2>&1; then
    echo "$(date -u +%FT%TZ) core.orchestrator still running — waiting 60s" >> "$LOG"; sleep 60; continue
  fi
  break
done
echo "$(date -u +%FT%TZ) writer free — launching classifier (all pending)" >> "$LOG"
export CALL_OUTCOME_LOCK_MAX_WAIT_S=1800
.venv/bin/python scripts/classify_call_outcomes.py >> "$LOG" 2>&1
RC=$?
echo "$(date -u +%FT%TZ) classifier exit rc=$RC" >> "$LOG"
if [ "$RC" = "0" ]; then
  echo "$(date -u +%FT%TZ) rebuilding core.conversion_event to surface warm-caller bookings" >> "$LOG"
  .venv/bin/python - >> "$LOG" 2>&1 <<"PYEOF"
from datetime import datetime, timezone
from core import db as dbm
from core.config import DB_PATH
import entities.conversion_event as ce
conn = dbm.connect(DB_PATH)
try:
    res = ce.rebuild(conn, datetime.now(timezone.utc))
    print("conversion_event.rebuild ->", res)
finally:
    conn.close()
PYEOF
  echo "$(date -u +%FT%TZ) conversion_event rebuild rc=$?" >> "$LOG"
fi
echo "$(date -u +%FT%TZ) DONE backfill+surface" >> "$LOG"
