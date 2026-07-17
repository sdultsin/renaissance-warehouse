#!/usr/bin/env bash
# with_warehouse_lock.sh — run ANY warehouse-writing command under the single-writer lock.
#
# WHY: DuckDB is single-writer. Two concurrent read-write opens of warehouse.duckdb collide
# ("Conflicting lock held by PID ..."). The 03:30Z nightly kept failing when an AD-HOC heal
# (e.g. a hand-launched `core.orchestrator --phase sendivo` re-pull) opened the writer at the
# same time. This wrapper makes any writer SELF-SEQUENCE: it acquires the box-local
# warehouse-writer flock (/root/core/warehouse.write.lock) ACQUIRE-OR-WAIT, then runs the
# command, then releases. A second writer queues behind the first instead of clobbering it.
#
# It exports WAREHOUSE_WRITE_LOCK_HELD=1 so the in-process safety net in core/db.py
# (which would otherwise take the SAME lock again on a NEW fd → deadlock) SKIPS — the two
# mechanisms compose: wrapper-level lock for shell/CLI writers, db.py-level lock as the
# belt-and-suspenders for python writers launched WITHOUT this wrapper.
#
# Usage:
#   with_warehouse_lock.sh <command> [args...]
#   with_warehouse_lock.sh duckdb /root/core/warehouse.duckdb 'CHECKPOINT'
#   WAREHOUSE_LOCK_WAIT_S=3600 with_warehouse_lock.sh .venv/bin/python -m core.orchestrator --phase sendivo --ingest sms_logs
#
# Env:
#   WAREHOUSE_LOCK_FILE   (default /root/core/warehouse.write.lock)
#   WAREHOUSE_LOCK_WAIT_S (default 3600 [2026-07-16, was 1800] — seconds to wait for the lock;
#                          flock -w. Raised because publisher.py now HOLDS this flock across its
#                          entire ~25-30min copy+validate+swap — writers queue behind a promote.)
#
# Exit codes: the wrapped command's exit code; 75 (EX_TEMPFAIL) if the lock could not be
# acquired within the wait window (so a cron can be retried without masking a real failure).
set -uo pipefail

LOCK_FILE="${WAREHOUSE_LOCK_FILE:-/root/core/warehouse.write.lock}"
WAIT_S="${WAREHOUSE_LOCK_WAIT_S:-3600}"

if [ "$#" -lt 1 ]; then
  echo "usage: with_warehouse_lock.sh <command> [args...]" >&2
  exit 2
fi

mkdir -p "$(dirname "$LOCK_FILE")" 2>/dev/null || true

# fd 200 holds the lock for the lifetime of the wrapped command, then auto-releases on exit.
exec 200>"$LOCK_FILE"
if ! flock -w "$WAIT_S" 200; then
  echo "with_warehouse_lock: could not acquire $LOCK_FILE within ${WAIT_S}s — another writer holds it; skipping. $(date -u +%FT%TZ)" >&2
  exit 75
fi

# Mark the lock as held so core/db.py's in-process safety net does NOT re-lock (deadlock guard).
export WAREHOUSE_WRITE_LOCK_HELD=1

exec "$@"
