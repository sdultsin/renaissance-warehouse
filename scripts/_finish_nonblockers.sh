#!/usr/bin/env bash
export WAREHOUSE_WRITE_LOCK_HELD=1  # outer flock holds the warehouse-writer lock; tell core/db.py not to re-lock (deadlock guard) [warehouse-ops 2026-06-17]
set -uo pipefail
cd /root/renaissance-warehouse; PY=.venv/bin/python; L=/root/core/warehouse.write.lock
echo "=== NB1: account-truth daily mirror (sending_dq) ==="
flock -w 120 "$L" -c "$PY -m core.orchestrator --phase derived --ingest sending_dq 2>&1 | tail -6"
echo "=== NB2: purchased_at Porkbun backfill ==="
flock -w 120 "$L" -c "$PY scripts/backfill_purchased_at_porkbun.py --from-cache /root/core/porkbun_dates.parquet 2>&1 | tail -3"
echo "=== refresh registry ==="
flock -w 120 "$L" -c "$PY scripts/refresh_sync_registry.py 2>&1 | tail -1"
echo "=== DoD verify + #cc-sam ==="
$PY scripts/verify_hardening_dod.py 2>&1 | tail -25
echo "=== FINISH DONE ==="
