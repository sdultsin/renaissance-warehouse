#!/usr/bin/env bash
export WAREHOUSE_WRITE_LOCK_HELD=1  # outer flock holds the warehouse-writer lock; tell core/db.py not to re-lock (deadlock guard) [warehouse-ops 2026-06-17]
# One-shot write-window batch for the 2026-06-08 warehouse-hardening tracks.
# Run when the single-writer lock is free (reply backfill / nightly not active).
set -uo pipefail
cd /root/renaissance-warehouse
PY=.venv/bin/python
L=/root/core/warehouse.write.lock

echo "=== 1. apply DDL (sync_registry, infra views, campaign_daily) ==="
flock -w 60 "$L" -c "$PY scripts/setup_db.py 2>&1 | tail -4"

echo "=== 2. Track H — load campaign_daily from cache ==="
flock -w 300 "$L" -c "$PY scripts/build_campaign_daily.py --from-cache /tmp/campaign_daily_cache.json 2>&1 | tail -3"

echo "=== 3. Track I — backfill domain_registry NS ==="
flock -w 120 "$L" -c "$PY scripts/backfill_domain_registry.py --ns /root/core/ns_sweep.parquet 2>&1 | tail -3"

echo "=== 4. Track E — refresh sync_registry ==="
flock -w 120 "$L" -c "$PY scripts/refresh_sync_registry.py 2>&1 | tail -2"

echo "=== 5. F2 — checkpoint WAL then publish serving copy (unfreeze) ==="
flock -w 120 "$L" -c "duckdb /root/core/warehouse.duckdb 'CHECKPOINT' 2>&1 | tail -1"
flock -w 600 "$L" -c "bash scripts/publish_serving.sh 2>&1 | tail -3"

echo "=== 6. Track E — QA (no post) ==="
$PY scripts/warehouse_qa.py --no-post 2>&1 | tail -20

echo "=== 7. DoD verification + #cc-sam self-report ==="
$PY scripts/verify_hardening_dod.py 2>&1 | tail -25
echo "=== BATCH DONE ==="
