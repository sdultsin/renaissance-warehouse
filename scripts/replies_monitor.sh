#!/usr/bin/env bash
# Lightweight read-only progress sampler for the replies backfill.
DB=/root/core/warehouse.duckdb
MONLOG=/root/renaissance-warehouse/logs/replies_backfill_monitor.log
for i in $(seq 1 240); do  # up to 20h at 5-min cadence
  ts=$(date -u +%FT%TZ)
  stats=$(duckdb -readonly "$DB" -noheader -list "SELECT count(*)||' rows, '||count(DISTINCT workspace_id)||' ws, '||count(DISTINCT campaign_id)||' campaigns' FROM raw_instantly_email" 2>/dev/null)
  alive=$(pgrep -f "replies_backfill.sh" >/dev/null && echo RUNNING || echo STOPPED)
  echo "$ts [$alive] $stats" >> "$MONLOG"
  if [ "$alive" = "STOPPED" ]; then
    echo "$ts backfill process gone — final sample taken, monitor exiting" >> "$MONLOG"
    break
  fi
  sleep 300
done
