#!/usr/bin/env bash
# Nightly DuckDB backup. Copies warehouse.duckdb to /root/archive/mac-offload/core/
# and prunes anything older than RETENTION_DAYS.
#
# Cron line (after orchestrator finishes):
#   45 5 * * * /root/renaissance-warehouse/scripts/backup.sh >> /root/renaissance-warehouse/logs/backup.log 2>&1

set -euo pipefail

SRC="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
DEST_DIR="${CORE_BACKUP_DIR:-/root/archive/mac-offload/core}"
RETENTION_DAYS="${CORE_BACKUP_RETENTION_DAYS:-14}"
MIN_FREE_GB_AFTER_COPY="${MIN_FREE_GB_AFTER_COPY:-40}"

if [[ ! -f "$SRC" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: source not found: $SRC" >&2
    exit 1
fi

mkdir -p "$DEST_DIR"

SRC_SIZE=$(stat -c '%s' "$SRC")
AVAILABLE_BYTES=$(df -PB1 "$DEST_DIR" | awk 'NR==2 {print $4}')
MIN_FREE_BYTES=$((MIN_FREE_GB_AFTER_COPY * 1024 * 1024 * 1024))

if (( AVAILABLE_BYTES - SRC_SIZE < MIN_FREE_BYTES )); then
    echo "$(date -u +%FT%TZ) SKIP: insufficient disk for backup (available=${AVAILABLE_BYTES} source=${SRC_SIZE} required_post_copy_free=${MIN_FREE_BYTES})"
    exit 0
fi

TS=$(date -u +%Y-%m-%d)
DEST="$DEST_DIR/warehouse-$TS.duckdb"

# `cp` is fine — DuckDB file is self-contained. If a write is in progress
# we may get a partial copy; orchestrator finishes by ~05:45 and this runs
# at 05:45+, so contention should be rare. For safety we cp + verify size.
cp "$SRC" "$DEST"
DEST_SIZE=$(stat -c '%s' "$DEST")
if [[ "$SRC_SIZE" != "$DEST_SIZE" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: backup size mismatch (src=$SRC_SIZE dest=$DEST_SIZE)" >&2
    rm -f "$DEST"
    exit 2
fi

echo "$(date -u +%FT%TZ) backed up $SRC -> $DEST ($DEST_SIZE bytes)"

# Prune old backups
find "$DEST_DIR" -name "warehouse-*.duckdb" -type f -mtime "+$RETENTION_DAYS" -print -delete

echo "$(date -u +%FT%TZ) prune done (retention=${RETENTION_DAYS}d)"
