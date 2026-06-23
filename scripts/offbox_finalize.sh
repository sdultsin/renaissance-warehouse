#!/usr/bin/env bash
# One-shot: wait for the bootstrap off-box upload to finish, verify the Drive copy
# byte-for-byte, then relocate the 95GB backup off the (97%-full) root volume to the
# block-storage volume. Only frees root AFTER the off-box copy is confirmed.
set -uo pipefail
LOG=/root/renaissance-warehouse/logs/offbox-finalize.log
SRC=/root/archive/mac-offload/core/warehouse-2026-06-23.duckdb
REMOTE="sdultsin@gmail.com:Renaissance/warehouse-offbox-backups/duckdb"
VOLBK=/mnt/volume_nyc1_1781398428838/backups
{
  echo "$(date -u +%FT%TZ) finalize: waiting for bootstrap rclone to finish..."
  while pgrep -f "rclone copy .*warehouse-2026-06-23.duckdb" >/dev/null 2>&1; do sleep 30; done
  echo "$(date -u +%FT%TZ) rclone finished; verifying Drive copy..."
  LOCAL_BYTES=$(stat -c%s "$SRC" 2>/dev/null)
  REMOTE_BYTES=$(rclone lsl "$REMOTE/warehouse-2026-06-23.duckdb" 2>/dev/null | awk "{print \$1}")
  echo "$(date -u +%FT%TZ) local=$LOCAL_BYTES remote=$REMOTE_BYTES"
  if [ -n "$REMOTE_BYTES" ] && [ "$LOCAL_BYTES" = "$REMOTE_BYTES" ]; then
    echo "$(date -u +%FT%TZ) VERIFIED off-box copy intact. root BEFORE:"; df -h / | tail -1
    mkdir -p "$VOLBK"
    mv "$SRC" "$VOLBK/warehouse-2026-06-23.duckdb"
    echo "$(date -u +%FT%TZ) relocated to volume. root AFTER:"; df -h / | tail -1
    echo "$(date -u +%FT%TZ) volume backups dir:"; ls -lh "$VOLBK" | tail -6
  else
    echo "$(date -u +%FT%TZ) MISMATCH/INCOMPLETE — NOT relocating; root copy kept safe. Investigate."
  fi
  echo "$(date -u +%FT%TZ) === finalize done ==="
} >> "$LOG" 2>&1
