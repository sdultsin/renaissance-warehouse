#!/usr/bin/env bash
# Nightly warehouse backup — LOCAL (block-storage volume) + OFF-BOX (Google Drive via rclone).
#
# History (why this is shaped the way it is):
#   - Prior version wrote the ~95GB backup to /root (root volume), which pushed root to 97%
#     and made >1 retained copy impossible. Backups now go to the 1TB block-storage volume.
#   - Backups were LOCAL-ONLY on the same droplet => a droplet/volume loss took the backups too.
#     We now also push off-box to Google Drive (existing rclone remote), so a full droplet loss
#     still leaves a recoverable copy of the consolidated archive + the irreplaceable seed_data.
#   - seed_data/ (GBC revenue CSV, LLM-labeled reply corpora, partner feedback) is git-ignored and
#     exists nowhere else. It is now backed up too (tarball + a live additive mirror off-box).
#   - 2026-07-09: /root/core/warehouse.duckdb became a SYMLINK to the 2TB volume on 06-16, and
#     `stat -c%s` (no -L) returned the symlink length (52) -> every night since 06-17 failed
#     "size mismatch (src=52)", deleted the fresh copy, and never reached the off-box push
#     (22 consecutive silent failure nights). Fix: source the backup from the SERVED-SNAPSHOT
#     pointer (immutable, validated by the promote gate, promoted 5-6x/day) instead of the live
#     writer file — kills both the symlink-stat bug and the torn-copy risk of cp'ing a file the
#     single writer is actively mutating — and resolve the pointer ONCE with readlink -f.
#     Real failures now alert to Slack via scripts/alert_slack.py (success stays silent).
#
# Cron (after orchestrator finishes ~05:45 UTC):
#   45 5 * * * /root/renaissance-warehouse/scripts/backup.sh >> /root/renaissance-warehouse/logs/backup.log 2>&1

set -Eeuo pipefail

# Source: the served-snapshot pointer — NOT the live writer file (see 2026-07-09 note above).
SRC_PTR="${CORE_BACKUP_SRC:-/opt/duckdb/warehouse_current.duckdb}"
SEED_DIR="${CORE_SEED_DIR:-/root/renaissance-warehouse/seed_data}"
# Local backups live on the 1TB volume, NOT on root.
DEST_DIR="${CORE_BACKUP_DIR:-/mnt/volume_nyc1_1781398428838/backups}"
# Big .duckdb copies are ~96GB and growing; volume has finite room -> keep few locally, lean on off-box.
RETENTION_DAYS="${CORE_BACKUP_RETENTION_DAYS:-4}"
MIN_FREE_GB_AFTER_COPY="${MIN_FREE_GB_AFTER_COPY:-60}"

# ---- Off-box (Google Drive via existing rclone remote) ----
OFFBOX_ENABLED="${CORE_OFFBOX_ENABLED:-1}"
RCLONE_REMOTE="${CORE_OFFBOX_REMOTE:-sdultsin@gmail.com:Renaissance/warehouse-offbox-backups}"
OFFBOX_RETENTION_DAYS="${CORE_OFFBOX_RETENTION_DAYS:-10}"

log() { echo "$(date -u +%FT%TZ) $*"; }

# Fail-loud: real failures post to Slack (#cc-sam) via the fleet's scripts/alert_slack.py
# (stdlib-only; reads SLACK_TOKEN/SLACK_ALERT_CHANNEL from the repo .env). Success is silent.
REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
alert() {
  local py="$REPO_DIR/.venv/bin/python"; [[ -x "$py" ]] || py="python3"
  "$py" "$REPO_DIR/scripts/alert_slack.py" \
    ":rotating_light: warehouse backup: $1 — $(hostname) $(date -u +%FT%TZ), see logs/backup.log" \
    >/dev/null 2>&1 || true
}
on_err() { local rc=$? line="${BASH_LINENO[0]:-?}"; log "ERROR: aborted (rc=$rc near line $line)"; alert "ABORTED unexpectedly (rc=$rc near line $line)"; }
trap on_err ERR

# Serialize: never let two backup runs overlap (off-box push can run for hours).
exec 9>/tmp/warehouse-backup.lock
if ! flock -n 9; then
  log "SKIP: another backup run holds the lock"
  # Benign when a long off-box push overlaps the next cron; a REAL failure if backups stopped landing.
  if ! find "$DEST_DIR" -name "warehouse-*.duckdb" -type f -mtime -1 2>/dev/null | grep -q .; then
    alert "SKIPPED (another run holds the lock) and no local backup is <24h old — a previous run may be hung"
  fi
  exit 0
fi

# Resolve the pointer ONCE: we keep copying the same immutable snapshot file even if a promote
# retargets warehouse_current mid-copy (stat/cp then operate on the resolved regular file).
SRC="$(readlink -f -- "$SRC_PTR" 2>/dev/null || true)"
if [[ -z "$SRC" || ! -f "$SRC" ]]; then
  log "ERROR: backup source not found: $SRC_PTR (resolved: ${SRC:-<none>})"
  alert "source not found: $SRC_PTR"
  exit 1
fi
mkdir -p "$DEST_DIR"

SRC_SIZE=$(stat -c '%s' "$SRC")
AVAILABLE_BYTES=$(df -PB1 "$DEST_DIR" | awk 'NR==2 {print $4}')
MIN_FREE_BYTES=$((MIN_FREE_GB_AFTER_COPY * 1024 * 1024 * 1024))
if (( AVAILABLE_BYTES - SRC_SIZE < MIN_FREE_BYTES )); then
    log "SKIP: insufficient disk on $DEST_DIR (available=${AVAILABLE_BYTES} source=${SRC_SIZE} required_post_copy_free=${MIN_FREE_BYTES})"
    alert "SKIPPED: insufficient disk on $DEST_DIR (available=${AVAILABLE_BYTES}B, source=${SRC_SIZE}B, need ${MIN_FREE_GB_AFTER_COPY}GB free post-copy) — NO backup made tonight"
    exit 0
fi

TS=$(date -u +%Y-%m-%d)
DEST="$DEST_DIR/warehouse-$TS.duckdb"

# ---- Local .duckdb backup (cp + size verify; DuckDB file is self-contained) ----
cp "$SRC" "$DEST"
DEST_SIZE=$(stat -c '%s' "$DEST")
if [[ "$SRC_SIZE" != "$DEST_SIZE" ]]; then
    log "ERROR: backup size mismatch (src=$SRC_SIZE dest=$DEST_SIZE)"
    alert "size mismatch after copy (src=$SRC_SIZE dest=$DEST_SIZE) — fresh copy deleted, NO backup made"
    rm -f "$DEST"; exit 2
fi
log "local backup OK: $DEST ($DEST_SIZE bytes) <- $SRC"

# ---- seed_data tarball (irreplaceable, small) ----
SEED_TGZ=""
if [[ -d "$SEED_DIR" ]]; then
    SEED_TGZ="$DEST_DIR/seed_data-$TS.tar.gz"
    tar -czf "$SEED_TGZ" -C "$(dirname "$SEED_DIR")" "$(basename "$SEED_DIR")"
    log "seed_data tarball OK: $SEED_TGZ ($(stat -c '%s' "$SEED_TGZ") bytes)"
else
    log "WARN: seed_data dir not found: $SEED_DIR (skipped)"
fi

# ---- Prune local ----
find "$DEST_DIR" -name "warehouse-*.duckdb" -type f -mtime "+$RETENTION_DAYS" -print -delete || true
find "$DEST_DIR" -name "seed_data-*.tar.gz"  -type f -mtime "+$RETENTION_DAYS" -print -delete || true
log "local prune done (retention=${RETENTION_DAYS}d)"

# ---- Off-box push (Google Drive) ----
if [[ "$OFFBOX_ENABLED" == "1" ]] && command -v rclone >/dev/null 2>&1; then
    if rclone lsd "$RCLONE_REMOTE" >/dev/null 2>&1 || rclone mkdir "$RCLONE_REMOTE" 2>/dev/null; then
        PUSH_OK=1
        rclone copy "$DEST" "$RCLONE_REMOTE/duckdb" \
            --drive-chunk-size 256M --transfers 4 --checkers 8 --retries 5 --low-level-retries 10 2>&1 | tail -3 \
            || { PUSH_OK=0; log "WARN: off-box duckdb push had errors"; alert "off-box (Drive) duckdb push FAILED — local backup exists at $DEST but NO fresh off-box copy"; }
        if [[ -n "$SEED_TGZ" ]]; then
            rclone copy "$SEED_TGZ" "$RCLONE_REMOTE/seed_data" \
                --drive-chunk-size 128M --transfers 4 --retries 5 2>&1 | tail -2 || log "WARN: off-box seed tarball push had errors"
        fi
        # Additive live mirror of raw seed_data dir: copy (never sync) so an upstream deletion never erases the off-box copy.
        rclone copy "$SEED_DIR" "$RCLONE_REMOTE/seed_data_live" \
            --drive-chunk-size 128M --transfers 8 --checkers 8 2>&1 | tail -2 || log "WARN: off-box seed_data_live mirror had errors"
        if [[ "$PUSH_OK" == "1" ]]; then
            # Prune off-box dated artifacts ONLY after a successful duckdb push — never delete the
            # last good off-box copy right after a failed upload. (Live mirror intentionally never pruned.)
            rclone delete "$RCLONE_REMOTE/duckdb"    --min-age "${OFFBOX_RETENTION_DAYS}d" 2>/dev/null || true
            rclone delete "$RCLONE_REMOTE/seed_data" --min-age "${OFFBOX_RETENTION_DAYS}d" 2>/dev/null || true
            log "off-box push OK -> $RCLONE_REMOTE (retention=${OFFBOX_RETENTION_DAYS}d)"
        fi
    else
        log "WARN: off-box remote unreachable: $RCLONE_REMOTE (skipped — local backup still made)"
        alert "off-box remote unreachable ($RCLONE_REMOTE) — local backup made but NOTHING pushed off-box"
    fi
else
    log "off-box disabled or rclone missing (skipped)"
fi

log "backup done"
