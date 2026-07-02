#!/usr/bin/env bash
# Mirror ALL box data to Google Drive (off-box), EXCEPT secrets and non-data junk.
# Complements backup.sh, which backs up the warehouse + seed_data as consistent dated snapshots.
# This covers everything else: lead-mirror (~100GB), enrichment DBs, account_truth snapshots,
# job data/CSVs, archives, scratch DBs, /root/Renaissance, etc.
#
# Excludes (see scripts/offbox-mirror.filter): all secret/credential files (.env*, SSH keys,
# rclone/aws config, *.key/*.pem, *token*/*secret*), and non-data junk (venvs, node_modules,
# __pycache__, .git, caches, logs). The warehouse itself is excluded here (backup.sh owns it).
#
# Cron (after the warehouse backup + off-box push finish):
#   30 7 * * * /root/renaissance-warehouse/scripts/offbox_mirror.sh >> /root/renaissance-warehouse/logs/offbox-mirror.log 2>&1

set -uo pipefail
REMOTE="${BOX_MIRROR_REMOTE:-sdultsin@gmail.com:Renaissance/box-mirror}"
FILTER="${BOX_MIRROR_FILTER:-/root/renaissance-warehouse/scripts/offbox-mirror.filter}"
DRYRUN="${BOX_MIRROR_DRYRUN:-0}"
log() { echo "$(date -u +%FT%TZ) $*"; }

exec 9>/tmp/offbox-mirror.lock
flock -n 9 || { log "SKIP: another mirror run holds the lock"; exit 0; }
command -v rclone >/dev/null 2>&1 || { log "ERROR: rclone missing"; exit 1; }
[[ -f "$FILTER" ]] || { log "ERROR: filter not found: $FILTER"; exit 1; }

OPTS=(--filter-from "$FILTER" --transfers 6 --checkers 12 --drive-chunk-size 256M
      --retries 3 --low-level-retries 10 --fast-list --stats 2m --stats-one-line)
[[ "$DRYRUN" == "1" ]] && OPTS+=(--dry-run)

log "=== box->Drive mirror START (dryrun=$DRYRUN) ==="
for SRC in /root /mnt/volume_nyc1_1781398428838; do
  DEST="$REMOTE/$(basename "$SRC")"
  [[ "$SRC" == "/root" ]] && DEST="$REMOTE/root"
  log "mirroring $SRC -> $DEST"
  rclone sync "$SRC" "$DEST" "${OPTS[@]}" 2>&1 | tail -6
done
log "=== box->Drive mirror DONE (dryrun=$DRYRUN) ==="
