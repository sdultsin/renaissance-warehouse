#!/usr/bin/env bash
# Nightly LEAD-MIRROR off-box backup -> Cloudflare R2 (dated, 7-day rotation).
#
# Why (MIRROR-COVERAGE-AUDIT 2026-07-10 §7): the lead-mirror DuckDB (34.5M leads, PHONE MASTER,
# authoritative since 06-24) had all 3 on-box copies (live + serving + dedup snapshot) on the SAME
# DigitalOcean volume. The only off-box copy was offbox_mirror.sh's single UNVERSIONED `rclone sync`
# copy on Drive — no size verify, no failure alerting, and sync mirrors deletions/corruption: a
# volume unmount or a corrupted serving file would wipe/overwrite the only off-box copy on the next
# 07:30Z run. This script adds a REAL backup: dated copies, verified, pruned only on success.
#
# Source discipline (same class of fix as warehouse d6516ef, 2026-07-09):
#   - Source is the SERVING REPLICA lead_mirror_serving.duckdb — NEVER lead_mirror.duckdb (the
#     single-writer live file) and NEVER .writer.lock. The serving file is atomically promoted by
#     /opt/duckdb/bin/refresh_lead_serving.sh (cp -> bake -> mv, under the writer flock; the
#     intraday email sweeps invoke the same sanctioned script), so the inode at that path is
#     IMMUTABLE once promoted.
#   - We snapshot via HARDLINK: instant, zero-copy, and if a promote replaces the path mid-upload
#     our link still references the old (complete, consistent) inode. Link removed on exit.
#
# Destination: R2 s3://$R2_BUCKET/lead-mirror-backups/lead_mirror_serving-YYYY-MM-DD.duckdb
#   - R2 creds read BY NAME from the repo .env (same keys as export_parquet_r2.sh); never printed.
#   - PII-on-R2 is cleared by Sam (2026-07-10, SOC2 cloud). ~38GB/copy x 8 retained ≈ $4.5/mo.
#   - The R2 token cannot CreateBucket -> no_check_bucket=true is REQUIRED (probe-verified).
#
# Alerting (failure-only, via scripts/alert_slack.py -> #cc-sam; success is silent):
#   source missing/too small, creds missing, push failure, size mismatch on verify, unexpected
#   abort (ERR trap), hung-run lock skip with no recent success, and --verify-only staleness.
#
# Prune: dated R2 copies older than RETENTION_DAYS are deleted ONLY after a successful verified
# push — a failed upload can never delete the last good off-box copy.
#
# Modes:
#   (no args)      run the backup
#   --verify-only  outcome watchdog: alert unless a fresh (<20h), sane-sized dated copy is in R2
#
# Cron (installed on the droplet; 04:30Z = after the 03:50Z serving refresh finishes (~04:15Z),
# before the 05:30Z warehouse nightly / 05:45Z warehouse backup / 06:00Z R2 parquet export):
#   30 4 * * *  /root/renaissance-warehouse/scripts/backup_lead_mirror.sh >> /root/renaissance-warehouse/logs/backup_lead_mirror.log 2>&1
#   0 16 * * *  /root/renaissance-warehouse/scripts/backup_lead_mirror.sh --verify-only >> /root/renaissance-warehouse/logs/backup_lead_mirror.log 2>&1

set -Eeuo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
MIRROR_DIR="${LEAD_MIRROR_DIR:-/mnt/volume_nyc1_1781398428838/lead-mirror}"
SRC="${LEAD_MIRROR_BACKUP_SRC:-$MIRROR_DIR/lead_mirror_serving.duckdb}"   # serving replica ONLY
R2_PREFIX="${LEAD_MIRROR_R2_PREFIX:-lead-mirror-backups}"
RETENTION_DAYS="${LEAD_MIRROR_OFFBOX_RETENTION_DAYS:-7}"
MIN_SANE_BYTES="${LEAD_MIRROR_MIN_SANE_BYTES:-21474836480}"   # 20GB: refuse to rotate in a truncated/reset file
VERIFY_MAX_AGE_H="${LEAD_MIRROR_VERIFY_MAX_AGE_H:-20}"        # 16:00Z check catches a missed 04:30Z run same day
STATE_OK="$REPO_DIR/logs/backup_lead_mirror.last_ok"
LINK="$MIRROR_DIR/.offbox_inflight.duckdb.tmp"                # hardlink snapshot (.tmp: box-mirror filter ignores it)

log() { echo "$(date -u +%FT%TZ) $*"; }

# Fail-loud: real failures post to Slack (#cc-sam) via the fleet's scripts/alert_slack.py. Success is silent.
alert() {
  local py="$REPO_DIR/.venv/bin/python"; [[ -x "$py" ]] || py="python3"
  "$py" "$REPO_DIR/scripts/alert_slack.py" \
    ":rotating_light: lead-mirror off-box backup: $1 — $(hostname) $(date -u +%FT%TZ), see logs/backup_lead_mirror.log" \
    >/dev/null 2>&1 || true
}
on_err() { local rc=$? line="${BASH_LINENO[0]:-?}"; log "ERROR: aborted (rc=$rc near line $line)"; alert "ABORTED unexpectedly (rc=$rc near line $line)"; }
trap on_err ERR

# ---- R2 remote via rclone env-config; creds read BY NAME from repo .env, never printed ----
envget() { grep -E "^$1=" "$REPO_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' || true; }
R2_ACCOUNT_ID="$(envget R2_ACCOUNT_ID)"
R2_ACCESS_KEY_ID="$(envget R2_ACCESS_KEY_ID)"
R2_SECRET_ACCESS_KEY="$(envget R2_SECRET_ACCESS_KEY)"
R2_BUCKET="${R2_BUCKET:-$(envget R2_BUCKET)}"
if [[ -z "$R2_ACCOUNT_ID" || -z "$R2_ACCESS_KEY_ID" || -z "$R2_SECRET_ACCESS_KEY" || -z "$R2_BUCKET" ]]; then
  log "ERROR: R2 creds missing from $REPO_DIR/.env (need R2_ACCOUNT_ID/ACCESS_KEY_ID/SECRET_ACCESS_KEY/BUCKET)"
  alert "R2 creds missing from repo .env — NO off-box backup possible"
  exit 1
fi
export RCLONE_CONFIG_LEADR2_TYPE=s3 \
       RCLONE_CONFIG_LEADR2_PROVIDER=Cloudflare \
       RCLONE_CONFIG_LEADR2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
       RCLONE_CONFIG_LEADR2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
       RCLONE_CONFIG_LEADR2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
       RCLONE_CONFIG_LEADR2_NO_CHECK_BUCKET=true
DEST_DIR="LEADR2:${R2_BUCKET}/${R2_PREFIX}"
command -v rclone >/dev/null 2>&1 || { log "ERROR: rclone missing"; alert "rclone missing on box"; exit 1; }

# Newest dated copy in R2 -> "epoch_seconds bytes name" (empty if none). Used by verify + lock-skip.
newest_remote() {
  rclone lsjson "$DEST_DIR" --files-only 2>/dev/null | python3 -c '
import json,sys,datetime
try: items=json.load(sys.stdin)
except Exception: items=[]
best=None
for it in items:
    n=it.get("Name","")
    if not (n.startswith("lead_mirror_serving-") and n.endswith(".duckdb")): continue
    t=datetime.datetime.fromisoformat(it["ModTime"].replace("Z","+00:00")).timestamp()
    if best is None or t>best[0]: best=(t,it["Size"],n)
if best: print(f"{int(best[0])} {best[1]} {best[2]}")
' || true
}

# ---- --verify-only: OUTCOME watchdog (fresh, sane-sized off-box copy exists). Failure-only alerts. ----
if [[ "${1:-}" == "--verify-only" ]]; then
  NEWEST="$(newest_remote)"
  if [[ -z "$NEWEST" ]]; then
    log "VERIFY FAIL: no dated lead-mirror copy found in R2 ($R2_PREFIX)"
    alert "VERIFY: NO dated off-box copy found in R2 ${R2_PREFIX}/"
    exit 1
  fi
  read -r EPOCH BYTES NAME <<<"$NEWEST"
  AGE_H=$(( ( $(date -u +%s) - EPOCH ) / 3600 ))
  if (( AGE_H > VERIFY_MAX_AGE_H )); then
    log "VERIFY FAIL: newest off-box copy $NAME is ${AGE_H}h old (max ${VERIFY_MAX_AGE_H}h)"
    alert "VERIFY: newest off-box copy ($NAME) is ${AGE_H}h old — last night's backup likely FAILED"
    exit 1
  fi
  if (( BYTES < MIN_SANE_BYTES )); then
    log "VERIFY FAIL: newest off-box copy $NAME is only ${BYTES}B (< $MIN_SANE_BYTES)"
    alert "VERIFY: newest off-box copy ($NAME) is suspiciously small (${BYTES}B)"
    exit 1
  fi
  log "verify OK: $NAME (${BYTES}B, ${AGE_H}h old)"
  exit 0
fi

# ---- Serialize: never two backup runs at once ----
exec 9>/tmp/lead-mirror-offbox-backup.lock
if ! flock -n 9; then
  log "SKIP: another backup run holds the lock"
  # Benign on a rare overlap; a REAL failure if pushes stopped landing.
  if [[ ! -f "$STATE_OK" ]] || (( $(date -u +%s) - $(stat -c %Y "$STATE_OK") > 86400 )); then
    alert "SKIPPED (another run holds the lock) and no successful push in >24h — a previous run may be hung"
  fi
  exit 0
fi

# ---- Source sanity (serving replica; never the live writer) ----
if [[ ! -f "$SRC" ]]; then
  log "ERROR: serving replica not found: $SRC"
  alert "serving replica not found ($SRC) — NO backup made"
  exit 1
fi
rm -f "$LINK"
ln "$SRC" "$LINK"     # snapshot: serving inode is immutable post-promote (see header)
trap 'rm -f "$LINK"' EXIT
SRC_SIZE=$(stat -c '%s' "$LINK")
SRC_MTIME=$(stat -c '%Y' "$LINK")
if (( SRC_SIZE < MIN_SANE_BYTES )); then
  log "ERROR: serving replica suspiciously small (${SRC_SIZE}B < ${MIN_SANE_BYTES}B) — refusing to rotate it in"
  alert "serving replica suspiciously small (${SRC_SIZE}B) — refusing to back it up; investigate refresh_lead_serving"
  exit 2
fi
AGE_H=$(( ( $(date -u +%s) - SRC_MTIME ) / 3600 ))
if (( AGE_H > 36 )); then
  log "WARN: serving replica is ${AGE_H}h old (refresh_lead_serving may be broken) — still pushing"
  alert "WARNING: serving replica is ${AGE_H}h stale (refresh_lead_serving broken?) — pushing it anyway, but freshness is degraded"
fi

TS=$(date -u +%Y-%m-%d)
DEST="$DEST_DIR/lead_mirror_serving-${TS}.duckdb"
log "push start: $SRC (${SRC_SIZE}B, serving mtime $(date -u -d "@$SRC_MTIME" +%FT%TZ)) -> $DEST"

# ---- Push (multipart; ~70-150MB/s measured 2026-07-10 probe => ~5-10 min for 38GB) ----
if ! rclone copyto "$LINK" "$DEST" \
      --s3-upload-cutoff 200M --s3-chunk-size 128M --s3-upload-concurrency 8 \
      --retries 5 --low-level-retries 10 --stats 1m --stats-one-line 2>&1; then
  log "ERROR: R2 push failed"
  alert "R2 push FAILED — NO fresh off-box copy tonight (on-box copies unaffected)"
  exit 3
fi

# ---- Verify landed size ----
REMOTE_SIZE="$(rclone lsjson "$DEST" 2>/dev/null | python3 -c 'import json,sys
try: print(json.load(sys.stdin)[0]["Size"])
except Exception: print(-1)' || true)"
if [[ "$REMOTE_SIZE" != "$SRC_SIZE" ]]; then
  log "ERROR: size mismatch after push (local=$SRC_SIZE remote=$REMOTE_SIZE) — deleting bad remote copy"
  rclone deletefile "$DEST" 2>/dev/null || true
  alert "size mismatch after R2 push (local=${SRC_SIZE}B remote=${REMOTE_SIZE}B) — bad copy deleted, NO fresh off-box copy tonight"
  exit 4
fi
log "push OK + size verified: $DEST (${REMOTE_SIZE}B)"
mkdir -p "$REPO_DIR/logs"; touch "$STATE_OK"

# ---- Prune old dated copies ONLY after a successful verified push ----
rclone delete "$DEST_DIR" --min-age "${RETENTION_DAYS}d" --include "lead_mirror_serving-*.duckdb" 2>/dev/null \
  || log "WARN: off-box prune had errors (retention only; backup itself is fine)"
log "prune done (retention=${RETENTION_DAYS}d); backup done"
