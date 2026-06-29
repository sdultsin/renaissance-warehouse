#!/usr/bin/env bash
# install_infra_batch_cron.sh — idempotently install the WEEKLY infra-batch mirror
# refresh into root's crontab on the droplet.
#
# Schedule: Mondays 06:30 UTC — OUTSIDE the 03:30 nightly rebuild and 05:45 backup
# window (the writer window the handoff says to avoid), and before the daytime
# data-pipeline load (12:00+ UTC). The weekly cadence matches Sam's note that the
# batch sheets do not change daily. The job self-sequences behind the nightly via
# the warehouse writer flock if they ever overlap.
#
# Idempotent: re-running replaces the existing infra-batch line (matched by the
# script path) rather than appending a duplicate.
#
#   ssh root@<box> 'bash /root/renaissance-warehouse/scripts/install_infra_batch_cron.sh'
set -euo pipefail

REPO="${INFRA_BATCH_REPO:-/root/renaissance-warehouse}"
SCRIPT="$REPO/scripts/refresh_infra_batch.sh"
LOG="$REPO/logs/refresh_infra_batch.cron.log"
CRON_LINE="30 6 * * 1 CRON_TZ=UTC $SCRIPT >> $LOG 2>&1"
MARKER="refresh_infra_batch.sh"

[ -x "$SCRIPT" ] || chmod +x "$SCRIPT"

current="$(crontab -l 2>/dev/null || true)"
# Drop any prior infra-batch line, then append the canonical one.
filtered="$(printf '%s\n' "$current" | grep -v "$MARKER" || true)"
{
  printf '%s\n' "$filtered" | sed '/^$/d'
  printf '%s\n' "$CRON_LINE"
} | crontab -

echo "installed weekly infra-batch cron:"
crontab -l | grep "$MARKER"
