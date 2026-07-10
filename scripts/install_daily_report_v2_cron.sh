#!/usr/bin/env bash
# install_daily_report_v2_cron.sh — cut the Daily RevOps Report over to the v2 renderer.
#
#   --apply     back up the crontab, COMMENT OUT the old daily-report crons (same-day evening/relock/
#               backfill + sla_watchdog — they render the SAME tabs v2 now owns, so they must not race),
#               and install ONE gated v2 cron. Keeps render_mtd.py (separate tab) untouched.
#   --rollback  restore the most recent crontab backup (re-enables the old renderer, removes v2).
#   (no arg)    DRY-RUN: print the resulting crontab diff without installing.
#
# Reversible by design [handoff hard rule: keep rollback trivial]. The old scripts stay on disk; only
# the cron lines are toggled. Run AFTER commit+push of scripts/render_daily_v2.py + scripts/daily_report_v2.sh.
set -uo pipefail
REPO_DIR="/root/renaissance-warehouse"
V2_LINE='*/20 6-14 * * * /root/renaissance-warehouse/scripts/daily_report_v2.sh >> /root/renaissance-warehouse/logs/daily_report_v2.log 2>&1   # Daily RevOps Report v2 — D-1-FINAL warehouse-only, gated [2026-07-09]'
BACKUP_DIR="$REPO_DIR/logs/crontab-backups"
MODE="${1:-dry}"
mkdir -p "$BACKUP_DIR"

CUR="$(crontab -l 2>/dev/null)"
disable_old(){  # comment out any active (non-#) line invoking the old daily-report scripts
    echo "$CUR" | awk '
      /^[^#]/ && (/daily_report_sync\.sh/ || /daily_report_sla_watchdog\.sh/) { print "#v2cutover# " $0; next }
      { print }'
}

case "$MODE" in
  --rollback)
    LATEST="$(ls -1t "$BACKUP_DIR"/crontab-*.bak 2>/dev/null | head -1)"
    [[ -n "$LATEST" ]] || { echo "no backup found in $BACKUP_DIR"; exit 1; }
    crontab "$LATEST"
    echo "ROLLED BACK crontab from $LATEST (old renderer re-enabled, v2 cron removed if it was in that backup)"
    crontab -l | grep -E 'daily_report' || true
    ;;
  --apply)
    TS="$(date -u +%Y%m%dT%H%M%SZ)"
    echo "$CUR" > "$BACKUP_DIR/crontab-$TS.bak"
    echo "backed up crontab -> $BACKUP_DIR/crontab-$TS.bak"
    NEW="$(disable_old)"
    # add the v2 line once (idempotent)
    if ! echo "$NEW" | grep -qF 'daily_report_v2.sh'; then
        NEW="$NEW"$'\n'"$V2_LINE"
    fi
    echo "$NEW" | crontab -
    echo "APPLIED. daily-report crons now:"
    crontab -l | grep -E 'daily_report|render_mtd' || true
    ;;
  *)
    echo "=== DRY-RUN — resulting daily-report crons would be: ==="
    NEW="$(disable_old)"
    echo "$NEW" | grep -qF 'daily_report_v2.sh' || NEW="$NEW"$'\n'"$V2_LINE"
    echo "$NEW" | grep -E 'daily_report|render_mtd' || true
    echo "=== (run with --apply to install, --rollback to revert) ==="
    ;;
esac
