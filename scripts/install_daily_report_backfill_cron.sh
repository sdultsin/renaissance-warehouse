#!/usr/bin/env bash
# install_daily_report_backfill_cron.sh — idempotently install the post-nightly D-1 backfill
# re-render of the Daily RevOps Report into root's crontab on the droplet [2026-07-02].
#
# Why: the evening (01:00Z) + relock (04:30Z) renders both fire BEFORE the 05:30Z heavy nightly
# that lands the D-1 sources (sendivo log-level delivered/fail -> §2 Fail%, core.sending_account_daily
# -> §4 Actual OTD/Google split, core.email_message -> §6). Without this pass, yesterday's tab keeps
# its evening state forever. `daily_report_sync.sh backfill` re-renders YESTERDAY-ET's tab from the
# freshly promoted serving snapshot, gated on the nightly having actually completed (see the GUARD
# block in daily_report_sync.sh — not-ready -> skip + Slack-warn, never a stale re-render).
#
# Schedule: 12:45 UTC — the nightly starts 05:30Z and is expected to take ~2-4h post-compaction
# (so done by ~09:30Z with slack for slow nights). On a pathological nightly (e.g. the 10.6h
# 2026-07-01 run) the guard skips + warns rather than rendering stale data. Same
# /tmp/daily_report_sync.lock flock as the evening/relock entries (they can never overlap).
# NB: appended at the END of the crontab, where the prevailing CRON_TZ block is UTC (same mechanism
# the existing daily_report entries rely on). If the tail TZ ever changed to ET the job would just
# fire at 12:45 ET — hours later, still post-nightly, still guard-protected.
#
# Idempotent: re-running replaces the existing backfill line (matched by "daily_report_sync.sh backfill")
# rather than appending a duplicate — same pattern as install_infra_batch_cron.sh.
#
#   ssh root@<box> 'bash /root/renaissance-warehouse/scripts/install_daily_report_backfill_cron.sh'
set -euo pipefail

REPO="${DAILY_REPORT_REPO:-/root/renaissance-warehouse}"
SCRIPT="$REPO/scripts/daily_report_sync.sh"
LOG="$REPO/logs/daily_report_sync.log"
CRON_LINE="45 12 * * * /usr/bin/flock -n /tmp/daily_report_sync.lock $SCRIPT backfill >> $LOG 2>&1   # Daily report post-nightly D-1 backfill re-render (guarded; skips+warns if nightly late) [2026-07-02]"

[[ -x "$SCRIPT" ]] || { echo "ERROR: $SCRIPT not found/executable — run on the droplet after git pull" >&2; exit 1; }

( crontab -l 2>/dev/null | grep -vF "daily_report_sync.sh backfill" ; echo "$CRON_LINE" ) | crontab -
echo "installed: $CRON_LINE"
crontab -l | grep -F "daily_report_sync.sh backfill"
