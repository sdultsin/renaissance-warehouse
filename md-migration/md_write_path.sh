#!/usr/bin/env bash
# MotherDuck write-path cron wrapper — staged 2026-07-12 (Lane B), execute-only from Tue 2026-07-14.
#
# Cron (install per RUNBOOK-TUESDAY.md; replaces the 09:00Z md_load_tables_v3 storefront refresh):
#   */30 6-15 * * * /usr/bin/flock -n /tmp/md_write_path.lock /root/md-migration/md_write_path.sh >> /root/md-migration/md_write_path.log 2>&1
#
# Semantics: tries every 30 min in the post-nightly window; md_write_path.py's GATE returns rc=1
# (silent no-op) until tonight's nightly has COMMITTED and the promoted snapshot passes canaries,
# so this fires exactly once per green nightly regardless of how late the nightly ends.
# rc=0 stamps the day done; rc>=2 already alerted #cc-sam inside the runner.
set -uo pipefail
STAMP=/root/md-migration/.md_write_path_done
TODAY=$(date -u +%F)
[ -f "$STAMP" ] && [ "$(cat "$STAMP" 2>/dev/null)" = "$TODAY" ] && exit 0

/usr/bin/python3 /root/md-migration/md_write_path.py "$@"
rc=$?
if [ "$rc" -eq 0 ]; then
    echo "$TODAY" > "$STAMP"
fi
exit "$rc"
