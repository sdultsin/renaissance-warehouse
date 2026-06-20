#!/usr/bin/env bash
# Cron wrapper for the Sendivo /sms/logs E1/E2 watchdog (audit 2026-06-14).
# Detect+alert by default; set SMS_WATCHDOG_HEAL=1 to also auto re-pull dropped/short days.
#
# Cron (UTC, droplet) — daily, after the 03:30 nightly + dashboards fully settle:
#   30 7 * * * /root/renaissance-warehouse/scripts/sendivo_sms_logs_watchdog.sh >> \
#              /root/renaissance-warehouse/logs/sms_logs_watchdog.log 2>&1
set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR" || exit 0

[[ -f .venv/bin/activate ]] && source .venv/bin/activate
PYTHON="${PYTHON:-python3}"

ARGS=()
# Self-heal is OFF until the operator confirms the compaction-swap landed and DDL 66 applied
# (re-pull writes the warehouse; the watchdog already opens read-only first and skips mid-swap,
# but we gate the WRITE path explicitly per the swap hard-rule). Flip on with SMS_WATCHDOG_HEAL=1.
if [[ "${SMS_WATCHDOG_HEAL:-0}" == "1" ]]; then
  ARGS+=(--heal)
fi

echo "=== sms_logs_watchdog @ $(date -u +%FT%TZ) (heal=${SMS_WATCHDOG_HEAL:-0}) ==="
"$PYTHON" scripts/sendivo_sms_logs_watchdog.py "${ARGS[@]}"
echo "exit=$?"
