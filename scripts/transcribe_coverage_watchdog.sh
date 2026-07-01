#!/usr/bin/env bash
# Cron wrapper for the warm-call transcription coverage watchdog
# (handoff 2026-06-16-call-transcription-backfill).
# Detect+alert by default; set TRANSCRIBE_WATCHDOG_HEAL=1 to also auto-run the (idempotent,
# lock-robust) transcribe job when a gap is found.
#
# Cron (UTC, droplet) — daily at 01:00, after the 23:30 transcribe run has had time to finish
# (both moved out of the nightly writer window 2026-07-01 — nightly can hold the lock 10+ hours):
#   0 1 * * * /root/renaissance-warehouse/scripts/transcribe_coverage_watchdog.sh >> \
#             /root/renaissance-warehouse/logs/transcribe_watchdog.log 2>&1
set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR" || exit 0

[[ -f .venv/bin/activate ]] && source .venv/bin/activate
PYTHON="${PYTHON:-python3}"

ARGS=()
if [[ "${TRANSCRIBE_WATCHDOG_HEAL:-1}" == "1" ]]; then
  # Heal is SAFE here: transcribe_calls.py never holds the writer except in short lock-aware
  # flush bursts and is fully idempotent. Set TRANSCRIBE_WATCHDOG_HEAL=0 for detect-only.
  ARGS+=(--heal)
fi
# Optional: pass --check-only via TRANSCRIBE_WATCHDOG_CHECK_ONLY=1
if [[ "${TRANSCRIBE_WATCHDOG_CHECK_ONLY:-0}" == "1" ]]; then
  ARGS+=(--check-only)
fi

echo "=== transcribe_coverage_watchdog @ $(date -u +%FT%TZ) (heal=${TRANSCRIBE_WATCHDOG_HEAL:-1}) ==="
"$PYTHON" scripts/transcribe_coverage_watchdog.py "${ARGS[@]}"
echo "exit=$?"
