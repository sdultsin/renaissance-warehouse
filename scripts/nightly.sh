#!/usr/bin/env bash
# Nightly sync entry. Cron target.
#
# Cron line (UTC, droplet):
#   30 3 * * * /root/renaissance-warehouse/scripts/nightly.sh >> /root/renaissance-warehouse/logs/nightly.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

mkdir -p logs

LOG_FILE="logs/$(date -u +%Y-%m-%d).log"

echo "=== nightly @ $(date -u +%FT%TZ) ===" | tee -a "$LOG_FILE"

# Activate venv if present
if [[ -f .venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

PYTHON="${PYTHON:-python3}"
ORCHESTRATOR_ARGS="${ORCHESTRATOR_ARGS:-}"

# shellcheck disable=SC2086
"$PYTHON" -m core.orchestrator $ORCHESTRATOR_ARGS 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "publishing lens serving copy" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/publish_serving.sh" 2>&1 | tee -a "$LOG_FILE"
    PUBLISH_CODE=${PIPESTATUS[0]}
    if [[ "$PUBLISH_CODE" -ne 0 ]]; then
        echo "publish_exit=$PUBLISH_CODE" | tee -a "$LOG_FILE"
        exit "$PUBLISH_CODE"
    fi
else
    echo "orchestrator failed; keeping existing lens serving copy" | tee -a "$LOG_FILE"
fi

echo "exit=$EXIT_CODE" | tee -a "$LOG_FILE"
exit $EXIT_CODE
