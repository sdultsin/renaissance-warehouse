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

# Publish dashboards on success (0) OR partial (1). A partial run means every
# phase executed and the warehouse tables were rebuilt; only peripheral ingests
# failed (dead workspaces 401/402, archive-DB lock, etc.) which do NOT feed the
# dashboards. Gating publish on a perfectly-clean exit froze the feeds for days
# (2026-06-04 fix). Only a hard abort (exit 2 / crash) keeps the old copies.
# Each publish step is non-fatal so one failure can't block the others, and the
# nightly's final exit code still reflects the orchestrator status for monitoring.
if [[ "$EXIT_CODE" -eq 0 || "$EXIT_CODE" -eq 1 ]]; then
    if [[ "$EXIT_CODE" -eq 1 ]]; then
        echo "orchestrator partial (some ingests failed); publishing dashboards anyway" | tee -a "$LOG_FILE"
    fi

    echo "compacting warehouse (skips unless bloated)" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/compact_warehouse.sh" 2>&1 | tee -a "$LOG_FILE" || echo "compaction non-fatal failure/skip" | tee -a "$LOG_FILE"

    echo "refreshing campaign-performance dashboard data" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/refresh_campaign_performance.sh" 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN campaign_performance_refresh_failed (continuing)" | tee -a "$LOG_FILE"

    echo "refreshing sms-campaign-performance dashboard data" | tee -a "$LOG_FILE"
    "$PYTHON" -m scripts.sms_campaign_dashboard_data --out /root/lens/sms-campaign-performance/data/latest.json 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN sms_feed_failed (continuing)" | tee -a "$LOG_FILE"

    echo "refreshing overview data.json" | tee -a "$LOG_FILE"
    if "$PYTHON" scripts/dashboard_data.py > /root/lens/overview/data.json.tmp 2>>"$LOG_FILE" && [[ -s /root/lens/overview/data.json.tmp ]]; then
        mv -f /root/lens/overview/data.json.tmp /root/lens/overview/data.json
        echo "overview data.json refreshed" | tee -a "$LOG_FILE"
    else
        rm -f /root/lens/overview/data.json.tmp
        echo "WARN overview_data_refresh_failed (continuing)" | tee -a "$LOG_FILE"
    fi

    echo "publishing lens serving copy" | tee -a "$LOG_FILE"
    "$SCRIPT_DIR/publish_serving.sh" 2>&1 | tee -a "$LOG_FILE" \
        || echo "WARN publish_serving_failed (continuing)" | tee -a "$LOG_FILE"
else
    echo "orchestrator hard-failed (exit=$EXIT_CODE); keeping existing dashboards + serving copy" | tee -a "$LOG_FILE"
fi

echo "exit=$EXIT_CODE" | tee -a "$LOG_FILE"
exit $EXIT_CODE
