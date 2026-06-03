#!/usr/bin/env bash
# Regenerate the static Campaign Performance JSON served by Lens.

set -euo pipefail

PYTHON="${LENS_PYTHON:-/root/lens/backend/.venv/bin/python}"
GENERATOR="${CAMPAIGN_PERFORMANCE_GENERATOR:-/root/lens/scripts/daily_performance_warehouse.py}"
DB_PATH="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
JSON_OUT="${CAMPAIGN_PERFORMANCE_JSON_OUT:-/root/lens/campaign-performance/data/latest.json}"
DAYS="${CAMPAIGN_PERFORMANCE_DAYS:-35}"

if [[ ! -x "$PYTHON" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: Lens python not executable: $PYTHON" >&2
    exit 1
fi
if [[ ! -f "$GENERATOR" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: generator not found: $GENERATOR" >&2
    exit 1
fi

"$PYTHON" "$GENERATOR" --days "$DAYS" --db "$DB_PATH" --json-out "$JSON_OUT"
echo "$(date -u +%FT%TZ) refreshed campaign-performance JSON -> $JSON_OUT"
