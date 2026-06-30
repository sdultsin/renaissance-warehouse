#!/usr/bin/env bash
# One-shot (detached): wait for the next promote to land June-29 sends in the serving snapshot,
# then render the June-29 tab + Slack-ping. Survives chat session; ~12h cap.
cd /root/renaissance-warehouse
export GOOGLE_TOKEN=/root/.config/mcp-google-sheets/token.json
TOK=$(awk -F'\t' '$2=="cc-service-reader"{print $1}' /opt/duckdb/allowed_tokens.txt)
API=https://renaissance-droplet.tailae5c80.ts.net/query
SQL='{"sql":"SELECT COALESCE(SUM(sent),0) FROM main.raw_pipeline_campaign_daily_metrics WHERE date = current_date - 1"}'
for i in $(seq 1 720); do
  R=$(curl -s -m 20 -X POST "$API" -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d "$SQL" 2>/dev/null || true)
  SENT=$(echo "$R" | grep -oE '\[\[[0-9]+' | grep -oE '[0-9]+' | head -1)
  if [ -n "$SENT" ] && [ "$SENT" -gt 1000 ]; then
    echo "[$(date -u +%FT%TZ)] June-29 sends landed in serving (sent=$SENT) — rendering June 29 tab"
    .venv/bin/python scripts/render_daily.py 2026-06-29 "June 29" 2>&1
    .venv/bin/python scripts/alert_slack.py ":bar_chart: Daily RevOps Report — *June 29* tab rendered post-nightly-promote (sent=$SENT). Pilot proof of the new server-side daily sync." 2>/dev/null || true
    exit 0
  fi
  sleep 60
done
echo "[$(date -u +%FT%TZ)] gave up after ~12h — June-29 sends never landed in serving"
.venv/bin/python scripts/alert_slack.py ":warning: Daily report June-29 re-render watcher gave up after 12h." 2>/dev/null || true
