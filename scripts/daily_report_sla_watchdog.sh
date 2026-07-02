#!/usr/bin/env bash
# daily_report_sla_watchdog.sh — missed-SLA guard for the Daily RevOps Report 10 PM-ET sync.
#
# OUTCOME-based (per the watchdog SOP: watch the thing that has to LAND, not a proxy):
#   1) the warehouse serving snapshot has TODAY's (ET) campaign sends  (SUM(sent) > SENT_FLOOR), and
#   2) the report sheet has a tab named for today  (e.g. "June 29").
# If either is missing at 10:05 PM ET, the evening sync missed its SLA -> SELF-HEAL (re-run the
# evening sync once), re-check, and only then ALERT #cc-sam if still broken. A healthy run is silent
# (no healthy pings — alert on real failure only).
#
# Cron (UTC):  5 2 * * *  /root/renaissance-warehouse/scripts/daily_report_sla_watchdog.sh >> .../logs/daily_report_sla_watchdog.log 2>&1   # 10:05 PM ET
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"
mkdir -p logs

SENT_FLOOR="${SENT_FLOOR:-1000}"     # a real day sends 100k+; <1000 == the evening pull didn't land
REPORT_DATE="$(TZ=America/New_York date +%F)"
TAB="$(date -d "$REPORT_DATE" +'%b %-d')"   # "Jun 30" — MUST match render_daily.py's %b tab name (was %B "June 30" -> false "no tab '<Month> DD'" SLA MISS)
READER_TOK="$(awk -F'\t' '$2=="cc-service-reader"{print $1}' /opt/duckdb/allowed_tokens.txt)"
WH_API="https://renaissance-droplet.tailae5c80.ts.net/query"
PY=".venv/bin/python"
export GOOGLE_TOKEN="/root/.config/mcp-google-sheets/token.json"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
alert(){ $PY scripts/alert_slack.py "$1" >/dev/null 2>&1 || true; }

# returns today's SUM(sent) from the serving snapshot, or empty on error
sent_today(){
  curl -s -m 25 -X POST "$WH_API" -H "Authorization: Bearer $READER_TOK" -H 'Content-Type: application/json' \
    -d "{\"sql\":\"SELECT COALESCE(SUM(sent),0) FROM main.raw_pipeline_campaign_daily_metrics WHERE date = DATE '$REPORT_DATE'\"}" 2>/dev/null \
    | grep -oE '\[\[[0-9]+' | grep -oE '[0-9]+' | head -1
}

# returns "yes" if the report sheet has a tab named "$TAB"
tab_exists(){
  $PY - "$TAB" <<'PY' 2>/dev/null
import sys, json, urllib.request, os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
tab=sys.argv[1]; TOK=os.environ["GOOGLE_TOKEN"]
SID="1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
c=Credentials.from_authorized_user_file(TOK); c.refresh(Request())
req=urllib.request.Request(f"https://sheets.googleapis.com/v4/spreadsheets/{SID}?fields=sheets(properties(title))",
    headers={"Authorization":f"Bearer {c.token}"})
m=json.load(urllib.request.urlopen(req,timeout=30))
print("yes" if any(s["properties"]["title"]==tab for s in m["sheets"]) else "no")
PY
}

check_ok(){  # echoes "OK" or a reason
  local s t; s="$(sent_today)"; t="$(tab_exists)"
  if [[ -z "$s" ]]; then echo "read-API unreachable (cannot confirm sent for $REPORT_DATE)"; return; fi
  if [[ "$s" -lt "$SENT_FLOOR" ]]; then echo "warehouse missing today's sends (SUM(sent)=$s < $SENT_FLOOR for $REPORT_DATE)"; return; fi
  if [[ "$t" != "yes" ]]; then echo "report sheet has no tab '$TAB'"; return; fi
  echo "OK"
}

REASON="$(check_ok)"
if [[ "$REASON" == "OK" ]]; then
  log "SLA OK ($REPORT_DATE): sends landed + tab '$TAB' present. (silent)"
  exit 0
fi

log "SLA MISS ($REPORT_DATE): $REASON — self-healing: re-running evening sync once ..."
bash scripts/daily_report_sync.sh evening "$REPORT_DATE" >> logs/daily_report_sync.log 2>&1 || true

REASON2="$(check_ok)"
if [[ "$REASON2" == "OK" ]]; then
  log "SELF-HEAL OK ($REPORT_DATE): recovered after re-run."
  alert ":white_check_mark: Daily RevOps Report ($TAB) — missed the 10 PM-ET SLA but the watchdog self-healed (re-ran the evening sync). Tab is now current. (Was: $REASON)"
  exit 0
fi

log "SLA MISS PERSISTS ($REPORT_DATE): $REASON2"
alert ":rotating_light: *Daily RevOps Report MISSED 10 PM-ET SLA* ($TAB) and self-heal did NOT recover. Reason: $REASON2. Needs a human — check the box: /root/renaissance-warehouse/logs/daily_report_sync.log"
exit 1
