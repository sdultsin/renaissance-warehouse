#!/usr/bin/env bash
# Mirror Campaign Control's cc_* operational tables from Cloudflare D1 into the
# warehouse (raw_cc_*), for consolidation/BI. Droplet-native: uses the D1 export
# HTTP API (no wrangler on the droplet) -> SQLite -> DuckDB sqlite scanner.
#
# Requires env: CC_D1_API_TOKEN, CLOUDFLARE_RG_ACCOUNT_ID. CC_D1_DATABASE_ID
# defaults to the live campaign-control-state db.
set -euo pipefail

ACCT="${CLOUDFLARE_RG_ACCOUNT_ID:?missing CLOUDFLARE_RG_ACCOUNT_ID}"
TOKEN="${CC_D1_API_TOKEN:?missing CC_D1_API_TOKEN}"
DBID="${CC_D1_DATABASE_ID:-25a32aa3-9d95-42a3-9e9e-8cd3a9e3f3eb}"
WAREHOUSE="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

SQL="$WORK/cc_d1.sql"
SQLITE="$WORK/cc_d1.sqlite"

# 1) Drive the D1 export API (async polling) and download the SQL dump.
python3 - "$ACCT" "$TOKEN" "$DBID" "$SQL" <<'PY'
import json, sys, time, urllib.request, urllib.error
acct, token, dbid, out = sys.argv[1:5]
base = f"https://api.cloudflare.com/client/v4/accounts/{acct}/d1/database/{dbid}/export"
hdr = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def post(body, retries=5):
    # The export/poll endpoint intermittently 400s mid-generation; treat
    # 4xx/5xx as transient and retry with backoff rather than crash the run.
    last = None
    for i in range(retries):
        req = urllib.request.Request(base, data=json.dumps(body).encode(), headers=hdr)
        try:
            return json.load(urllib.request.urlopen(req))
        except urllib.error.HTTPError as e:
            last = e
            time.sleep(2 * (i + 1))
    raise last

# Full-DB export (the table-filter form 400s). The dump includes campaign_data /
# d1_migrations too, but the DuckDB load below only references the cc_* tables.
resp = post({"output_format":"polling"})
signed = None
for _ in range(120):
    if not resp.get("success"):
        print("export error:", resp.get("errors")); sys.exit(1)
    r = resp.get("result", {})
    # When status == "complete", the signed URL is nested under result.result.
    inner = r.get("result") or {}
    signed = inner.get("signed_url") or r.get("signed_url")
    if signed:
        break
    if r.get("status") == "complete":
        print("complete but no signed_url:", json.dumps(r)[:300]); sys.exit(1)
    bm = r.get("at_bookmark")
    time.sleep(2)
    resp = post({"output_format":"polling","current_bookmark":bm})
if not signed:
    print("export did not complete in time"); sys.exit(1)
urllib.request.urlretrieve(signed, out)
print("downloaded D1 export SQL")
PY

# 2) Materialize a real SQLite db from the dump. Disable fsync/journal — this is
#    a throwaway temp db, and per-INSERT fsync on the droplet disk makes the
#    116MB dump take >4min; with synchronous=OFF it's ~11s.
{ echo "PRAGMA synchronous=OFF; PRAGMA journal_mode=MEMORY;"; cat "$SQL"; } | sqlite3 "$SQLITE"

# 3) Load into the warehouse as raw_cc_* (overwrite). Runs after the orchestrator
#    has released the writer lock.
duckdb "$WAREHOUSE" <<SQL2
INSTALL sqlite; LOAD sqlite;
ATTACH '$SQLITE' AS ccsrc (TYPE sqlite);
-- _mirrored_at stamps the mirror-run wall-clock so freshness QA (Track E) tracks
-- "did the mirror run", not the (event-sparse) source created_at.
CREATE OR REPLACE TABLE main.raw_cc_audit_logs          AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_audit_logs;
CREATE OR REPLACE TABLE main.raw_cc_audit_results       AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_audit_results;
CREATE OR REPLACE TABLE main.raw_cc_daily_snapshots     AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_daily_snapshots;
CREATE OR REPLACE TABLE main.raw_cc_dashboard_items     AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_dashboard_items;
CREATE OR REPLACE TABLE main.raw_cc_dashboard_staging   AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_dashboard_staging;
CREATE OR REPLACE TABLE main.raw_cc_investigation_queue AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_investigation_queue;
CREATE OR REPLACE TABLE main.raw_cc_leads_audit_logs    AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_leads_audit_logs;
CREATE OR REPLACE TABLE main.raw_cc_notifications       AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_notifications;
CREATE OR REPLACE TABLE main.raw_cc_resolution_log      AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_resolution_log;
CREATE OR REPLACE TABLE main.raw_cc_run_summaries       AS SELECT *, now() AS _mirrored_at FROM ccsrc.cc_run_summaries;
DETACH ccsrc;
SQL2
echo "cc_* mirror complete"
