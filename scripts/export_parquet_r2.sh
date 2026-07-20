#!/usr/bin/env bash
# Nightly canonical-table Parquet export -> Cloudflare R2 (off-box durability).
#
# STATUS: LIVE — R2 creds are in the warehouse .env and the 06:00 UTC cron line is installed.
#
# Why: the existing backup.sh copies the 60GB DuckDB file to /root/archive on the
# SAME droplet — a droplet/disk loss takes both. This pushes a restorable,
# columnar copy of the canonical tables off-box to R2 (~$0.015/GB, no egress).
# This is S0's #1 durability recommendation.
#
# 2026-06-09: fixed table selection — canonical tables live in the `core` and
# `derived` schemas (NOT as `core_%`-prefixed tables in `main`). The old filter
# matched only the 14 raw_comms_* tables and skipped the entire canonical
# warehouse. Now exports schema-qualified: core.* + derived.* + main.raw_comms_*.
#
# 2026-07-09: read from the SERVED SNAPSHOT pointer, never the live writer file — even
# `duckdb -readonly` takes a shared file lock, so every run since ~06-17 died at 06:00 with
# "Conflicting lock is held" against the single writer (25 logged conflicts). The served
# snapshot is immutable + validated, and read-only opens of it compose with the serving API's
# own read-only handle. Resolve the pointer ONCE so all tables export from one consistent
# snapshot even if a promote retargets it mid-run. Real failures now alert to Slack.

set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/root/renaissance-warehouse}"
# Served-snapshot pointer (see 2026-07-09 note above) — resolved once, read-only, no writer contention.
DB_PTR="${R2_EXPORT_DB:-/opt/duckdb/warehouse_current.duckdb}"
DB="$(readlink -f -- "$DB_PTR" 2>/dev/null || true)"
cd "$REPO_DIR"

# Fail-loud: real failures post to Slack (#cc-sam) via the fleet's scripts/alert_slack.py. Success is silent.
alert() {
  local py="$REPO_DIR/.venv/bin/python"; [[ -x "$py" ]] || py="python3"
  "$py" "$REPO_DIR/scripts/alert_slack.py" \
    ":rotating_light: R2 parquet export: $1 — $(hostname) $(date -u +%FT%TZ), see logs/export_parquet_r2.log" \
    >/dev/null 2>&1 || true
}
on_err() { local rc=$?; echo "$(date -u +%FT%TZ) ERROR: export aborted (rc=$rc)"; alert "ABORTED unexpectedly (rc=$rc)"; }
trap on_err ERR

if [[ -z "$DB" || ! -f "$DB" ]]; then
  echo "$(date -u +%FT%TZ) ERROR: served snapshot not found: $DB_PTR"
  alert "served snapshot not found: $DB_PTR"
  exit 1
fi

# Load R2 creds from .env (best-effort; .env is not always shell-sourceable).
R2_ACCOUNT_ID="$(grep -E '^R2_ACCOUNT_ID=' .env 2>/dev/null | cut -d= -f2- | tr -d '"' || true)"
R2_ACCESS_KEY_ID="$(grep -E '^R2_ACCESS_KEY_ID=' .env 2>/dev/null | cut -d= -f2- | tr -d '"' || true)"
R2_SECRET_ACCESS_KEY="$(grep -E '^R2_SECRET_ACCESS_KEY=' .env 2>/dev/null | cut -d= -f2- | tr -d '"' || true)"
R2_BUCKET="${R2_BUCKET:-$(grep -E '^R2_BUCKET=' .env 2>/dev/null | cut -d= -f2- | tr -d '"' || true)}"

if [[ -z "$R2_ACCOUNT_ID" || -z "$R2_ACCESS_KEY_ID" || -z "$R2_SECRET_ACCESS_KEY" || -z "$R2_BUCKET" ]]; then
  echo "$(date -u +%FT%TZ) SKIP: R2 creds not set (R2_ACCOUNT_ID/ACCESS_KEY_ID/SECRET_ACCESS_KEY/BUCKET). This script is ready but dormant."
  exit 0
fi

DAY="$(date -u +%Y-%m-%d)"
PREFIX="s3://${R2_BUCKET}/warehouse-parquet/dt=${DAY}"
ENDPOINT="${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ── MEMORY DISCIPLINE [2026-07-20] ────────────────────────────────────────────────────────────
# The per-table `COPY (SELECT * FROM t) TO parquet (ZSTD)` below ran `duckdb -readonly` with NO
# memory_limit and NO threads cap = the DuckDB CLI default (~80% of RAM, one row-group + ZSTD
# compression buffer PER core). On 2026-07-19 06:02Z that OOM-killed at 23GB RSS exporting a big
# table (PID 1256415, this script line ~86) — the largest single-process overshoot on the box and,
# alone, more than a 15GB box can hold. Bound it: a small pool + few writer threads spill to disk
# and keep worst-case export RSS ~5-6GB. OUTPUT-PRESERVING — same rows/schema per parquet, just
# fewer threads and a bounded pool (the SELECT * projection is unchanged). Env-overridable.
R2_EXPORT_MEM="${R2_EXPORT_MEM:-5GB}"
R2_EXPORT_THREADS="${R2_EXPORT_THREADS:-2}"
R2_EXPORT_TMP="${R2_EXPORT_TMP:-/mnt/volume_nyc1_1781398428838/duckdb_tmp}"
mkdir -p "$R2_EXPORT_TMP" 2>/dev/null || true
DUCKDB_PRELUDE="SET memory_limit='${R2_EXPORT_MEM}'; SET threads=${R2_EXPORT_THREADS}; SET temp_directory='${R2_EXPORT_TMP}'; SET preserve_insertion_order=false;"

# Tables to export, schema-qualified as <schema>.<table>:
#   - core.*        canonical analytics tables (the durability priority)
#   - derived.*     derived marts
#   - main.raw_comms_*  SMS-AIM raw mirror (incl. raw_comms_sendivo_outbound)
TABLES="$(duckdb -readonly -noheader -csv "$DB" "
  SELECT schema_name || '.' || table_name
  FROM duckdb_tables()
  WHERE schema_name IN ('core','derived')
     OR (schema_name='main' AND table_name LIKE 'raw_comms_%')
  ORDER BY 1;")"

echo "$(date -u +%FT%TZ) exporting $(echo "$TABLES" | grep -c .) tables from $DB -> ${PREFIX}"

FAILED=0
for qt in $TABLES; do
  # qt = schema.table ; flatten to schema__table.parquet for a flat object key
  fname="$(echo "$qt" | tr '.' '_')"
  duckdb -readonly "$DB" "
    ${DUCKDB_PRELUDE}
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='${ENDPOINT}';
    SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
    SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
    SET s3_url_style='path';
    SET s3_region='auto';
    COPY (SELECT * FROM ${qt}) TO '${PREFIX}/${fname}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
  " && echo "  ok ${qt}" || { echo "  WARN failed ${qt} (continuing)"; FAILED=$((FAILED+1)); }
done

echo "$(date -u +%FT%TZ) parquet->R2 export done for dt=${DAY} (${FAILED} failed)"
if (( FAILED > 0 )); then
  alert "dt=${DAY}: ${FAILED} table(s) FAILED to export"
fi

# Cron (installed): 0 6 * * * /root/renaissance-warehouse/scripts/export_parquet_r2.sh >> /root/renaissance-warehouse/logs/export_parquet_r2.log 2>&1
#
# Restore test (do once): pick a table, read it back from R2 and compare counts:
#   duckdb -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='<acct>.r2.cloudflarestorage.com'; SET s3_access_key_id='...'; SET s3_secret_access_key='...'; SET s3_url_style='path'; SET s3_region='auto';
#              SELECT count(*) FROM read_parquet('s3://<bucket>/warehouse-parquet/dt=<day>/core_reply.parquet');"
