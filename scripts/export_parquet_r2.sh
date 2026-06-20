#!/usr/bin/env bash
# Nightly canonical-table Parquet export -> Cloudflare R2 (off-box durability).
#
# STATUS: READY-TO-RUN. Needs R2 credentials in the warehouse .env
# (R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET).
# Once R2 is enabled on the Cloudflare account + those keys are set, add the
# cron line at the bottom of this file.
#
# Why: the existing backup.sh copies the 60GB DuckDB file to /root/archive on the
# SAME droplet — a droplet/disk loss takes both. This pushes a restorable,
# columnar copy of the canonical tables off-box to R2 (~$0.015/GB, no egress).
# This is S0's #1 durability recommendation.
#
# Runs AFTER the orchestrator + backup (read-only on the warehouse), so it never
# contends with the single writer.
#
# 2026-06-09: fixed table selection — canonical tables live in the `core` and
# `derived` schemas (NOT as `core_%`-prefixed tables in `main`). The old filter
# matched only the 14 raw_comms_* tables and skipped the entire canonical
# warehouse. Now exports schema-qualified: core.* + derived.* + main.raw_comms_*.

set -euo pipefail

REPO_DIR="${REPO_DIR:-/root/renaissance-warehouse}"
DB="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
cd "$REPO_DIR"

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

echo "$(date -u +%FT%TZ) exporting $(echo "$TABLES" | grep -c .) tables -> ${PREFIX}"

for qt in $TABLES; do
  # qt = schema.table ; flatten to schema__table.parquet for a flat object key
  fname="$(echo "$qt" | tr '.' '_')"
  duckdb -readonly "$DB" "
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='${ENDPOINT}';
    SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
    SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
    SET s3_url_style='path';
    SET s3_region='auto';
    COPY (SELECT * FROM ${qt}) TO '${PREFIX}/${fname}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
  " && echo "  ok ${qt}" || echo "  WARN failed ${qt} (continuing)"
done

echo "$(date -u +%FT%TZ) parquet->R2 export done for dt=${DAY}"

# To enable, add to crontab (after backup at 05:45 UTC; 06:00 is free, 06:30 taken):
#   0 6 * * * /root/renaissance-warehouse/scripts/export_parquet_r2.sh >> /root/renaissance-warehouse/logs/export_parquet_r2.log 2>&1
#
# Restore test (do once): pick a table, read it back from R2 and compare counts:
#   duckdb -c "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='<acct>.r2.cloudflarestorage.com'; SET s3_access_key_id='...'; SET s3_secret_access_key='...'; SET s3_url_style='path'; SET s3_region='auto';
#              SELECT count(*) FROM read_parquet('s3://<bucket>/warehouse-parquet/dt=<day>/core_reply.parquet');"
