#!/usr/bin/env bash
# Publish a read-only serving copy of the primary warehouse DB for Lens.
#
# Lens keeps a persistent DuckDB handle. It should read warehouse_serving.duckdb
# while the orchestrator writes warehouse.duckdb. This script atomically swaps in
# a fresh serving copy after a successful write, then restarts Lens so it opens
# the new file.

set -euo pipefail

PRIMARY="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"
SERVING="${LENS_WAREHOUSE_PATH:-/root/core/warehouse_serving.duckdb}"
TMP="${SERVING}.tmp"
MIN_FREE_GB_AFTER_COPY="${MIN_FREE_GB_AFTER_COPY:-40}"

if [[ ! -f "$PRIMARY" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: primary DB not found: $PRIMARY" >&2
    exit 1
fi

SERVING_DIR="$(dirname "$SERVING")"
mkdir -p "$SERVING_DIR"

PRIMARY_BYTES=$(stat -c '%s' "$PRIMARY")
AVAILABLE_BYTES=$(df -PB1 "$SERVING_DIR" | awk 'NR==2 {print $4}')
MIN_FREE_BYTES=$((MIN_FREE_GB_AFTER_COPY * 1024 * 1024 * 1024))

if (( AVAILABLE_BYTES - PRIMARY_BYTES < MIN_FREE_BYTES )); then
    echo "$(date -u +%FT%TZ) ERROR: insufficient disk for serving copy: available=${AVAILABLE_BYTES} primary=${PRIMARY_BYTES} required_post_copy_free=${MIN_FREE_BYTES}" >&2
    exit 2
fi

rm -f "$TMP"
cp -f "$PRIMARY" "$TMP"

TMP_BYTES=$(stat -c '%s' "$TMP")
if [[ "$TMP_BYTES" != "$PRIMARY_BYTES" ]]; then
    echo "$(date -u +%FT%TZ) ERROR: serving copy size mismatch (primary=$PRIMARY_BYTES tmp=$TMP_BYTES)" >&2
    rm -f "$TMP"
    exit 3
fi

mv -f "$TMP" "$SERVING"
systemctl restart lens
echo "$(date -u +%FT%TZ) published $PRIMARY -> $SERVING ($PRIMARY_BYTES bytes)"
