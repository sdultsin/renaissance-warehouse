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
# Post-copy free-space floor. Sized down 40->20GB on 2026-06-08 (F2): the warehouse
# grew to ~44GB so a fixed 40GB floor blocked every publish (serving frozen since
# 06-03). 20GB is ample headroom — the DB is quiescent during publish and grows only
# a few GB between nightly runs; compaction (which runs first) keeps it ~36GB.
MIN_FREE_GB_AFTER_COPY="${MIN_FREE_GB_AFTER_COPY:-20}"
ENV_FILE="${ENV_FILE:-/root/renaissance-warehouse/.env}"
SLACK_CHANNEL="${SLACK_CHANNEL:-}"  # alert channel ID; set via env

# Fail-loud: post a #cc-sam alert if the serving publish errors out at all (F2 DoD).
alert() {
    local msg="$1" token cookie
    token="$(grep '^SLACK_TOKEN=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '"')"
    cookie="$(grep '^SLACK_COOKIE=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '"')"
    [[ -z "$token" || -z "$SLACK_CHANNEL" ]] && return 0
    curl -s -X POST https://slack.com/api/chat.postMessage \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json; charset=utf-8" \
        ${cookie:+-H "Cookie: d=$cookie"} \
        -d "{\"channel\":\"$SLACK_CHANNEL\",\"text\":\"$msg\"}" >/dev/null 2>&1 || true
}
trap 'rc=$?; if [[ $rc -ne 0 ]]; then alert ":rotating_light: publish_serving FAILED (exit $rc) — serving copy NOT refreshed (stale)."; fi' EXIT

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
