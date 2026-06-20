#!/usr/bin/env bash
# Read-only DuckDB query wrapper. Sends SQL to the droplet warehouse.
#
# Usage:
#   ./scripts/query.sh "SELECT count(*) FROM core.campaign"
#   ./scripts/query.sh -f path/to/query.sql
#   ./scripts/query.sh                                  # interactive REPL
#
# Read-only. Won't conflict with the nightly sync (no lock taken).

set -euo pipefail

HOST="${WAREHOUSE_HOST:-renaissance-worker}"
DB="${CORE_DB_PATH:-/root/core/warehouse.duckdb}"

if [[ $# -eq 0 ]]; then
    # Interactive REPL on the droplet
    exec ssh -t "$HOST" "duckdb -readonly '$DB'"
fi

if [[ "$1" == "-f" ]]; then
    if [[ $# -lt 2 || ! -f "$2" ]]; then
        echo "usage: $0 -f <path-to-sql-file>" >&2
        exit 2
    fi
    ssh "$HOST" "duckdb -readonly '$DB'" < "$2"
else
    # Inline SQL; pass through as a single arg, escape internal single-quotes by replacing with ''
    SQL="${*//\'/\'\'}"
    ssh "$HOST" "duckdb -readonly '$DB' \"$SQL\""
fi
