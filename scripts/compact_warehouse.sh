#!/usr/bin/env bash
# Compact the warehouse DuckDB to reclaim bloat (deleted/superseded row-group space).
#
# Runs from nightly.sh AFTER the orchestrator finishes — the one window the warehouse is reliably
# quiescent. SAFE by construction: builds a fresh compacted copy in a NEW file, verifies EXACT row
# counts on every table (COUNT(*), not estimated_size — that counts physical bloat) + view presence,
# and only atomically swaps if verification passes. The original is renamed to a dated pre-swap
# backup (never deleted here); a post-swap sanity check rolls back on any failure.
#
# Skips unless the DB exceeds the bloat threshold, so most nights are a fast no-op.
set -uo pipefail

DB=/root/core/warehouse.duckdb
NEW=/root/core/warehouse_compact_new.duckdb
EXP=/root/core/compact_export_tmp
LOCK=/root/core/.warehouse_write.lock
THRESHOLD_BYTES=$((18*1024*1024*1024))   # only compact when primary > 18GB
DUCKDB=$(command -v duckdb)
# Bound DuckDB memory so the EXPORT/IMPORT cannot OOM the 16GB box (root cause of the 2026-06-12
# nightly oom-kill: the IMPORT hit ~12.9GB RSS at the default ~80%-RAM limit). Spill to disk instead.
MEMLIMIT="${COMPACT_MEMORY_LIMIT:-8GB}"
TMPDIR=/root/core/duckdb_tmp
mkdir -p "$TMPDIR"
SET_PRELUDE="SET memory_limit='$MEMLIMIT'; SET temp_directory='$TMPDIR';"
# Disk guard factor (free >= SIZE * NUM/10). Default 7 (conservative). Override for a one-time
# compaction on a tight-but-sufficient disk: COMPACT_FREE_FACTOR_NUM=4 (the real export+import
# peak is ~30GB for a high-bloat DB, well under the default 0.7*SIZE).
FREE_FACTOR_NUM="${COMPACT_FREE_FACTOR_NUM:-7}"
LOG(){ echo "[compact $(date -u +%FT%TZ)] $*"; }

SIZE=$(stat -c%s "$DB" 2>/dev/null || echo 0)
if [ "$SIZE" -lt "$THRESHOLD_BYTES" ]; then
  LOG "primary $((SIZE/1024/1024/1024))GB < threshold 18GB — skip (no-op)"; exit 0
fi

if [ -f "$LOCK" ]; then LOG "ABORT: warehouse write-lock held: $(cat "$LOCK")"; exit 1; fi
echo "compact $(date -u +%FT%TZ)" > "$LOCK"
trap 'rm -f "$LOCK"' EXIT

FREE=$(df --output=avail -B1 /root | tail -1)
if [ "$FREE" -lt $((SIZE*FREE_FACTOR_NUM/10)) ]; then
  LOG "ABORT: insufficient disk (free=$((FREE/1024/1024/1024))GB, need ~$((SIZE*FREE_FACTOR_NUM/10/1024/1024/1024))GB, factor=${FREE_FACTOR_NUM}/10)"; exit 1
fi

rm -rf "$NEW" "$EXP"

# exact-count manifest query over every base table (dynamic UNION ALL)
CQ=$("$DUCKDB" -readonly "$DB" -noheader -list \
  "SELECT string_agg('SELECT '''||schema_name||'.'||table_name||''' t, count(*) n FROM '||schema_name||'.'||table_name, ' UNION ALL ') FROM duckdb_tables()")
"$DUCKDB" -readonly "$DB" -csv "$CQ ORDER BY t" > /tmp/compact_pre.csv 2>/dev/null
VIEWS_OLD=$("$DUCKDB" -readonly "$DB" -noheader -list "SELECT count(*) FROM duckdb_views() WHERE NOT internal")

LOG "exporting primary ($((SIZE/1024/1024/1024))GB, memory_limit=$MEMLIMIT)..."
"$DUCKDB" -readonly "$DB" "$SET_PRELUDE EXPORT DATABASE '$EXP' (FORMAT PARQUET)" || { LOG "ABORT: export failed"; rm -rf "$EXP"; exit 1; }
LOG "importing into fresh DB (memory_limit=$MEMLIMIT)..."
"$DUCKDB" "$NEW" "$SET_PRELUDE IMPORT DATABASE '$EXP'" || { LOG "ABORT: import failed"; rm -rf "$EXP" "$NEW"; exit 1; }

"$DUCKDB" -readonly "$NEW" -csv "$CQ ORDER BY t" > /tmp/compact_post.csv 2>/dev/null
VIEWS_NEW=$("$DUCKDB" -readonly "$NEW" -noheader -list "SELECT count(*) FROM duckdb_views() WHERE NOT internal")

# VERIFY — any mismatch aborts (original untouched)
if ! diff -q /tmp/compact_pre.csv /tmp/compact_post.csv >/dev/null; then
  LOG "ABORT: row-count mismatch (DB changed during compaction); keeping original. Diff:"
  diff /tmp/compact_pre.csv /tmp/compact_post.csv | head -20
  rm -rf "$EXP" "$NEW"; exit 2
fi
if [ "$VIEWS_OLD" != "$VIEWS_NEW" ]; then
  LOG "ABORT: view count mismatch (old=$VIEWS_OLD new=$VIEWS_NEW); keeping original"
  rm -rf "$EXP" "$NEW"; exit 3
fi
LOG "verified: $(wc -l < /tmp/compact_pre.csv) tables exact-match, $VIEWS_NEW views preserved"

# atomic swap with rollback
BK="/root/core/warehouse.precompact-$(date -u +%Y%m%d-%H%M%S).duckdb"
mv "$DB" "$BK" && mv "$NEW" "$DB"
if ! "$DUCKDB" -readonly "$DB" "SELECT 1" >/dev/null 2>&1; then
  LOG "ABORT: swapped DB unreadable — ROLLING BACK"; rm -f "$DB"; mv "$BK" "$DB"; rm -rf "$EXP"; exit 4
fi
NEWSIZE=$(stat -c%s "$DB")
LOG "SWAP OK: $((SIZE/1024/1024/1024))GB -> $((NEWSIZE/1024/1024/1024))GB freed $(((SIZE-NEWSIZE)/1024/1024/1024))GB. pre-swap backup: $BK"
rm -rf "$EXP"
# keep the dated pre-swap backup one cycle; prune older ones (keep newest 1)
ls -1t /root/core/warehouse.precompact-*.duckdb 2>/dev/null | tail -n +2 | xargs -r rm -f
exit 0
