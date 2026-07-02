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
# The warehouse lives on the 2TB data volume (1.2TB free), reached via the /root/core symlink;
# /root itself (vda1) has only ~50GB free. The export/import staging + the new compacted DB MUST
# go on the SAME filesystem as the real file, or the disk guard aborts (needs ~0.7*SIZE free) and
# a naive `mv` of the symlink would even RELOCATE the DB onto vda1. Resolve the symlink and stage
# everything next to the REAL file on the volume; preserve the symlink across the swap. If DB is a
# plain file (no symlink), readlink -f returns it unchanged and behaviour is identical to before.
REAL_DB="$(readlink -f "$DB")"
VOL_DIR="$(dirname "$REAL_DB")"
NEW="$VOL_DIR/warehouse_compact_new.duckdb"
EXP="$VOL_DIR/compact_export_tmp"
# THE writer lock — the SAME file every Python writer flocks via core/db.py
# (_WRITE_LOCK_PATH). The old private marker file (/root/core/.warehouse_write.lock)
# was invisible to real writers in BOTH directions: compaction could export+swap while
# a writer held the real flock (a writer opening the old inode mid-swap would commit
# into the pre-swap backup = silently lost data), and writers happily wrote during a
# compaction. Fixed 2026-07-01: hold the REAL flock for the whole export->swap window
# so writers queue behind compaction (core/db.py waits up to WAREHOUSE_WRITE_LOCK_WAIT_S,
# default 1800s) and compaction waits briefly for a straggler writer instead of aborting.
LOCK="${WAREHOUSE_WRITE_LOCK_PATH:-/root/core/warehouse.write.lock}"
LOCK_WAIT_S="${COMPACT_LOCK_WAIT_S:-900}"
# Only compact when the primary exceeds this. Calibrated 2026-07-01: the first successful
# compaction landed at 75GB — that is the REAL data size, not bloat (the old 18GB threshold
# predates the email-thread/message tables and would re-trigger a pointless ~47-min full
# compaction EVERY night on a freshly-compacted file). 120GB = real size + ~45GB of genuine
# bloat headroom, so the nightly compacts only when there is real space to reclaim.
# Override for a one-off: COMPACT_THRESHOLD_GB=1 scripts/compact_warehouse.sh
THRESHOLD_BYTES=$(( ${COMPACT_THRESHOLD_GB:-120}*1024*1024*1024 ))
DUCKDB=$(command -v duckdb)
# Bound DuckDB memory so the EXPORT/IMPORT cannot OOM the 16GB box (root cause of the 2026-06-12
# nightly oom-kill: the IMPORT hit ~12.9GB RSS at the default ~80%-RAM limit). Spill to disk instead.
MEMLIMIT="${COMPACT_MEMORY_LIMIT:-8GB}"
TMPDIR="$VOL_DIR/duckdb_tmp"
mkdir -p "$TMPDIR"
# preserve_insertion_order=false is REQUIRED for EXPORT/IMPORT of a large DB under a memory_limit:
# order-preserving EXPORT buffers rows and cannot fully spill -> OOMs even at 8GB (observed
# 2026-06-14). Row ORDER within a table is not semantically meaningful here (queries use ORDER BY)
# and compaction reorders anyway; exact COUNT(*) verification below proves data identity.
SET_PRELUDE="SET preserve_insertion_order=false; SET memory_limit='$MEMLIMIT'; SET temp_directory='$TMPDIR';"
# Disk guard factor (free >= SIZE * NUM/10). Default 7 (conservative). Override for a one-time
# compaction on a tight-but-sufficient disk: COMPACT_FREE_FACTOR_NUM=4 (the real export+import
# peak is ~30GB for a high-bloat DB, well under the default 0.7*SIZE).
FREE_FACTOR_NUM="${COMPACT_FREE_FACTOR_NUM:-7}"
LOG(){ echo "[compact $(date -u +%FT%TZ)] $*"; }

# stat the REAL file: `stat -c%s` on the /root/core symlink returns the LINK size (~52B), not the
# target — so the old `stat "$DB"` read 52 and silently skipped compaction EVERY night since the DB
# was symlinked to the volume (2026-06-16). That is the true root cause the warehouse ballooned to
# 165GB unchecked. REAL_DB is the dereferenced path, so SIZE is the actual file size.
SIZE=$(stat -c%s "$REAL_DB" 2>/dev/null || echo 0)
if [ "$SIZE" -lt "$THRESHOLD_BYTES" ]; then
  LOG "primary $((SIZE/1024/1024/1024))GB < threshold $((THRESHOLD_BYTES/1024/1024/1024))GB — skip (no-op)"; exit 0
fi

# Acquire the real writer flock (fd 9, held until this process exits). flock -w waits
# out a straggler writer (e.g. a tag backfill's final upsert) instead of hard-aborting.
# The pid marker matches core/db.py's format so its stale-marker clearing recognizes us.
# WAREHOUSE_WRITE_LOCK_HELD=1 = an ancestor (flock'd wrapper) already owns the flock —
# acquiring on a NEW fd would deadlock against ourselves (same convention as core/db.py).
if [ "${WAREHOUSE_WRITE_LOCK_HELD:-0}" != "1" ]; then
  exec 9>>"$LOCK"
  if ! flock -w "$LOCK_WAIT_S" 9; then
    LOG "ABORT: warehouse writer flock still held after ${LOCK_WAIT_S}s: $(cat "$LOCK" 2>/dev/null)"; exit 1
  fi
  printf 'pid=%s acquired_by=compact_warehouse.sh\n' "$$" > "$LOCK"
  # blank the marker on exit (fd 9 is still open during the trap, so the flock is still
  # ours — no other writer can have written its marker yet). The flock itself releases
  # when the process exits and fd 9 closes.
  trap ': > "$LOCK"' EXIT
fi

FREE=$(df --output=avail -B1 "$VOL_DIR" | tail -1)   # free space on the VOLUME where staging lives
if [ "$FREE" -lt $((SIZE*FREE_FACTOR_NUM/10)) ]; then
  LOG "ABORT: insufficient disk (free=$((FREE/1024/1024/1024))GB, need ~$((SIZE*FREE_FACTOR_NUM/10/1024/1024/1024))GB, factor=${FREE_FACTOR_NUM}/10)"; exit 1
fi

# $NEW.wal too: a hard-killed prior import (OOM-kill precedent 2026-06-12) leaves a stale
# WAL next to $NEW; a fresh import DB created beside it would replay garbage.
rm -rf "$NEW" "$NEW.wal" "$EXP"

# Fold any pending WAL into the primary while we hold the writer flock (quiesced), so the
# export sees a fully-checkpointed file and the swap never leaves a stale warehouse.duckdb.wal
# behind for the FRESH file to replay (a WAL from the old file replayed into the compacted one
# would corrupt it). Best-effort: a -readonly export replays WAL in-memory anyway; the swap
# below also defensively moves any residual .wal aside with the backup. Run under the same
# memory prelude — a huge WAL replay at the CLI's default ~80%-RAM limit is the 06-12 OOM class.
"$DUCKDB" "$DB" "$SET_PRELUDE CHECKPOINT" >/dev/null 2>&1 || LOG "WARN: pre-export CHECKPOINT failed (continuing; residual WAL is moved aside at swap)"

# exact-count manifest query over every base table (dynamic UNION ALL). The manifest and BOTH
# count captures are load-bearing for swap safety: an empty CQ or an empty pre/post CSV would
# make the diff below pass VACUOUSLY (two empty files) and swap in an unverified DB — abort.
CQ=$("$DUCKDB" -readonly "$DB" -noheader -list \
  "SELECT string_agg('SELECT '''||schema_name||'.'||table_name||''' t, count(*) n FROM '||schema_name||'.'||table_name, ' UNION ALL ') FROM duckdb_tables()")
[ -n "$CQ" ] || { LOG "ABORT: count-manifest query returned empty (cannot verify) — keeping original"; exit 7; }
"$DUCKDB" -readonly "$DB" -csv "$CQ ORDER BY t" > /tmp/compact_pre.csv 2>/dev/null
[ -s /tmp/compact_pre.csv ] || { LOG "ABORT: pre-count capture empty (cannot verify) — keeping original"; exit 7; }
VIEWS_OLD=$("$DUCKDB" -readonly "$DB" -noheader -list "SELECT count(*) FROM duckdb_views() WHERE NOT internal")

LOG "exporting primary ($((SIZE/1024/1024/1024))GB, memory_limit=$MEMLIMIT)..."
"$DUCKDB" -readonly "$DB" "$SET_PRELUDE EXPORT DATABASE '$EXP' (FORMAT PARQUET)" || { LOG "ABORT: export failed"; rm -rf "$EXP"; exit 1; }
LOG "importing into fresh DB (memory_limit=$MEMLIMIT)..."
# NOT `IMPORT DATABASE`: that replays schema.sql top-to-bottom and dies on the first
# view-on-view forward reference (EXPORT does not topologically sort views — this aborted
# the 2026-07-01 compaction with "Table with name v_inbox_overview does not exist").
# compact_import.py replays schema.sql MULTI-PASS (deferred statements retry once their
# dependencies exist) then load.sql, and fails LOUD listing any unresolvable statement.
SCRIPT_DIR_SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR_SELF/compact_import.py" "$EXP" "$NEW" "$MEMLIMIT" "$TMPDIR" \
  || { LOG "ABORT: import failed"; rm -rf "$EXP" "$NEW" "$NEW.wal"; exit 1; }

"$DUCKDB" -readonly "$NEW" -csv "$CQ ORDER BY t" > /tmp/compact_post.csv 2>/dev/null
[ -s /tmp/compact_post.csv ] || { LOG "ABORT: post-count capture empty (cannot verify) — keeping original"; rm -rf "$EXP" "$NEW" "$NEW.wal"; exit 7; }
VIEWS_NEW=$("$DUCKDB" -readonly "$NEW" -noheader -list "SELECT count(*) FROM duckdb_views() WHERE NOT internal")

# VERIFY — any mismatch aborts (original untouched)
if ! diff -q /tmp/compact_pre.csv /tmp/compact_post.csv >/dev/null; then
  LOG "ABORT: row-count mismatch (DB changed during compaction); keeping original. Diff:"
  diff /tmp/compact_pre.csv /tmp/compact_post.csv | head -20
  rm -rf "$EXP" "$NEW" "$NEW.wal"; exit 2
fi
if [ "$VIEWS_OLD" != "$VIEWS_NEW" ]; then
  LOG "ABORT: view count mismatch (old=$VIEWS_OLD new=$VIEWS_NEW); keeping original"
  rm -rf "$EXP" "$NEW" "$NEW.wal"; exit 3
fi
LOG "verified: $(wc -l < /tmp/compact_pre.csv) tables exact-match, $VIEWS_NEW views preserved"

# atomic swap with rollback — operate on the REAL file (on the volume), NOT the /root/core symlink,
# so the warehouse stays on the volume and /root/core/warehouse.duckdb keeps pointing at it.
BK="$VOL_DIR/warehouse.precompact-$(date -u +%Y%m%d-%H%M%S).duckdb"
# Check each mv explicitly (no `set -e`): a failed backup-mv must ABORT, not fall through to a
# false "SWAP OK" on the still-original DB. mv2 failure rolls the backup back into place.
mv "$REAL_DB" "$BK" || { LOG "ABORT: backup mv failed — original untouched"; rm -rf "$EXP" "$NEW" "$NEW.wal"; exit 5; }
# a residual WAL belongs to the OLD file: move it WITH the backup so the fresh DB can
# never replay it (we hold the writer flock, so no live writer owns it).
[ -f "$REAL_DB.wal" ] && mv "$REAL_DB.wal" "$BK.wal"
mv "$NEW" "$REAL_DB" || { LOG "ABORT: swap-in mv failed — restoring backup"; mv "$BK" "$REAL_DB"; [ -f "$BK.wal" ] && mv "$BK.wal" "$REAL_DB.wal"; rm -rf "$EXP"; exit 6; }
if ! "$DUCKDB" -readonly "$DB" "SELECT 1" >/dev/null 2>&1; then
  # restore the WAL with the backup: if the pre-export CHECKPOINT failed, committed
  # transactions live ONLY in that WAL — dropping it here would silently lose them.
  LOG "ABORT: swapped DB unreadable — ROLLING BACK"
  rm -f "$REAL_DB"; mv "$BK" "$REAL_DB"; [ -f "$BK.wal" ] && mv "$BK.wal" "$REAL_DB.wal"
  rm -rf "$EXP"; exit 4
fi
NEWSIZE=$(stat -c%s "$REAL_DB")
LOG "SWAP OK: $((SIZE/1024/1024/1024))GB -> $((NEWSIZE/1024/1024/1024))GB freed $(((SIZE-NEWSIZE)/1024/1024/1024))GB. pre-swap backup: $BK"
rm -rf "$EXP"
# keep the dated pre-swap backup one cycle; prune older ones (keep newest 1, + its .wal)
ls -1t "$VOL_DIR"/warehouse.precompact-*.duckdb 2>/dev/null | tail -n +2 | xargs -r rm -f
ls -1t "$VOL_DIR"/warehouse.precompact-*.duckdb.wal 2>/dev/null | tail -n +2 | xargs -r rm -f
exit 0
