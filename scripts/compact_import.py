#!/usr/bin/env python3
"""compact_import.py — dependency-tolerant IMPORT step for compact_warehouse.sh.

WHY THIS EXISTS (2026-07-01): DuckDB's one-shot `IMPORT DATABASE '<dir>'` executes the
exported schema.sql TOP-TO-BOTTOM and aborts on the FIRST error. EXPORT DATABASE does
NOT topologically sort view-on-view dependencies, so a view that reads another view
(e.g. something SELECTing from core.v_inbox_overview emitted before v_inbox_overview
itself) kills the whole import:

    Catalog Error: Table with name v_inbox_overview does not exist!

That aborted the 2026-07-01 nightly compaction and left the writer DB at 171 GB.

This driver replays the export in THREE phases instead of IMPORT DATABASE's two:

1. SCHEMA (multi-pass, indexes deferred): statements failing on catalog/binder errors
   (missing dependency) retry on the next pass, when their dependencies exist.
   Fixpoint with failures remaining -> exit non-zero listing every failing statement,
   so compact_warehouse.sh ABORTS and keeps the original DB. CREATE [UNIQUE] INDEX
   statements are NOT run here — see phase 3.
2. DATA (load.sql COPYs, order-independent, threads-bounded): running the COPYs into
   INDEX-FREE tables. Loading a 32.5M-row table (raw_pipeline_conversation_messages,
   16GB parquet) with its 5 ART indexes live blew the 8GB memory_limit at COMMIT
   ("failed to pin block of size 256.0 KiB (7.4 GiB/7.4 GiB used)") at threads=8 AND
   threads=2, and a failed index-commit leaves partial rows so a same-table retry then
   dies on "Duplicate key" (both observed live 2026-07-01). Deferring index creation
   removes index maintenance from the COPY commit entirely (standard bulk-load
   practice). threads=2 caps parallel row-group writer memory; a COPY that still fails
   gets ONE retry at threads=1 after clearing its partial rows (DELETE) + CHECKPOINT.
   A second failure aborts loud.
3. INDEXES: the deferred CREATE [UNIQUE] INDEX statements run one-at-a-time on the
   fully-loaded tables (each build gets the whole memory budget; a UNIQUE index build
   also re-verifies uniqueness). Any failure aborts loud — the original DB is kept.

Usage: compact_import.py <export_dir> <new_db_path> <memory_limit> <temp_dir>
Exit codes: 0 ok; 2 bad args/missing export files; 3 unresolvable schema statements;
4 data load failure; 5 index build failure.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import duckdb

MAX_PASSES = 25  # dependency chains are shallow; each pass must make progress anyway
_INDEX_RE = re.compile(r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\b", re.IGNORECASE)
_COPY_TARGET_RE = re.compile(r"^\s*COPY\s+(\S+)\s+FROM\b", re.IGNORECASE)


def _first_line(s: str, n: int = 160) -> str:
    line = " ".join(s.strip().split())
    return line[:n] + ("…" if len(line) > n else "")


def main() -> int:
    if len(sys.argv) != 5:
        print("usage: compact_import.py <export_dir> <new_db> <memory_limit> <temp_dir>",
              file=sys.stderr)
        return 2
    exp_dir, new_db, memlimit, tmpdir = sys.argv[1:5]
    schema_sql = Path(exp_dir) / "schema.sql"
    load_sql = Path(exp_dir) / "load.sql"
    if not schema_sql.is_file() or not load_sql.is_file():
        print(f"ABORT: export incomplete — missing {schema_sql} or {load_sql}", file=sys.stderr)
        return 2

    con = duckdb.connect(new_db)
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{memlimit}'")
    con.execute(f"SET temp_directory='{tmpdir}'")

    # --- 1. schema (multi-pass; CREATE INDEX deferred to phase 3) --------------------
    all_stmts = [
        s for s in duckdb.extract_statements(schema_sql.read_text())
        if s.query.strip().rstrip(";").strip()
    ]
    deferred_indexes = [s for s in all_stmts if _INDEX_RE.match(s.query)]
    pending = [s for s in all_stmts if not _INDEX_RE.match(s.query)]
    total = len(pending)
    for pass_no in range(1, MAX_PASSES + 1):
        failed: list[tuple] = []  # (statement, error)
        for stmt in pending:
            try:
                con.execute(stmt.query)
            except (duckdb.CatalogException, duckdb.BinderException) as exc:
                # missing dependency (created later in the file) — retry next pass
                failed.append((stmt, exc))
            # any OTHER exception type is a real error: fail loud immediately
        if not failed:
            print(f"schema: {total} statements applied in {pass_no} pass(es) "
                  f"({len(deferred_indexes)} indexes deferred)")
            break
        if len(failed) == len(pending):
            # fixpoint with failures left -> genuinely broken statements, not ordering
            print(f"ABORT: {len(failed)} schema statement(s) unresolvable after "
                  f"{pass_no} pass(es):", file=sys.stderr)
            for stmt, exc in failed:
                print(f"  - {_first_line(stmt.query)}\n    -> {_first_line(str(exc))}",
                      file=sys.stderr)
            return 3
        pending = [s for s, _ in failed]
    else:
        print(f"ABORT: schema did not converge within {MAX_PASSES} passes "
              f"({len(pending)} pending)", file=sys.stderr)
        return 3

    # --- 2. data: COPY ... FROM parquet into index-free tables -----------------------
    con.execute("SET threads=2")
    loads = [
        s for s in duckdb.extract_statements(load_sql.read_text())
        if s.query.strip().rstrip(";").strip()
    ]
    for i, stmt in enumerate(loads, 1):
        try:
            con.execute(stmt.query)
        except (duckdb.OutOfMemoryException, duckdb.TransactionException) as exc:
            print(f"data: statement {i}/{len(loads)} hit {type(exc).__name__} at threads=2; "
                  f"clearing partial rows + retrying at threads=1: {_first_line(stmt.query, 100)}")
            try:
                # a failed commit can leave partial rows behind — clear the target table
                # first or the retry dies on duplicates (observed live 2026-07-01).
                m = _COPY_TARGET_RE.match(stmt.query)
                if m:
                    con.execute(f"DELETE FROM {m.group(1)}")
                con.execute("CHECKPOINT")
                con.execute("SET threads=1")
                con.execute(stmt.query)
                con.execute("SET threads=2")
            except Exception as exc2:  # noqa: BLE001
                print(f"ABORT: data load failed on statement {i}/{len(loads)} even at "
                      f"threads=1: {_first_line(stmt.query)}\n  -> {exc2}", file=sys.stderr)
                return 4
        except Exception as exc:  # noqa: BLE001 — report which COPY failed, then abort
            print(f"ABORT: data load failed on statement {i}/{len(loads)}: "
                  f"{_first_line(stmt.query)}\n  -> {exc}", file=sys.stderr)
            return 4
    print(f"data: {len(loads)} COPY statements applied")

    # --- 3. indexes: build one-at-a-time on the loaded tables ------------------------
    con.execute("CHECKPOINT")  # flush load buffers so index builds get the full budget
    for j, stmt in enumerate(deferred_indexes, 1):
        try:
            con.execute(stmt.query)
        except Exception as exc:  # noqa: BLE001
            print(f"ABORT: index build failed ({j}/{len(deferred_indexes)}): "
                  f"{_first_line(stmt.query)}\n  -> {exc}", file=sys.stderr)
            return 5
    print(f"indexes: {len(deferred_indexes)} built")

    con.execute("CHECKPOINT")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
