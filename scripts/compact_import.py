#!/usr/bin/env python3
"""compact_import.py — dependency-tolerant IMPORT step for compact_warehouse.sh.

WHY THIS EXISTS (2026-07-01): DuckDB's one-shot `IMPORT DATABASE '<dir>'` executes the
exported schema.sql TOP-TO-BOTTOM and aborts on the FIRST error. EXPORT DATABASE does
NOT topologically sort view-on-view dependencies, so a view that reads another view
(e.g. something SELECTing from core.v_inbox_overview emitted before v_inbox_overview
itself) kills the whole import:

    Catalog Error: Table with name v_inbox_overview does not exist!

That aborted the 2026-07-01 nightly compaction and left the writer DB at 171 GB.

This driver replays the export the same way IMPORT DATABASE does (schema.sql, then
load.sql), but runs schema.sql in MULTI-PASS: statements that fail with a catalog/
binder error (missing dependency) are retried on the next pass, when their
dependencies exist. Fixpoint reached with failures remaining -> exit non-zero and
print every failing statement + error, so compact_warehouse.sh ABORTS and keeps the
original DB (fail-loud, never a silently incomplete copy). Data COPYs (load.sql) are
order-independent and run once; ANY failure there is fatal.

Usage: compact_import.py <export_dir> <new_db_path> <memory_limit> <temp_dir>
Exit codes: 0 ok; 2 bad args/missing export files; 3 unresolvable schema statements;
4 data load failure.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

MAX_PASSES = 25  # dependency chains are shallow; each pass must make progress anyway


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

    # --- schema: multi-pass so view-on-view ordering can never abort the import ------
    pending = [
        s for s in duckdb.extract_statements(schema_sql.read_text())
        if s.query.strip().rstrip(";").strip()
    ]
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
            print(f"schema: {total} statements applied in {pass_no} pass(es)")
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

    # --- data: COPY ... FROM parquet, order-independent, one shot, fatal on error ----
    loads = [
        s for s in duckdb.extract_statements(load_sql.read_text())
        if s.query.strip().rstrip(";").strip()
    ]
    for i, stmt in enumerate(loads, 1):
        try:
            con.execute(stmt.query)
        except Exception as exc:  # noqa: BLE001 — report which COPY failed, then abort
            print(f"ABORT: data load failed on statement {i}/{len(loads)}: "
                  f"{_first_line(stmt.query)}\n  -> {exc}", file=sys.stderr)
            return 4
    print(f"data: {len(loads)} COPY statements applied")

    con.execute("CHECKPOINT")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
