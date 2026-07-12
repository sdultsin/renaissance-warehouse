#!/usr/bin/env python3
"""repair_20260712_art_rebuild.py — drop + recreate canonical tables whose ART (PK)
index has gone inconsistent with committed row data.

INCIDENT [2026-07-12]: nightly run 20260712T053056Z-cee72d FATAL'd in canonical/domain:
  "Invalid Input Error: Failed to delete all rows from index. Only deleted 966 out of
   2048 rows" (entities/domain.py `DELETE FROM core.domain`) — the ART index on
core.domain is missing entries for rows the table holds, so the entity's full-table
DELETE cannot proceed and the error invalidates the whole DB connection (exit 2,
serving kept last-good). The 07-11 nightly livelocked 15h inside the core.reply bulk
INSERT (entities/f_reply_canonical.py:201) — the same index-op class on core.reply's
PK. Prime-suspect origin: the 07-10 EXPORT/IMPORT compaction (173→111GB). Prior art:
the 06-09 schema_consumers corrupt-index FATAL — table rebuild is the established fix.

Both core.domain and core.reply are FULL-REBUILD-EACH-RUN canonical tables (the entity
does DELETE FROM + INSERT ... SELECT from committed raw tables every nightly), so the
minimal, loss-free repair is:
  1. capture the LIVE CREATE TABLE sql from the catalog (duckdb_tables().sql — includes
     the PK and any post-DDL ALTERs, e.g. core.reply.eaccount from ddl 48) and the
     explicit secondary-index sqls (duckdb_indexes().sql),
  2. DROP TABLE (drops the damaged ART index with it; no row-level index maintenance),
  3. recreate the table EMPTY with the identical schema + PK + secondary indexes,
  4. let the next `--phase canonical` run rebuild the contents (its DELETE is now a
     no-op and its bulk INSERT builds a fresh, consistent ART).

Safety: refuses to touch a table any FOREIGN KEY references; verifies the recreated
column list is byte-identical to the pre-drop DESCRIBE; CHECKPOINTs at the end.

Run ONLY under the single-writer lock:
  bash scripts/with_warehouse_lock.sh .venv/bin/python \
      scripts/repair_20260712_art_rebuild.py [--dry-run] [--tables core.domain core.reply]
"""
from __future__ import annotations

import argparse
import sys

from core import db as db_module


def qname(t: str) -> tuple[str, str]:
    if "." not in t:
        raise SystemExit(f"table must be schema-qualified: {t}")
    s, n = t.split(".", 1)
    return s, n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="+", default=["core.domain", "core.reply"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = db_module.connect()

    for table in args.tables:
        schema, name = qname(table)
        print(f"=== {table} ===")

        row = conn.execute(
            "SELECT sql FROM duckdb_tables() WHERE database_name = current_database() "
            "AND schema_name = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
        if not row or not row[0]:
            print(f"ABORT: no CREATE sql found in catalog for {table}", file=sys.stderr)
            return 2
        create_sql = row[0]

        idx_rows = conn.execute(
            "SELECT index_name, sql FROM duckdb_indexes() WHERE database_name = current_database() "
            "AND schema_name = ? AND table_name = ? AND sql IS NOT NULL",
            [schema, name],
        ).fetchall()

        # FK safety: refuse if any OTHER table's FOREIGN KEY references this table.
        fk = conn.execute(
            "SELECT table_name, constraint_text FROM duckdb_constraints() "
            "WHERE constraint_type = 'FOREIGN KEY' AND constraint_text ILIKE ?",
            [f"%{name}%"],
        ).fetchall()
        fk = [r for r in fk if r[0] != name]
        if fk:
            print(f"ABORT: FOREIGN KEYs reference {table}: {fk}", file=sys.stderr)
            return 2

        n_before = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        cols_before = conn.execute(f"DESCRIBE {table}").fetchall()
        print(f"rows_before={n_before} cols={len(cols_before)} secondary_indexes={len(idx_rows)}")
        print("-- captured CREATE:")
        print(create_sql)
        for iname, isql in idx_rows:
            print(f"-- captured INDEX {iname}: {isql}")

        if args.dry_run:
            print("[dry-run] skipping drop/recreate")
            continue

        conn.execute(f"DROP TABLE {table}")
        conn.execute(create_sql)
        for _iname, isql in idx_rows:
            conn.execute(isql)

        cols_after = conn.execute(f"DESCRIBE {table}").fetchall()
        if cols_after != cols_before:
            print(
                f"FATAL: recreated column list differs for {table}!\n"
                f"before={cols_before}\nafter={cols_after}",
                file=sys.stderr,
            )
            return 2
        n_after = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        # exercise the exact op class that failed (no-op on the empty table)
        conn.execute(f"DELETE FROM {table}")
        print(f"recreated EMPTY ok (rows={n_after}); schema identical; DELETE exercises clean")

    if not args.dry_run:
        conn.execute("CHECKPOINT")
        print("CHECKPOINT ok")
    conn.close()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
