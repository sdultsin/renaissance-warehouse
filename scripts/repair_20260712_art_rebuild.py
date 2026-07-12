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

UPDATE (same day, 15:19Z): the canonical re-run after the first repair FATAL'd on a
THIRD table — core.conversion_event ("Only deleted 1890 out of 2048 rows") — which had
rebuilt CLEANLY at 10:01Z in the run that then aborted. Evidence points at commits that
sat in the WAL when a run aborted abnormally (FATAL invalidation / SIGTERM) replaying
with inconsistent ART state on the next open. Iterating one FATAL at a time is a
treadmill, so --copy mode rebuilds ALL canonical-phase tables in one pass,
DATA-PRESERVING (some canonical tables — account_status_history, rollup_history,
sendivo_cost — are append/upsert accumulators, NOT rebuildable from raw):
  CREATE <table>__rebuild (same DDL) → INSERT SELECT * (full scan; never touches the
  damaged index) → verify counts → DROP old → RENAME → recreate secondary indexes.

Run ONLY under the single-writer lock:
  bash scripts/with_warehouse_lock.sh .venv/bin/python \
      scripts/repair_20260712_art_rebuild.py [--dry-run] [--copy] [--tables core.domain core.reply]
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
    ap.add_argument(
        "--copy",
        action="store_true",
        help="data-preserving rebuild (copy-swap) instead of recreate-empty; "
        "REQUIRED for append/upsert accumulator tables",
    )
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
            print("[dry-run] skipping rebuild")
            continue

        if args.copy:
            # data-preserving copy-swap: fresh table + fresh ART, old rows carried over
            # via full scan (never touches the damaged index).
            tmp_name = f"{name}__rebuild"
            tmp = f'{schema}."{tmp_name}"'
            conn.execute(f"DROP TABLE IF EXISTS {tmp}")
            tmp_create = None
            for pat in (f'CREATE TABLE {schema}."{name}"', f"CREATE TABLE {schema}.{name}"):
                if pat in create_sql:
                    tmp_create = create_sql.replace(pat, f'CREATE TABLE {schema}."{tmp_name}"', 1)
                    break
            if tmp_create is None:
                print(f"ABORT: cannot rewrite CREATE sql for {table}", file=sys.stderr)
                return 2
            conn.execute(tmp_create)
            conn.execute(f"INSERT INTO {tmp} SELECT * FROM {table}")
            n_new = conn.execute(f"SELECT count(*) FROM {tmp}").fetchone()[0]
            if n_new != n_before:
                print(
                    f"ABORT: copy count mismatch for {table}: old={n_before} new={n_new} "
                    f"(leaving {tmp} for inspection)",
                    file=sys.stderr,
                )
                return 2
            conn.execute(f"DROP TABLE {table}")
            conn.execute(f'ALTER TABLE {tmp} RENAME TO "{name}"')
            for _iname, isql in idx_rows:
                conn.execute(isql)
            cols_after = conn.execute(f"DESCRIBE {table}").fetchall()
            if cols_after != cols_before:
                print(
                    f"FATAL: rebuilt column list differs for {table}!\n"
                    f"before={cols_before}\nafter={cols_after}",
                    file=sys.stderr,
                )
                return 2
            print(f"copy-swap rebuilt ok (rows={n_new}); schema identical; fresh ART")
        else:
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
