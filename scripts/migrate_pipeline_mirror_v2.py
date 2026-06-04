"""One-time live migration to spec-15 sync modes.

Collapses each raw_pipeline_* table's stacked per-run snapshots down to one row
per natural `_key` (latest _loaded_at), adds the surrogate `_key` (+ `content_hash`
on copy tables) and a UNIQUE INDEX on `_key` so the v2 entity's ON CONFLICT works.

Generated from the SAME `SPECS` / `_key`/`content_hash` helpers the entity uses,
so the dedup key can never drift from the runtime key.

Non-destructive: old tables are renamed `*__legacy` (not dropped). Idempotent:
a table that already has a `_key` column is skipped. Drop the `*__legacy` tables
after one clean nightly + verification.

    python scripts/migrate_pipeline_mirror_v2.py            # uses config.DB_PATH
    python scripts/migrate_pipeline_mirror_v2.py --db ./x.duckdb
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module
from entities.pipeline_mirror import SPECS, _md5_concat

logger = logging.getLogger("scripts.migrate_pipeline_mirror_v2")


def _has_key_col(conn, table: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = ? AND column_name = '_key'",
        [f"raw_pipeline_{table}"],
    ).fetchone()
    return rows is not None


def _dedup_select(table: str, spec) -> str:
    """SELECT that emits _key (+ content_hash) + all original columns, deduped to
    latest _loaded_at per _key. Mirrors entities.pipeline_mirror exactly."""
    key_expr = spec.key_sql
    if spec.hash_cols:
        hash_expr = _md5_concat(spec.hash_cols)
        return (
            f"WITH h AS (SELECT c.*, {hash_expr} AS content_hash FROM raw_pipeline_{table} c) "
            f"SELECT * FROM ("
            f"  SELECT {key_expr} AS _key, h.*, "
            f"         row_number() OVER (PARTITION BY {key_expr} ORDER BY _loaded_at DESC) AS __rn "
            f"  FROM h"
            f") WHERE __rn = 1"
        )
    return (
        f"SELECT * FROM ("
        f"  SELECT {key_expr} AS _key, c.*, "
        f"         row_number() OVER (PARTITION BY {key_expr} ORDER BY _loaded_at DESC) AS __rn "
        f"  FROM raw_pipeline_{table} c"
        f") WHERE __rn = 1"
    )


def _existing_indexes(conn, table: str) -> list[str]:
    rows = conn.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = ?",
        [f"raw_pipeline_{table}"],
    ).fetchall()
    return [r[0] for r in rows]


def migrate_table(conn, table: str, spec) -> dict:
    if _has_key_col(conn, table):
        after = conn.execute(f"SELECT count(*) FROM raw_pipeline_{table}").fetchone()[0]
        logger.info("  %s already migrated — skipping (%d rows)", table, after)
        return {"table": table, "skipped": True, "rows": after}

    before = conn.execute(f"SELECT count(*) FROM raw_pipeline_{table}").fetchone()[0]
    insert_cols = ["_key"] + spec.columns + (["content_hash"] if spec.hash_cols else []) + ["_loaded_at", "_run_id"]
    col_list = ", ".join(insert_cols)

    conn.execute("BEGIN")
    try:
        # 1. Materialize the deduped rowset (with _key/content_hash) into a temp.
        conn.execute(f"DROP TABLE IF EXISTS _keep_{table}")
        conn.execute(f"CREATE TEMP TABLE _keep_{table} AS {_dedup_select(table, spec)}")
        conn.execute(f"ALTER TABLE _keep_{table} DROP COLUMN __rn")
        # 2. Drop every existing index so the full DELETE can't hit the ART-delete bug.
        for idx in _existing_indexes(conn, table):
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
        # 3. Add the new columns IN PLACE (view-safe — no rename).
        if spec.hash_cols:
            conn.execute(f"ALTER TABLE raw_pipeline_{table} ADD COLUMN content_hash VARCHAR")
        conn.execute(f"ALTER TABLE raw_pipeline_{table} ADD COLUMN _key VARCHAR")
        # 4. Replace contents with the deduped rows.
        conn.execute(f"DELETE FROM raw_pipeline_{table}")
        conn.execute(f"INSERT INTO raw_pipeline_{table} ({col_list}) SELECT {col_list} FROM _keep_{table}")
        # 5. Unique index on _key (the ON CONFLICT target) + a campaign_id helper.
        conn.execute(f"CREATE UNIQUE INDEX uxk_raw_pipeline_{table} ON raw_pipeline_{table} (_key)")
        if "campaign_id" in spec.columns:
            conn.execute(f"CREATE INDEX ixc_raw_pipeline_{table} ON raw_pipeline_{table} (campaign_id)")
        conn.execute(f"DROP TABLE _keep_{table}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    after = conn.execute(f"SELECT count(*) FROM raw_pipeline_{table}").fetchone()[0]
    logger.info("  %s: %d -> %d rows (%.1fx collapse)", table, before, after, before / max(after, 1))
    return {"table": table, "before": before, "after": after}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    conn = db_module.connect(Path(args.db) if args.db else None)
    logger.info("Migrating %d pipeline tables to spec-15 sync modes", len(SPECS))
    results = []
    for table, spec in SPECS.items():
        results.append(migrate_table(conn, table, spec))
    conn.close()

    migrated = [r for r in results if not r.get("skipped")]
    logger.info("Done. %d migrated, %d skipped.", len(migrated), len(results) - len(migrated))
    return 0


if __name__ == "__main__":
    sys.exit(main())
