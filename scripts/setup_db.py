"""Initialize a DuckDB file with all DDL applied. Idempotent.

Usage:
    python scripts/setup_db.py                  # uses config.DB_PATH
    python scripts/setup_db.py --db ./core.duckdb  # local dev
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.setup_db")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default=None, help="Override DB path")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    db_path = Path(args.db) if args.db else None
    conn = db_module.connect(db_path)

    ddl_dir = REPO_ROOT / "sql" / "ddl"
    files = sorted(ddl_dir.glob("*.sql"))
    if not files:
        logger.warning("No DDL files found at %s", ddl_dir)
        return 0

    for sql_file in files:
        # Convention: filename starts with NN_ where NN is the version.
        stem = sql_file.stem
        try:
            version = int(stem.split("_", 1)[0])
        except ValueError:
            logger.warning("Skipping %s — filename does not start with version int", sql_file.name)
            continue
        applied = db_module.apply_ddl_file(conn, sql_file, version=version)
        logger.info("%s %s (version=%d)", "applied" if applied else "already-applied", sql_file.name, version)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
