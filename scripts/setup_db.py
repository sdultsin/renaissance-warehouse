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

    # Parse the NN_ version prefix and sort NUMERICALLY (not lexically). Lexical sort applied "1005_…"
    # before "92_…" — so a break in a low-numbered DDL would not even block the 10xx range, and worse,
    # versions applied out of true dependency order. Numeric order = real apply order.
    def _version_of(p: Path) -> int | None:
        try:
            return int(p.stem.split("_", 1)[0])
        except ValueError:
            return None

    versioned = [(v, f) for f in ddl_dir.glob("*.sql") if (v := _version_of(f)) is not None]
    skipped = [f for f in ddl_dir.glob("*.sql") if _version_of(f) is None]
    for f in skipped:
        logger.warning("Skipping %s — filename does not start with version int", f.name)
    if not versioned:
        logger.warning("No versioned DDL files found at %s", ddl_dir)
        conn.close()
        return 0
    versioned.sort(key=lambda vf: vf[0])

    # Apply each DDL inside its own try/except so ONE bad DDL never cascade-blocks the rest. Before
    # this guard, apply_ddl_file raising (e.g. v92's read_csv on a missing seed file) killed the whole
    # loop, silently leaving every later version unapplied for all writers. Now a failure is logged,
    # recorded, and we CONTINUE; the run still exits non-zero so the failure stays visible to the caller.
    failures: list[tuple[int, str, str]] = []
    for version, sql_file in versioned:
        try:
            applied = db_module.apply_ddl_file(conn, sql_file, version=version)
            logger.info("%s %s (version=%d)", "applied" if applied else "already-applied", sql_file.name, version)
        except Exception as exc:  # noqa: BLE001 — resilience is the whole point; never let one DDL abort the rest
            failures.append((version, sql_file.name, str(exc)))
            logger.error("FAILED %s (version=%d): %s — continuing to remaining DDLs", sql_file.name, version, exc)

    conn.close()
    if failures:
        logger.error("setup_db completed with %d failed DDL(s): %s",
                     len(failures), ", ".join(f"{v}:{n}" for v, n, _ in failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
