"""Initialize a DuckDB file with all DDL applied. Idempotent.

Usage:
    python scripts/setup_db.py                  # uses config.DB_PATH
    python scripts/setup_db.py --db ./core.duckdb  # local dev
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.setup_db")


def _committed_ddl_names(ddl_dir: Path) -> set[str] | None:
    """Filenames in sql/ddl that are TRACKED at HEAD and NOT locally modified — i.e. the gated,
    committed DDL set. Returns None (fail-open) only if git is unavailable, with a loud warning.

    WHY: the box runs from a git checkout that, by design, cannot push (ticket A4), so a chat can
    leave an UNTRACKED or locally-edited DDL on the box. Globbing sql/ddl/*.sql blindly would then
    apply that un-gated code straight into the live warehouse on the nightly rebuild — the exact
    hole behind the 2026-06-29 `98_sms_campaign_offer.sql` incident. Restricting to committed-at-HEAD
    files means the nightly only ever materializes DDL that went through the moderator/two-key gate.
    """
    try:
        tracked = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "ls-files", "sql/ddl/*.sql"],
            capture_output=True, text=True, check=True, timeout=20).stdout.split()
        modified = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "diff", "--name-only", "HEAD", "--", "sql/ddl"],
            capture_output=True, text=True, check=True, timeout=20).stdout.split()
        committed = {Path(p).name for p in tracked} - {Path(p).name for p in modified}
        return committed
    except Exception as exc:  # noqa: BLE001 — git missing/odd: fail OPEN (don't break the nightly) but LOUD
        logger.warning("committed-DDL filter unavailable (%s) — applying ALL sql/ddl/*.sql incl. any "
                       "untracked/uncommitted (un-gated) files this run", exc)
        return None


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

    # Gate: never apply an untracked/uncommitted DDL left on the box (un-gated code). committed=None
    # => git unavailable => fail-open (already warned). committed=set => apply only those.
    committed = _committed_ddl_names(ddl_dir)
    all_sql = list(ddl_dir.glob("*.sql"))
    ungated = [f for f in all_sql if committed is not None and f.name not in committed]
    for f in ungated:
        logger.warning("SKIPPING un-gated DDL %s — not committed at HEAD (untracked or locally modified); "
                       "ship it via /warehouse-ship so the nightly can apply it", f.name)
    candidate_sql = [f for f in all_sql if committed is None or f.name in committed]
    versioned = [(v, f) for f in candidate_sql if (v := _version_of(f)) is not None]
    skipped = [f for f in candidate_sql if _version_of(f) is None]
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
