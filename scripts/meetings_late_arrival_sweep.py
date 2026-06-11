#!/usr/bin/env python3
"""Late-arrival sweep for raw_pipeline_meetings_booked_raw (2026-06-11 hygiene Task 1).

The meetings mirror is watermark-incremental on posted_at (insert-only): an upstream
row that ARRIVES late carrying an old posted_at (Slack scrape backfills, manual
unmatched-queue resolutions re-synced with their original post date) falls permanently
behind the watermark and is never pulled. By 2026-06-11 that tail had accumulated to
309 pre-Jun-4 rows (~1% of the table). This pulls exactly those rows by anti-join on
the mirror key (_key = upstream id) — cheap (two key scans), safe (ON CONFLICT DO
NOTHING), and complete (no watermark involved).

Run weekly via meetings_late_arrival_sweep.sh, which also rebuilds core.meeting and
the campaign_daily meetings column afterwards. The CALLER holds the warehouse write
lock; this script assumes it has the writer.

Usage:
    python scripts/meetings_late_arrival_sweep.py [--db /path/to.duckdb] [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.config import DB_PATH  # noqa: E402
from core.credentials import load_credentials  # noqa: E402
from entities.pipeline_mirror import SPECS  # noqa: E402

TABLE = "meetings_booked_raw"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Anti-join late-arrival pull for the meetings mirror")
    parser.add_argument("--db", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true", help="count the gap, insert nothing")
    args = parser.parse_args(argv)

    pg_url = load_credentials().require("PIPELINE_SUPABASE_DB_URL")
    spec = SPECS[TABLE]

    con = duckdb.connect(args.db or str(DB_PATH), read_only=args.dry_run)
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    try:
        con.execute("DETACH pg")
    except Exception:
        pass
    con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")

    gap = con.execute(
        f"SELECT count(*) FROM pg.public.{TABLE} u "
        f"WHERE CAST(u.id AS VARCHAR) NOT IN "
        f"(SELECT _key FROM raw_pipeline_{TABLE})"
    ).fetchone()[0]
    print(f"late-arrival gap: {gap} upstream rows missing from raw_pipeline_{TABLE}")

    if args.dry_run or gap == 0:
        con.close()
        return 0

    cols = ", ".join(spec.columns)
    con.execute(
        f"INSERT INTO raw_pipeline_{TABLE} (_key, {cols}, _loaded_at, _run_id) "
        f"SELECT CAST(u.id AS VARCHAR), {cols}, now(), 'late_arrival_sweep' "
        f"FROM pg.public.{TABLE} u "
        f"WHERE CAST(u.id AS VARCHAR) NOT IN (SELECT _key FROM raw_pipeline_{TABLE}) "
        f"ON CONFLICT (_key) DO NOTHING"
    )

    up, wh = con.execute(
        f"SELECT (SELECT count(*) FROM pg.public.{TABLE}), "
        f"       (SELECT count(*) FROM raw_pipeline_{TABLE})"
    ).fetchone()
    con.execute("DETACH pg")
    con.close()
    print(f"inserted {gap}; upstream={up} warehouse={wh} ({'EXACT' if up == wh else 'residual ' + str(up - wh)})")
    return 0 if up == wh else 1


if __name__ == "__main__":
    sys.exit(main())
