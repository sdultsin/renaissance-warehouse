"""One-time FROZEN backfill of the bookings portal im_bookings table -> raw_im_bookings.

⚠ SUPERSEDED for freshness (Scope A, 2026-06-09): entities/im_bookings.py now mirrors the
portal table NIGHTLY ('im_bookings' phase) — do not run this for routine refreshes. This
script remains only for capturing an additional FROZEN snapshot under a new --snapshot-date.
NOTE: the nightly entity deletes every snapshot except the frozen 2026-05-31 one on each
run, so any extra frozen snapshot captured here must also be added to its
FROZEN_SNAPSHOT_DATE handling or it will be swept on the next nightly.

Workstream F. This is NOT part of the nightly orchestrator. It captures a single
frozen snapshot of the bookings portal table for funding-partner attribution analysis,
then never runs again (unless a fresh snapshot is wanted under a new --snapshot-date).

WHAT IT DOES
  1. Pages through PostgREST on <SUPABASE_URL>/rest/v1/im_bookings (select=*, limit=1000,
     offset=N, ordered by id) using the portal's publishable key. Both the URL and the
     publishable key are read from the environment (IM_BOOKINGS_SUPABASE_URL /
     IM_BOOKINGS_SUPABASE_ANON_KEY).
  2. Accumulates all rows in memory (small).
  3. Loads them into DuckDB table raw_im_bookings with the extra columns:
       _snapshot_date (constant, default below), _source (source tag), _loaded_at = now().
     Idempotent for a given snapshot_date: deletes existing rows for that _snapshot_date
     first, then inserts (so a re-run replaces, never duplicates).

USAGE (run ON the droplet, where warehouse.duckdb + duckdb live):
    cd <repo> && source .venv/bin/activate && python scripts/backfill_im_bookings.py

    # custom freeze date / dry-run (fetch + print counts, NO warehouse write):
    python scripts/backfill_im_bookings.py --snapshot-date 2026-05-31 --dry-run

PREREQUISITE: sql/ddl/27_raw_im_bookings.sql must be applied first (creates the table).

⚠ WRITE WINDOW: writes to the warehouse, so do NOT run during the nightly sync window
  (single-writer lock). The fetch phase is read-only and safe anytime.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

# --- Source config (read from environment) ---------------------------------------------
# The bookings portal exposes im_bookings via PostgREST using a publishable (RLS-gated,
# anon-role) key. Both the project URL and the key are read from the environment so they
# are not committed to the repo.
SUPABASE_URL = os.environ.get("IM_BOOKINGS_SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.environ.get("IM_BOOKINGS_SUPABASE_ANON_KEY", "")
TABLE = "im_bookings"
PAGE = 1000

WAREHOUSE_DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
DEFAULT_SNAPSHOT_DATE = "2026-05-31"
SOURCE_TAG = "portal_im_bookings"

# Column order must match sql/ddl/27_raw_im_bookings.sql (the 22 source cols, in table order).
SOURCE_COLUMNS = [
    "id", "type", "date", "offer", "partner", "advisor", "owner_name", "company",
    "first_name", "last_name", "email", "phone", "job_title", "num_employees",
    "annual_revenue", "workspace", "our_email", "campaign", "status", "inbox_manager",
    "campaign_manager", "interested_in",
]


def _headers() -> dict[str, str]:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Accept": "application/json",
    }


def fetch_all(limit_pages: int | None = None) -> list[dict]:
    """Page through PostgREST until a short page (or empty) is returned."""
    rows: list[dict] = []
    offset = 0
    page_no = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/{TABLE}"
            f"?select=*&order=id&limit={PAGE}&offset={offset}"
        )
        req = urllib.request.Request(url, headers=_headers())
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                batch = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            raise SystemExit(f"PostgREST {e.code} at offset {offset}: {body}") from e
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        page_no += 1
        print(f"  page {page_no}: +{len(batch)} rows (total {len(rows)})", file=sys.stderr)
        if len(batch) < PAGE:
            break
        offset += PAGE
        if limit_pages is not None and page_no >= limit_pages:
            break
    return rows


def load_into_duckdb(rows: list[dict], snapshot_date: str, db_path: str) -> int:
    """Idempotent load for a given snapshot_date: delete-then-insert. Writes the warehouse."""
    import duckdb  # local import so --dry-run / fetch needs no duckdb

    loaded_at = datetime.now(timezone.utc)
    # Project each source dict into the table's column order; missing keys -> None.
    payload = [
        [r.get(col) for col in SOURCE_COLUMNS] + [snapshot_date, SOURCE_TAG, loaded_at]
        for r in rows
    ]
    placeholders = ", ".join(["?"] * (len(SOURCE_COLUMNS) + 3))
    col_list = ", ".join(SOURCE_COLUMNS + ["_snapshot_date", "_source", "_loaded_at"])

    conn = duckdb.connect(db_path)  # read-write
    try:
        conn.execute("BEGIN")
        conn.execute(
            "DELETE FROM raw_im_bookings WHERE _snapshot_date = ?", [snapshot_date]
        )
        conn.executemany(
            f"INSERT INTO raw_im_bookings ({col_list}) VALUES ({placeholders})", payload
        )
        conn.execute("COMMIT")
        n = conn.execute(
            "SELECT count(*) FROM raw_im_bookings WHERE _snapshot_date = ?",
            [snapshot_date],
        ).fetchone()[0]
    finally:
        conn.close()
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshot-date", default=DEFAULT_SNAPSHOT_DATE,
                    help="Frozen-as-of date stored in _snapshot_date (YYYY-MM-DD).")
    ap.add_argument("--db", default=WAREHOUSE_DB, help="Path to warehouse.duckdb.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + report counts only. Does NOT write the warehouse.")
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="For smoke tests: stop after N pages.")
    args = ap.parse_args()

    print(f"Fetching {TABLE} from {SUPABASE_URL} ...", file=sys.stderr)
    rows = fetch_all(limit_pages=args.limit_pages)
    print(f"Fetched {len(rows)} rows.", file=sys.stderr)

    if rows:
        partners: dict[str, int] = {}
        for r in rows:
            partners[r.get("partner")] = partners.get(r.get("partner"), 0) + 1
        print("Distinct partner counts:", file=sys.stderr)
        for p, c in sorted(partners.items(), key=lambda kv: -kv[1]):
            print(f"  {p}: {c}", file=sys.stderr)

    if args.dry_run:
        print("[dry-run] skipping warehouse write.", file=sys.stderr)
        return

    n = load_into_duckdb(rows, args.snapshot_date, args.db)
    print(f"Loaded {n} rows into raw_im_bookings "
          f"(_snapshot_date={args.snapshot_date}, _source={SOURCE_TAG}).", file=sys.stderr)


if __name__ == "__main__":
    main()
