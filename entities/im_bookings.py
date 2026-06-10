"""Nightly mirror of the bookings portal im_bookings table -> raw_im_bookings.

Scope A (IM conversion attribution, 2026-06-09). Supersedes the one-time frozen snapshot
loader (scripts/backfill_im_bookings.py, _snapshot_date 2026-05-31) as the FRESHNESS path —
the frozen snapshot itself is preserved untouched.

WHY: raw_im_bookings is the only conversion source carrying prospect identity (email/phone)
per booked meeting — it feeds the identity-bearing 'portal_im_bookings' feeder of
core.conversion_event (entities/conversion_event.py). Without a recurring pull, conversions
go stale at the last manual snapshot.

SEMANTICS (table now holds exactly two snapshot generations):
  • The FROZEN 2026-05-31 snapshot (_source='portal_im_bookings') — preserved verbatim for
    the funding-partner attribution analysis that froze it. NEVER deleted here.
  • ONE live snapshot (_snapshot_date = run date, _source='portal_im_bookings_nightly') —
    each run deletes any prior live snapshot(s) and inserts a fresh full pull (~40k rows).
Consumers wanting current state read WHERE _snapshot_date = (SELECT max(_snapshot_date) …).

SOURCE: PostgREST on the bookings-portal Supabase project, table im_bookings, paged by id
(publishable / anon-role key — RLS-gated read). Credentials from .env:
  IM_BOOKINGS_SUPABASE_URL / IM_BOOKINGS_SUPABASE_ANON_KEY

SAFETY: the portal table only ever grows (~40.7k rows on 2026-06-09). If a pull returns
fewer than MIN_EXPECTED_ROWS (key rotated? RLS changed? portal truncated?) we raise WITHOUT
touching the warehouse — last-known-good live snapshot is retained.
"""
from __future__ import annotations

import json
import logging
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.im_bookings")

TABLE = "im_bookings"
PAGE = 1000
FROZEN_SNAPSHOT_DATE = "2026-05-31"      # the one-time analysis freeze — never deleted
SOURCE_TAG = "portal_im_bookings_nightly"
MIN_EXPECTED_ROWS = 36_000               # portal is append-mostly; below this = broken pull

# Column order must match sql/ddl/27_raw_im_bookings.sql (the 22 source cols, in table order).
SOURCE_COLUMNS = [
    "id", "type", "date", "offer", "partner", "advisor", "owner_name", "company",
    "first_name", "last_name", "email", "phone", "job_title", "num_employees",
    "annual_revenue", "workspace", "our_email", "campaign", "status", "inbox_manager",
    "campaign_manager", "interested_in",
]


def _fetch_all(base_url: str, anon_key: str) -> list[dict]:
    """Page through PostgREST until a short page (or empty) is returned."""
    headers = {
        "apikey": anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Accept": "application/json",
    }
    rows: list[dict] = []
    offset = 0
    while True:
        url = f"{base_url}/rest/v1/{TABLE}?select=*&order=id&limit={PAGE}&offset={offset}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                batch = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(f"im_bookings PostgREST {e.code} at offset {offset}: {body}") from e
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        if len(batch) < PAGE:
            break
        offset += PAGE
    return rows


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    base_url = ctx.credentials.require("IM_BOOKINGS_SUPABASE_URL").rstrip("/")
    anon_key = ctx.credentials.require("IM_BOOKINGS_SUPABASE_ANON_KEY")

    rows = _fetch_all(base_url, anon_key)
    logger.info("fetched %d im_bookings rows from portal", len(rows))
    if len(rows) < MIN_EXPECTED_ROWS:
        # Fail LOUD, write nothing — keep last-known-good live snapshot.
        raise RuntimeError(
            f"im_bookings pull returned {len(rows)} rows (< floor {MIN_EXPECTED_ROWS}) — "
            "key/RLS/portal problem? Refusing to replace the live snapshot."
        )

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    loaded_at = datetime.now(timezone.utc)
    payload = [
        [r.get(col) for col in SOURCE_COLUMNS] + [snapshot_date, SOURCE_TAG, loaded_at]
        for r in rows
    ]
    placeholders = ", ".join(["?"] * (len(SOURCE_COLUMNS) + 3))
    col_list = ", ".join(SOURCE_COLUMNS + ["_snapshot_date", "_source", "_loaded_at"])

    db.execute("BEGIN")
    # Drop ALL prior live snapshots (anything that isn't the frozen freeze) — exactly one
    # live generation survives. The frozen 2026-05-31 snapshot is never touched.
    db.execute(
        "DELETE FROM raw_im_bookings WHERE _snapshot_date <> ?", [FROZEN_SNAPSHOT_DATE]
    )
    db.executemany(
        f"INSERT INTO raw_im_bookings ({col_list}) VALUES ({placeholders})", payload
    )
    db.execute("COMMIT")

    n_live = db.execute(
        "SELECT count(*) FROM raw_im_bookings WHERE _snapshot_date = ?", [snapshot_date]
    ).fetchone()[0]
    n_email = db.execute(
        "SELECT count(email) FROM raw_im_bookings WHERE _snapshot_date = ?", [snapshot_date]
    ).fetchone()[0]
    logger.info("live snapshot %s: %d rows (%d with email)", snapshot_date, n_live, n_email)
    return PhaseResult(
        rows_in=len(rows),
        rows_out=n_live,
        notes={"snapshot_date": snapshot_date, "with_email": n_email,
               "frozen_snapshot_preserved": FROZEN_SNAPSHOT_DATE},
    )


def register(registry: Registry) -> None:
    registry.add_phase("im_bookings", "im_bookings", run)
