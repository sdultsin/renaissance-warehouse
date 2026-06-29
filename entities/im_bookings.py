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

SOURCE: PostgREST on the bookings-portal Supabase project, table im_bookings, paged by id.
Credentials from .env: IM_BOOKINGS_SUPABASE_URL + a key. The portal's `anon` role lost its
SELECT GRANT on im_bookings ~2026-06-29 (direct reads now 401 "permission denied for table
im_bookings"), so we prefer the project SERVICE-ROLE key (IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY)
and fall back to the legacy anon key (IM_BOOKINGS_SUPABASE_ANON_KEY) only if it is restored.
Service-role bypasses RLS — used read-only here.

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

# Source columns mirrored verbatim. The INSERT lists columns explicitly, so this is the set of
# im_bookings keys to pull (order need only match the payload build below, not the table). The
# original 22 cols are in sql/ddl/27_raw_im_bookings.sql; the booking-SLOT + lifecycle cols
# (meeting_date/time/tz, created_at, deleted_at, lead_type, subject_line) are added in DDL 1048 and
# feed core.v_meeting_reminders.meeting_slot_at. Unknown-to-source keys come back as NULL (.get).
SOURCE_COLUMNS = [
    "id", "type", "date", "offer", "partner", "advisor", "owner_name", "company",
    "first_name", "last_name", "email", "phone", "job_title", "num_employees",
    "annual_revenue", "workspace", "our_email", "campaign", "status", "inbox_manager",
    "campaign_manager", "interested_in",
    # booking-slot + lifecycle (DDL 1048) — the reminder-time source:
    "meeting_date", "meeting_time", "meeting_tz", "created_at", "deleted_at",
    "lead_type", "subject_line",
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
    # Prefer the service-role key (anon SELECT was revoked ~2026-06-29); fall back to anon if restored.
    api_key = (ctx.credentials.optional("IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY")
               or ctx.credentials.require("IM_BOOKINGS_SUPABASE_ANON_KEY"))

    rows = _fetch_all(base_url, api_key)
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
