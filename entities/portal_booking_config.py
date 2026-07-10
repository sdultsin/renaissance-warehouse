"""Nightly mirror of the bookings-portal booking-form CONFIG tables.

Mirror-coverage-audit gap #4 (2026-07-09): ``booking_partners`` (partner→offer→
Slack-channel routing) and ``booking_options`` (form option config) are original,
Darcy-entered config that exists nowhere else — custody requires a warehouse copy.

    portal booking_partners -> raw_portal_booking_partners  (11 rows live 2026-07-09)
    portal booking_options  -> raw_portal_booking_options   (3 rows live 2026-07-09)

SOURCE: PostgREST on the bookings-portal Supabase — the exact same source +
credential names as entities/im_bookings.py (IM_BOOKINGS_SUPABASE_URL; prefer the
SERVICE_ROLE key, fall back to ANON). Registered under the existing ``im_bookings``
phase so it rides the same nightly slot with no orchestrator change.

SEMANTICS: full-refresh REPLACE — each run keeps exactly ONE live snapshot per
table (DELETE all + INSERT fresh, one transaction per table, idempotent). Metadata
columns follow the raw_im_bookings convention (_snapshot_date / _source /
_loaded_at).

SAFETY: these tables are config, not events — they should never legitimately be
empty. A 0-row pull (key rotated? RLS changed? table dropped?) raises WITHOUT
touching the warehouse, keeping the last-known-good snapshot. DDL:
sql/ddl/1093_raw_portal_booking_config.sql.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.portal_booking_config")

PAGE = 1000
SOURCE_TAG = "portal_booking_config_nightly"

# (portal_table, raw_table, order_column, source columns pulled verbatim)
_TABLES: list[tuple[str, str, str, list[str]]] = [
    (
        "booking_partners",
        "raw_portal_booking_partners",
        "partner",
        ["partner", "offer", "slack_channel_id", "slack_channel_name", "active", "created_at"],
    ),
    (
        "booking_options",
        "raw_portal_booking_options",
        "id",
        ["id", "field", "scope", "value", "active", "created_by", "created_at"],
    ),
]


def _fetch_all(base_url: str, api_key: str, table: str, order_col: str) -> list[dict]:
    """Page through PostgREST until a short/empty page is returned."""
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    rows: list[dict] = []
    offset = 0
    while True:
        url = (
            f"{base_url}/rest/v1/{table}?select=*&order={order_col}"
            f"&limit={PAGE}&offset={offset}"
        )
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                batch = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(
                f"{table} PostgREST {e.code} at offset {offset}: {body}"
            ) from e
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
    # Same key preference as im_bookings.py (anon SELECT was revoked ~2026-06-29).
    api_key = (
        ctx.credentials.optional("IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY")
        or ctx.credentials.require("IM_BOOKINGS_SUPABASE_ANON_KEY")
    )

    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    loaded_at = datetime.now(timezone.utc)
    total = 0
    counts: dict[str, int] = {}

    for portal_table, raw_table, order_col, source_cols in _TABLES:
        rows = _fetch_all(base_url, api_key, portal_table, order_col)
        logger.info("fetched %d %s rows from portal", len(rows), portal_table)
        if not rows:
            # Config tables are never legitimately empty — refuse to replace the
            # last-known-good snapshot (key/RLS/portal breakage guard).
            raise RuntimeError(
                f"{portal_table} pull returned 0 rows — key/RLS/portal problem? "
                "Refusing to replace the live snapshot."
            )

        payload = [
            [r.get(col) for col in source_cols] + [snapshot_date, SOURCE_TAG, loaded_at]
            for r in rows
        ]
        col_list = ", ".join(source_cols + ["_snapshot_date", "_source", "_loaded_at"])
        placeholders = ", ".join(["?"] * (len(source_cols) + 3))

        # Atomic REPLACE: exactly one live snapshot survives per table.
        db.execute("BEGIN")
        try:
            db.execute(f"DELETE FROM {raw_table}")
            db.executemany(
                f"INSERT INTO {raw_table} ({col_list}) VALUES ({placeholders})", payload
            )
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise
        n = db.execute(f"SELECT count(*) FROM {raw_table}").fetchone()[0]
        logger.info("mirrored portal %s -> %s: %d rows", portal_table, raw_table, n)
        counts[raw_table] = n
        total += n

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={"snapshot_date": snapshot_date, **counts},
    )


def register(registry: Registry) -> None:
    # Rides the existing im_bookings nightly slot (same source system).
    registry.add_phase("im_bookings", "portal_booking_config", run)
