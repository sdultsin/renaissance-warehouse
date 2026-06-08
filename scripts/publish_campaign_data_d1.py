"""Publish the campaign_data read-model snapshot from the warehouse to Cloudflare D1.

This is the warehouse-side half of Campaign Control's Lane C-read migration
(retiring CC's last dependency on Pipeline Supabase). CC reads campaign
performance from a `campaign_data` table; historically that lived in Pipeline
Supabase. This script reads the equivalent rows from the DuckDB warehouse mirror
(`raw_pipeline_campaign_data`) and overwrites the `campaign_data` table in CC's
Cloudflare D1 database. CC then serves campaign_data from D1 when its
CC_READMODEL_BACKEND var is flipped to "d1".

Runs ON the droplet (where warehouse.duckdb + duckdb live) but is a READ-ONLY
DuckDB consumer, so it is safe to run anytime -- it does NOT need the 03:30-05:45
UTC writer window. Slot it into nightly.sh AFTER the orchestrator finishes (so it
publishes fresh data), or run it on demand.

Column set = the EXACT subset of campaign_data that CC reads (see
campaign-control/migrations/d1/0002_campaign_data.sql for the field-by-field
provenance). Anything CC does not read is intentionally omitted to keep the
snapshot small.

D1 write transport: the Cloudflare D1 HTTP API
  POST /accounts/{account_id}/d1/database/{database_id}/query
batched as multiple SQL statements per request. We DELETE the whole table then
INSERT the current snapshot inside one logical publish so CC always sees a
complete, self-consistent set (freeze-on-delete in the warehouse means deleted
campaigns keep their last-known rows; we mirror that as-is).

Required environment (read from the repo .env via core.config or the process env):
  CLOUDFLARE_RG_ACCOUNT_ID   - Cloudflare account id
  CC_D1_DATABASE_ID          - CC state D1 database id
  CC_D1_API_TOKEN            - Cloudflare API token with D1:Edit on that account.
                               NOTE: a token must carry D1 scope. Mint a token
                               scoped to Account > D1 > Edit for the account and
                               set CC_D1_API_TOKEN before this runs in prod. Until
                               then, use --dry-run or --sql-out to exercise the
                               snapshot locally.

Usage:
    # On the droplet, after the nightly orchestrator:
    python scripts/publish_campaign_data_d1.py

    # Inspect what WOULD be published (no D1 writes), counts only:
    python scripts/publish_campaign_data_d1.py --dry-run

    # Emit the full SQL the publish would run (for local D1 parity testing):
    python scripts/publish_campaign_data_d1.py --sql-out /tmp/campaign_data.sql
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import duckdb

DEFAULT_DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
# CC state D1 database id — from env (account-specific id; not committed).
DEFAULT_D1_DATABASE_ID = os.environ.get("CC_D1_DATABASE_ID", "")

# The exact column set CC reads (order matters for the INSERT). v_disabled is
# emitted as 0/1 so it lands in SQLite as INTEGER (CC's d1-client decodes it back
# to a real boolean -- see d1-client.ts BOOL_COLUMNS).
COLUMNS = [
    "campaign_id",
    "campaign_name",
    "workspace_id",
    "status",
    "step",
    "variant",
    "emails_sent",
    "opportunities",
    "v_disabled",
    "daily_limit",
    "infra_type",
    "total_leads",
    "lead_sequence_started",
    "leads_completed",
    "leads_bounced",
    "leads_unsubscribed",
    "synced_at",
]

# Latest row per (campaign_id, step, variant). The warehouse mirror is
# insert_hash + freeze-on-delete, so multiple historical rows can exist per key
# when copy changes over time; we take the most-recently-synced one. (Today the
# snapshot happens to be 1 row per key, but dedup keeps the publisher correct as
# the mirror accumulates history.)
#
# PARITY NOTE (freeze-on-delete): the warehouse never deletes keys that vanish
# upstream, so a campaign deleted in Pipeline Supabase keeps its last-known rows
# here with a stale synced_at and (often) a frozen status='1'. Verified
# 2026-06-07: the warehouse showed 9 such "active" campaigns that Pipeline
# Supabase had already dropped (8 of them last synced 5 days prior). The delta
# is strictly additive -- 0 live campaigns are missing from the warehouse -- so
# publishing as-is can only surface a few EXTRA dead campaigns, which CC handles
# via its existing ghost-exemption / inactive-campaign paths (no bad kills). A
# time-window freshness filter is NOT safe by default: many genuinely-live
# campaigns are not re-touched every run and legitimately carry older synced_at.
# The optional --max-stale-days flag drops rows older than N days from the
# latest sync, for operators who want to trim clearly-frozen-deleted campaigns;
# default (None) = exact warehouse parity.
SELECT_SQL = f"""
WITH ranked AS (
    SELECT
        campaign_id,
        campaign_name,
        workspace_id,
        status,
        step,
        variant,
        emails_sent,
        opportunities,
        CASE WHEN v_disabled THEN 1 ELSE 0 END AS v_disabled,
        daily_limit,
        infra_type,
        total_leads,
        lead_sequence_started,
        leads_completed,
        leads_bounced,
        leads_unsubscribed,
        synced_at,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, step, variant
            ORDER BY synced_at DESC NULLS LAST, _loaded_at DESC NULLS LAST
        ) AS rn
    FROM raw_pipeline_campaign_data
    WHERE campaign_id IS NOT NULL AND step IS NOT NULL AND variant IS NOT NULL
)
SELECT {", ".join(COLUMNS)}
FROM ranked
WHERE rn = 1
"""

# Optional stale-trim variant: drop keys whose latest synced_at is older than
# N days before the table-wide max synced_at. Off by default (see PARITY NOTE).
SELECT_SQL_STALE = SELECT_SQL.rstrip() + (
    "\nAND synced_at >= ("
    "  SELECT max(synced_at) - INTERVAL {days} DAY FROM raw_pipeline_campaign_data"
    ")\n"
)


def fetch_rows(db_path: str, max_stale_days: int | None = None) -> list[tuple]:
    sql = SELECT_SQL if max_stale_days is None else SELECT_SQL_STALE.format(days=int(max_stale_days))
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _sql_literal(val) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return repr(val)
    # datetimes -> ISO string
    if isinstance(val, datetime):
        val = val.astimezone(timezone.utc).isoformat()
    s = str(val).replace("'", "''")
    return f"'{s}'"


def build_statements(rows: list[tuple], published_at: str, source: str) -> list[str]:
    """Build the ordered SQL statements for one atomic publish.

    DELETE the table, INSERT the snapshot (chunked), then refresh the publish
    meta row. D1's /query endpoint runs the statements in order in a single
    request; a failure rolls the batch back, so CC never sees a half-written
    table.
    """
    stmts: list[str] = ["DELETE FROM campaign_data;"]

    col_sql = ", ".join(COLUMNS)
    # Chunk INSERTs to keep each statement well under D1's SQL size limits.
    chunk = 200
    for i in range(0, len(rows), chunk):
        values = []
        for row in rows[i : i + chunk]:
            vals = ", ".join(_sql_literal(v) for v in row)
            values.append(f"({vals})")
        stmts.append(
            f"INSERT INTO campaign_data ({col_sql}) VALUES " + ", ".join(values) + ";"
        )

    stmts.append(
        "INSERT INTO campaign_data_publish_meta (id, published_at, row_count, source) "
        f"VALUES (1, {_sql_literal(published_at)}, {len(rows)}, {_sql_literal(source)}) "
        "ON CONFLICT (id) DO UPDATE SET "
        "published_at = excluded.published_at, "
        "row_count = excluded.row_count, "
        "source = excluded.source;"
    )
    return stmts


def d1_query(account_id: str, database_id: str, token: str, sql: str) -> dict:
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/d1/database/{database_id}/query"
    )
    body = json.dumps({"sql": sql}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def publish(account_id: str, database_id: str, token: str, statements: list[str]) -> None:
    # One request per statement keeps each payload small and isolates failures.
    # The DELETE runs first; if any INSERT fails we abort loudly (CC keeps its
    # last good D1 snapshot only if the DELETE had not yet run -- so on failure
    # AFTER the delete, re-run the publish; the flag can also be flipped back to
    # supabase for an instant rollback).
    for idx, sql in enumerate(statements):
        for attempt in range(3):
            try:
                res = d1_query(account_id, database_id, token, sql)
                if not res.get("success"):
                    raise RuntimeError(f"D1 query failed: {res.get('errors')}")
                break
            except (urllib.error.URLError, RuntimeError) as exc:
                if attempt == 2:
                    raise RuntimeError(
                        f"publish aborted at statement {idx}/{len(statements)}: {exc}"
                    ) from exc
                time.sleep(2 ** attempt)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB, help="warehouse DuckDB path")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="read + build the snapshot but make no D1 writes; print counts",
    )
    ap.add_argument(
        "--sql-out",
        default=None,
        help="write the publish SQL to this file instead of calling D1",
    )
    ap.add_argument(
        "--max-stale-days",
        type=int,
        default=None,
        help=(
            "drop keys whose latest synced_at is older than N days before the "
            "warehouse-wide max synced_at (trims frozen-deleted campaigns). "
            "OFF by default = exact warehouse parity; see PARITY NOTE."
        ),
    )
    args = ap.parse_args()

    published_at = datetime.now(timezone.utc).isoformat()
    source = "warehouse:raw_pipeline_campaign_data"

    rows = fetch_rows(args.db, args.max_stale_days)
    print(f"[publish_campaign_data_d1] read {len(rows)} rows from {args.db}", file=sys.stderr)

    statements = build_statements(rows, published_at, source)

    if args.sql_out:
        with open(args.sql_out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(statements) + "\n")
        print(f"[publish_campaign_data_d1] wrote {len(statements)} statements to {args.sql_out}", file=sys.stderr)
        return 0

    if args.dry_run:
        active = sum(1 for r in rows if r[3] in ("1", "Active") and r[4] == "__ALL__" and r[5] == "__ALL__")
        print(
            f"[publish_campaign_data_d1] DRY RUN: would publish {len(rows)} rows "
            f"({len(statements)} statements); {active} active campaigns (__ALL__ rollup, status in 1/Active)",
            file=sys.stderr,
        )
        return 0

    account_id = os.environ.get("CLOUDFLARE_RG_ACCOUNT_ID")
    database_id = os.environ.get("CC_D1_DATABASE_ID", DEFAULT_D1_DATABASE_ID)
    token = os.environ.get("CC_D1_API_TOKEN")
    missing = [
        name
        for name, val in (
            ("CLOUDFLARE_RG_ACCOUNT_ID", account_id),
            ("CC_D1_DATABASE_ID", database_id),
            ("CC_D1_API_TOKEN", token),
        )
        if not val
    ]
    if missing:
        print(
            f"[publish_campaign_data_d1] ERROR: missing env {', '.join(missing)}. "
            f"Mint a D1:Edit token for the account and set CC_D1_API_TOKEN. "
            f"Use --dry-run / --sql-out to test without it.",
            file=sys.stderr,
        )
        return 2

    publish(account_id, database_id, token, statements)
    print(
        f"[publish_campaign_data_d1] published {len(rows)} rows to D1 {database_id} @ {published_at}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
