#!/usr/bin/env python3
"""Hourly campaign-counter snapshot -> raw_instantly_campaign_hourly_snapshot (DDL 1095).

WHY (2026-07-10, Ben's send-time ask): "reply rate by SEND hour" is not
computable from anything the warehouse stores — Instantly only exposes daily
grain and nothing records at what hour emails actually went out. This job
snapshots each campaign's CUMULATIVE counters once per hour; sends-per-hour is
the successive diff between consecutive snapshots (see v_campaign_sends_hourly).

Hourly sibling of entities/campaign_analytics_snapshot.py (the DAILY snapshot,
v1066): same endpoint (GET /campaigns/analytics, one call per workspace — the
only source whose counters match the Instantly UI), same differencing logic,
finer grain. Direct REST via sources.instantly.InstantlyClient (serial across
workspaces, built-in 429/5xx backoff) — NEVER the Instantly MCP.

DESIGN
  * Phase 1 (no DB writer lock held): pull every workspace's campaign analytics
    into memory. Keys come from INSTANTLY_KEY_<SLUG> in .env.instantly via
    core.credentials (by NAME — values are never logged). Duplicate keys for
    the same workspace are skipped; a failing workspace is logged and skipped
    (a missed workspace-hour is a gap; diffs tolerate gaps).
  * Phase 2 (writer lock, seconds): UPSERT one row per (snapshot_hour,
    workspace_id, campaign_id) — a same-hour re-run overwrites (idempotent).
  * A missed tick is fine by design. Exit 0 if at least one workspace landed;
    exit 1 only when NOTHING could be pulled (that is what the cron wrapper's
    consecutive-failure alert counts).

Scheduling: cron :52 hourly under scripts/with_warehouse_lock.sh (see
scripts/instantly_hourly_snapshot.sh). ~20 API calls/hour across keys — trivial
vs Instantly rate limits.

USAGE
    python scripts/instantly_hourly_snapshot.py             # real tick
    python scripts/instantly_hourly_snapshot.py --dry-run   # pull + report, no write
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from core import db as db_module
from core.credentials import load_credentials
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("scripts.instantly_hourly_snapshot")

# Cumulative counter fields copied 1:1 from the /campaigns/analytics JSON.
# Matches the column list in sql/ddl/1095_campaign_hourly_snapshot.sql.
_JSON_FIELDS = [
    "campaign_name", "campaign_status",
    "leads_count", "contacted_count", "new_leads_contacted_count",
    "emails_sent_count", "reply_count", "reply_count_unique",
    "reply_count_automatic", "bounced_count", "unsubscribed_count",
    "completed_count", "total_opportunities", "total_opportunity_value",
    "link_click_count", "open_count",
]

_COLS = ["snapshot_hour", "campaign_id", "workspace_id", "workspace_slug",
         "snapshot_ts"] + _JSON_FIELDS + ["_loaded_at", "_run_id"]

_UPSERT_SQL = (
    f"INSERT INTO raw_instantly_campaign_hourly_snapshot ({', '.join(_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _COLS)}) "
    "ON CONFLICT (snapshot_hour, workspace_id, campaign_id) DO UPDATE SET "
    + ", ".join(f"{c} = excluded.{c}" for c in _COLS
                if c not in ("snapshot_hour", "workspace_id", "campaign_id"))
)


def pull_all_workspaces() -> tuple[list[list], list[str], list[dict]]:
    """Phase 1 — API pulls only, NO warehouse lock needed. Returns (rows, done, failures)."""
    keys = load_credentials().instantly_workspace_keys()
    if not keys:
        raise RuntimeError("No INSTANTLY_KEY_* found in env/.env.instantly")

    now = datetime.now(timezone.utc)
    snapshot_hour = now.replace(minute=0, second=0, microsecond=0)
    run_id = now.strftime("%Y%m%dT%H%M%SZ-hourly-snap")

    rows: list[list] = []
    workspaces_done: list[str] = []
    failures: list[dict] = []
    seen_workspace_ids: set[str] = set()

    for slug in sorted(keys):
        try:
            with InstantlyClient(keys[slug]) as client:
                ws = client.get_current_workspace()
                workspace_id = ws.get("id")
                if not workspace_id:
                    failures.append({"slug": slug, "error": "missing_workspace_id"})
                    continue
                if workspace_id in seen_workspace_ids:
                    logger.info("Skipping duplicate workspace slug=%s", slug)
                    continue
                seen_workspace_ids.add(workspace_id)

                w_rows = 0
                for a in client.campaign_analytics():
                    campaign_id = a.get("campaign_id")
                    if not campaign_id:
                        continue
                    rows.append(
                        [snapshot_hour, campaign_id,
                         a.get("workspace_id") or workspace_id, slug, now]
                        + [a.get(f) for f in _JSON_FIELDS]
                        + [now, run_id]
                    )
                    w_rows += 1
                workspaces_done.append(slug)
                logger.info("Workspace %s: %d campaign rows", slug, w_rows)
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, str(exc)[:300])
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return rows, workspaces_done, failures


def write_rows(rows: list[list]) -> None:
    """Phase 2 — writer-locked UPSERT (seconds; API work already done)."""
    conn = db_module.connect()
    conn.execute("BEGIN")
    try:
        conn.executemany(_UPSERT_SQL, rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="pull + report, no write")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")

    rows, done, failures = pull_all_workspaces()
    if not done:
        logger.error("ALL workspaces failed (%d failures) — nothing to write", len(failures))
        return 1

    if args.dry_run:
        logger.info("DRY-RUN: would upsert %d rows from %d workspaces (failures: %s)",
                    len(rows), len(done), [f["slug"] for f in failures] or "none")
        return 0

    write_rows(rows)
    logger.info("Upserted %d rows from %d workspaces (failures: %s)",
                len(rows), len(done), [f["slug"] for f in failures] or "none")
    return 0


if __name__ == "__main__":
    sys.exit(main())
