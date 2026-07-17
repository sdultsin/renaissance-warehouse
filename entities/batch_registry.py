"""core.batch_registry — the per-batch roster David edits in the Data Hub (Fleet > Batches), mirrored to
the warehouse nightly so it is queryable alongside the rest of the fleet data.

The LIVE editable copy is a JSON file on the Hub's persistent volume; this phase full-replaces
core.batch_registry from the Hub's token-gated /api/batches_export. Retires the old "Batches" Google
Sheet. [2026-07-17, David: "I write this in the Hub, and we have all the information in the warehouse
... we don't need the sheet anymore at all"]

Full replace each run (DELETE + INSERT in one txn) = idempotent. Graceful: skips cleanly if the table
is missing or HUB_BATCH_EXPORT_URL is not set, and never fails the whole run over one optional mirror.
Schema: sql/ddl/1123_batch_registry.sql.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.batch_registry")

def _hub_url() -> str:
    """The token-gated Hub export URL. Read from os.environ first, then fall back to the repo .env via
    dotenv_values — because nightly.sh does NOT shell-source .env (a ')' in a comment breaks `source`),
    so a plain os.environ lookup would miss it and the mirror would silently no-op. [2026-07-17]"""
    u = os.environ.get("HUB_BATCH_EXPORT_URL", "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get("HUB_BATCH_EXPORT_URL", "") or ""
    except Exception:
        return ""
_COLS = ["batch_key", "provider", "workspace", "n_domains", "n_inboxes", "sip_date",
         "warmup_start", "cold_start", "billing_date", "offer", "email_provider",
         "batch_url", "notes", "updated_at", "updated_by"]


def _table_exists(conn) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'core' AND table_name = 'batch_registry'").fetchone()[0] > 0


def run_batch_registry(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn):
        logger.error("batch_registry SKIP: table missing (ddl 1123 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    hub_url = _hub_url()
    if not hub_url:
        logger.error("batch_registry SKIP: HUB_BATCH_EXPORT_URL not set (env or .env).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_url"})
    try:
        with urllib.request.urlopen(hub_url, timeout=60) as resp:
            rows = (json.loads(resp.read()) or {}).get("rows", [])
    except Exception as exc:  # one optional mirror must never break the nightly run
        logger.error("batch_registry SKIP: hub fetch failed: %s", str(exc)[:140])
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "fetch_failed"})

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.batch_registry")
        placeholders = ", ".join(["?"] * len(_COLS))
        for r in rows:
            conn.execute(
                f"INSERT INTO core.batch_registry ({', '.join(_COLS)}, _loaded_at, _run_id) "
                f"VALUES ({placeholders}, now(), ?)",
                [str(r.get(c) if r.get(c) is not None else "") for c in _COLS] + [ctx.run_id],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = len(rows)
    logger.info("batch_registry: mirrored %d batches from the Hub.", n)
    return PhaseResult(rows_in=n, rows_out=n, notes={"batches": n})


def register(registry: Registry) -> None:
    # portal_core runs in PASS A and is where the Data-Hub-facing core tables land, so the morning
    # snapshot includes the batch roster. Reads no upstream warehouse table — the source is the Hub.
    registry.add_phase("portal_core", "batch_registry", run_batch_registry)
