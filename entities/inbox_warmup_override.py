"""core.inbox_warmup_override — nightly mirror of the per-inbox warm-up-start overrides David sets in
the Inbox Hub.

Deliberately a copy of entities/batch_registry.py, not a new mechanism: the LIVE editable copy is a
JSON file on the Hub's persistent volume, and this phase FULL-REPLACES the warehouse table from the
Hub's token-gated export. One writer (the Hub), one write path. Claude Code edits go through the same
Hub endpoint — never a direct warehouse write.

Full replace in one transaction = idempotent, and it is the correct semantic here: an override that
David REMOVES in the Hub must disappear from the warehouse, which an append or upsert would not do.
That is the opposite of core.inbox_date_history (DDL 1149), which is append-only because it records
observed facts rather than a current decision.

Graceful: skips cleanly if the table is missing or HUB_WARMUP_OVERRIDE_EXPORT_URL is unset, and never
fails the whole run over one optional mirror. Schema: sql/ddl/1150_inbox_warmup_override.sql
[2026-07-21, David]
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.inbox_warmup_override")

_COLS = ["email", "warmup_start_override", "reason", "set_by", "set_at"]


def _hub_url() -> str:
    """Token-gated Hub export URL. os.environ first, then the repo .env via dotenv_values — nightly.sh
    does NOT shell-source .env (a ')' in a comment breaks `source`), so an os.environ-only lookup would
    miss it and the mirror would silently no-op. Same trap batch_registry documents."""
    u = os.environ.get("HUB_WARMUP_OVERRIDE_EXPORT_URL", "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get(
            "HUB_WARMUP_OVERRIDE_EXPORT_URL", "") or ""
    except Exception:
        return ""


def _table_exists(conn) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'core' AND table_name = 'inbox_warmup_override'").fetchone()[0] > 0


def run_inbox_warmup_override(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn):
        logger.error("inbox_warmup_override SKIP: table missing (ddl 1150 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    hub_url = _hub_url()
    if not hub_url:
        logger.error("inbox_warmup_override SKIP: HUB_WARMUP_OVERRIDE_EXPORT_URL not set.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_url"})
    try:
        with urllib.request.urlopen(hub_url, timeout=120) as resp:
            rows = (json.loads(resp.read()) or {}).get("rows", [])
    except Exception as exc:   # one optional mirror must never break the nightly
        logger.error("inbox_warmup_override SKIP: hub fetch failed: %s", str(exc)[:140])
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "fetch_failed"})

    # An EMPTY payload is ambiguous — it means either "David removed every override" or "the Hub
    # answered with nothing". Deleting real overrides on a bad response would silently change what
    # the Activation Pipeline flips, so an empty result NEVER wipes a non-empty table.
    if not rows:
        held = conn.execute("SELECT count(*) FROM core.inbox_warmup_override").fetchone()[0]
        if held:
            logger.error("inbox_warmup_override SKIP: hub returned 0 rows but we hold %d — "
                         "refusing to wipe on an ambiguous empty response.", held)
            return PhaseResult(rows_in=0, rows_out=held, notes={"skipped": "empty_payload_guard"})

    seen, clean = set(), []
    for r in rows:
        e = str(r.get("email") or "").strip().lower()
        if not e or e in seen:            # the unique index would reject a dupe and abort the txn
            continue
        seen.add(e)
        clean.append([e] + [str(r.get(c) if r.get(c) is not None else "") for c in _COLS[1:]])

    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.inbox_warmup_override")
        ph = ", ".join(["?"] * len(_COLS))
        for row in clean:
            conn.execute(
                f"INSERT INTO core.inbox_warmup_override ({', '.join(_COLS)}, _loaded_at, _run_id) "
                f"VALUES ({ph}, now(), ?)", row + [ctx.run_id])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = len(clean)
    logger.info("inbox_warmup_override: mirrored %d overrides from the Hub (%d payload rows).",
                n, len(rows))
    return PhaseResult(rows_in=len(rows), rows_out=n, notes={"overrides": n})


def register(registry: Registry) -> None:
    # portal_core (PASS A), same as batch_registry: it is where the Data-Hub-facing core tables land,
    # and it must run BEFORE anything that reads the override — v_inbox_overview coalesces it.
    registry.add_phase("portal_core", "inbox_warmup_override", run_inbox_warmup_override)
