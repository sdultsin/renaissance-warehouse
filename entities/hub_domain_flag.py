"""core.hub_domain_flag — nightly mirror of the Inbox Hub's website/special domain marks.
[2026-07-22 history brief §3b — migration priority 3] Human-typed, not re-derivable: these marks
keep our own websites out of the reuse pool. Full-replace (a removed mark must disappear here too)
with the same empty-payload guard as inbox_warmup_override — 0 rows against a non-empty table is
treated as a Hub failure and refused loudly, never mirrored as a wipe.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.hub_domain_flag")


def _hub_url() -> str:
    u = os.environ.get("HUB_DOMAIN_FLAG_EXPORT_URL", "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get(
            "HUB_DOMAIN_FLAG_EXPORT_URL", "") or ""
    except Exception:
        return ""


def run_hub_domain_flag(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='hub_domain_flag'").fetchone()[0]:
        logger.error("hub_domain_flag SKIP: table missing (ddl 1153 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    hub_url = _hub_url()
    if not hub_url:
        logger.error("hub_domain_flag SKIP: HUB_DOMAIN_FLAG_EXPORT_URL not set.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_url"})
    try:
        with urllib.request.urlopen(hub_url, timeout=120) as resp:
            rows = (json.loads(resp.read()) or {}).get("rows", [])
    except Exception as exc:
        logger.error("hub_domain_flag SKIP: hub fetch failed: %s", str(exc)[:140])
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "fetch_failed"})
    if not rows:
        held = conn.execute("SELECT count(*) FROM core.hub_domain_flag").fetchone()[0]
        if held:
            logger.error("hub_domain_flag SKIP: hub returned 0 rows but we hold %d — refusing "
                         "to wipe on an ambiguous empty response.", held)
            return PhaseResult(rows_in=0, rows_out=held, notes={"skipped": "empty_payload_guard"})

    seen, clean = set(), []
    for r in rows:
        d = str(r.get("domain") or "").strip().lower()
        if not d or d in seen:
            continue
        seen.add(d)
        clean.append([d, str(r.get("kind") or "")])
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.hub_domain_flag")
        conn.executemany("INSERT INTO core.hub_domain_flag VALUES (?, ?, now())", clean)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info("hub_domain_flag: mirrored %d marks from the Hub.", len(clean))
    return PhaseResult(rows_in=len(rows), rows_out=len(clean), notes={"marks": len(clean)})


def register(registry: Registry) -> None:
    registry.add_phase("portal_core", "hub_domain_flag", run_hub_domain_flag)
