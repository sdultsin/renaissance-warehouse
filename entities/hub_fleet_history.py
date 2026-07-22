"""core.hub_fleet_history — nightly mirror of the Inbox Hub's provider / provstate / lifecycle
history. [2026-07-22 history brief §3b, David's decision — migration priority 1]

Same Hub-export mechanism as entities/batch_registry.py / inbox_warmup_override.py, with ONE
deliberate difference: this loader UPSERTS BY (metric, d) and NEVER deletes days absent from the
export. Durability is the point — if the Hub's Railway volume dies, the Hub restarts empty; a
full-replace mirror would wipe the surviving warehouse copy at the next nightly, which is the
exact moment it must not. The warehouse deliberately keeps days the Hub no longer has.

Why not recompute instead of mirror: these metrics join the census to core.v_inbox_overview — a
CURRENT-state view — so a recompute labels past days with today's provider and silently loses
deleted inboxes. The Hub's stored rows are the truth for their day (brief §3, verified).
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.hub_fleet_history")


def _hub_url() -> str:
    u = os.environ.get("HUB_FLEET_HISTORY_EXPORT_URL", "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get(
            "HUB_FLEET_HISTORY_EXPORT_URL", "") or ""
    except Exception:
        return ""


def run_hub_fleet_history(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='hub_fleet_history'").fetchone()[0]:
        logger.error("hub_fleet_history SKIP: table missing (ddl 1152 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    hub_url = _hub_url()
    if not hub_url:
        logger.error("hub_fleet_history SKIP: HUB_FLEET_HISTORY_EXPORT_URL not set.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_url"})
    try:
        with urllib.request.urlopen(hub_url, timeout=180) as resp:
            rows = (json.loads(resp.read()) or {}).get("rows", [])
    except Exception as exc:   # one optional mirror must never break the nightly
        logger.error("hub_fleet_history SKIP: hub fetch failed: %s", str(exc)[:140])
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "fetch_failed"})
    if not rows:
        # empty export = Hub problem or fresh volume; upsert semantics make this a natural no-op,
        # but say it loudly so a silently-dead export cannot pass for a healthy one.
        logger.error("hub_fleet_history: hub returned 0 rows — nothing upserted, mirror unchanged.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "empty_payload"})

    clean = []
    for r in rows:
        m, d = str(r.get("metric") or ""), str(r.get("d") or "")[:10]
        if m not in ("provider", "provstate", "lifecycle") or len(d) != 10:
            continue
        clean.append([m, d, str(r.get("ws") or ""), str(r.get("k") or ""),
                      int(r.get("inbox_n") or 0), int(r.get("dom_n") or 0)])
    days = sorted({(r[0], r[1]) for r in clean})
    conn.execute("BEGIN")
    try:
        # upsert by (metric, d): replace exactly the days the export carries, keep all others
        conn.execute("CREATE OR REPLACE TEMP TABLE _hfh AS "
                     "SELECT * FROM core.hub_fleet_history LIMIT 0")
        if clean:   # executemany raises on an empty list (same trap as hub_problem_ack)
            conn.executemany("INSERT INTO _hfh VALUES (?,?,?,?,?,?, now())", clean)
        conn.execute(
            "DELETE FROM core.hub_fleet_history WHERE (metric, d) IN (SELECT DISTINCT metric, d FROM _hfh)")
        conn.execute("INSERT INTO core.hub_fleet_history SELECT * FROM _hfh")
        conn.execute("DROP TABLE _hfh")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    total = conn.execute("SELECT count(*) FROM core.hub_fleet_history").fetchone()[0]
    logger.info("hub_fleet_history: upserted %d rows across %d metric-days; table now %d rows.",
                len(clean), len(days), total)
    return PhaseResult(rows_in=len(rows), rows_out=len(clean),
                       notes={"metric_days": len(days), "table_total": int(total)})


def register(registry: Registry) -> None:
    registry.add_phase("portal_core", "hub_fleet_history", run_hub_fleet_history)
