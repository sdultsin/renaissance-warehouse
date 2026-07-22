"""core.hub_problem_ack(+_log,_member) — nightly mirror of the Hub's checklist acknowledgements.
[2026-07-22 history brief §3b — priority 2] Human-typed, not re-derivable. Same pattern as
entities/hub_domain_flag.py: full-replace with the empty-payload guard (an ack the Hub removed
must disappear here; a 0-row payload against non-empty tables is refused loudly, never a wipe).
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.hub_problem_ack")


def _hub_url() -> str:
    u = os.environ.get("HUB_PROBLEM_ACK_EXPORT_URL", "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get(
            "HUB_PROBLEM_ACK_EXPORT_URL", "") or ""
    except Exception:
        return ""


def run_hub_problem_ack(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='hub_problem_ack'").fetchone()[0]:
        logger.error("hub_problem_ack SKIP: table missing (ddl 1154 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    hub_url = _hub_url()
    if not hub_url:
        logger.error("hub_problem_ack SKIP: HUB_PROBLEM_ACK_EXPORT_URL not set.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_url"})
    try:
        with urllib.request.urlopen(hub_url, timeout=120) as resp:
            data = json.loads(resp.read()) or {}
    except Exception as exc:
        logger.error("hub_problem_ack SKIP: hub fetch failed: %s", str(exc)[:140])
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "fetch_failed"})
    acks, log, members = data.get("acks", []), data.get("log", []), data.get("members", [])
    n_in = len(acks) + len(log) + len(members)
    if n_in == 0:
        held = conn.execute("SELECT (SELECT count(*) FROM core.hub_problem_ack)"
                            "+(SELECT count(*) FROM core.hub_problem_ack_log)"
                            "+(SELECT count(*) FROM core.hub_problem_ack_member)").fetchone()[0]
        if held:
            logger.error("hub_problem_ack SKIP: hub returned 0 rows but we hold %d — refusing "
                         "to wipe on an ambiguous empty response.", held)
            return PhaseResult(rows_in=0, rows_out=held, notes={"skipped": "empty_payload_guard"})
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.hub_problem_ack")
        # DuckDB's executemany RAISES on an empty parameter list — an empty sub-table (e.g. no log
        # rows yet) must not abort the whole mirror. Found live on the first backfill run.
        if acks: conn.executemany(
            "INSERT INTO core.hub_problem_ack VALUES (?,?,?,?,?,?,?,?,?,?, now())",
            [[r.get("gkey"), r.get("problem"), r.get("workspace"), bool(r.get("acked")),
              r.get("note"), r.get("acked_by"), r.get("acked_at"), r.get("first_flagged"),
              r.get("last_seen"), int(r.get("last_count") or 0)] for r in acks])
        conn.execute("DELETE FROM core.hub_problem_ack_log")
        if log: conn.executemany(
            "INSERT INTO core.hub_problem_ack_log VALUES (?,?,?,?,?,?,?,?, now())",
            [[r.get("ts"), r.get("gkey"), r.get("problem"), r.get("workspace"),
              int(r.get("cnt") or 0), r.get("action"), r.get("note"), r.get("who")] for r in log])
        conn.execute("DELETE FROM core.hub_problem_ack_member")
        if members: conn.executemany(
            "INSERT INTO core.hub_problem_ack_member VALUES (?,?,?, now())",
            [[r.get("gkey"), r.get("email"), r.get("snap_at")] for r in members])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info("hub_problem_ack: mirrored %d acks / %d log rows / %d members.",
                len(acks), len(log), len(members))
    return PhaseResult(rows_in=n_in, rows_out=n_in,
                       notes={"acks": len(acks), "log": len(log), "members": len(members)})


def register(registry: Registry) -> None:
    registry.add_phase("portal_core", "hub_problem_ack", run_hub_problem_ack)
