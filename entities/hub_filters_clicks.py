"""core.hub_saved_filter + core.hub_click_log — nightly mirrors of the Hub's last two
Railway-only datasets. [2026-07-22 history brief §3b — priorities 4 and 5]
Saved filters: full-replace with the empty-payload guard. Click log: APPEND-ONLY — pulls
?since=<max ts held>, so the warehouse copy only grows and a Hub volume loss cannot shrink it.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.hub_filters_clicks")


def _url(var: str) -> str:
    u = os.environ.get(var, "")
    if u:
        return u
    try:
        from dotenv import dotenv_values
        from core.config import REPO_ROOT
        return (dotenv_values(str(REPO_ROOT / ".env")) or {}).get(var, "") or ""
    except Exception:
        return ""


def run_hub_filters_clicks(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    have = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='core' "
        "AND table_name IN ('hub_saved_filter','hub_click_log')").fetchone()[0]
    if have < 2:
        logger.error("hub_filters_clicks SKIP: tables missing (ddl 1155 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    notes, n_in, n_out = {}, 0, 0

    fu = _url("HUB_FILTERS_EXPORT_URL")
    if fu:
        try:
            rows = (json.loads(urllib.request.urlopen(fu, timeout=60).read()) or {}).get("rows", [])
            n_in += len(rows)
            if rows:
                conn.execute("BEGIN")
                try:
                    conn.execute("DELETE FROM core.hub_saved_filter")
                    conn.executemany("INSERT INTO core.hub_saved_filter VALUES (?,?, now())",
                                     [[r.get("name"), r.get("payload")] for r in rows])
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK"); raise
                n_out += len(rows)
            else:
                held = conn.execute("SELECT count(*) FROM core.hub_saved_filter").fetchone()[0]
                if held:
                    logger.error("hub_saved_filter: 0 rows vs %d held — refusing to wipe.", held)
            notes["filters"] = len(rows)
        except Exception as exc:
            logger.error("hub_saved_filter SKIP: %s", str(exc)[:120])
    else:
        logger.error("hub_filters_clicks: HUB_FILTERS_EXPORT_URL not set.")

    cu = _url("HUB_CLICK_LOG_EXPORT_URL")
    if cu:
        try:
            since = conn.execute("SELECT coalesce(CAST(max(ts) AS VARCHAR),'') FROM core.hub_click_log").fetchone()[0]
            sep = "&" if "?" in cu else "?"
            u = cu + ((sep + "since=" + urllib.request.quote(since)) if since else "")
            rows = (json.loads(urllib.request.urlopen(u, timeout=120).read()) or {}).get("rows", [])
            n_in += len(rows)
            if rows:
                conn.executemany(
                    "INSERT INTO core.hub_click_log VALUES (?,?,?,?,?,?,?,?,?, now())",
                    [[r.get("ts"), r.get("email"), r.get("tab"), r.get("label"), r.get("kind"),
                      r.get("fx"), r.get("fy"), r.get("vw"), r.get("vh")] for r in rows])
                n_out += len(rows)
            notes["clicks_appended"] = len(rows)
        except Exception as exc:
            logger.error("hub_click_log SKIP: %s", str(exc)[:120])
    else:
        logger.error("hub_filters_clicks: HUB_CLICK_LOG_EXPORT_URL not set.")

    logger.info("hub_filters_clicks: %s", notes)
    return PhaseResult(rows_in=n_in, rows_out=n_out, notes=notes)


def register(registry: Registry) -> None:
    registry.add_phase("portal_core", "hub_filters_clicks", run_hub_filters_clicks)
