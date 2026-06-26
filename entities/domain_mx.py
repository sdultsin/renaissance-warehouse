"""domain_mx: nightly MX/DNS health for sending domains -> core.account_domain_dns.

That table shipped empty because its only filler was the HEAVY full DNS sweep
(entities/dns_sweep.py) that is run MANUALLY off-cron (60-90 min, holds the write lock).
This is a LIGHTWEIGHT, MX-only, BOUNDED generator that DOES run in the nightly: it resolves
MX for sending domains that are new or stale (>14d), capped per run, so it backfills the
~56k domains over a few nights then stays incremental (the DDL-112 "backfills over runs"
intent). Lets the Inbox Hub flag inboxes whose domain has NO MX (can't warm up / receive).

Resolve is in-memory + concurrent (no DB lock held); only the final writes touch the DB.
Engine = sources/dns.resolve_mx (handles resolver/error gotchas). Registers under the
'instantly' phase (guaranteed nightly), NOT 'dns_sweep' (which is off-cron).
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.dns import resolve_mx

logger = logging.getLogger("entities.domain_mx")

_LIMIT = int(os.environ.get("DOMAIN_MX_LIMIT", "15000"))   # domains per nightly run
_WORKERS = int(os.environ.get("DOMAIN_MX_WORKERS", "40"))
_STALE_DAYS = 14


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "domain_mx", run_domain_mx)


def run_domain_mx(ctx: RunContext) -> PhaseResult:
    # sending domains that are new OR not checked in _STALE_DAYS, oldest-checked first
    rows = ctx.db.execute(
        f"""
        WITH dom AS (
            SELECT DISTINCT lower("domain") AS domain
            FROM core.account_census
            WHERE census_date = (SELECT max(census_date) FROM core.account_census)
              AND NULLIF("domain", '') IS NOT NULL
        )
        SELECT dom.domain
        FROM dom
        LEFT JOIN core.account_domain_dns d ON d.domain = dom.domain
        WHERE d.domain IS NULL OR d.dns_checked_at < now() - INTERVAL '{_STALE_DAYS} days'
        ORDER BY d.dns_checked_at NULLS FIRST
        LIMIT {_LIMIT}
        """
    ).fetchall()
    domains = [r[0] for r in rows]
    if not domains:
        return PhaseResult(notes={"checked": 0, "note": "all domains fresh"})

    now = datetime.now(timezone.utc)
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        futs = {ex.submit(resolve_mx, dom): dom for dom in domains}
        for f in as_completed(futs):
            dom = futs[f]
            try:
                results[dom] = f.result()
            except Exception:  # noqa: BLE001 — a single resolver failure must not kill the run
                results[dom] = None

    missing = 0
    for dom, mx in results.items():
        recs = (mx or {}).get("mx_records") or []
        has_mx = bool(recs)
        if not has_mx:
            missing += 1
        prov = (mx or {}).get("mx_provider")
        # UPSERT only the MX columns — do NOT delete the row, so the SPF/DKIM/DMARC/
        # registrar columns that the heavy dns_sweep / RDAP jobs fill on the SAME table
        # are preserved (a DELETE+INSERT here would wipe them — moderator-caught).
        ctx.db.execute(
            "INSERT INTO core.account_domain_dns "
            "(domain, has_mx, mx_provider, dns_checked_at, _loaded_at, _run_id) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (domain) DO UPDATE SET "
            "  has_mx = excluded.has_mx, mx_provider = excluded.mx_provider, "
            "  dns_checked_at = excluded.dns_checked_at, _loaded_at = excluded._loaded_at, "
            "  _run_id = excluded._run_id",
            [dom, has_mx, prov, now, now, ctx.run_id],
        )

    logger.info("domain_mx: checked %d domains, %d with NO MX", len(domains), missing)
    return PhaseResult(notes={"checked": len(domains), "no_mx": missing})
