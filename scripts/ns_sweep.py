#!/usr/bin/env python3
"""Track I — nameserver (NS) sweep. The existing DNS sweep captures MX/A/SPF/DKIM/
DMARC/PTR but NOT NS records, so core.domain_registry.nameserver_host is 0%.

This resolves NS records concurrently and writes them to a parquet file the
domain_registry backfill then loads. Reads the domain list from a READ-ONLY DuckDB
(the serving copy by default, so it never contends the warehouse writer lock) and
writes only to a flat file — fully lock-free.

By default targets ACTIVE domains (assigned to a workspace or with inboxes) — the set
the DoD measures (NS coverage >=90% of active domains).

Usage:
    python scripts/ns_sweep.py --out /root/core/ns_sweep.parquet
    python scripts/ns_sweep.py --all --out /root/core/ns_sweep_all.parquet
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import dns.resolver
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.ns_sweep")

_RES = dns.resolver.Resolver()
_RES.lifetime = 5.0
_RES.timeout = 5.0


def resolve_ns(domain: str) -> tuple[str, list[str]]:
    try:
        ans = _RES.resolve(domain, "NS")
        hosts = sorted({str(r.target).rstrip(".").lower() for r in ans})
        return domain, hosts
    except Exception:
        return domain, []


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-db", default="/root/core/warehouse.duckdb",
                    help="read-only DB to pull the domain list from")
    ap.add_argument("--out", required=True)
    ap.add_argument("--all", action="store_true", help="sweep all domains, not just active")
    ap.add_argument("--missing-only", action="store_true",
                    help="only sweep domains whose nameserver_host is still NULL (delta/drift sweep)")
    ap.add_argument("--workers", type=int, default=60)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)

    con = duckdb.connect(args.src_db, read_only=True)
    where = "" if args.all else \
        "WHERE (assigned_workspace IS NOT NULL OR COALESCE(inbox_count,0) > 0)"
    if args.missing_only:
        where += (" AND " if where else "WHERE ") + "nameserver_host IS NULL"
    lim = f" LIMIT {args.limit}" if args.limit else ""
    domains = [r[0] for r in con.execute(
        f"SELECT DISTINCT domain FROM core.domain_registry {where}{lim}"
    ).fetchall() if r[0]]
    con.close()
    logger.info("sweeping NS for %d domains (workers=%d, all=%s)", len(domains), args.workers, args.all)

    rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(resolve_ns, d) for d in domains]
        for fut in as_completed(futs):
            domain, hosts = fut.result()
            rows.append((domain, hosts[0] if hosts else None, hosts))
            done += 1
            if done % 10000 == 0:
                logger.info("  %d/%d", done, len(domains))

    resolved = sum(1 for _, h, _ in rows if h)
    logger.info("resolved NS for %d/%d (%.1f%%)", resolved, len(rows), 100.0 * resolved / max(len(rows), 1))

    # write parquet via a scratch in-memory duckdb (no warehouse contention)
    now = dt.datetime.now(dt.timezone.utc)
    out = duckdb.connect()
    out.execute("CREATE TABLE ns (domain VARCHAR, nameserver_host VARCHAR, nameservers VARCHAR[], ns_resolved_at TIMESTAMPTZ)")
    out.executemany("INSERT INTO ns VALUES (?,?,?,?)", [[d, h, hs, now] for d, h, hs in rows])
    out.execute(f"COPY ns TO '{args.out}' (FORMAT PARQUET)")
    out.close()
    logger.info("wrote %s", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
