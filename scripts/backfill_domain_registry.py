#!/usr/bin/env python3
"""Track I — backfill core.domain_registry nameserver_host + ns_at_cloudflare from the
NS sweep (scripts/ns_sweep.py output) plus the registrar/Cloudflare raw tables.

Coverage already complete (no-op here): tld 100%, registrar 100%, registrar_account
100%, CF 51 zones. This fills the NS gap (nameserver_host was 0%).

purchased_at (20.7%) is NOT improved here — reaching ~95% needs registrar-API pulls for
the ~120k domains not in raw_registrar_domains, which is a separate batched job. See
deliverables/2026-06-08-warehouse-hardening-session.md.

Needs the single-writer lock (run in a quiet window / wrap in flock).

Usage:
    python scripts/backfill_domain_registry.py --ns /root/core/ns_sweep.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.backfill_domain_registry")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--ns", default="/root/core/ns_sweep.parquet")
    args = ap.parse_args(argv)

    conn = db_module.connect(Path(args.db) if args.db else None)

    before = conn.execute(
        "SELECT round(100.0*avg((nameserver_host IS NOT NULL)::int),1) FROM core.domain_registry"
    ).fetchone()[0]

    conn.execute("BEGIN")
    try:
        # 1. NS from the sweep (primary, covers active domains).
        if Path(args.ns).exists():
            conn.execute("CREATE OR REPLACE TEMP TABLE ns_swept AS SELECT * FROM read_parquet(?)", [args.ns])
            conn.execute(
                """
                UPDATE core.domain_registry d
                SET nameserver_host = s.nameserver_host
                FROM ns_swept s
                WHERE s.domain = d.domain AND s.nameserver_host IS NOT NULL
                  AND d.nameserver_host IS NULL
                """
            )
        else:
            logger.warning("NS parquet %s not found — skipping sweep load", args.ns)

        # 2. Fallback NS from registrar.nameservers (first entry) where still null.
        conn.execute(
            """
            UPDATE core.domain_registry d
            SET nameserver_host = lower(rtrim(
                  CASE WHEN typeof(r.nameservers) LIKE 'VARCHAR[]%' THEN r.nameservers[1]
                       ELSE try_cast(json_extract_string(r.nameservers, '$[0]') AS VARCHAR) END, '.'))
            FROM raw_registrar_domains r
            WHERE r.domain = d.domain AND d.nameserver_host IS NULL AND r.nameservers IS NOT NULL
            """
        )

        # 3. Fallback NS from Cloudflare zones where still null.
        conn.execute(
            """
            UPDATE core.domain_registry d
            SET nameserver_host = lower(rtrim(
                  CASE WHEN typeof(z.nameservers) LIKE 'VARCHAR[]%' THEN z.nameservers[1]
                       ELSE try_cast(json_extract_string(z.nameservers, '$[0]') AS VARCHAR) END, '.'))
            FROM raw_cloudflare_zones z
            WHERE z.domain = d.domain AND d.nameserver_host IS NULL AND z.nameservers IS NOT NULL
            """
        )

        # 4. Refresh ns_at_cloudflare from the resolved NS host + the CF zone list.
        conn.execute(
            """
            UPDATE core.domain_registry d
            SET ns_at_cloudflare = (
                COALESCE(d.nameserver_host LIKE '%cloudflare%', FALSE)
                OR d.domain IN (SELECT domain FROM raw_cloudflare_zones)
            )
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    after_total = conn.execute(
        "SELECT round(100.0*avg((nameserver_host IS NOT NULL)::int),1) FROM core.domain_registry"
    ).fetchone()[0]
    after_active = conn.execute(
        "SELECT round(100.0*avg((nameserver_host IS NOT NULL)::int),1) FROM core.domain_registry "
        "WHERE assigned_workspace IS NOT NULL OR COALESCE(inbox_count,0) > 0"
    ).fetchone()[0]
    cf = conn.execute("SELECT count(*) FROM core.domain_registry WHERE ns_at_cloudflare").fetchone()[0]
    logger.info("nameserver_host coverage: %.1f%% -> %.1f%% (all) | %.1f%% (active) | ns_at_cloudflare=%d",
                before or 0, after_total, after_active, cf)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
