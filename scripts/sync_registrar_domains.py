#!/usr/bin/env python3
"""Track I — populate raw_registrar_domains + rebuild core.registrar_snapshot from the
per-registrar date caches (Porkbun/Spaceship/Dynadot), for EVERY configured account.

Why this exists: backfill_purchased_at_registrars.py already fetches every registrar
account nightly (into {reg}_dates.parquet) but only used the result to backfill dates —
it threw the domain list away. raw_registrar_domains was a stale one-time load (3 Dynadot
accounts, 2026-06-01) and core.registrar_snapshot (what the Data Hub reads) had no builder
at all. This script closes both: it reads the ENRICHED caches (domain, dates, account,
auto_renew, nameservers, status — no new API calls) and writes:
  1. raw_registrar_domains   (one row per domain per account; consumed by rebuild_domain_registry)
  2. core.registrar_snapshot (the Hub's table: registrar, account, expiry, auto_renew, ...)

Both writes are guarded: a shrink guard refuses to replace a healthy table with a
suspiciously small one, and registrar_snapshot is built into a _new table and atomically
renamed so the live Hub never sees a half-built table.

Requires the ENRICHED caches (8-col, written by the updated backfill_purchased_at_registrars.py
--refresh-cache). Run AFTER that in the nightly.

Usage:
    python scripts/sync_registrar_domains.py [--cache-dir /root/core] [--db PATH] [--dry]
    --dry  builds raw_registrar_domains_test + registrar_snapshot_new and reports, without
           touching the live tables.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.sync_registrar_domains")

REGS = ("porkbun", "spaceship", "dynadot", "namecheap")   # [2026-07-22] namecheap added

# Parse an ISO8601 string OR the 'epoch_ms:<int>' sentinel the fetchers emit -> TIMESTAMPTZ.
PARSE = ("CASE WHEN {c} LIKE 'epoch_ms:%' "
         "THEN to_timestamp(TRY_CAST(substr({c},10) AS BIGINT)/1000.0) "
         "ELSE TRY_CAST({c} AS TIMESTAMPTZ) END")


def _cache_union(cache_dir: str) -> str:
    caches = [str(Path(cache_dir) / f"{r}_dates.parquet") for r in REGS]
    have = [c for c in caches if Path(c).exists()]
    if not have:
        raise SystemExit("no registrar caches found — run backfill_purchased_at_registrars.py --refresh-cache first")
    return " UNION ALL ".join(f"SELECT * FROM read_parquet('{c}')" for c in have), have


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", default="/root/core")
    ap.add_argument("--db", default=None)
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args(argv)

    union, have = _cache_union(args.cache_dir)
    logger.info("reading %d registrar caches: %s", len(have), ", ".join(Path(c).name for c in have))

    conn = db_module.connect(Path(args.db) if args.db else None)

    # Guard: the enriched caches must carry registrar_account (else they're the old 4-col shape).
    cols = [r[0] for r in conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{have[0]}')").fetchall()]
    if "registrar_account" not in cols:
        logger.warning("cache %s is the OLD 4-col shape (cols=%s) — skipping this run; the next "
                       "--refresh-cache with the enriched writer will populate it", have[0], cols)
        conn.close()
        return 0

    # One clean row per (domain, account) from the union. A domain lives in exactly one
    # account, but guard with arg_max on the (nonexistent) load time -> just DISTINCT-ish.
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE reg_src AS
        WITH raw AS ({union})
        SELECT lower(domain) AS domain,
               any_value(registrar)                       AS registrar,
               any_value(registrar_account)               AS registrar_account,
               max({PARSE.format(c='create_raw')})        AS purchased_ts,
               max({PARSE.format(c='expire_raw')})         AS expires_ts,
               any_value(auto_renew)                       AS auto_renew,
               any_value(nameservers)                      AS nameservers,
               any_value(domain_status)                    AS domain_status
        FROM raw
        WHERE NULLIF(domain,'') IS NOT NULL AND NULLIF(registrar_account,'') IS NOT NULL
        GROUP BY 1
    """)
    n_src = conn.execute("SELECT count(*) FROM reg_src").fetchone()[0]
    n_acct = conn.execute("SELECT count(DISTINCT registrar_account) FROM reg_src").fetchone()[0]
    logger.info("cache union: %d domains across %d accounts", n_src, n_acct)
    for r in conn.execute("SELECT registrar_account, count(*) FROM reg_src GROUP BY 1 ORDER BY registrar_account").fetchall():
        logger.info("   %-14s %d", r[0], r[1])

    if n_src < 1000:
        raise SystemExit(f"only {n_src} domains in the cache union — refusing to proceed (looks broken)")

    raw_tbl = "raw_registrar_domains_test" if args.dry else "raw_registrar_domains"
    snap_new = "core.registrar_snapshot_new"

    conn.execute("BEGIN")
    try:
        # ---- 1. raw_registrar_domains (shrink-guarded truncate+reload) ----
        if not args.dry:
            old = conn.execute("SELECT count(*) FROM raw_registrar_domains").fetchone()[0]
            if old > 1000 and n_src < old * 0.5:
                raise SystemExit(f"SHRINK GUARD: new raw_registrar_domains {n_src} < 50% of old {old}; keeping old")
            conn.execute("DELETE FROM raw_registrar_domains")
        else:
            conn.execute("DROP TABLE IF EXISTS raw_registrar_domains_test")
            conn.execute("CREATE TABLE raw_registrar_domains_test AS SELECT * FROM raw_registrar_domains WHERE FALSE")
        conn.execute(f"""
            INSERT INTO {raw_tbl}
              (domain, registrar, registrar_account, purchased_at, expires_at, auto_renew, nameservers, _loaded_at, _run_id)
            SELECT domain, registrar, registrar_account,
                   CAST(purchased_ts AS DATE), CAST(expires_ts AS DATE),
                   auto_renew,
                   CASE WHEN NULLIF(nameservers,'') IS NULL THEN NULL ELSE string_split(nameservers, ',') END,
                   now(), 'sync_registrar_domains'
            FROM reg_src
        """)
        n_raw = conn.execute(f"SELECT count(*) FROM {raw_tbl}").fetchone()[0]

        # ---- 2. core.registrar_snapshot (build _new, shrink-guard, atomic swap) ----
        conn.execute(f"DROP TABLE IF EXISTS {snap_new}")
        conn.execute(f"""
            CREATE TABLE {snap_new} AS
            SELECT domain,
                   lower(regexp_extract(domain, '\\.([a-z0-9-]+)$', 1))          AS tld,
                   registrar,
                   registrar_account,
                   nameservers,
                   CASE
                     WHEN nameservers ILIKE '%cloudflare%' THEN 'Cloudflare'
                     WHEN nameservers ILIKE '%dynadot%'    THEN 'Dynadot'
                     WHEN nameservers ILIKE '%porkbun%'    THEN 'Porkbun'
                     WHEN nameservers ILIKE '%spaceship%' OR nameservers ILIKE '%hosting.systems%' THEN 'Spaceship'
                     WHEN NULLIF(nameservers,'') IS NULL   THEN NULL
                     ELSE 'other'
                   END                                                            AS ns_provider,
                   (nameservers ILIKE '%cloudflare%')                             AS ns_at_cloudflare,
                   CAST(expires_ts AS DATE)                                       AS expires_at,
                   CAST(date_diff('day', current_date, CAST(expires_ts AS DATE)) AS BIGINT) AS days_to_expiry,
                   auto_renew,
                   domain_status,
                   now()                                                          AS _loaded_at
            FROM reg_src
        """)
        n_snap = conn.execute(f"SELECT count(*) FROM {snap_new}").fetchone()[0]
        if not args.dry:
            old_snap = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='core' AND table_name='registrar_snapshot'"
            ).fetchone()[0]
            if old_snap:
                oc = conn.execute("SELECT count(*) FROM core.registrar_snapshot").fetchone()[0]
                if oc > 1000 and n_snap < oc * 0.5:
                    raise SystemExit(f"SHRINK GUARD: new registrar_snapshot {n_snap} < 50% of old {oc}; keeping old")
                conn.execute("DROP TABLE core.registrar_snapshot")
            conn.execute(f"ALTER TABLE {snap_new} RENAME TO registrar_snapshot")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    logger.info("%s: %d rows | registrar_snapshot%s: %d rows",
                raw_tbl, n_raw, "_new(dry)" if args.dry else "", n_snap)
    # account coverage report
    tgt = "raw_registrar_domains_test" if args.dry else "core.registrar_snapshot"
    logger.info("account coverage in %s:", tgt)
    for r in conn.execute(f"SELECT registrar_account, count(*) FROM {tgt} GROUP BY 1 ORDER BY registrar_account").fetchall():
        logger.info("   %-14s %d", r[0], r[1])
    conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
