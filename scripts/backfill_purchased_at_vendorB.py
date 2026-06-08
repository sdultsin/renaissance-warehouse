#!/usr/bin/env python3
"""Track I — backfill core.domain_registry.purchased_at from registrar vendor B's API
(its listAll endpoint returns a per-domain registration/create date). Covers the
vendor-B-owned domains still missing a date. Other registrar accounts need their own creds.

OTD (vendor-provisioned) domains legitimately have NO registration date (we don't own
the registration) — they are excluded from the coverage target, not backfilled.

Fetch (lock-free) then load (needs the writer lock).

CONFIG (from env / repo-root .env):
    VENDOR_B_API_KEY            registrar vendor B api key
    VENDOR_B_SECRET_KEY         registrar vendor B secret key
    VENDOR_B_LISTALL_URL        the vendor's listAll endpoint (POST JSON)

Usage:
    python scripts/backfill_purchased_at_vendorB.py --cache /root/core/vendorB_dates.parquet
    python scripts/backfill_purchased_at_vendorB.py --from-cache /root/core/vendorB_dates.parquet
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.request
from pathlib import Path

import duckdb

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.backfill_purchased_at_vendorB")

# Repo-root .env (Renaissance parent dir), overridable via ENV_FILE.
REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = os.environ.get("ENV_FILE", str(REPO_ROOT.parent / ".env"))
LISTALL_URL = os.environ.get("VENDOR_B_LISTALL_URL", "")


def load_creds():
    env = {}
    p = Path(ENV_PATH)
    if p.exists():
        for line in p.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    ak = os.environ.get("VENDOR_B_API_KEY") or env.get("VENDOR_B_API_KEY")
    sk = (os.environ.get("VENDOR_B_SECRET_KEY") or env.get("VENDOR_B_SECRET_KEY")
          or env.get("VENDOR_B_SECRET_API_KEY"))
    url = LISTALL_URL or env.get("VENDOR_B_LISTALL_URL", "")
    return ak, sk, url


def fetch_all(ak, sk, url) -> list[tuple[str, str]]:
    if not url:
        raise RuntimeError("VENDOR_B_LISTALL_URL not configured")
    out, start = [], 0
    while True:
        body = json.dumps({"apikey": ak, "secretapikey": sk, "start": str(start),
                           "includeLabels": "no"}).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"})
        r = json.load(urllib.request.urlopen(req, timeout=60))
        if r.get("status") != "SUCCESS":
            raise RuntimeError(f"vendor B error: {r}")
        page = r.get("domains", [])
        for d in page:
            if d.get("domain") and d.get("createDate"):
                out.append((d["domain"].lower(), d["createDate"]))
        if len(page) < 1000:
            break
        start += 1000
    logger.info("vendor B listAll: %d domains with createDate", len(out))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--cache", default="/root/core/vendorB_dates.parquet")
    ap.add_argument("--from-cache", default=None)
    args = ap.parse_args(argv)

    if args.from_cache:
        cache = args.from_cache
    else:
        ak, sk, url = load_creds()
        if not ak or not sk:
            logger.error("no vendor B creds in %s", ENV_PATH)
            return 1
        rows = fetch_all(ak, sk, url)
        mem = duckdb.connect()
        mem.execute("CREATE TABLE d (domain VARCHAR, create_date VARCHAR)")
        mem.executemany("INSERT INTO d VALUES (?,?)", rows)
        mem.execute(f"COPY d TO '{args.cache}' (FORMAT PARQUET)")
        mem.close()
        cache = args.cache
        logger.info("cached -> %s", cache)

    conn = db_module.connect(Path(args.db) if args.db else None)
    before = conn.execute("SELECT round(100.0*avg((purchased_at IS NOT NULL)::int),1) "
                          "FROM core.domain_registry WHERE registrar_account NOT ILIKE '%OTD%'").fetchone()[0]
    conn.execute("BEGIN")
    try:
        conn.execute("CREATE OR REPLACE TEMP TABLE pb AS SELECT domain, "
                     "TRY_CAST(create_date AS TIMESTAMPTZ) AS d FROM read_parquet(?)", [cache])
        conn.execute("""
            UPDATE core.domain_registry r SET purchased_at = pb.d, purchased_at_is_derived = FALSE
            FROM pb WHERE pb.domain = r.domain AND r.purchased_at IS NULL AND pb.d IS NOT NULL
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    after = conn.execute("SELECT round(100.0*avg((purchased_at IS NOT NULL)::int),1) "
                         "FROM core.domain_registry WHERE registrar_account NOT ILIKE '%OTD%'").fetchone()[0]
    logger.info("purchased_at (excl OTD): %.1f%% -> %.1f%%", before or 0, after)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
