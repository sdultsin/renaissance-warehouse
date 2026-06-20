#!/usr/bin/env python3
"""Track I — backfill core.domain_registry.purchased_at from the Porkbun API
(domain/listAll returns createDate = registration date). Covers the Porkbun-owned
domains still missing a date. Dynadot/Spaceship accounts need their own creds.

OTD (vendor-provisioned) domains legitimately have NO registration date (we don't own
the registration) — they are excluded from the coverage target, not backfilled.

Fetch (lock-free) then load (needs the writer lock).

Usage:
    python scripts/backfill_purchased_at_porkbun.py --cache /root/core/porkbun_dates.parquet
    python scripts/backfill_purchased_at_porkbun.py --from-cache /root/core/porkbun_dates.parquet
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.request
from pathlib import Path

import duckdb

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.backfill_purchased_at_porkbun")

ENV_PATH = "/root/Renaissance/.env"


def load_creds():
    env = {}
    for line in Path(ENV_PATH).read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    ak = env.get("PORKBUN_API_KEY")
    sk = env.get("PORKBUN_SECRET_API_KEY") or env.get("PORKBUN_SECRET_KEY")
    return ak, sk


def fetch_all(ak, sk) -> list[tuple[str, str]]:
    out, start = [], 0
    while True:
        body = json.dumps({"apikey": ak, "secretapikey": sk, "start": str(start),
                           "includeLabels": "no"}).encode()
        req = urllib.request.Request("https://api.porkbun.com/api/json/v3/domain/listAll",
                                     data=body, headers={"Content-Type": "application/json"})
        r = json.load(urllib.request.urlopen(req, timeout=60))
        if r.get("status") != "SUCCESS":
            raise RuntimeError(f"porkbun error: {r}")
        page = r.get("domains", [])
        for d in page:
            if d.get("domain") and d.get("createDate"):
                out.append((d["domain"].lower(), d["createDate"]))
        if len(page) < 1000:
            break
        start += 1000
    logger.info("porkbun listAll: %d domains with createDate", len(out))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--cache", default="/root/core/porkbun_dates.parquet")
    ap.add_argument("--from-cache", default=None)
    args = ap.parse_args(argv)

    if args.from_cache:
        cache = args.from_cache
    else:
        ak, sk = load_creds()
        if not ak or not sk:
            logger.error("no Porkbun creds in %s", ENV_PATH)
            return 1
        rows = fetch_all(ak, sk)
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
