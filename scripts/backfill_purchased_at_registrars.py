#!/usr/bin/env python3
"""Track I — backfill core.domain_registry.purchased_at (+ exact expires_at) from ALL
three registrar APIs (Porkbun, Spaceship, Dynadot), across every configured account.

Each registrar's listAll-style endpoint returns the registration (createDate) and
expiration date per owned domain. We fetch every account, union the results, cache to a
per-registrar parquet (weekly refresh, like the legacy Porkbun cache), then load into
core.domain_registry under the single-writer lock.

Improves on the legacy single-registrar (Porkbun-only) backfill in two ways:
  1. Covers Spaceship + Dynadot as well as Porkbun.
  2. UPGRADES derived dates -> API-exact (legacy only filled NULLs). Where a registrar
     returns an exact createDate, purchased_at is overwritten and purchased_at_is_derived
     is set FALSE — even if the Domain Tech Sheet had previously derived (expiry-1y) it.
     Exact expires_at is filled where NULL.

OTD (vendor-provisioned) domains legitimately have NO registration we own — they aren't
in any of our registrar accounts, so they're untouched (and excluded from the % target).

Fetch (lock-free, network) then load (needs the DuckDB writer lock). The nightly fetches
weekly and loads from cache nightly.

CONFIG (from env / repo-root .env — same files core/config.py reads):
    PORKBUN[_<n>]_API_KEY / PORKBUN[_<n>]_SECRET_API_KEY (or _SECRET_KEY)  default + 1..N
    SPACESHIP_<n>_API_KEY / SPACESHIP_<n>_API_SECRET                        1..N
    DYNADOT_<n>_API_KEY                                                     1..N
    (legacy aliases VENDOR_B_API_KEY/VENDOR_B_SECRET_KEY are read as a Porkbun account too)

Usage:
    # weekly: hit the APIs + refresh every cache
    python scripts/backfill_purchased_at_registrars.py --refresh-cache
    # nightly: load from existing caches (no API calls), apply under the lock
    python scripts/backfill_purchased_at_registrars.py --from-cache
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.backfill_purchased_at_registrars")

REPO_ROOT = Path(__file__).resolve().parents[1]
# Search the same .env files core/config.py uses, in order (first match wins per key).
ENV_CANDIDATES = [
    os.environ.get("ENV_FILE"),
    str(REPO_ROOT / ".env"),
    str(REPO_ROOT.parent / ".env"),  # /root/Renaissance/.env on the droplet
    "/Users/sam/Documents/Claude Code/Renaissance/.env",
]
CACHE_DIR = os.environ.get("REGISTRAR_CACHE_DIR", "/root/core")

# Cloudflare-fronted registrar endpoints reject the default urllib UA (error 1010 / 403).
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def _yn(v):
    """Normalise a registrar's auto-renew flag to 'yes'/'no'/None."""
    if v is None:
        return None
    sv = str(v).strip().lower()
    if sv in ("1", "true", "yes", "on", "enabled"):
        return "yes"
    if sv in ("0", "false", "no", "off", "disabled", ""):
        return "no"
    return None


def _dyn_renew(v):
    """Dynadot RenewOption -> yes/no. 'auto*' = auto-renew on; everything else off."""
    if v is None:
        return None
    sv = str(v).strip().lower()
    return "yes" if "auto" in sv else "no"


def _ns_join(v):
    """Extract nameserver HOSTNAMES from whatever shape the API returned (string, list, or a
    nested dict like Dynadot's NameServerSettings), as a clean comma string. Anything that is
    not a bare hostname (brackets, spaces, the literal 'custom') is dropped."""
    hosts = []

    def walk(x):
        if isinstance(x, str):
            h = x.strip().lower()
            if "." in h and not any(c in h for c in "[] ,'\""):
                hosts.append(h)
        elif isinstance(x, (list, tuple)):
            for i in x:
                walk(i)
        elif isinstance(x, dict):
            for i in x.values():
                walk(i)

    walk(v)
    seen = set()
    uniq = [h for h in hosts if not (h in seen or seen.add(h))]
    return ",".join(uniq) or None


def load_env() -> dict:
    env: dict[str, str] = {}
    for cand in ENV_CANDIDATES:
        if not cand:
            continue
        p = Path(cand)
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                # first match wins; don't let a later (empty) slot clobber a real value
                if k not in env or (not env[k] and v):
                    env[k] = v
    return env


# ---- account discovery -------------------------------------------------------

def porkbun_accounts(env: dict) -> list[tuple[str, str, str]]:
    """(label, apikey, secretkey) for the default + every numbered Porkbun account
    that has a non-empty key pair. VENDOR_B_* (alias of the default) is folded in."""
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    def add(label, ak, sk):
        if ak and sk and ak not in seen:
            seen.add(ak)
            out.append((label, ak, sk))

    add("porkbun.default", env.get("PORKBUN_API_KEY", ""),
        env.get("PORKBUN_SECRET_API_KEY") or env.get("PORKBUN_SECRET_KEY") or "")
    add("porkbun.vendorB", env.get("VENDOR_B_API_KEY", ""),
        env.get("VENDOR_B_SECRET_KEY") or env.get("VENDOR_B_SECRET_API_KEY") or "")
    for n in sorted({int(m.group(1)) for k in env
                     for m in [re.match(r"^PORKBUN_(\d+)_API_KEY$", k)] if m}):
        add(f"porkbun.{n}", env.get(f"PORKBUN_{n}_API_KEY", ""),
            env.get(f"PORKBUN_{n}_SECRET_API_KEY") or env.get(f"PORKBUN_{n}_SECRET_KEY") or "")
    return out


def spaceship_accounts(env: dict) -> list[tuple[str, str, str]]:
    out = []
    for n in sorted({int(m.group(1)) for k in env
                     for m in [re.match(r"^SPACESHIP_(\d+)_API_KEY$", k)] if m}):
        ak = env.get(f"SPACESHIP_{n}_API_KEY", "")
        sk = env.get(f"SPACESHIP_{n}_API_SECRET") or env.get(f"SPACESHIP_{n}_SECRET_KEY") or ""
        if ak and sk:
            out.append((f"spaceship.{n}", ak, sk))
    return out


def dynadot_accounts(env: dict) -> list[tuple[str, str]]:
    out = []
    for n in sorted({int(m.group(1)) for k in env
                     for m in [re.match(r"^DYNADOT_(\d+)_API_KEY$", k)] if m}):
        ak = env.get(f"DYNADOT_{n}_API_KEY", "")
        if ak:
            out.append((f"dynadot.{n}", ak))
    return out


# ---- fetchers (return list[(domain, create_iso_or_none, expire_iso_or_none)]) ----

def fetch_porkbun(label, ak, sk) -> list[tuple]:
    out, start = [], 0
    url = "https://api.porkbun.com/api/json/v3/domain/listAll"
    while True:
        body = json.dumps({"apikey": ak, "secretapikey": sk, "start": str(start),
                           "includeLabels": "no"}).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json", "User-Agent": UA})
        r = json.load(urllib.request.urlopen(req, timeout=90))
        if r.get("status") != "SUCCESS":
            raise RuntimeError(f"{label} error: {r.get('message') or r.get('status')}")
        page = r.get("domains", [])
        for d in page:
            dom = (d.get("domain") or "").lower()
            if dom:
                out.append((dom, d.get("createDate"), d.get("expireDate"),
                            _yn(d.get("autoRenew")), None, d.get("status")))
        if len(page) < 1000:
            break
        start += 1000
        time.sleep(0.5)
    return out


def fetch_spaceship(label, ak, sk) -> list[tuple]:
    out, skip, take = [], 0, 100
    while True:
        req = urllib.request.Request(
            f"https://spaceship.dev/api/v1/domains?take={take}&skip={skip}",
            headers={"X-Api-Key": ak, "X-Api-Secret": sk, "Accept": "application/json", "User-Agent": UA})
        r = json.load(urllib.request.urlopen(req, timeout=90))
        items = r.get("items", []) or []
        for d in items:
            dom = (d.get("name") or "").lower()
            if dom:
                out.append((dom, d.get("registrationDate"), d.get("expirationDate"),
                            _yn(d.get("autoRenew")), _ns_join(d.get("nameservers")),
                            d.get("lifecycleStatus")))
        total = r.get("total", 0)
        skip += take
        if skip >= total or not items:
            break
        time.sleep(0.5)
    return out


def fetch_dynadot(label, ak) -> list[tuple]:
    # Single call returns the whole account's MainDomains; dates are epoch millis.
    url = f"https://api.dynadot.com/api3.json?key={ak}&command=list_domain"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    r = json.load(urllib.request.urlopen(req, timeout=120))
    body = r.get("ListDomainInfoResponse", {})
    if body.get("ResponseCode") not in (0, "0", None) and body.get("Status") not in ("success", None):
        raise RuntimeError(f"{label} error: {body.get('Status')} / {body.get('Error')}")
    out = []
    for d in body.get("MainDomains", []) or []:
        dom = (d.get("Name") or "").lower()
        if not dom:
            continue
        reg, exp = d.get("Registration"), d.get("Expiration")
        # epoch millis -> ISO (let DuckDB cast). Keep as int strings; parsed at load.
        out.append((dom, f"epoch_ms:{reg}" if reg else None, f"epoch_ms:{exp}" if exp else None,
                    _dyn_renew(d.get("RenewOption")), _ns_join(d.get("NameServerSettings")),
                    d.get("Status")))
    return out


# ---- cache + load ------------------------------------------------------------

def fetch_registrar(reg: str, env: dict) -> list[tuple]:
    rows: list[tuple] = []
    if reg == "porkbun":
        accts = porkbun_accounts(env)
        fn = lambda a: fetch_porkbun(*a)
    elif reg == "spaceship":
        accts = spaceship_accounts(env)
        fn = lambda a: fetch_spaceship(*a)
    elif reg == "dynadot":
        accts = dynadot_accounts(env)
        fn = lambda a: fetch_dynadot(*a)
    else:
        raise ValueError(reg)
    if not accts:
        logger.warning("%s: no configured accounts", reg)
        return rows
    for acct in accts:
        label = acct[0]
        # normalise the discovery label to registrar_snapshot form: 'porkbun.2'->'porkbun2',
        # 'porkbun.default'/'porkbun.vendorB' are the default (unnumbered) account = #1.
        _rg, _sfx = (label.split(".", 1) + [""])[:2]
        acct_id = f"{_rg}{_sfx}" if _sfx.isdigit() else (f"{_rg}1" if _sfx in ("default", "vendorB") else label.replace(".", ""))
        try:
            got = fn(acct)
            logger.info("%s: %d domains", label, len(got))
            rows.extend((g[0], g[1], g[2], g[3], g[4], g[5], acct_id) for g in got)
        except urllib.error.HTTPError as e:
            logger.error("%s: HTTP %s (%s) — skipping account", label, e.code, e.reason)
        except Exception as e:  # noqa: BLE001
            logger.error("%s: %r — skipping account", label, e)
    return rows


def write_cache(reg: str, rows: list[tuple], cache: str) -> None:
    # rows are 7-tuples: (domain, create_raw, expire_raw, auto_renew, nameservers, domain_status, account_id)
    mem = duckdb.connect()
    mem.execute("CREATE TABLE d (domain VARCHAR, create_raw VARCHAR, expire_raw VARCHAR, registrar VARCHAR, "
                "registrar_account VARCHAR, auto_renew VARCHAR, nameservers VARCHAR, domain_status VARCHAR)")
    mem.executemany("INSERT INTO d VALUES (?,?,?,?,?,?,?,?)",
                    [(d, c, e, reg, acct, ar, ns, st) for (d, c, e, ar, ns, st, acct) in rows])
    mem.execute(f"COPY d TO '{cache}' (FORMAT PARQUET)")
    mem.close()
    logger.info("%s: cached %d rows -> %s", reg, len(rows), cache)


# Parse either an ISO8601 string or our 'epoch_ms:<int>' sentinel into a TIMESTAMPTZ.
PARSE = ("CASE WHEN {c} LIKE 'epoch_ms:%' "
         "THEN to_timestamp(TRY_CAST(substr({c},10) AS BIGINT)/1000.0) "  # 'epoch_ms:' is 9 chars; payload starts at 10
         "ELSE TRY_CAST({c} AS TIMESTAMPTZ) END")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--refresh-cache", action="store_true",
                    help="hit the registrar APIs and rewrite the per-registrar caches")
    ap.add_argument("--from-cache", action="store_true",
                    help="load from existing caches without hitting the APIs")
    ap.add_argument("--no-load", action="store_true",
                    help="fetch + write caches only; skip the DuckDB date-load (isolated testing)")
    ap.add_argument("--registrars", default="porkbun,spaceship,dynadot")
    ap.add_argument("--cache-dir", default=CACHE_DIR)
    args = ap.parse_args(argv)

    regs = [r.strip() for r in args.registrars.split(",") if r.strip()]
    caches = {r: str(Path(args.cache_dir) / f"{r}_dates.parquet") for r in regs}

    if args.refresh_cache or not args.from_cache:
        env = load_env()
        for r in regs:
            rows = fetch_registrar(r, env)
            if rows:
                write_cache(r, rows, caches[r])
            elif not Path(caches[r]).exists():
                logger.warning("%s: no rows and no existing cache — will be skipped at load", r)
        if args.no_load:
            logger.info("--no-load: caches written, skipping DB load")
            return 0

    # ---- load (needs writer lock) ----
    available = [c for c in caches.values() if Path(c).exists()]
    if not available:
        logger.error("no registrar caches available to load")
        return 1

    conn = db_module.connect(Path(args.db) if args.db else None)

    def cov():
        return conn.execute(
            "SELECT "
            "count(*) FILTER (WHERE registrar_account NOT ILIKE '%OTD%') AS non_otd, "
            "count(*) FILTER (WHERE purchased_at IS NOT NULL AND NOT COALESCE(purchased_at_is_derived,TRUE)) AS exact, "
            "count(*) FILTER (WHERE purchased_at IS NOT NULL AND COALESCE(purchased_at_is_derived,TRUE)) AS derived, "
            "count(*) FILTER (WHERE purchased_at IS NULL AND registrar_account NOT ILIKE '%OTD%') AS null_non_otd, "
            "count(*) FILTER (WHERE expires_at IS NOT NULL) AS exp "
            "FROM core.domain_registry"
        ).fetchone()

    before = cov()
    conn.execute("BEGIN")
    try:
        union = " UNION ALL ".join(f"SELECT * FROM read_parquet('{c}')" for c in available)
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE reg AS "
            f"WITH raw AS ({union}) "
            f"SELECT domain, "
            f"  max({PARSE.format(c='create_raw')}) AS create_ts, "
            f"  max({PARSE.format(c='expire_raw')}) AS expire_ts "
            f"FROM raw GROUP BY domain"
        )
        n_reg = conn.execute("SELECT count(*) FROM reg").fetchone()[0]
        # purchased_at: set API-exact wherever the registrar gave a date AND ours is
        # currently null OR derived (upgrade). Never clobber an already-exact value.
        conn.execute("""
            UPDATE core.domain_registry r
            SET purchased_at = reg.create_ts, purchased_at_is_derived = FALSE
            FROM reg
            WHERE reg.domain = r.domain AND reg.create_ts IS NOT NULL
              AND (r.purchased_at IS NULL OR COALESCE(r.purchased_at_is_derived, TRUE) = TRUE)
        """)
        # expires_at: fill exact where missing.
        conn.execute("""
            UPDATE core.domain_registry r
            SET expires_at = reg.expire_ts
            FROM reg
            WHERE reg.domain = r.domain AND reg.expire_ts IS NOT NULL AND r.expires_at IS NULL
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    after = cov()
    logger.info("registrar union domains=%d | non_otd=%d", n_reg, after[0])
    logger.info("purchased_at EXACT: %d -> %d | DERIVED: %d -> %d | NULL(excl-OTD): %d -> %d | expires_at: %d -> %d",
                before[1], after[1], before[2], after[2], before[3], after[3], before[4], after[4])
    conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
