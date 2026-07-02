#!/usr/bin/env python3
"""Refresh raw_cloudflare_zones from the live Cloudflare API (RG + Sam accounts).

Replaces the one-time 2026-06-01 snapshot with a live, schedulable loader so
core.domain_registry (which unions cloudflare_zones) can be rebuilt fresh.

Full-snapshot semantics: fetch all zones for each configured account, then
DELETE + INSERT in one transaction (the table is a current-state snapshot, not
an event log). Uses core.db.connect so it acquires the single-writer flock
(acquire-or-wait) and never clobbers a concurrent nightly/moderator writer.

Env (extracted from .env, which is not shell-sourceable):
  CLOUDFLARE_RG_API_TOKEN / CLOUDFLARE_RG_ACCOUNT_ID
  CLOUDFLARE_SAM_API_TOKEN / CLOUDFLARE_SAM_ACCOUNT_ID
Reversible: each run first copies the current rows to raw_cloudflare_zones_bak
(rolling last-known-good) inside the same txn, so a bad load is recoverable via
`INSERT INTO raw_cloudflare_zones SELECT * FROM raw_cloudflare_zones_bak`.
"""
import os
import sys
import time
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

sys.path.insert(0, "/root/renaissance-warehouse")
from core import db  # noqa: E402

CF_API = "https://api.cloudflare.com/client/v4/zones"

# (label, token_env, account_env) — label lands in cloudflare_account.
ACCOUNTS = [
    ("RG", "CLOUDFLARE_RG_API_TOKEN", "CLOUDFLARE_RG_ACCOUNT_ID"),
    ("Sam", "CLOUDFLARE_SAM_API_TOKEN", "CLOUDFLARE_SAM_ACCOUNT_ID"),
]


def _env_from_dotenv(key: str) -> str | None:
    """.env is not shell-sourceable; grep the value the same way the watchdog does."""
    path = "/root/renaissance-warehouse/.env"
    try:
        with open(path) as fh:
            for line in fh:
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return os.environ.get(key)


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        return None


def fetch_zones(label, token, account_id):
    """Page through /zones for one account. Returns (row tuples, ok). ok=False on any error."""
    rows, page, pages = [], 1, 1
    try:
        while page <= pages:
            url = f"{CF_API}?account.id={account_id}&per_page=50&page={page}"
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            data = None
            for attempt in range(3):
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        data = json.load(resp)
                    break
                except (urllib.error.URLError, TimeoutError, OSError):
                    if attempt == 2:
                        raise
                    time.sleep(2 * (attempt + 1))
            if not data.get("success"):
                print(f"[cf_zones] {label}: CF API error {data.get('errors')}")
                return [], False
            info = data.get("result_info", {})
            pages = info.get("total_pages", 1) or 1
            for z in data.get("result", []):
                rows.append((
                    (z.get("name") or "").lower(),
                    f"cloudflare_{label}",
                    z.get("id"),
                    z.get("status"),
                    z.get("name_servers") or [],
                    _parse_ts(z.get("created_on")),
                    _parse_ts(z.get("activated_on")),
                    (z.get("plan") or {}).get("name"),
                ))
            page += 1
    except Exception as e:  # noqa: BLE001
        print(f"[cf_zones] {label}: fetch failed: {e}")
        return [], False
    return rows, True


def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    loaded_at = datetime.now(timezone.utc).isoformat()
    all_rows, summary = [], []
    # R1 RISK A/B: strict per-account guard — abort the whole wipe if ANY configured
    # (token-present) account fails or returns zero zones. A single bad token must NOT
    # silently delete the other account's domains (21/51 zones are cloudflare-only =
    # exist in core.domain_registry solely via this table).
    for label, tok_env, acct_env in ACCOUNTS:
        token, acct = _env_from_dotenv(tok_env), _env_from_dotenv(acct_env)
        if not token or not acct:
            print(f"[cf_zones] SKIP {label}: missing {tok_env}/{acct_env} (not configured)")
            continue
        rows, ok = fetch_zones(label, token, acct)
        if not ok:
            # A real API error (not a legitimately-empty account) — never wipe on partial data.
            print(f"[cf_zones] ABORT: account {label} API ERROR — refusing to DELETE+INSERT "
                  f"(would drop {label}'s zones). No write performed.")
            return 2
        if not rows:
            # Verified-legitimate empty account (e.g. Sam holds 0 CF zones) — allow, contributes nothing.
            print(f"[cf_zones] {label}: 0 zones (account has none) — continuing")
            summary.append(f"{label}=0")
            continue
        all_rows.extend(rows)
        summary.append(f"{label}={len(rows)}")
        print(f"[cf_zones] {label}: {len(rows)} zones")

    if not all_rows:
        print("[cf_zones] ERROR: no zones fetched from any account; refusing to wipe table")
        return 2

    conn = db.connect(read_only=False)
    try:
        # R1 RISK B: superset pre-flight — warn loudly if any currently-tracked domain
        # is missing from the live fetch (recoverable via the backup table below).
        fetched = {r[0] for r in all_rows}
        cur = [d for (d,) in conn.execute(
            "SELECT DISTINCT lower(domain) FROM raw_cloudflare_zones "
            "WHERE NULLIF(domain,'') IS NOT NULL").fetchall()]
        missing = sorted(set(cur) - fetched)
        if missing:
            print(f"[cf_zones] WARN: {len(missing)} currently-tracked domain(s) NOT in live "
                  f"fetch (kept in backup table): {missing[:10]}{'…' if len(missing) > 10 else ''}")
        conn.execute("BEGIN")
        # R1 RISK C: pre-DELETE rolling backup (last-known-good) inside the txn.
        conn.execute("DROP TABLE IF EXISTS raw_cloudflare_zones_bak")
        conn.execute("CREATE TABLE raw_cloudflare_zones_bak AS SELECT * FROM raw_cloudflare_zones")
        conn.execute("DELETE FROM raw_cloudflare_zones")
        conn.executemany(
            "INSERT INTO raw_cloudflare_zones "
            "(domain, cloudflare_account, zone_id, status, nameservers, created_on, "
            " activated_on, plan, _loaded_at, _run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [r + (loaded_at, run_id) for r in all_rows],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    n = len(all_rows)
    print(f"[cf_zones] loaded {n} rows ({', '.join(summary)}) @ {loaded_at} "
          f"(prior rows backed up to raw_cloudflare_zones_bak)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
