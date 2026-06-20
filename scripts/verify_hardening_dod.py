#!/usr/bin/env python3
"""Run the 2026-06-08 warehouse-hardening DoD checks (read-only) and post a PASS/FAIL
summary to #cc-sam. Called at the end of the write batch so the autonomous landing
self-reports.

Usage:
    python scripts/verify_hardening_dod.py            # check + post summary
    python scripts/verify_hardening_dod.py --no-post   # print only
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

import duckdb

from core.config import DB_PATH, REPO_ROOT

SLACK_CHANNEL = "C0AR0EA21C1"


def slack_post(text: str) -> dict:
    env = {}
    p = REPO_ROOT / ".env"
    if p.exists():
        for line in p.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    token, cookie = env.get("SLACK_TOKEN"), env.get("SLACK_COOKIE")
    if not token:
        return {"ok": False, "error": "no_token"}
    body = json.dumps({"channel": SLACK_CHANNEL, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8",
                 **({"Cookie": f"d={cookie}"} if cookie else {})}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}


def q1(con, sql):
    try:
        return con.execute(sql).fetchone()[0]
    except Exception as exc:  # noqa: BLE001
        return f"ERR({exc})"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--no-post", action="store_true")
    args = ap.parse_args(argv)
    con = duckdb.connect(str(Path(args.db) if args.db else DB_PATH), read_only=True)

    checks = []  # (label, value, pass?)
    def chk(label, val, ok):
        checks.append((label, val, bool(ok)))

    # Track E
    rc = q1(con, "SELECT count(*) FROM core.sync_registry")
    chk("E: sync_registry feeds >=30", rc, isinstance(rc, int) and rc >= 30)
    miss = q1(con, "SELECT count(*) FROM information_schema.tables t WHERE t.table_name LIKE 'raw\\_%' ESCAPE '\\' "
                   "AND NOT EXISTS (SELECT 1 FROM core.sync_registry r WHERE r.name=t.table_name)")
    chk("E: every raw_ table registered (=0)", miss, miss == 0)
    vf = q1(con, "SELECT count(*) FROM v_warehouse_freshness")
    chk("E: v_warehouse_freshness rows", vf, isinstance(vf, int) and vf >= 30)

    # Track G
    ic = q1(con, "SELECT count(*) FROM v_infra_capacity_daily")
    chk("G: v_infra_capacity_daily rows>0", ic, isinstance(ic, int) and ic > 0)
    bad = q1(con, "SELECT count(*) FROM v_infra_capacity_daily WHERE sendable_per_day > theoretical_per_day")
    chk("G: sendable<=theoretical (=0)", bad, bad == 0)
    health = q1(con, "SELECT sum(unsendable) FROM v_account_health")
    chk("G: v_account_health filtering (unsendable>0 somewhere)", health, isinstance(health, (int, float)) and health > 0)

    # Track H
    cd = q1(con, "SELECT count(*) FROM core.campaign_daily")
    chk("H: campaign_daily rows>0", cd, isinstance(cd, int) and cd > 0)
    cv = q1(con, "SELECT count(*) FROM core.campaign_variant")
    chk("H: campaign_variant rows>0", cv, isinstance(cv, int) and cv > 0)
    # variant reconciles to campaign-daily total sent (sample: a campaign present in both)
    recon = q1(con, """
        WITH d AS (SELECT campaign_id, max(sent_cum) s FROM core.campaign_daily GROUP BY 1),
             v AS (SELECT campaign_id, sum(sent) s FROM core.campaign_variant GROUP BY 1)
        SELECT count(*) FROM d JOIN v USING (campaign_id) WHERE abs(d.s - v.s) > greatest(d.s,1)*0.02
    """)
    chk("H: variant sent reconciles to campaign (<=2% off)", recon, recon == 0)

    # Track I
    nsa = q1(con, "SELECT round(100.0*avg((nameserver_host IS NOT NULL)::int),1) FROM core.domain_registry "
                  "WHERE assigned_workspace IS NOT NULL OR COALESCE(inbox_count,0)>0")
    chk("I: NS coverage of active >=90%", nsa, isinstance(nsa, (int, float)) and nsa >= 90)
    tld = q1(con, "SELECT round(100.0*avg((tld IS NOT NULL)::int),1) FROM core.domain_registry")
    chk("I: tld 100%", tld, isinstance(tld, (int, float)) and tld >= 99.9)

    # Global invariant: no daily feed stale >36h
    stale = q1(con, "SELECT count(*) FROM core.sync_registry WHERE expected_cadence='daily' "
                    "AND status='active' AND last_synced_at < now() - INTERVAL 36 HOUR")
    chk("INV: no daily feed stale >36h (=0)", stale, stale == 0)

    con.close()

    passed = sum(1 for _, _, ok in checks if ok)
    total = len(checks)
    lines = [f"{'✅' if ok else '❌'} {label} = {val}" for label, val, ok in checks]
    summary = f"*Warehouse hardening DoD: {passed}/{total} pass*\n" + "\n".join(lines)
    print(summary)

    if not args.no_post:
        emoji = ":white_check_mark:" if passed == total else ":warning:"
        out = slack_post(f"{emoji} Warehouse-hardening write batch landed — DoD {passed}/{total}:\n"
                         + "\n".join(lines))
        print("posted:", out.get("ok"), out.get("error") or "")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
