#!/usr/bin/env python3
"""Track H — build core.campaign_daily + core.campaign_variant from the Instantly
analytics API (UI-faithful, NOT pipeline-dependent).

Per campaign across the 16 live workspaces:
  GET /api/v2/campaigns/analytics/daily  -> per-day {sent, unique_replies,
        unique_replies_automatic, unique_opportunities}
  GET /api/v2/campaigns/analytics/steps  -> per-variant {sent, unique_replies, ...}

human/auto split is native per-day:
  replies_human = unique_replies   replies_auto = unique_replies_automatic
opportunities = unique_opportunities. meetings_booked joins core.meeting (posted_at::date).
bounces: NOT in the daily endpoint — fetched per campaign-day from the campaign-summary
  endpoint (GET /campaigns/analytics with start_date=end_date=<day> -> bounced_count)
  into core.instantly_bounce_daily (DDL 55), which survives the full rebuild and is
  UPDATE-joined into campaign_daily.bounces. Default window = last BOUNCE_DAYS days with
  sent > 0; one-time backfill via --bounce-days 45.

LIFECYCLE-AWARE: ACTIVE campaigns densified min(date)..today (no-send day = sent 0);
paused/completed/deleted keep only their real send days (endpoint stops at last activity).

The FETCH phase is lock-free; only the final warehouse write needs the single-writer lock.

Usage:
    python scripts/build_campaign_daily.py                 # fetch + write
    python scripts/build_campaign_daily.py --fetch-only --cache /tmp/cd.json
    python scripts/build_campaign_daily.py --from-cache /tmp/cd.json   # write only
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.build_campaign_daily")

KEYS_ENV = os.environ.get("INSTANTLY_KEYS_ENV", "/root/codex-ops/instantly-api-keys.env")
BASE = "https://api.instantly.ai/api/v2"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")  # node/curl UA trips CF 1010
# Must predate the earliest campaign send (portfolio started ~2025) or the daily series
# truncates historical sends and undercounts older campaigns vs the all-time steps grain.
# The endpoint only returns real activity dates, so an early start has no downside.
START_DATE = "2024-01-01"

# Instantly v2 campaign status codes.
STATUS_LABEL = {0: "draft", 1: "active", 2: "paused", 3: "completed", 4: "running_subsequences", -99: "suspended"}


def load_keys() -> dict[str, str]:
    txt = Path(KEYS_ENV).read_text()
    # file is `export INSTANTLY_API_KEYS={...json...}`
    for line in txt.splitlines():
        if "INSTANTLY_API_KEYS=" in line:
            payload = line.split("INSTANTLY_API_KEYS=", 1)[1].strip().strip("'").strip('"')
            return json.loads(payload)
    raise RuntimeError("INSTANTLY_API_KEYS not found in " + KEYS_ENV)


def _get(path: str, key: str, params: dict):
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {key}", "User-Agent": UA})
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.load(r)
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))  # immediate retries just re-trip 429 rate limits
    raise last


def list_campaigns(slug: str, key: str) -> list[dict]:
    out, cursor = [], None
    while True:
        params = {"limit": 100}
        if cursor:
            params["starting_after"] = cursor
        resp = _get("/campaigns", key, params)
        items = resp.get("items") if isinstance(resp, dict) else resp
        items = items or []
        for c in items:
            out.append({"id": c.get("id"), "name": c.get("name"),
                        "status": c.get("status"), "workspace_slug": slug})
        cursor = resp.get("next_starting_after") if isinstance(resp, dict) else None
        if not cursor or not items:
            break
    return out


def fetch_campaign(c: dict, key: str, today: str) -> dict:
    cid = c["id"]
    daily = _get("/campaigns/analytics/daily", key,
                 {"campaign_id": cid, "start_date": START_DATE, "end_date": today})
    steps = _get("/campaigns/analytics/steps", key, {"campaign_id": cid})
    daily = daily if isinstance(daily, list) else (daily.get("items") or [])
    steps = steps if isinstance(steps, list) else (steps.get("items") or [])
    return {"campaign": c, "daily": daily, "steps": steps}


def fetch_all(keys: dict[str, str], today: str, workers: int = 8) -> list[dict]:
    # 1. campaigns per workspace
    campaigns: list[tuple[dict, str]] = []
    for slug, key in keys.items():
        try:
            for c in list_campaigns(slug, key):
                if c["id"]:
                    campaigns.append((c, key))
        except Exception as exc:  # noqa: BLE001
            logger.warning("list_campaigns(%s) failed: %s", slug, exc)
    logger.info("fetched %d campaigns across %d workspaces", len(campaigns), len(keys))

    # 2. daily + steps per campaign (concurrent)
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_campaign, c, key, today): c["id"] for c, key in campaigns}
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                logger.warning("fetch_campaign(%s) failed: %s", futs[fut], exc)
    logger.info("fetched analytics for %d/%d campaigns", len(results), len(campaigns))
    return results


def bounce_pairs(results: list[dict], days: int, today: str) -> list[tuple[str, str, str]]:
    """(campaign_id, date, workspace_slug) pairs needing a bounce fetch: days within the
    window that had sent > 0. Zero-send days keep bounces = 0 (bounce events land on the
    send day at the grain this endpoint reports)."""
    cutoff = (dt.date.fromisoformat(today) - dt.timedelta(days=days)).isoformat()
    pairs = []
    for r in results:
        c = r["campaign"]
        for d in r["daily"]:
            ds = (d.get("date") or "")[:10]
            if ds >= cutoff and int(d.get("sent") or 0) > 0:
                pairs.append((c["id"], ds, c["workspace_slug"]))
    return pairs


def fetch_bounces(pairs: list[tuple[str, str, str]], keys: dict[str, str],
                  workers: int = 6) -> list[tuple[str, str, str, int]]:
    """One campaign-summary call per (campaign, day) -> bounced_count rows."""
    def one(pair):
        cid, day, slug = pair
        resp = _get("/campaigns/analytics", keys[slug],
                    {"id": cid, "start_date": day, "end_date": day})
        rec = resp[0] if isinstance(resp, list) and resp else (resp or {})
        # order matches the instantly_bounce_daily INSERT column list
        return (cid, day, int(rec.get("bounced_count") or 0), slug)

    rows, failed = [], 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(one, p): p for p in pairs}
        for i, fut in enumerate(as_completed(futs)):
            try:
                rows.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                failed += 1
                if failed <= 5:
                    logger.warning("bounce fetch %s failed: %s", futs[fut][:2], exc)
            if (i + 1) % 500 == 0:
                logger.info("bounce fetch %d/%d", i + 1, len(pairs))
    logger.info("fetched bounces for %d/%d campaign-days (%d failed)",
                len(rows), len(pairs), failed)
    return rows


def build_rows(results: list[dict], today: str):
    """Return (daily_rows, variant_rows) with densification + cumulative."""
    today_d = dt.date.fromisoformat(today)
    daily_rows, variant_rows = [], []
    for r in results:
        c = r["campaign"]
        cid, slug = c["id"], c["workspace_slug"]
        status = STATUS_LABEL.get(c.get("status"), str(c.get("status")))
        # index daily by date
        by_date: dict[str, dict] = {}
        for d in r["daily"]:
            dd = d.get("date")
            if dd:
                by_date[dd[:10]] = d
        if by_date:
            dates = sorted(by_date)
            lo = dt.date.fromisoformat(dates[0])
            # densify ACTIVE campaigns lo..today; others keep only observed days
            if status == "active":
                span = [(lo + dt.timedelta(days=i)).isoformat()
                        for i in range((today_d - lo).days + 1)]
            else:
                span = dates
            cs = co = crh = cra = 0
            for ds in span:
                d = by_date.get(ds, {})
                sent = int(d.get("sent") or 0)
                opp = int(d.get("unique_opportunities") or 0)
                rh = int(d.get("unique_replies") or 0)
                ra = int(d.get("unique_replies_automatic") or 0)
                cs += sent; co += opp; crh += rh; cra += ra
                daily_rows.append([cid, ds, slug, status, sent, opp, 0,  # meetings UPDATEd in SQL; 0 = none
                                   rh, ra, 0, cs, co, crh, cra])
        # variants
        for s in r["steps"]:
            step, variant = s.get("step"), s.get("variant")
            if step is None and variant is None:
                continue  # the null/null totals row
            variant_rows.append([cid, str(step), str(variant),
                                  int(s.get("sent") or 0),
                                  int(s.get("unique_replies") or 0),
                                  int(s.get("unique_replies_automatic") or 0)])
    return daily_rows, variant_rows


def write_warehouse(daily_rows, variant_rows, bounce_rows=None, db_path=None) -> None:
    conn = db_module.connect(db_path)
    ddl = REPO_ROOT / "sql" / "ddl" / "40_campaign_daily.sql"
    conn.execute(ddl.read_text())
    conn.execute((REPO_ROOT / "sql" / "ddl" / "55_instantly_bounce_daily.sql").read_text())
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-camdaily")
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM core.campaign_daily")
        conn.execute("DELETE FROM core.campaign_variant")
        conn.executemany(
            "INSERT INTO core.campaign_daily (campaign_id,date,workspace_slug,campaign_status,"
            "sent,opportunities,meetings_booked,replies_human,replies_auto,bounces,"
            "sent_cum,opportunities_cum,replies_human_cum,replies_auto_cum,_loaded_at,_run_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?)",
            [row + [run_id] for row in daily_rows],
        )
        conn.executemany(
            "INSERT INTO core.campaign_variant (campaign_id,step,variant,sent,"
            "replies_human,replies_auto,_loaded_at,_run_id) VALUES (?,?,?,?,?,?,now(),?)",
            [row + [run_id] for row in variant_rows],
        )
        # meetings_booked from core.meeting (posted_at::date, campaign_id)
        conn.execute(
            """
            UPDATE core.campaign_daily d SET meetings_booked = COALESCE(m.n, 0)
            FROM (
              SELECT campaign_id, CAST(posted_at AS DATE) AS date, count(*) n
              FROM core.meeting WHERE campaign_id IS NOT NULL GROUP BY 1, 2
            ) m
            WHERE m.campaign_id = d.campaign_id AND m.date = d.date
            """
        )
        # refresh the durable bounce store, then apply it to the rebuilt table
        if bounce_rows:
            conn.executemany(
                "INSERT OR REPLACE INTO core.instantly_bounce_daily "
                "(campaign_id, date, bounced, workspace_slug, _fetched_at) "
                "VALUES (?,?,?,?,now())",
                bounce_rows,
            )
        conn.execute(
            """
            UPDATE core.campaign_daily d SET bounces = b.bounced
            FROM core.instantly_bounce_daily b
            WHERE b.campaign_id = d.campaign_id AND b.date = d.date
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    nd = conn.execute("SELECT count(*) FROM core.campaign_daily").fetchone()[0]
    nv = conn.execute("SELECT count(*) FROM core.campaign_variant").fetchone()[0]
    logger.info("core.campaign_daily=%d rows, core.campaign_variant=%d rows", nd, nv)
    conn.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--fetch-only", action="store_true")
    ap.add_argument("--cache", default=None, help="write fetched analytics JSON here")
    ap.add_argument("--from-cache", default=None, help="skip fetch; load analytics JSON")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--bounce-days", type=int, default=3,
                    help="fetch bounced_count for sent>0 days this far back (45 = backfill)")
    ap.add_argument("--skip-bounces", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()

    keys = None
    if args.from_cache:
        results = json.loads(Path(args.from_cache).read_text())
    else:
        keys = load_keys()
        results = fetch_all(keys, today, workers=args.workers)
        if args.cache:
            Path(args.cache).write_text(json.dumps(results, default=str))
            logger.info("cached -> %s", args.cache)

    if args.fetch_only:
        logger.info("fetch-only; %d campaigns", len(results))
        return 0

    bounce_rows = None
    if not args.skip_bounces:
        if keys is None:
            keys = load_keys()
        pairs = bounce_pairs(results, args.bounce_days, today)
        logger.info("fetching bounces for %d campaign-days (last %d days, sent>0)",
                    len(pairs), args.bounce_days)
        bounce_rows = fetch_bounces(pairs, keys, workers=min(args.workers, 6))

    daily_rows, variant_rows = build_rows(results, today)
    logger.info("built %d daily rows, %d variant rows", len(daily_rows), len(variant_rows))
    write_warehouse(daily_rows, variant_rows, bounce_rows, Path(args.db) if args.db else None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
