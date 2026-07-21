#!/usr/bin/env python3
"""Intraday booking-form KPI feed: TODAY's sent/opps LIVE from Instantly -> Portal cache.

Companion to push_booking_kpi_to_portal.py. That script publishes only COMPLETED days
(warehouse mirror, HAVING sent>0) and by design skips the current day (the nightly-fed
mirror reads 0 until the next load, so "Today" on the KPIs tab is blank until ~1:30am ET
the next day). Its own comment flagged the gap: "Intraday freshness for the current day
needs an intraday warehouse metrics feed."

This IS that feed. It pulls TODAY's sent + opportunities straight from the Instantly
analytics API per workspace (live, no nightly needed) and UPSERTs today's rows into the
portal via the same `kpi_ingest_snapshot(jsonb)` RPC. Result: "Today" on the KPIs tab is
populated within one cron tick, so all stats are visible well before 9pm ET.

Composition (why the two feeds don't fight):
  - kpi_ingest_snapshot is UPSERT-by-(date,workspace). This feed emits ONLY today's rows.
  - The warehouse push emits ONLY completed days (never today). No key overlaps.
  - Tomorrow's nightly seals today; the warehouse push then overwrites this feed's live
    estimate with the exact figure. So today = live estimate, past = sealed truth.

Today's number is a LIVE estimate: Instantly analytics run a few % above the sealed
warehouse feed and climb through the day. Meetings/KPI are computed tab-side (kpi_compute
joins today's sent here with live im_bookings). We only publish sent + opps.

Reads:  INSTANTLY_KEY_* (one per workspace; from .env.instantly / env)
Writes: PORTAL_SUPABASE_URL (default pxrdmjjaxtqycuxhxmgi) +
        RENAISSANCE_PORTAL_SUPABASE_SERVICE_ROLE_KEY | IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY

Date basis: the KPIs tab's "Today" is the ET calendar date. Instantly analytics bucket by
UTC, but the fleet's sending window (ET business hours) falls inside the matching UTC date,
so pulling Instantly for the ET date is accurate to a small evening tail that the nightly
reconciles. We store under the ET date to match what the tab queries.

Cron (sync-runner-1, durable): hourly across the ET day, e.g.
  4,34 11-2 * * *  (UTC) -> covers ~7am-10pm ET every 30 min
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# canonical Instantly key env-var -> KPIs-tab workspace display name (the 10 tab rows).
# Fallback key names (legacy aliases) tried in order if the canonical one is unset.
WS_KEYS = [
    ("Funding 1 (Samuel)",       ["INSTANTLY_KEY_FUNDING_1_SAMUEL", "INSTANTLY_KEY_FUNDING_1", "INSTANTLY_KEY_RENAISSANCE_4"]),
    ("Funding 2 (Ido)",          ["INSTANTLY_KEY_FUNDING_2_IDO", "INSTANTLY_KEY_FUNDING_2", "INSTANTLY_KEY_RENAISSANCE_5"]),
    ("Funding 3 (Leo)",          ["INSTANTLY_KEY_FUNDING_3_LEO", "INSTANTLY_KEY_FUNDING_3", "INSTANTLY_KEY_PROSPECTS_POWER"]),
    ("Funding 4 (Sam)",          ["INSTANTLY_KEY_FUNDING_4_SAM", "INSTANTLY_KEY_FUNDING_4", "INSTANTLY_KEY_KOI_AND_DESTROY"]),
    ("Funding 5 (Eyver)",        ["INSTANTLY_KEY_FUNDING_5_EYVER", "INSTANTLY_KEY_FUNDING_5", "INSTANTLY_KEY_RENAISSANCE_2"]),
    ("Renaissance 1 (Instantly)", ["INSTANTLY_KEY_RENAISSANCE_1"]),
    ("Warm leads",               ["INSTANTLY_KEY_WARM_LEADS"]),
    ("Max's workspace",          ["INSTANTLY_KEY_MAXS", "INSTANTLY_KEY_THE_GATEKEEPERS", "INSTANTLY_KEY_MAX"]),
    ("Tariffs",                  ["INSTANTLY_KEY_TARIFFS"]),
    ("Section 125",              ["INSTANTLY_KEY_SECTION_125"]),
]
KPI_WORKSPACES = [(n, i + 1) for i, (n, _) in enumerate(WS_KEYS)]
INSTANTLY_DAILY = "https://api.instantly.ai/api/v2/campaigns/analytics/daily"


def _load_env_file(path: str) -> None:
    if not path or not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def _load_envs() -> None:
    # repo .env (portal key) + .env.instantly (workspace keys), both overridable.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for p in (os.environ.get("ENV_FILE"), os.path.join(repo_root, ".env"),
              os.environ.get("INSTANTLY_ENV_FILE"),
              os.path.join(repo_root, ".env.instantly"),
              "/root/renaissance-warehouse/.env.instantly",
              "/root/.env.instantly"):
        if p:
            _load_env_file(p)


def _env(*names, default=None):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


# Instantly sits behind Cloudflare, which 403s (error 1010) the default urllib
# User-Agent. A normal UA passes (curl works for the same reason).
_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def _get(url: str, key: str, tries: int = 4):
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {key}", "User-Agent": _UA})
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = f"HTTP {e.code}"
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 * (attempt + 1))
                continue
            return {"_error": last}
        except Exception as e:  # noqa: BLE001
            last = str(e)[:120]
            time.sleep(2 * (attempt + 1))
    return {"_error": last}


def instantly_today(day: str, key: str) -> tuple[int, int] | None:
    """(sent, opps) for `day` (YYYY-MM-DD) for the workspace this key belongs to."""
    out = _get(f"{INSTANTLY_DAILY}?start_date={day}&end_date={day}", key)
    if isinstance(out, dict) and out.get("_error"):
        return None
    rows = out if isinstance(out, list) else out.get("data", []) if isinstance(out, dict) else []
    sent = sum(int(r.get("sent") or 0) for r in rows)
    opps = sum(int(r.get("opportunities") or 0) for r in rows)
    return sent, opps


def portal_ingest(snapshot: dict) -> dict:
    base = _env("PORTAL_SUPABASE_URL", "IM_BOOKINGS_SUPABASE_URL",
                default="https://pxrdmjjaxtqycuxhxmgi.supabase.co")
    key = _env("RENAISSANCE_PORTAL_SUPABASE_SERVICE_ROLE_KEY",
               "IM_BOOKINGS_SUPABASE_SERVICE_ROLE_KEY")
    if not key:
        raise SystemExit("portal service-role key not set")
    req = urllib.request.Request(
        base.rstrip("/") + "/rest/v1/rpc/kpi_ingest_snapshot",
        data=json.dumps({"p": snapshot}).encode(),
        method="POST",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)


def main() -> int:
    _load_envs()
    dry = "--dry" in sys.argv
    day = _env("KPI_INTRADAY_DAY")  # override for testing
    if not day:
        day = datetime.now(ZoneInfo("America/New_York")).date().isoformat()

    rows, missing, errors = [], [], []
    for ws_name, key_names in WS_KEYS:
        key = _env(*key_names)
        if not key:
            missing.append(ws_name)
            continue
        res = instantly_today(day, key)
        if res is None:
            errors.append(ws_name)
            continue
        sent, opps = res
        if sent > 0:  # match warehouse push: never regress a day to 0
            rows.append({"d": day, "ws": ws_name, "sent": sent, "opps": opps})

    ts = datetime.now(timezone.utc).isoformat()
    if dry:
        print(f"[dry {ts}] day={day} rows={len(rows)} missing={missing} errors={errors}")
        for r in sorted(rows, key=lambda x: -x["sent"]):
            print(f"   {r['ws']:28s} sent={r['sent']:>9} opps={r['opps']:>4}")
        return 0

    if not rows:
        print(f"[{ts}] intraday: no positive-sent workspaces for {day} "
              f"(missing={missing} errors={errors}) — nothing pushed")
        return 0

    snapshot = {
        "generated_at": ts,
        "workspace_daily": rows,
        "workspaces": [{"name": n, "sort_order": so} for n, so in KPI_WORKSPACES],
    }
    res = portal_ingest(snapshot)
    total_sent = sum(r["sent"] for r in rows)
    print(f"[{ts}] intraday KPI -> portal: day={day} ws={len(rows)} "
          f"sent_total={total_sent} opps_total={sum(r['opps'] for r in rows)} "
          f"ingest={res} missing={missing} errors={errors}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
