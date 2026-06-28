#!/usr/bin/env python3
"""
account_errors_dashboard_data.py — feed generator for the portal "Account Errors" lens
(slug: lens-account-errors). Firefighting view for the infra team: per-workspace COUNTS of
live / connected / errored / paused / disconnected accounts, % healthy, and MISSED EMAILS
(daily send capacity lost to broken accounts), refreshed HOURLY. Scope toggle: All / OTD / Google.

SOURCE (read-only): the live Instantly /accounts poll already produced hourly by
  /root/renaissance-worker/jobs/live-accounts-snapshot/poll_live_accounts.py
which writes /root/core/live_accounts/latest.parquet (+ latest_summary.json). We DO NOT re-poll
Instantly — we read that snapshot.

Live status enum (Instantly v2):  1=connected, 2=paused, -1=connection_error,
                                  -2=soft_bounce, -3=sending_error.
provider_code: 1=OTD/custom SMTP, 2=Google, 3=Microsoft/Outlook.

Per-workspace COUNTS (never per-account lists):
  connected        = status = 1
  connection_error = status = -1
  sending_error    = status = -3
  disconnected     = status IS NULL OR status NOT IN (1,2,-1,-3)   # unknown / removed / soft-bounce
  paused           = status = 2
  total            = all rows
  missed (emails)  = SUM(daily_limit) over accounts that are neither connected nor paused
                     — the daily sends we *could* have sent if these accounts weren't broken.
  errored          = connection_error + sending_error   (computed client-side)
  pct_healthy      = 100 * connected / total            (computed client-side)
Each measure is shipped for three scopes: ALL accounts, OTD only (provider_code=1), Google only
(provider_code=2) — so the lens "All / OTD / Google" toggle is pure client-side.

Names + the active-workspace filter come from core.workspace (canonical name; is_active hides
deleted/cancelled workspaces). A workspace whose live poll FAILED (summary ok=false) is emitted
ok=false so the lens shows "no poll" instead of a false "healthy".

Usage:
  python3 account_errors_dashboard_data.py --out /root/portal/dashboards/lens-account-errors/data/latest.json
Env: CORE_DB_PATH=/opt/duckdb/warehouse_current.duckdb (names + active filter; optional/fail-open)
"""
from __future__ import annotations
import argparse, json, os, sys
from datetime import datetime, timezone

import duckdb

LIVE = os.environ.get("LIVE_ACCOUNTS_PARQUET", "/root/core/live_accounts/latest.parquet")
SUMMARY = os.environ.get("LIVE_ACCOUNTS_SUMMARY", "/root/core/live_accounts/latest_summary.json")
WAREHOUSE = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")

# FALLBACK ONLY (warehouse unattachable). Canonical names come from core.workspace.name; these
# mirror it so the convention holds even when degraded.
WS_NAME = {
    "renaissance-4": "Funding 1 (Samuel)", "renaissance-5": "Funding 2 (Ido)",
    "prospects-power": "Funding 3 (Leo)", "koi-and-destroy": "Funding 4 (Sam)",
    "renaissance-2": "Funding 5 (Eyver)", "the-gatekeepers": "Max's workspace",
    "section-125-1": "R&D Credit", "warm-leads": "Warm leads",
    "renaissance-1": "Renaissance 1 (Instantly)", "the-eagles": "My Organization",
}

BUCKETS = ("total", "connected", "connection_error", "sending_error", "paused", "disconnected", "missed")

# Per (workspace, provider_code): the status buckets + missed-email capacity. We fold provider_code
# into the three scopes (all / otd=1 / google=2) in Python.
COUNTS_SQL = """
SELECT workspace_slug, provider_code,
       count(*)                          AS total,
       sum((status = 1)::int)            AS connected,
       sum((status = -1)::int)           AS connection_error,
       sum((status = -3)::int)           AS sending_error,
       sum((status = 2)::int)            AS paused,
       sum((status IS NULL OR status NOT IN (1,2,-1,-3))::int) AS disconnected,
       CAST(COALESCE(sum(CASE WHEN status IS NULL OR status NOT IN (1,2)
                              THEN daily_limit END), 0) AS BIGINT) AS missed
FROM read_parquet('{live}')
GROUP BY 1, 2
"""


def empty():
    return {k: 0 for k in BUCKETS}


def add_into(acc, vals):
    for k in BUCKETS:
        acc[k] = acc.get(k, 0) + int(vals.get(k) or 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if not os.path.exists(LIVE):
        sys.exit(f"FATAL: live snapshot not found at {LIVE} (run poll_live_accounts.py first)")

    con = duckdb.connect()
    snap_at = con.execute(f"SELECT max(snapshot_at) FROM read_parquet('{LIVE}')").fetchone()[0]

    # per-workspace, three scopes (all / otd / google)
    scoped = {}  # slug -> {"all":{}, "otd":{}, "google":{}}
    for r in con.execute(COUNTS_SQL.format(live=LIVE)).fetchall():
        slug, pc = r[0], r[1]
        vals = dict(zip(BUCKETS, r[2:]))
        d = scoped.setdefault(slug, {"all": empty(), "otd": empty(), "google": empty()})
        add_into(d["all"], vals)
        if pc == 1:
            add_into(d["otd"], vals)
        elif pc == 2:
            add_into(d["google"], vals)

    # canonical names + active-workspace filter from core.workspace (fail-open if unattachable)
    active_names = {}
    if os.path.exists(WAREHOUSE):
        try:
            con.execute(f"ATTACH '{WAREHOUSE}' AS wh (READ_ONLY)")
            active_names = {row[0]: row[1] for row in con.execute(
                "SELECT slug, name FROM wh.core.workspace WHERE is_active").fetchall()}
        except Exception as e:  # noqa: BLE001
            sys.stderr.write(f"workspace name/active lookup skipped ({e})\n")
    con.close()

    # which workspaces FAILED the live poll (so the lens shows 'no poll', not false-healthy)
    failed = set()
    polled = set(scoped)
    if os.path.exists(SUMMARY):
        try:
            s = json.loads(open(SUMMARY).read())
            for w in s.get("workspaces", []):
                if w.get("ok") is False:
                    failed.add(w["workspace_slug"])
                polled.add(w["workspace_slug"])
        except Exception:  # noqa: BLE001
            pass

    workspaces = []
    tot = {"all": empty(), "otd": empty(), "google": empty()}
    for slug in sorted(polled, key=lambda s: -int((scoped.get(s, {}).get("all", {}) or {}).get("total", 0))):
        # hide deleted/cancelled workspaces (not is_active). fail-open if warehouse unavailable.
        if active_names and slug not in active_names:
            continue
        ok = slug not in failed
        d = scoped.get(slug, {"all": empty(), "otd": empty(), "google": empty()})
        row = {"workspace_slug": slug,
               "workspace_name": active_names.get(slug) or WS_NAME.get(slug, slug),
               "ok": ok, **d["all"], "otd": d["otd"], "google": d["google"]}
        workspaces.append(row)
        if ok:
            for sc in ("all", "otd", "google"):
                add_into(tot[sc], d[sc])

    out = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "snapshot_at": str(snap_at),
        "snapshot_age_min": round((datetime.now(timezone.utc)
                                   - datetime.fromisoformat(str(snap_at))).total_seconds() / 60, 1)
                            if snap_at else None,
        "source": "live Instantly /accounts poll (poll_live_accounts.py) — hourly",
        "status_enum": {"1": "connected", "2": "paused", "-1": "connection_error", "-3": "sending_error"},
        "cadence": "hourly",
        "scopes": ["all", "otd", "google"],
        "workspaces": workspaces,
        "totals": {**tot["all"], "otd": tot["otd"], "google": tot["google"]},
    }
    tmp = args.out + ".tmp"
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    os.replace(tmp, args.out)
    sys.stderr.write(f"wrote {args.out}: {len(workspaces)} workspaces, "
                     f"total {tot['all']['total']:,}, errored "
                     f"{tot['all']['connection_error'] + tot['all']['sending_error']:,}, "
                     f"missed {tot['all']['missed']:,}/day, snapshot {snap_at}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
