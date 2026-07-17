#!/usr/bin/env python3
"""Daily census reconciliation — the ground-truth check that makes fleet counts VERIFIABLE.

Independently re-counts each active workspace LIVE from Instantly (the poller's own plain /accounts
pager, paged to completion) and compares to core.account_census. Any workspace whose census count
drifts >THRESHOLD from the live count is FLAGGED (and alerted to #cc-sam with --alert). This is the
check that would have caught the-gatekeepers reading 46,993 when the true count was 20,742 — a wrong
number can no longer sit in the census unnoticed.

Reuses the poller's WORKSPACES map + page_accounts (no parallel pull path). Read-only on the DB.
Run after the nightly census promote (cron), or ad-hoc: python scripts/reconcile_census_live.py [--alert]
"""
from __future__ import annotations
import argparse, glob, os, sys, subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import duckdb

POLLER_DIR = "/root/renaissance-worker/jobs/live-accounts-snapshot"
CENSUS_DIR = "/root/core/live_accounts"   # the hourly poller's raw parquets = the ONLY backfill route


def load_env(path="/root/Renaissance/.env.instantly") -> dict:
    env = {}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("="); env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def live_count(slug: str, key: str) -> dict:
    try:
        accts = PAGE_G(key)
        emails = {(a.get("email") or "").strip().lower() for a in accts if a.get("email")}
        return {"slug": slug, "ok": True, "live": len(emails)}
    except Exception as e:  # 402 (retired) or persistent 500 -> can't independently verify; not a census error
        return {"slug": slug, "ok": False, "err": str(e)[:80]}


def census_gaps(con, lookback: int) -> tuple[list[str], int]:
    """Days in the last `lookback` days with NO census row + how stale the newest day is.

    WHY: the drift check below grades whatever max(census_date) happens to be, so a day that never
    landed is INVISIBLE to it — it compares an OLDER day against live Instantly, finds them equal,
    and prints "verified". That is exactly how 2026-06-30 was lost while this very check ran daily.
    Silence is not proof the history advanced. Read-only.
    """
    missing = [r[0] for r in con.execute(f"""
        SELECT CAST(day AS VARCHAR) FROM (
            SELECT unnest(generate_series(CURRENT_DATE - INTERVAL {int(lookback)} DAY,
                                          CURRENT_DATE - INTERVAL 1 DAY,
                                          INTERVAL 1 DAY))::DATE AS day
        ) s
        WHERE day NOT IN (SELECT DISTINCT census_date FROM core.account_census)
        ORDER BY 1
    """).fetchall()]
    stale = con.execute(
        "SELECT date_diff('day', max(census_date), CURRENT_DATE) FROM core.account_census").fetchone()[0]
    return missing, int(stale or 0)


def raw_poll_exists(day: str) -> bool:
    """True if the poller's raw parquet for `day` is still on disk — i.e. the gap is still
    BACKFILLABLE. Lets the alert say "fix it today" vs "that history is gone"."""
    return bool(glob.glob(f"{CENSUS_DIR}/accounts_live_{day.replace('-', '')}T*.parquet"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/root/core/warehouse.duckdb")
    ap.add_argument("--threshold", type=float, default=0.08, help="fractional drift that triggers a flag")
    ap.add_argument("--alert", action="store_true", help="post a #cc-sam alert on any drift")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--lookback", type=int, default=14,
                    help="days back to verify every census day is present")
    args = ap.parse_args()

    sys.path.insert(0, POLLER_DIR)   # box-only; imported at runtime so the module loads anywhere (CI)
    from poll_live_accounts import WORKSPACES, page_accounts  # DRY: same map + plain pager the census uses
    global WORKSPACES_G, PAGE_G; WORKSPACES_G, PAGE_G = WORKSPACES, page_accounts
    env = load_env()
    con = duckdb.connect(args.db, read_only=True)
    cen = dict(con.execute("""SELECT workspace_slug, count(*) FROM core.account_census
                              WHERE census_date=(SELECT max(census_date) FROM core.account_census)
                              GROUP BY 1""").fetchall())
    cen_day = con.execute("SELECT CAST(max(census_date) AS VARCHAR) FROM core.account_census").fetchone()[0]
    # Additive + fail-soft: a gap-check error must never take down the drift check that works.
    try:
        missing_days, stale_days = census_gaps(con, args.lookback)
    except Exception as exc:  # noqa: BLE001
        missing_days, stale_days = [], 0
        print(f"WARN census gap-check unavailable ({exc}) — drift check continues")
    con.close()

    jobs = [(s, env.get(WORKSPACES_G[s][1])) for s in WORKSPACES_G if env.get(WORKSPACES_G[s][1])]
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(lambda j: live_count(*j), jobs))

    ok = [r for r in results if r["ok"]]
    unver = [r for r in results if not r["ok"]]
    drifts = []
    for r in ok:
        c = cen.get(r["slug"], 0); l = r["live"]
        if l > 0 and abs(l - c) / l > args.threshold:
            drifts.append((r["slug"], c, l, round(100 * (c - l) / l, 1)))

    print(f"census reconciliation (census_date={cen_day}): {len(ok)} verified, {len(unver)} unverifiable, {len(drifts)} drift(s) >{int(args.threshold*100)}%")
    for r in ok:
        c = cen.get(r["slug"], 0)
        tag = "  DRIFT!" if any(d[0] == r["slug"] for d in drifts) else ""
        print(f"  {r['slug']:24s} census={c:>8,} live={r['live']:>8,}{tag}")
    for r in unver:
        print(f"  {r['slug']:24s} UNVERIFIABLE ({r['err']})")

    if drifts and args.alert:
        msg = (":warning: *census count drift vs live Instantly* (census_date=" + str(cen_day) + ") — "
               + "; ".join(f"{s}: census {c:,} vs live {l:,} ({p:+.1f}%)" for s, c, l, p in drifts)
               + ". The census may have a bad/partial pull — verify before trusting these workspace counts.")
        alert = Path(__file__).resolve().parents[1] / "scripts" / "alert_slack.py"
        if alert.exists():
            subprocess.run([sys.executable, str(alert), msg], check=False)
        print("ALERTED #cc-sam")

    # Continuity check LAST + fail-soft: the drift check above is the pre-existing, working
    # behaviour and must never be suppressed by anything new. See feedback_no_breaking_guards.
    gap_problem = False
    try:
        gap_problem = bool(missing_days) or stale_days > 1
        if gap_problem:
            recoverable = [d for d in missing_days if raw_poll_exists(d)]
            lost = [d for d in missing_days if d not in recoverable]
            print(f"CENSUS CONTINUITY: newest={cen_day} ({stale_days}d old); "
                  f"{len(missing_days)} missing day(s) in last {args.lookback}: {', '.join(missing_days) or '-'}")
            if recoverable:
                print(f"  backfillable (raw poll still on disk): {', '.join(recoverable)}")
            if lost:
                print(f"  NOT recoverable (no raw poll on disk): {', '.join(lost)}")
        else:
            print(f"census continuity: OK — no missing days in last {args.lookback}, newest={cen_day}")

        if gap_problem and args.alert:
            bits = []
            if stale_days > 1:
                bits.append(f"newest census day is {cen_day} ({stale_days} days old) — the daily history "
                            f"has STOPPED ADVANCING")
            if missing_days:
                rec = [d for d in missing_days if raw_poll_exists(d)]
                lst = [d for d in missing_days if d not in rec]
                if rec:
                    bits.append(f"missing day(s) {', '.join(rec)} — raw poll still on disk, BACKFILLABLE now")
                if lst:
                    bits.append(f"missing day(s) {', '.join(lst)} — no raw poll on disk, that history is GONE")
            msg = (":rotating_light: *census history gap* — " + "; ".join(bits)
                   + ". We cannot reconstruct what was live on a missing day. Backfill from the raw poll "
                     "before it ages out.")
            alert = Path(__file__).resolve().parents[1] / "scripts" / "alert_slack.py"
            if alert.exists():
                subprocess.run([sys.executable, str(alert), msg], check=False)
            print("ALERTED #cc-sam (census gap)")

    except Exception as exc:  # noqa: BLE001
        print(f"WARN census continuity report failed ({exc}) — drift result above stands")
    return 2 if drifts else (3 if gap_problem else 0)


if __name__ == "__main__":
    sys.exit(main())
