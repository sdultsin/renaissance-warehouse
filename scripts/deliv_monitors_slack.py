#!/usr/bin/env python3
"""Daily DELIVERABILITY safe-monitors Slack post (#cc-sam).

Posts BOTH D2 monitors with their RECENT-WINDOW trend, once/day:
  1. REPLY-LAG    — send->first-reply median latency, last-7d vs prior-7d (rising = leading
                    soft-foldering signal). Reads core.deliv_reply_lag (built by
                    build_deliv_reply_lag.py) via v_deliv_reply_lag_daily_org + the
                    deliv_reply_lag_rollup() macro.
  2. HUMAN-vs-AUTO— daily human vs auto reply RATE + the auto:human ratio (ratio > 1 =
                    auto outpaces human = classic soft-foldering). Reads
                    v_deliv_human_auto_reply_daily_org (Instantly-native truth).

This is a HEALTH MONITOR — it posts the daily numbers every day (wanted signal, not noise,
per the spec). The gate below is ONCE-PER-DAY (a sentinel file), not a quiet-on-healthy gate.

READ PATH: opens the latest READ-ONLY serving snapshot (no writer contention), same family
the warehouse query API serves. Falls back to the live warehouse read-only if no snapshot.
Never writes the warehouse.

CHANNEL/TOKEN: #cc-sam = C0AR0EA21C1 (same as warehouse_qa.py / backfill_enriched_phones.py).
Token resolution is tolerant of the box's key naming (SLACK_TOKEN on the droplet,
CC_SLACK_BOT_TOKEN / SLACK_BROWSER_TOKEN in the repo .env) — tries in order.

Usage:
    python scripts/deliv_monitors_slack.py                 # post if not already posted today
    python scripts/deliv_monitors_slack.py --force         # ignore the once-a-day gate
    python scripts/deliv_monitors_slack.py --dry-run       # print the post, do not send
    python scripts/deliv_monitors_slack.py --db /path/to/snapshot.duckdb
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"
SLACK_CHANNEL = "C0AR0EA21C1"  # #cc-sam
SNAPSHOT_GLOB = "/opt/duckdb/snapshots/warehouse_*.duckdb"
LIVE_DB = "/root/core/warehouse.duckdb"
SENTINEL = Path(os.environ.get("DELIV_MONITORS_SENTINEL", "/root/core/deliv_monitors_last_post.txt"))


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def slack_post(env: dict[str, str], text: str) -> dict:
    token = (
        os.environ.get("SLACK_TOKEN")
        or env.get("SLACK_TOKEN")
        or env.get("CC_SLACK_BOT_TOKEN")
        or env.get("SLACK_BROWSER_TOKEN")
    )
    cookie = (
        os.environ.get("SLACK_COOKIE")
        or env.get("SLACK_COOKIE")
        or env.get("SLACK_BROWSER_COOKIE")
    )
    if not token:
        print("deliv_monitors_slack: no Slack token, skipping post", flush=True)
        return {"ok": False, "error": "no_token"}
    body = json.dumps({"channel": SLACK_CHANNEL, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            **({"Cookie": f"d={cookie}"} if cookie else {}),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            out = json.loads(resp.read().decode("utf-8"))
        if not out.get("ok"):
            print(f"deliv_monitors_slack: slack error {out.get('error')}", flush=True)
        return out
    except Exception as exc:  # noqa: BLE001
        print(f"deliv_monitors_slack: slack post failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


def _has_objects(db_path: str) -> bool:
    """True if this DB carries the DDL-84 objects (so a stale pre-84 snapshot is skipped)."""
    try:
        import duckdb  # local import
        c = duckdb.connect(db_path, read_only=True)
        try:
            c.execute("SELECT 1 FROM v_deliv_human_auto_reply_daily_org LIMIT 1")
            c.execute("SELECT 1 FROM v_deliv_reply_lag_daily_org LIMIT 1")
            return True
        finally:
            c.close()
    except Exception:  # noqa: BLE001
        return False


def pick_db(explicit: str | None) -> str:
    """Latest serving snapshot THAT HAS the DDL-84 objects; else the live writer (read-only).

    The 07:00 UTC cron runs after the nightly publishes a fresh snapshot, so the snapshot
    normally has the objects. But if a nightly failed to republish (stale pre-84 snapshot),
    we fall back to the live writer read-only rather than crash — the monitor must survive a
    bad nightly. Live read-only never contends (DuckDB allows concurrent read-only opens).
    """
    if explicit:
        return explicit
    for snap in sorted(glob.glob(SNAPSHOT_GLOB), reverse=True):
        if _has_objects(snap):
            return snap
    if _has_objects(LIVE_DB):
        return LIVE_DB
    # last resort — return the newest snapshot (will surface a clear error if truly empty)
    snaps = sorted(glob.glob(SNAPSHOT_GLOB))
    return snaps[-1] if snaps else LIVE_DB


def _fmt_min(m) -> str:
    if m is None:
        return "n/a"
    m = float(m)
    if m < 90:
        return f"{m:.0f}m"
    return f"{m/60:.1f}h"


def _arrow(cur, prev, *, higher_is_worse=True) -> str:
    if cur is None or prev is None or prev == 0:
        return ""
    delta = (cur - prev) / prev * 100.0
    if abs(delta) < 5:
        return "→ flat"
    up = delta > 0
    bad = up if higher_is_worse else not up
    icon = ":small_red_triangle:" if bad else ":large_green_circle:"
    return f"{icon} {'+' if up else ''}{delta:.0f}% vs prior 7d"


def build_message(db) -> str:
    today = dt.date.today().isoformat()
    snap_id = Path(db.execute("PRAGMA database_list").fetchall()[0][2] or "").name or "(live)"

    # --- Monitor 1: reply-lag, last-7d vs prior-7d (recompute from base) ---------
    cur = db.execute(
        "SELECT n_replies, median_lag_min, p75_lag_min, p90_lag_min, pct_over_6h "
        "FROM deliv_reply_lag_rollup(current_date - 7, current_date - 1)"
    ).fetchone()
    prev = db.execute(
        "SELECT n_replies, median_lag_min, p75_lag_min, p90_lag_min, pct_over_6h "
        "FROM deliv_reply_lag_rollup(current_date - 14, current_date - 8)"
    ).fetchone()
    # 7-day daily sample
    # Daily sample: last 7 COMPLETE days (exclude today's partial day — it is noisy
    # until the nightly fact catches up, and the lag fact lags ~2d anyway).
    lag_days = db.execute(
        "SELECT reply_date, n_replies, median_lag_min, p75_lag_min, pct_over_6h "
        "FROM v_deliv_reply_lag_daily_org "
        "WHERE reply_date >= current_date - 7 AND reply_date < current_date ORDER BY reply_date"
    ).fetchall()

    # --- Monitor 2: human-vs-auto, last-7d vs prior-7d (sum measures, recompute rates) --
    ha_cur = db.execute(
        "SELECT 100.0*SUM(human_replies)/NULLIF(SUM(sent),0), "
        "       100.0*SUM(auto_replies)/NULLIF(SUM(sent),0), "
        "       1.0*SUM(auto_replies)/NULLIF(SUM(human_replies),0) "
        "FROM v_deliv_human_auto_reply_daily_org "
        "WHERE reply_date >= current_date - 7 AND reply_date < current_date"
    ).fetchone()
    ha_prev = db.execute(
        "SELECT 100.0*SUM(human_replies)/NULLIF(SUM(sent),0), "
        "       100.0*SUM(auto_replies)/NULLIF(SUM(sent),0), "
        "       1.0*SUM(auto_replies)/NULLIF(SUM(human_replies),0) "
        "FROM v_deliv_human_auto_reply_daily_org "
        "WHERE reply_date >= current_date - 14 AND reply_date < current_date - 7"
    ).fetchone()
    ha_days = db.execute(
        "SELECT reply_date, sent, human_replies, auto_replies, "
        "       human_reply_rate_pct, auto_reply_rate_pct, auto_to_human_ratio "
        "FROM v_deliv_human_auto_reply_daily_org "
        "WHERE reply_date >= current_date - 7 AND reply_date < current_date ORDER BY reply_date"
    ).fetchall()

    lines = []
    lines.append(f":satellite_antenna: *Deliverability monitors — {today}*  _(P0 / north-star; soft-foldering early-warning)_")
    lines.append(f"_snapshot: `{snap_id}`_")
    lines.append("")

    # Tile 2 first (it's the headline soft-foldering signal)
    lines.append("*2 · Human vs Auto reply rate*  _(human folds to spam, auto holds up → ratio climbs)_")
    if ha_cur and ha_cur[0] is not None:
        h7, a7, r7 = ha_cur
        rp = ha_prev[2] if ha_prev else None
        lines.append(
            f"   last 7d: human `{h7:.3f}%`  ·  auto `{a7:.3f}%`  ·  *auto:human `{r7:.2f}`*  "
            f"{_arrow(r7, rp)}"
        )
        if r7 and r7 > 1.0:
            lines.append("   :warning: auto ≥ human — soft-foldering signature is PRESENT.")
    lines.append("   ```")
    lines.append("   date        sent     human  auto   h-rr%  a-rr%  auto:human")
    for d in ha_days:
        rd, sent, hr, ar, hrr, arr, ratio = d
        lines.append(
            f"   {rd}  {int(sent or 0):>8,}  {int(hr or 0):>5}  {int(ar or 0):>5}  "
            f"{(hrr or 0):>5.3f}  {(arr or 0):>5.3f}  {(ratio or 0):>6.2f}"
        )
    lines.append("   ```")
    lines.append("")

    # Tile 1
    lines.append("*1 · Reply-lag (send → first-reply)*  _(rising median = mail seen later = placement decay)_")
    if cur and cur[1] is not None:
        med_arrow = _arrow(cur[1], prev[1] if prev else None)
        lines.append(
            f"   last 7d: median `{_fmt_min(cur[1])}`  ·  p75 `{_fmt_min(cur[2])}`  ·  "
            f"p90 `{_fmt_min(cur[3])}`  ·  >6h `{(cur[4] or 0):.1f}%`   {med_arrow}"
        )
    lines.append("   ```")
    lines.append("   date        n_repl  median   p75    >6h%")
    for d in lag_days:
        rd, n, med, p75, over6 = d
        lines.append(
            f"   {rd}  {int(n or 0):>6}  {_fmt_min(med):>6}  {_fmt_min(p75):>6}  {(over6 or 0):>5.1f}"
        )
    lines.append("   ```")
    lines.append("")
    lines.append(
        "_defs: human=unique_replies, auto=unique_replies_automatic (Instantly native); "
        "rates over sends. Lag = prospect first-reply − the campaign send it answered. "
        "Org-wide, UTC. Fact lags ~1d (sends) / ~2d (lag, late replies)._"
    )
    return "\n".join(lines)


def already_posted_today() -> bool:
    if not SENTINEL.exists():
        return False
    try:
        return SENTINEL.read_text().strip() == dt.date.today().isoformat()
    except Exception:  # noqa: BLE001
        return False


def mark_posted_today() -> None:
    try:
        SENTINEL.parent.mkdir(parents=True, exist_ok=True)
        SENTINEL.write_text(dt.date.today().isoformat())
    except Exception as exc:  # noqa: BLE001
        print(f"deliv_monitors_slack: could not write sentinel: {exc}", flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="duckdb path (default = latest serving snapshot)")
    ap.add_argument("--force", action="store_true", help="ignore the once-a-day gate")
    ap.add_argument("--dry-run", action="store_true", help="print the post, do not send")
    args = ap.parse_args(argv)

    if not args.force and not args.dry_run and already_posted_today():
        print("deliv_monitors_slack: already posted today, skipping (use --force).", flush=True)
        return 0

    import duckdb  # local import so --help works without the dep

    db_path = pick_db(args.db)
    conn = duckdb.connect(db_path, read_only=True)
    try:
        msg = build_message(conn)
    finally:
        conn.close()

    if args.dry_run:
        print(msg)
        return 0

    env = load_env(ENV_PATH)
    out = slack_post(env, msg)
    if out.get("ok"):
        mark_posted_today()
        print("deliv_monitors_slack: posted to #cc-sam.", flush=True)
        return 0
    print(f"deliv_monitors_slack: post failed ({out.get('error')}).", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
