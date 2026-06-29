"""E1/E2 watchdog for the nightly Sendivo /sms/logs rollup (audit 2026-06-14).

The /sms/logs pull rolls ~1.1M messages/day into raw_sendivo_campaign_daily with PER-DAY
try/except isolation and backfill=1 — so a flaky day is simply MISSING and (without this) never
retried, and a mid-run page failure UNDER-COUNTS while the nightly still reports green. Neither is
visible anywhere. This watchdog watches the gap between *attempted* (the API's full-table count) and
*committed* (sum(n_messages) in the warehouse) — exactly where the silent failure hides.

For each of the last N days (excluding today, which is incomplete):
  committed = sum(n_messages) for that metric_date, latest run, from raw_sendivo_campaign_daily.
  api_total = a fresh, cheap GET /sms/logs page-1 -> pagination.total (read-only).
  MISSING  if committed == 0 but api_total > 0  (day dropped entirely).
  SHORT    if api_total - committed > max(TOL_ABS, TOL_FRAC * api_total)  (under-count / gap).

Default: detect + Slack-alert #cc-sam (gated by a state file so transient blips don't spam). With
--heal it re-pulls the bad days via `python -m core.orchestrator --phase sendivo --ingest sms_logs`
(SENDIVO_LOGS_TARGET_DAYS override). The re-pull opens the DuckDB writer; if nightly holds it the
open fails and we just defer to the next run — single-writer safety needs no extra lock.

Cron (UTC, droplet) — daily after nightly + dashboards settle:
  30 7 * * * /root/renaissance-warehouse/scripts/sendivo_sms_logs_watchdog.sh >> \
             /root/renaissance-warehouse/logs/sms_logs_watchdog.log 2>&1
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from core import db as db_module  # noqa: E402
from core.config import DB_PATH  # noqa: E402
from sources.sendivo import SendivoClient  # noqa: E402

ENV_PATH = REPO_ROOT / ".env"
STATE_PATH = Path(os.environ.get("SMS_WATCHDOG_STATE", "/root/core/sms_logs_watchdog_state.json"))
SLACK_CHANNEL = "C0AR0EA21C1"  # #cc-sam

DAYS_BACK = int(os.environ.get("SMS_WATCHDOG_DAYS", "7"))
TOL_FRAC = float(os.environ.get("SMS_WATCHDOG_TOL_FRAC", "0.01"))  # 1%
TOL_ABS = int(os.environ.get("SMS_WATCHDOG_TOL_ABS", "1000"))

# Read the PUBLISHED serving snapshot, never the live writer DB. The nightly orchestrator holds the
# warehouse writer lock and (lately) runs past this watchdog's 07:30 slot, so opening DB_PATH
# read-only RACED the lock and the watchdog silently SKIPPED every run since 2026-06-23 ("Conflicting
# lock is held"). The serving snapshot is a static, published read-only copy -> no contention, always
# openable. (committed-vs-API uses the published numbers, which is exactly what we want to validate.)
SERVING_GLOB = os.environ.get(
    "WAREHOUSE_SERVING_GLOB", "/mnt/volume_nyc1_1781398428838/serving/snapshots/warehouse_*.duckdb"
)
SERVING_KNOWN_GOOD = Path("/opt/duckdb/snapshots/_known_good")


def serving_snapshot() -> str:
    """Path to the latest PUBLISHED warehouse serving snapshot (read-only, never lock-contended)."""
    try:
        if SERVING_KNOWN_GOOD.exists():
            return str(SERVING_KNOWN_GOOD.resolve())
    except OSError:
        pass
    import glob
    snaps = sorted(glob.glob(SERVING_GLOB), key=os.path.getmtime)
    if snaps:
        return snaps[-1]
    raise FileNotFoundError(f"no serving snapshot ({SERVING_KNOWN_GOOD} / {SERVING_GLOB})")


def load_env(path: Path) -> dict:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def slack_post(text: str) -> None:
    env = load_env(ENV_PATH)
    token = env.get("SLACK_TOKEN") or env.get("CC_SLACK_BOT_TOKEN")
    cookie = env.get("SLACK_COOKIE")
    if not token:
        print("watchdog: no SLACK_TOKEN, skipping alert", flush=True)
        return
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
                print(f"watchdog: slack error {out.get('error')}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"watchdog: slack post failed: {exc}", flush=True)


def committed_for_day(conn, day: str) -> int:
    """sum(n_messages) for the latest run of this metric_date (mirrors v_sms_campaign_performance)."""
    row = conn.execute(
        """
        WITH rr AS (
          SELECT _run_id, ROW_NUMBER() OVER (ORDER BY max(_loaded_at) DESC) rn
          FROM raw_sendivo_campaign_daily WHERE metric_date = ? GROUP BY _run_id
        )
        SELECT COALESCE(sum(cd.n_messages), 0)
        FROM raw_sendivo_campaign_daily cd
        JOIN rr ON cd._run_id = rr._run_id AND rr.rn = 1
        WHERE cd.metric_date = ?
        """,
        [day, day],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def api_total_for_day(cli: SendivoClient, day: str) -> int | None:
    try:
        d = cli.sms_logs_page(day, 1, per_page=1)
        return (d.get("pagination") or {}).get("total")
    except Exception as exc:  # noqa: BLE001
        print(f"watchdog: api_total {day} failed: {exc}", flush=True)
        return None


def evaluate(conn, cli, days: list[str], max_published: str | None = None) -> list[dict]:
    issues = []
    for day in days:
        # A day NEWER than the published snapshot's latest committed metric_date is not a gap --
        # it simply hasn't been published yet (e.g. a long-running nightly that pulled it but has
        # not promoted the snapshot). Skip it so the watchdog never false-alarms on not-yet-published
        # days. Real gaps (a missing/short day WITHIN the published range) are still caught.
        if max_published is not None and day > max_published:
            continue
        committed = committed_for_day(conn, day)
        total = api_total_for_day(cli, day)
        if not isinstance(total, int) or total <= 0:
            # No API truth (transient / nothing sent) — can't judge; skip rather than false-alarm.
            continue
        gap = total - committed
        tol = max(TOL_ABS, int(TOL_FRAC * total))
        kind = None
        if committed == 0:
            kind = "MISSING"
        elif gap > tol:
            kind = "SHORT"
        if kind:
            issues.append({"day": day, "kind": kind, "committed": committed,
                           "api_total": total, "gap": gap, "tol": tol})
    return issues


def heal(days: list[str]) -> tuple[bool, str]:
    """Re-pull the given days through the normal orchestrator path. Returns (ran, message)."""
    env = dict(os.environ)
    env["SENDIVO_LOGS_TARGET_DAYS"] = ",".join(days)
    cmd = [sys.executable, "-m", "core.orchestrator", "--phase", "sendivo", "--ingest", "sms_logs"]
    try:
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=7200)
    except subprocess.TimeoutExpired:
        return False, "heal timed out (>2h)"
    tail = (r.stdout or "")[-400:] + (r.stderr or "")[-400:]
    locked = "Conflicting lock" in tail or "Could not set lock" in tail or "database is locked" in tail.lower()
    if locked:
        return False, "DB busy (writer held) — deferred to next run"
    return (r.returncode == 0), f"orchestrator rc={r.returncode}"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Sendivo /sms/logs committed-vs-attempted watchdog (E1/E2)")
    p.add_argument("--days", type=int, default=DAYS_BACK)
    p.add_argument("--heal", action="store_true", help="re-pull MISSING/SHORT days, then re-check")
    p.add_argument("--check-only", action="store_true", help="never heal even if --heal env is set")
    args = p.parse_args(argv)

    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=i)).isoformat() for i in range(args.days, 0, -1)]

    try:
        snap = serving_snapshot()
        conn = db_module.connect(Path(snap), read_only=True)
    except Exception as exc:  # noqa: BLE001 — no published snapshot yet: skip, retry next run
        print(f"watchdog: cannot open serving snapshot read-only ({exc}); skipping this run", flush=True)
        return 0

    # Latest metric_date present in the PUBLISHED snapshot -> the high-water mark we can judge.
    row = conn.execute("SELECT max(metric_date) FROM raw_sendivo_campaign_daily").fetchone()
    max_pub = row[0].isoformat() if row and row[0] is not None else None

    key = load_env(ENV_PATH).get("SENDIVO_API_KEY")
    if not key:
        print("watchdog: no SENDIVO_API_KEY; skipping", flush=True)
        return 0

    with SendivoClient(key) as cli:
        issues = evaluate(conn, cli, days, max_pub)
    conn.close()

    healed_note = ""
    if issues and args.heal and not args.check_only:
        bad_days = sorted({i["day"] for i in issues})
        print(f"watchdog: healing {bad_days}", flush=True)
        ran, msg = heal(bad_days)
        healed_note = f"\n:arrows_counterclockwise: heal {bad_days}: {msg}"
        if ran:
            # re-evaluate after the re-pull
            try:
                conn = db_module.connect(DB_PATH, read_only=True)
                with SendivoClient(key) as cli:
                    issues = evaluate(conn, cli, days)
                conn.close()
            except Exception as exc:  # noqa: BLE001
                healed_note += f" (re-check failed: {exc})"

    # State-gated alerting: only ping when the bad-day signature CHANGES (new trouble or recovery).
    sig = ",".join(f"{i['day']}:{i['kind']}" for i in sorted(issues, key=lambda x: x["day"]))
    prev = ""
    try:
        prev = json.loads(STATE_PATH.read_text()).get("sig", "")
    except Exception:  # noqa: BLE001
        pass

    if issues:
        if sig != prev:
            lines = [f":rotating_light: *Sendivo /sms/logs gap detected* ({len(issues)} day(s)) — silently dropped/under-counted:"]
            for i in issues:
                lines.append(f"  • {i['day']}: {i['kind']} — committed {i['committed']:,} vs API {i['api_total']:,} "
                             f"(gap {i['gap']:,}, tol {i['tol']:,})")
            lines.append("Re-pull: `SENDIVO_LOGS_TARGET_DAYS=<days> python -m core.orchestrator --phase sendivo --ingest sms_logs`"
                         " (or run this watchdog with --heal).")
            slack_post("\n".join(lines) + healed_note)
        print(f"watchdog: ISSUES {sig}", flush=True)
    else:
        if prev:
            slack_post(f":white_check_mark: Sendivo /sms/logs gaps cleared — last {args.days} days reconcile within tolerance." + healed_note)
        print(f"watchdog: OK (last {args.days} days reconcile)", flush=True)

    try:
        STATE_PATH.write_text(json.dumps({"sig": sig, "checked_at": today.isoformat()}))
    except Exception as exc:  # noqa: BLE001
        print(f"watchdog: could not write state: {exc}", flush=True)

    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main())
