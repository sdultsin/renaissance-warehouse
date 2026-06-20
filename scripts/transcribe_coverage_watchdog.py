"""Coverage watchdog for warm-call transcription (handoff 2026-06-16-call-transcription-backfill).

Watches the gap between *attempted* (Close calls WITH a recording) and *committed* (those that
have a row in core.call_transcript) — exactly where the silent transcription gap hides. The daily
transcribe job (scripts/transcribe_calls.py) used to die on the DuckDB single-writer lock and stop
producing transcripts while recordings kept arriving; nothing noticed for days (Jun-12 329/0,
Jun-15 279/0). This is the OUTCOME monitor: it counts transcripts actually committed, not "the cron
fired".

For each of the last N days (EXCLUDING today, which has no nightly close-sync yet):
  recorded     = core.call rows with has_recording AND recording_url (the day's recordings).
  transcribed  = those that have a core.call_transcript row.
  gap          = recorded - transcribed.
  BAD if gap > max(TOL_ABS, TOL_FRAC * recorded)  (a whole missed day is ~300; dead-url residual 1-2).

Anti-flap: a day must read BAD on >=2 CONSECUTIVE runs before it pages (state file holds the
consecutive-fail count). Pages #cc-sam (tag Sam) only when the paged-day signature CHANGES; sends a
recovery ping when previously-paged days clear. With --heal it first runs the (idempotent, now
lock-robust) transcribe job to self-heal, then re-checks.

Reads the LIVE warehouse read-only; if it's locked it falls back to the serving snapshot, and if
both are unreachable it skips this run (retries next) rather than false-alarm.

Cron (UTC) — after the daily transcribe (08:30) has had time to run:
    0 9 * * * /root/renaissance-warehouse/scripts/transcribe_coverage_watchdog.sh >> \
              /root/renaissance-warehouse/logs/transcribe_watchdog.log 2>&1
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

ENV_PATH = REPO_ROOT / ".env"
STATE_PATH = Path(os.environ.get("TRANSCRIBE_WATCHDOG_STATE", "/root/core/transcribe_watchdog_state.json"))
SNAPSHOT = Path(os.environ.get("CALL_TRANSCRIBE_SNAPSHOT", "/opt/duckdb/warehouse_current.duckdb"))
SLACK_CHANNEL = "C0AR0EA21C1"  # #cc-sam
SAM = "<@U0AM2CQHW9E>"

DAYS_BACK = int(os.environ.get("TRANSCRIBE_WATCHDOG_DAYS", "7"))
TOL_FRAC = float(os.environ.get("TRANSCRIBE_WATCHDOG_TOL_FRAC", "0.05"))  # 5%
TOL_ABS = int(os.environ.get("TRANSCRIBE_WATCHDOG_TOL_ABS", "5"))
FAIL_THRESHOLD = int(os.environ.get("TRANSCRIBE_WATCHDOG_FAIL_THRESHOLD", "2"))  # consecutive bad runs


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


def _connect_ro():
    """Live warehouse read-only; fall back to the serving snapshot if the writer holds the lock.
    Returns (conn, source) or (None, None) if neither is reachable."""
    try:
        return db_module.connect(DB_PATH, read_only=True), "live"
    except Exception as exc:  # noqa: BLE001
        print(f"watchdog: live RO unavailable ({str(exc)[:80]}); trying snapshot", flush=True)
    if SNAPSHOT.exists():
        try:
            return db_module.connect(SNAPSHOT, read_only=True), "snapshot"
        except Exception as exc:  # noqa: BLE001
            print(f"watchdog: snapshot unavailable ({str(exc)[:80]})", flush=True)
    return None, None


def evaluate(conn, days: list[str]) -> list[dict]:
    rows = conn.execute(
        """
        SELECT occurred_at::date AS day,
               count(*) FILTER (WHERE has_recording AND recording_url IS NOT NULL) AS recorded,
               count(*) FILTER (WHERE has_recording AND recording_url IS NOT NULL
                    AND call_id IN (SELECT call_id FROM core.call_transcript)) AS transcribed
        FROM core.call
        WHERE occurred_at::date IN (SELECT unnest(?::date[]))
        GROUP BY 1
        """,
        [days],
    ).fetchall()
    by_day = {str(r[0]): (int(r[1]), int(r[2])) for r in rows}
    issues = []
    for day in days:
        recorded, transcribed = by_day.get(day, (0, 0))
        if recorded <= 0:
            continue  # nothing recorded that day (weekend / not yet synced) — can't be behind
        gap = recorded - transcribed
        tol = max(TOL_ABS, int(TOL_FRAC * recorded))
        if gap > tol:
            issues.append({"day": day, "recorded": recorded, "transcribed": transcribed,
                           "gap": gap, "tol": tol})
    return issues


def heal() -> str:
    """Run the (idempotent, lock-robust) transcribe job to catch up, then let the re-check verify."""
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "transcribe_calls.py")]
    try:
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=10800)
    except subprocess.TimeoutExpired:
        return "heal timed out (>3h)"
    return f"transcribe rc={r.returncode}"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Warm-call transcription coverage watchdog")
    p.add_argument("--days", type=int, default=DAYS_BACK)
    p.add_argument("--heal", action="store_true", help="run transcribe_calls.py to self-heal, then re-check")
    p.add_argument("--check-only", action="store_true")
    args = p.parse_args(argv)

    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=i)).isoformat() for i in range(1, args.days + 1)]  # exclude today

    conn, source = _connect_ro()
    if conn is None:
        print("watchdog: warehouse unreachable (live+snapshot) — skipping this run", flush=True)
        return 0
    issues = evaluate(conn, days)
    conn.close()

    healed_note = ""
    if issues and args.heal and not args.check_only:
        print(f"watchdog: healing (issues on {sorted(i['day'] for i in issues)})", flush=True)
        msg = heal()
        healed_note = f"\n:arrows_counterclockwise: heal: {msg}"
        conn, source = _connect_ro()
        if conn is not None:
            issues = evaluate(conn, days)
            conn.close()

    # ---- anti-flap state: a day must read BAD on >=FAIL_THRESHOLD consecutive runs to page ----
    try:
        state = json.loads(STATE_PATH.read_text())
    except Exception:  # noqa: BLE001
        state = {}
    prev_fails = state.get("fails", {})  # day -> consecutive-bad count
    prev_paged_sig = state.get("paged_sig", "")

    bad_days = {i["day"]: i for i in issues}
    new_fails = {d: prev_fails.get(d, 0) + 1 for d in bad_days}  # reset (drop) any day no longer bad

    paging = [bad_days[d] for d, c in new_fails.items() if c >= FAIL_THRESHOLD]
    paging.sort(key=lambda x: x["day"])
    paged_sig = ",".join(f"{i['day']}:{i['gap']}" for i in paging)

    if paging:
        if paged_sig != prev_paged_sig:
            lines = [f":rotating_light: *Warm-call transcription gap* ({len(paging)} day(s), src={source}) "
                     f"{SAM} — recorded calls without a transcript:"]
            for i in paging:
                lines.append(f"  • {i['day']}: {i['transcribed']}/{i['recorded']} transcribed "
                             f"(gap {i['gap']}, tol {i['tol']})")
            lines.append("Self-heal: `scripts/transcribe_coverage_watchdog.sh --heal` "
                         "(or `scripts/transcribe_calls.py`).")
            slack_post("\n".join(lines) + healed_note)
        print(f"watchdog: PAGING {paged_sig}", flush=True)
    else:
        if prev_paged_sig:
            slack_post(f":white_check_mark: Warm-call transcription gap cleared — last {args.days} days "
                       f"within tolerance (src={source})." + healed_note)
        print(f"watchdog: OK (last {args.days} days within tolerance, src={source})", flush=True)

    try:
        STATE_PATH.write_text(json.dumps({
            "fails": new_fails, "paged_sig": paged_sig, "checked_at": today.isoformat(),
        }))
    except Exception as exc:  # noqa: BLE001
        print(f"watchdog: could not write state: {exc}", flush=True)

    return 1 if paging else 0


if __name__ == "__main__":
    sys.exit(main())
