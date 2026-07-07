#!/usr/bin/env python3
"""workspace_pull_watchdog — alert when an ACTIVE workspace's Instantly inbox pull is stuck.

WHY: the hourly /accounts poller carries a workspace's LAST-GOOD count forward when Instantly
fails to serve it (so the fleet never falsely shrinks). That's correct, but it means a workspace
whose pull keeps failing sits SILENTLY stale (Funding 1 was frozen ~4 days in July before anyone
noticed). This watchdog is the missing smoke detector: it reads the poller's own per-workspace
success flag + the census freshness, and if an ACTIVE workspace's pull is failing / its count is
frozen, it posts ONE Slack alert so a human looks — instead of weeks of silent staleness.

The "poke" is the poller itself: it re-attempts every workspace hourly (read-only re-fetch, changes
nothing). This watchdog fires ONLY when those retries have clearly not recovered an ACTIVE workspace.

Read-only. Never mutates. Never raises (a watchdog must not abort the nightly). Dedupes so it alerts
on the transition into stuck, not every run; auto-clears when a workspace recovers.

Run: from the nightly + a midday cron. Env: DB path via config; SLACK_TOKEN/SLACK_ALERT_CHANNEL
(same as alert_slack.py). STALE_DAYS overridable (default 3).
"""
from __future__ import annotations
import json, os, subprocess, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY = Path(os.environ.get("POLL_SUMMARY_PATH", "/root/core/live_accounts/latest_summary.json"))
STATE = Path(os.environ.get("WATCHDOG_STATE_PATH", "/root/core/live_accounts/pull_watchdog_state.json"))
DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
STALE_DAYS = int(os.environ.get("PULL_STALE_DAYS", "3"))

# slug -> friendly operator name (never show David a raw slug)
WSMAP = {
    "renaissance-4": "Funding 1 (Samuel)", "renaissance-5": "Funding 2 (Ido)",
    "prospects-power": "Funding 3 (Leo)", "koi-and-destroy": "Funding 4 (Sam)",
    "renaissance-2": "Funding 5 (Eyver)", "the-gatekeepers": "Funding 6 (Max)",
    "renaissance-1": "Instantly DFY", "warm-leads": "Warm leads",
}
def friendly(slug): return WSMAP.get(slug, slug)

def duck(sql: str) -> list[list]:
    """Read-only DuckDB query -> rows. Never raises."""
    try:
        out = subprocess.run(
            ["duckdb", "-readonly", "-json", DB, sql],
            capture_output=True, text=True, timeout=60)
        return json.loads(out.stdout) if out.stdout.strip() else []
    except Exception as e:  # noqa: BLE001
        print(f"watchdog: duck query failed: {e}", flush=True)
        return []

def alert(text: str) -> None:
    try:
        subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "alert_slack.py"), text], timeout=30)
    except Exception as e:  # noqa: BLE001
        print(f"watchdog: alert failed: {e}", flush=True)

def load_json(p: Path, default):
    try: return json.loads(p.read_text())
    except Exception: return default

def main() -> int:
    # 1) poller's per-workspace result (ok / n) from the last hourly poll
    summ = load_json(SUMMARY, {})
    poll = {w.get("workspace_slug"): w for w in summ.get("workspaces", []) if w.get("workspace_slug")}

    # 2) ACTIVE workspaces only + their last-known inbox count (for the alert message).
    #    TRIGGER = the poller's own ok=False (its /accounts call for this workspace FAILED) — the
    #    same signal the census carry-forward keys on. NOT "count didn't change": a small stable
    #    workspace legitimately has an unchanged count (tariffs/warm-leads/the-gatekeepers all sit
    #    flat yet poll fine), so a frozen count is a false positive. ok=False is the honest signal.
    rows = duck("""
      WITH active AS (SELECT workspace_id, slug FROM core.workspace WHERE is_active)
      SELECT a.slug,
             (SELECT count(*) FROM core.account_census c
                WHERE c.workspace_uuid=a.workspace_id
                  AND c.census_date=(SELECT max(census_date) FROM core.account_census)) AS latest_n
      FROM active a
    """)

    stuck, healthy = [], []
    for r in rows:
        slug = r["slug"]
        p = poll.get(slug)
        # stuck if: the workspace was polled and FAILED (ok=False), OR it was expected but absent
        # from the poll entirely (never attempted / dropped). A workspace with no inboxes that also
        # isn't in the poll is ignored (nothing to pull).
        polled_and_failed = p is not None and (p.get("ok") is False)
        absent_but_has_inboxes = p is None and (r.get("latest_n", 0) or 0) > 0
        if polled_and_failed or absent_but_has_inboxes:
            reason = ("last pull FAILED (Instantly /accounts errored)" if polled_and_failed
                      else "not returned by the poll at all (pull skipped/dropped)")
            stuck.append((slug, reason, r.get("latest_n")))
        else:
            healthy.append(slug)

    # 3) dedupe: alert on the transition into stuck; clear recovered ones
    state = load_json(STATE, {})            # slug -> last-alerted date string
    already = set(state.keys())
    now_stuck = {s for s, _, _ in stuck}

    for slug, reason, n in stuck:
        if slug in already:
            continue                        # already alerted; don't spam
        affected = f" (~{n:,} inboxes)" if n else ""
        alert(f":warning: *Inbox pull stuck* — *{friendly(slug)}*: {reason}. "
              f"Its inbox count is stale until Instantly recovers — someone should check.{affected}")
        state[slug] = str(n)
    recovered = already - now_stuck
    for slug in recovered:
        alert(f":white_check_mark: *Inbox pull recovered* — *{friendly(slug)}* is refreshing again.")
        state.pop(slug, None)

    try: STATE.write_text(json.dumps(state))
    except Exception: pass

    print(f"watchdog: active={len(rows)} stuck={len(stuck)} recovered={len(recovered)} healthy={len(healthy)}", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
