#!/usr/bin/env python3
"""MOF daily digest — a SHORT #cc-sam morning message off the MOF conversion-tracking views.

The digest half of the MOF measurement layer (07-12 DoD; charter-sanctioned daily digest —
NOT a healthy-ping violation). Posts ONE message per day: yesterday's (D-1-final, same framing
as daily report v2) opps + meetings by channel, opp->booked conversion off the last complete
7d cohort, dials/connects, form fills, WoW deltas, and a zero-guard for any channel at zero
that is not PAUSED.

Reads ONLY the v1096 views (they are the semantic contract — definitions live there):
    dash.v_mof_funnel_daily     day x channel x workspace funnel facts
    dash.v_mof_opp_outcomes     opp-cohort conversion windows

Pattern mirrors scripts/daily_report_v2.sh + render_daily_v2.py (same box conventions):
  * D-1 ET, rendered ONCE — a per-date marker file makes later cron ticks no-ops.
  * FRESHNESS-GATED on the outcome (exit 3 = not ready, a later tick retries): D-1 email
    sends present AND D-1 canonical meetings present in the served snapshot.
  * Late gate (>= 13:00 UTC still not ready) or a hard failure -> alert via the
    SLACK_TOKEN/SLACK_ALERT_CHANNEL path (alert_slack.py semantics), never silent.
  * Slack creds: SLACK_TOKEN + SLACK_ALERT_CHANNEL (#cc-sam) from env, then repo .env.

Cron (UTC, droplet; install AFTER commit+push — the hourly divergence guard reverts
uncommitted deploys on this box):
    */20 11-15 * * *  flock -n /tmp/mof_digest.lock /root/renaissance-warehouse/scripts/mof_daily_digest.py >> /root/renaissance-warehouse/logs/mof_daily_digest.log 2>&1

Usage:
    mof_daily_digest.py                 production: yesterday-ET, gated, marker, post
    mof_daily_digest.py 2026-07-09      explicit day (ignores marker, still posts)
    mof_daily_digest.py --dry           compute + print, do not post
    mof_daily_digest.py --test          post for the latest complete day, labeled TEST
"""
from __future__ import annotations

import datetime
import json
import os
import sys
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"
LOG_DIR = REPO_ROOT / "logs"

_flags = {a for a in sys.argv[1:] if a.startswith("--")}
_args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in _flags
TEST = "--test" in _flags

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    _today_et = datetime.datetime.now(ET).date()
except Exception:
    _today_et = datetime.date.today()

REPORT_DATE = _args[0] if _args else (_today_et - datetime.timedelta(days=1)).isoformat()
D = datetime.date.fromisoformat(REPORT_DATE)
MANUAL = bool(_args) or TEST or DRY

# ------------------------------ env / creds ------------------------------

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

FILE_ENV = load_env(ENV_PATH)

def env(key: str, default: str = "") -> str:
    return os.environ.get(key) or FILE_ENV.get(key, default) or default

# ------------------------------ warehouse read API ------------------------------
WH_BASE = env("WAREHOUSE_API_BASE", "https://renaissance-droplet.tailae5c80.ts.net")

def _wh_token() -> str:
    tok = env("WAREHOUSE_API_TOKEN")
    if tok:
        return tok
    path = os.environ.get("WAREHOUSE_TOKENS_FILE", "/opt/duckdb/allowed_tokens.txt")
    if os.path.exists(path):
        for line in open(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[1] == "cc-service-reader":
                return parts[0]
    raise RuntimeError("no warehouse token (WAREHOUSE_API_TOKEN / allowed_tokens.txt)")

_SNAPSHOT = {"id": None}

def wq(sql: str):
    req = urllib.request.Request(
        WH_BASE + "/query",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": f"Bearer {_wh_token()}", "Content-Type": "application/json"},
    )
    r = json.load(urllib.request.urlopen(req, timeout=120))
    _SNAPSHOT["id"] = r.get("snapshot_id")
    return r.get("rows", [])

# ------------------------------ slack ------------------------------

def slack_post(text: str) -> bool:
    token = env("SLACK_TOKEN")
    channel = env("SLACK_ALERT_CHANNEL")
    cookie = env("SLACK_COOKIE")
    if not token or not channel:
        print("mof_digest: no SLACK_TOKEN/SLACK_ALERT_CHANNEL — cannot post", flush=True)
        return False
    body = json.dumps({"channel": channel, "text": text, "unfurl_links": False}).encode()
    headers = {"Content-Type": "application/json; charset=utf-8", "Authorization": f"Bearer {token}"}
    if cookie:
        headers["Cookie"] = cookie
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=body, headers=headers)
    try:
        resp = json.load(urllib.request.urlopen(req, timeout=30))
        if not resp.get("ok"):
            print(f"mof_digest: slack error: {resp.get('error')}", flush=True)
        return bool(resp.get("ok"))
    except Exception as e:  # noqa: BLE001
        print(f"mof_digest: slack post failed: {e}", flush=True)
        return False

def alert(text: str) -> None:
    slack_post(text)

# ------------------------------ freshness gate ------------------------------

def gate_ready(day: str) -> tuple[bool, str]:
    """Outcome-level freshness: D-1 email sends AND D-1 canonical meetings present."""
    try:
        r = wq(f"""SELECT
                     (SELECT COALESCE(SUM(sent),0) FROM dash.v_mof_funnel_daily
                       WHERE metric_date = DATE '{day}' AND channel='email') AS email_sent,
                     (SELECT COALESCE(SUM(meetings),0) FROM dash.v_mof_funnel_daily
                       WHERE metric_date = DATE '{day}') AS meetings""")
        email_sent, meetings = int(r[0][0] or 0), int(r[0][1] or 0)
        ok = email_sent > 0 and meetings > 0
        return ok, f"email_sent={email_sent:,} meetings={meetings}"
    except Exception as e:  # noqa: BLE001
        return False, f"gate query failed: {e}"

# ------------------------------ digest ------------------------------

def fmt_n(v) -> str:
    return f"{int(v):,}" if v is not None else "—"

def pct_delta(cur, prev) -> str:
    if prev in (None, 0) or cur is None:
        return "n/a"
    d = 100.0 * (cur - prev) / prev
    return f"{d:+.0f}%"

def channel_rollup(day: str) -> dict:
    rows = wq(f"""SELECT channel, any_value(channel_status),
                         SUM(opps), SUM(meetings), SUM(dialed), SUM(connected), SUM(form_fills)
                  FROM dash.v_mof_funnel_daily WHERE metric_date = DATE '{day}'
                  GROUP BY channel""")
    out = {}
    for ch, status, opps, mtg, dial, conn, forms in rows:
        out[ch] = {"status": status,
                   "opps": None if opps is None else int(opps),
                   "meetings": None if mtg is None else int(mtg),
                   "dialed": None if dial is None else int(dial),
                   "connected": None if conn is None else int(conn),
                   "forms": None if forms is None else int(forms)}
    return out

def form_split(day: str) -> dict:
    rows = wq(f"""SELECT workspace_name, COALESCE(SUM(form_fills),0)
                  FROM dash.v_mof_funnel_daily
                  WHERE metric_date = DATE '{day}' AND channel='form' GROUP BY 1""")
    return {r[0]: int(r[1]) for r in rows}

def cohort_line(day: str) -> str:
    """Latest cohort with a COMPLETE 7d window on the report day = day-7."""
    cohort = (datetime.date.fromisoformat(day) - datetime.timedelta(days=7)).isoformat()
    rows = wq(f"""SELECT channel, opps_captured, pct_booked_7d, pct_booked_24h,
                         median_hours_to_book, pct_called
                  FROM dash.v_mof_opp_outcomes
                  WHERE cohort_date = DATE '{cohort}' ORDER BY channel""")
    if not rows:
        return f"opp→booked: no cohort rows for {cohort}"
    parts = []
    for ch, n, p7, p24, med, pcall in rows:
        med_s = f", med {med}h" if med is not None else ""
        p7_s = f"{p7}%" if p7 is not None else "—"
        parts.append(f"{ch} {p7_s} ≤7d (n={int(n)}{med_s})")
    label = datetime.date.fromisoformat(cohort).strftime("%b %-d")
    return f"*Opp→booked ({label} cohort, full 7d window):* " + " · ".join(parts)

def build_digest(day: str) -> str:
    d = datetime.date.fromisoformat(day)
    prev = (d - datetime.timedelta(days=7)).isoformat()
    cur = channel_rollup(day)
    wow = channel_rollup(prev)
    forms = form_split(day)

    def g(ch, k):
        return cur.get(ch, {}).get(k)

    opps_bits = []
    for ch in ("email", "sms", "whatsapp"):
        v = g(ch, "opps")
        paused = (cur.get(ch, {}).get("status") == "PAUSED")
        opps_bits.append(f"{ch} {fmt_n(v)}" + (" _(PAUSED)_" if paused else ""))

    mtg_total = sum(v["meetings"] or 0 for v in cur.values())
    mtg_bits = [f"{ch} {fmt_n(cur[ch]['meetings'] or 0)}"
                for ch in ("email", "sms", "whatsapp", "call", "other") if ch in cur]

    # WoW on the two headline numbers (same weekday last week)
    email_opps_now, email_opps_prev = g("email", "opps"), wow.get("email", {}).get("opps")
    mtg_prev_total = sum(v["meetings"] or 0 for v in wow.values()) if wow else None
    prev_label = datetime.date.fromisoformat(prev).strftime("%a %b %-d")

    # zero-guard: a channel at zero that is NOT paused (email/whatsapp opps; meetings total)
    warn = []
    for ch in ("email", "whatsapp"):
        if (g(ch, "opps") or 0) == 0 and cur.get(ch, {}).get("status") != "PAUSED":
            warn.append(f"{ch} opps = 0")
    if mtg_total == 0:
        warn.append("meetings = 0")
    if (g("call", "dialed") or 0) == 0 and d.weekday() < 5:  # weekday dials expected
        warn.append("dials = 0 (weekday)")

    lines = [
        f":bar_chart: *MOF daily digest — {d.strftime('%a %b %-d')}* (D-1 final)"
        + ("  `[TEST — wiring check, ignore]`" if TEST else ""),
        "*Opps:* " + " · ".join(opps_bits),
        f"*Meetings:* {fmt_n(mtg_total)} — " + " · ".join(mtg_bits),
        f"*Calls:* {fmt_n(g('call', 'dialed'))} dials · {fmt_n(g('call', 'connected'))} connects(≥60s)"
        f"   *Forms:* {fmt_n(forms.get('GBC application', 0))} GBC · {fmt_n(forms.get('Apply-now', 0))} apply-now",
        cohort_line(day),
        f"*WoW (vs {prev_label}):* email opps {fmt_n(email_opps_now)} ({pct_delta(email_opps_now, email_opps_prev)})"
        f" · meetings {fmt_n(mtg_total)} ({pct_delta(mtg_total, mtg_prev_total)})",
    ]
    if warn:
        lines.append(":warning: zero-check: " + " · ".join(warn))
    lines.append(f"_views: dash.v_mof_funnel_daily / v_mof_opp_outcomes · snapshot {_SNAPSHOT['id']}_")
    return "\n".join(lines)

# ------------------------------ main ------------------------------

def main() -> int:
    LOG_DIR.mkdir(exist_ok=True)
    marker = LOG_DIR / f"mof_digest_done_{REPORT_DATE}"
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    if not MANUAL and marker.exists():
        print(f"[{now_utc:%FT%TZ}] already posted for {REPORT_DATE} (marker) — skip")
        return 0

    ok, detail = gate_ready(REPORT_DATE)
    if not ok:
        if not MANUAL and now_utc.hour >= 13:
            alert(f":warning: *MOF digest* — freshness gate STILL not ready for {REPORT_DATE} "
                  f"at {now_utc:%H:%M}Z ({detail}). Nightly late/failed? See logs/mof_daily_digest.log.")
            print(f"[{now_utc:%FT%TZ}] gate not ready late ({detail}) — alerted")
        else:
            print(f"[{now_utc:%FT%TZ}] gate not ready ({detail}) — retry next tick")
        return 3

    try:
        text = build_digest(REPORT_DATE)
    except Exception as e:  # noqa: BLE001
        alert(f":rotating_light: *MOF digest* — build FAILED for {REPORT_DATE}: {e}")
        raise

    print(text)
    if DRY:
        print(f"[{now_utc:%FT%TZ}] --dry: not posting")
        return 0
    if not slack_post(text):
        alert(f":rotating_light: *MOF digest* — Slack post FAILED for {REPORT_DATE} (see log).")
        return 1
    if not MANUAL:
        marker.touch()
        for old in sorted(LOG_DIR.glob("mof_digest_done_*"))[:-10]:
            old.unlink(missing_ok=True)
    print(f"[{now_utc:%FT%TZ}] posted digest for {REPORT_DATE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
