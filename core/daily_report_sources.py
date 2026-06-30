"""Daily RevOps Report — canonical source-per-metric REGISTRY loader.

`config/daily_report_sources.json` is the single machine-readable map of
metric -> {source type, exact id, tab, desk/owner, dedup key, reconciliation anchor}.
scripts/render_daily.py resolves its human-managed / drift-prone sources (Pre-IPO desks,
booking sheet, sendivo sub-accounts, workspace roster) FROM here so registry = code = reality.

This module:
  - loads + schema-validates the registry (load_registry / validate_registry),
  - exposes typed accessors (workspace_roster / preipo_desks / bookings_sheet / sendivo_subs),
  - pulls the Pre-IPO reconciliation anchor live from #pre-ipo-success (fetch_preipo_slack_tally),
    so a chat / the renderer can confirm "my Pre-IPO count reconciles to the team's own tally"
    with ZERO human hand-feeding (the bug this whole registry exists to kill).

Self-contained (stdlib only); safe to import off-box (network calls degrade, data accessors work).
See handoffs/2026-06-30-DATA-TICKET-preipo-source-mapping-incompleteness.md.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _ET = None

REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "config" / "daily_report_sources.json"

_cache: dict | None = None


def load_registry(path: Path | str | None = None) -> dict:
    """Load + cache the registry JSON. Raises if the file is missing/unparseable
    (a missing source map must fail loud, never silently render the wrong thing)."""
    global _cache
    if _cache is not None and path is None:
        return _cache
    p = Path(path) if path else REGISTRY_PATH
    if not p.exists():
        raise FileNotFoundError(f"daily-report source registry not found at {p}")
    with open(p) as fh:
        data = json.load(fh)
    if path is None:
        _cache = data
    return data


# ----------------------------------------------------------------------------- validation
def validate_registry(reg: dict | None = None) -> list[str]:
    """Return a list of structural problems (empty == valid). Lets a self-test / --verify
    confirm the registry is well-formed before the renderer trusts it."""
    reg = reg or load_registry()
    errs: list[str] = []
    if not reg.get("workspaces", {}).get("roster"):
        errs.append("workspaces.roster is empty")
    for w in reg.get("workspaces", {}).get("roster", []):
        for k in ("slug", "display", "instantly_key_slug"):
            if not w.get(k):
                errs.append(f"workspace roster entry missing {k}: {w}")
    metrics = reg.get("metrics", {})
    if not metrics:
        errs.append("metrics is empty")
    for mid, m in metrics.items():
        if not m.get("section"):
            errs.append(f"metric {mid} missing section")
        if m.get("source_type") not in ("sheet", "warehouse", "api"):
            errs.append(f"metric {mid} bad source_type {m.get('source_type')!r}")
    pre = metrics.get("preipo_meetings", {})
    desks = pre.get("desks", [])
    if not desks:
        errs.append("preipo_meetings.desks is empty (the metric this registry exists for)")
    for d in desks:
        for k in ("desk", "spreadsheet_id", "tab", "date_column", "dedup_key"):
            if not d.get(k):
                errs.append(f"preipo desk {d.get('desk', '?')} missing {k}")
    recon = pre.get("reconciliation") or {}
    rx = recon.get("line_regex")
    if rx:
        try:
            re.compile(rx)
        except re.error as e:
            errs.append(f"preipo reconciliation.line_regex does not compile: {e}")
    # Guard the accessors the renderer relies on: a registry can be schema-shaped yet drop a key the
    # renderer reads, which would pass a naive validate then crash/silently-zero at render. Assert the
    # exact lookups render_daily makes resolve (sendivo Funding/Pre-IPO subs, the booking sheet).
    sms = metrics.get("sms_sent", {})
    subs = (sms.get("api") or {}).get("sub_accounts")
    if not subs:
        errs.append("sms_sent.api.sub_accounts missing/empty — §2 SMS would have no sub-accounts")
    else:
        chans = " ".join(str(s.get("channel", "")).lower() for s in subs)
        for needed in ("funding", "pre-ipo"):
            if needed not in chans:
                errs.append(f"sms_sent.api.sub_accounts has no '{needed}' channel -> sendivo_sub_id('{needed}') "
                            f"returns None -> §2 SMS for that channel silently renders 0")
        for s in subs:
            if not s.get("id"):
                errs.append(f"sms_sent sub_account missing id: {s}")
    bsheet = (metrics.get("email_meetings_leadtype", {}) or {}).get("sheet") or {}
    if not (bsheet.get("spreadsheet_id") and bsheet.get("tab")):
        errs.append("email_meetings_leadtype.sheet missing spreadsheet_id/tab — the consolidated booking sheet")
    return errs


# ----------------------------------------------------------------------------- accessors
def workspace_roster(reg: dict | None = None) -> list[tuple[str, str]]:
    """[(slug, display)] in render order."""
    reg = reg or load_registry()
    return [(w["slug"], w["display"]) for w in reg["workspaces"]["roster"]]


def instantly_key_slugs(reg: dict | None = None) -> dict[str, str]:
    """{warehouse_slug: instantly_key_slug} (identical today, but centralised so a future
    divergence is a one-line registry edit, not a code change)."""
    reg = reg or load_registry()
    return {w["slug"]: w.get("instantly_key_slug", w["slug"]) for w in reg["workspaces"]["roster"]}


def preipo_desks(reg: dict | None = None) -> list[dict]:
    """The active Pre-IPO booking desks: [{desk, spreadsheet_id, tab, date_column, dedup_key, ...}]."""
    reg = reg or load_registry()
    return list(reg["metrics"]["preipo_meetings"]["desks"])


def preipo_reconciliation(reg: dict | None = None) -> dict:
    reg = reg or load_registry()
    return dict(reg["metrics"]["preipo_meetings"].get("reconciliation") or {})


def preipo_known_good(reg: dict | None = None) -> dict:
    reg = reg or load_registry()
    return dict(reg["metrics"]["preipo_meetings"].get("known_good") or {})


def bookings_sheet(reg: dict | None = None) -> tuple[str, str]:
    """(spreadsheet_id, tab) for the consolidated portal-fed booking sheet (§1/§2/§5)."""
    reg = reg or load_registry()
    s = reg["metrics"]["email_meetings_leadtype"]["sheet"]
    return s["spreadsheet_id"], s["tab"]


def sendivo_subs(reg: dict | None = None) -> dict[int, str]:
    """{sub_account_id: label} for §2 SMS."""
    reg = reg or load_registry()
    return {int(s["id"]): s["label"] for s in reg["metrics"]["sms_sent"]["api"]["sub_accounts"]}


def sendivo_sub_id(channel_substr: str, reg: dict | None = None) -> int | None:
    """The sendivo sub_account_id whose `channel` contains `channel_substr` (case-insensitive),
    e.g. 'Funding' -> 12720, 'Pre-IPO' -> 13922, 'webform' -> 14603. None if not found."""
    reg = reg or load_registry()
    cs = channel_substr.lower()
    for s in reg["metrics"]["sms_sent"]["api"]["sub_accounts"]:
        if cs in str(s.get("channel", "")).lower():
            return int(s["id"])
    return None


# ----------------------------------------------------------------------------- Slack reconciliation anchor
def slack_creds() -> tuple[str, str, str]:
    """(token, cookie, channel_id) — same credential path as scripts/alert_slack.py
    (SLACK_TOKEN [+ optional SLACK_COOKIE] from env, then repo .env). channel_id from the registry."""
    env: dict[str, str] = {}
    envp = REPO_ROOT / ".env"
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    token = os.environ.get("SLACK_TOKEN") or env.get("SLACK_TOKEN", "")
    cookie = os.environ.get("SLACK_COOKIE") or env.get("SLACK_COOKIE", "")
    channel = preipo_reconciliation().get("channel_id", "")
    return token, cookie, channel


def _et_day_bounds_epoch(report_date_iso: str) -> tuple[float, float]:
    """[start, end) epoch seconds spanning the report date in America/New_York.
    A booking posted at 03:45Z 'next day' is still the prior ET day (counter resets by ET day),
    so the window is [report 00:00 ET, report+1 00:00 ET)."""
    d = datetime.date.fromisoformat(report_date_iso)
    if _ET is not None:
        start = datetime.datetime(d.year, d.month, d.day, tzinfo=_ET)
    else:  # crude UTC-4 fallback if zoneinfo is unavailable
        start = datetime.datetime(d.year, d.month, d.day, tzinfo=datetime.timezone(datetime.timedelta(hours=-4)))
    end = start + datetime.timedelta(days=1)
    return start.timestamp(), end.timestamp()


def _slack_get(method: str, params: dict, token: str, cookie: str) -> dict:
    url = "https://slack.com/api/" + method + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", **({"Cookie": f"d={cookie}"} if cookie else {})},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _msg_in_et_day(ts: str | float, report_date_iso: str) -> bool:
    """Is a Slack message ts (epoch seconds) on `report_date_iso` in America/New_York?
    The counter resets per ET day, so a 03:45Z 'next-day' post is still the prior ET day."""
    try:
        epoch = float(ts)
    except (TypeError, ValueError):
        return False
    tz = _ET or datetime.timezone(datetime.timedelta(hours=-4))
    return datetime.datetime.fromtimestamp(epoch, tz).date().isoformat() == report_date_iso


def tally_preipo_messages(messages: list[dict], report_date_iso: str,
                          known_desks: dict[str, str], pattern: "re.Pattern") -> dict:
    """PURE: given Slack messages [{ts, text, user}], the report ET day, the known {lower:Desk} map and
    the compiled line regex, return {"by_desk": {desk:N}, "unknown_desks": {label:N}, "messages": k}.

    Rule: the counter RESETS to 1 per ET day and EACH BOOKER runs their own independent 1..N. So take
    the MAX N per (desk, booker) within the ET day, then SUM across bookers — i.e. two people each
    booking up to 11 on the same desk total 22, not max(11,11)=11, while one booker's run split across
    several messages still collapses to their single max (no double-count). Bucket by message POST date
    in ET (the 'June DD' inside a line is the script name, not the booking date). No I/O — unit-testable."""
    desk_user_max: dict[tuple, int] = {}     # (Desk, booker) -> max N
    unknown_user_max: dict[tuple, int] = {}  # (label, booker) -> max N
    n = 0
    for msg in messages:
        if not _msg_in_et_day(msg.get("ts", ""), report_date_iso):
            continue
        text = msg.get("text", "") or ""
        if not text:
            continue
        booker = msg.get("user") or msg.get("username") or msg.get("bot_id") or "?"
        n += 1
        for m in pattern.finditer(text):
            label, num = m.group(1), int(m.group(2))
            key = label.lower()
            if key in known_desks:
                k = (known_desks[key], booker)
                desk_user_max[k] = max(desk_user_max.get(k, 0), num)
            else:
                k = (label, booker)
                unknown_user_max[k] = max(unknown_user_max.get(k, 0), num)
    by_desk: dict[str, int] = {}
    for (desk, _booker), v in desk_user_max.items():
        by_desk[desk] = by_desk.get(desk, 0) + v
    for desk in known_desks.values():
        by_desk.setdefault(desk, 0)
    unknown: dict[str, int] = {}
    for (label, _booker), v in unknown_user_max.items():
        unknown[label] = unknown.get(label, 0) + v
    return {"by_desk": by_desk, "unknown_desks": unknown, "messages": n}


def fetch_preipo_slack_tally(report_date_iso: str, reg: dict | None = None,
                             token: str | None = None, cookie: str | None = None) -> dict | None:
    """Live reconciliation anchor: read #pre-ipo-success around the report ET day and tally the per-desk
    'Collins/Summit Booked N' counter (delegates the parse/bucket math to tally_preipo_messages).
    Returns {"by_desk", "unknown_desks", "messages"} or None if creds/scopes are unavailable (caller
    then degrades to known_good / ANCHOR_UNAVAILABLE)."""
    reg = reg or load_registry()
    recon = preipo_reconciliation(reg)
    rx = recon.get("line_regex")
    if not rx:
        return None
    pat = re.compile(rx)
    known = {d["desk"].lower(): d["desk"] for d in preipo_desks(reg)}

    if token is None or cookie is None:
        t, c, _ = slack_creds()
        token = token if token is not None else t
        cookie = cookie if cookie is not None else c
    channel = recon.get("channel_id", "")
    if not token or not channel:
        return None

    # Pull a window covering the ET day (oldest=ET 00:00, latest=ET next-00:00 captures post-midnight-UTC
    # spillover that is still the same ET day); tally_preipo_messages re-filters by exact ET post date.
    oldest, latest = _et_day_bounds_epoch(report_date_iso)
    msgs: list[dict] = []
    cursor = ""
    try:
        for _ in range(8):  # paginate up to 8 pages (200/page) — a single ET day is far smaller
            params = {"channel": channel, "oldest": f"{oldest:.6f}", "latest": f"{latest:.6f}",
                      "inclusive": "true", "limit": "200"}
            if cursor:
                params["cursor"] = cursor
            data = _slack_get("conversations.history", params, token, cookie)
            if not data.get("ok"):
                raise RuntimeError(f"slack conversations.history error: {data.get('error')}")
            msgs.extend(data.get("messages", []))
            if not data.get("has_more"):
                break
            cursor = (data.get("response_metadata") or {}).get("next_cursor", "")
            if not cursor:
                break
    except Exception:
        return None
    return tally_preipo_messages(msgs, report_date_iso, known, pat)
