#!/usr/bin/env python3
"""Daily RevOps Report — DAILY tab, SINGLE-SOURCE-OF-TRUTH per metric.

Every metric resolves to ONE canonical source used by EVERY section + the yellow block, so a
workspace's number can never disagree section-to-section (the class of bug that produced
"§1 Samuel 513k vs §4 150k"). See handoffs/2026-06-30-BUILD-daily-report-full-automation-and-im-reply-time.md
and deliverables/2026-06-29-daily-report-10pm-sync/RENDERER-SPEC.md.

Canonical sources (verified 2026-06-30; booking cutover 2026-06-30):
 §1 Email sent + opps  -> Instantly API /campaigns/analytics/daily per workspace (memoized; the SAME
                          object feeds §4 Actual, so §1 sent and §4 Actual are literally one fetch).
 ALL meetings (§1/§2/§3/§5) -> the portal `raw_im_bookings` (offer='Funding', latest snapshot, deduped
                          email|phone) — THE post-cutover booking source. The Funding-Form 'Data' sheet
                          was frozen/retired after 2026-06-29 (0 rows for 06-30), which silently zeroed
                          every meeting column; im_bookings is the live portal writer (Darcy's form).
                          channel -> §1 Email / §2 SMS+WhatsApp / §3 Call; workspace -> §1 by-workspace
                          (ws_alias); partner -> §5; lead_type -> §1 cheap/regular split.
 §2 SMS sent           -> sendivo billing_report.sms_fees.quantity (12720=Ren1, 13922=Ren2, 14603=Ren3 webform).
 §2 SMS replies (human)-> raw_sendivo_inbound non-opt-out.
 §2 WhatsApp           -> v_sms_dash_wa_daily (sent/delivered/failed/replies); meetings -> im_bookings.
 §3 Close             -> core.call (dials/leads/connects @>=60s, ET day) — Close API SoR, under-captures.
 §4 Sending truth      -> Expected = active accounts' configured daily_limit by infra (core.account_label,
                          latest census, no lag); Actual = Instantly daily per workspace (== §1 sent).
 §5 Bookings/partner   -> im_bookings partner.
 §6 IM reply-time      -> core.email_message (native, fresh): first prospect reply (ue_type 2) -> first
                          our reply (ue_type 3) per thread; median/avg per workspace daily/weekly/monthly.
 (§1b EMAIL-KPIs-BY-INFRA was removed 2026-06-30 per Sam — layout now matches the June-26 gold tab.)

Usage:  render_daily.py 2026-06-29 ["Jun 29"]      (tab name defaults to "%b %-d" = "Jun 29")
        render_daily.py 2026-06-29 --dry            (print the data, do not write the sheet)
"""
import json, os, sys, datetime, urllib.request, urllib.parse, collections

# ---------- repo imports (box: run via .venv/bin/python from REPO_DIR) ----------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from core.credentials import load_credentials
    _CREDS = load_credentials()
except Exception as _e:  # dry/off-box fallback: external fetchers degrade, warehouse still works
    _CREDS = None
    print(f"WARN: credentials unavailable ({_e}); Instantly/sendivo sections will be empty", file=sys.stderr)

# Canonical source-per-metric REGISTRY (config/daily_report_sources.json). The renderer resolves its
# human-managed / drift-prone sources (Pre-IPO desks, booking sheet, sendivo subs, workspace roster)
# FROM here so registry = code = reality. A missing/invalid registry FAILS LOUD — never silently
# render the wrong source (the bug this registry exists to kill).
from core import daily_report_sources as REG
_REG_ERRS = REG.validate_registry()
if _REG_ERRS:
    print("ERROR: daily-report source registry (config/daily_report_sources.json) is INVALID:\n  - "
          + "\n  - ".join(_REG_ERRS), file=sys.stderr)
    sys.exit(3)

SID = "1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
TOK = os.environ.get("GOOGLE_TOKEN", "/root/.config/mcp-google-sheets/token.json")
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SID}"

args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in sys.argv
VERIFY = "--verify" in sys.argv  # source self-check: probe every source + reconcile Pre-IPO; no sheet write
# Default to TODAY (ET), never a hardcoded past date — a mis-templated cron must not silently render
# a stale day into a today-named tab (M6). The sync always passes REPORT_DATE explicitly.
try:
    from zoneinfo import ZoneInfo
    _today_et = datetime.datetime.now(ZoneInfo("America/New_York")).date().isoformat()
except Exception:
    _today_et = datetime.date.today().isoformat()
REPORT_DATE = args[0] if args else _today_et
_d = datetime.date.fromisoformat(REPORT_DATE)
DAILY = REPORT_DATE
DAILY_TAB = args[1] if len(args) > 1 else _d.strftime("%b %-d")  # "Jun 29"

# ---------------------------- workspace identity ----------------------------
# warehouse slug (== Instantly key slug == account_label.workspace_slug == email_message.workspace_id),
# render order — FROM the registry (config/daily_report_sources.json -> workspaces.roster).
WS = REG.workspace_roster()  # [(slug, display)]
SLUG2NAME = dict(WS)
REPORT_SLUGS = [s for s, _ in WS]
SLUGS_SQL = ",".join("'" + s + "'" for s in REPORT_SLUGS)

# free-text booking-workspace -> canonical display name (recurrence-ledger #10)
def ws_alias(raw):
    t = (raw or "").strip().lower()
    if not t:
        return None
    if t.startswith("funding 1") or t in ("f1",):                  return "Funding 1 (Samuel)"
    if t.startswith("funding 2") or t in ("f2",):                  return "Funding 2 (Ido)"
    if t.startswith("funding 3") or t in ("f3",):                  return "Funding 3 (Leo)"
    if t.startswith("funding 4") or t in ("f4",):                  return "Funding 4 (Sam)"
    if t.startswith("funding 5") or t in ("f5",):                  return "Funding 5 (Eyver)"
    if t.startswith("warm"):                                       return "Warm leads"
    if t.startswith("max") or "gatekeeper" in t:                   return "Max's workspace"
    if ("renaissance 1" in t or t in ("r1", "instantly")
            or "sendivo" in t):                                    return "Renaissance 1 (Instantly)"
    return None

# ---------------------------- warehouse read API ----------------------------
WH_BASE = "https://renaissance-droplet.tailae5c80.ts.net"
def _wh_token():
    path = os.environ.get("WAREHOUSE_TOKENS_FILE", "/opt/duckdb/allowed_tokens.txt")
    if os.path.exists(path):
        for line in open(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[1] == "cc-service-reader":
                return parts[0].strip()
    raise RuntimeError("cc-service-reader token not found in " + path)
WH_TOKEN = _wh_token()
def wq(sql):
    req = urllib.request.Request(WH_BASE + "/query", data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": f"Bearer {WH_TOKEN}", "Content-Type": "application/json"}, method="POST")
    return json.load(urllib.request.urlopen(req, timeout=180))["rows"]

# ---------------------------- external fetchers (memoized) ----------------------------
import httpx, time
def _retry(fn, tries=3, label=""):
    """Retry a transient network call (timeout/5xx) a few times before giving up (nightly robustness)."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(2 * (i + 1))
    raise last
_INST_BASE = "https://api.instantly.ai/api/v2"
_inst_cache = {}
def instantly_daily(date):
    """{slug: (sent, opportunities)} for `date`, from the per-workspace Instantly key.
    This is the ONE Instantly fetch; §1 sent AND §4 Actual both read it -> never disagree."""
    if date in _inst_cache:
        return _inst_cache[date]
    out = {}
    keys = _CREDS.instantly_workspace_keys() if _CREDS else {}
    for slug in REPORT_SLUGS:
        k = keys.get(slug)
        if not k:
            continue
        try:
            def _call(k=k):
                rr = httpx.get(_INST_BASE + "/campaigns/analytics/daily",
                               params={"start_date": date, "end_date": date},
                               headers={"Authorization": f"Bearer {k}", "User-Agent": "curl/8.4.0"},
                               timeout=60.0)
                rr.raise_for_status()
                return rr
            r = _retry(_call, label=slug)
            rows = r.json() or []
            day = next((x for x in rows if str(x.get("date", ""))[:10] == date), None)
            if day:
                out[slug] = (int(day.get("sent") or 0), int(day.get("opportunities") or 0))
        except Exception as e:
            print(f"WARN instantly_daily {slug} {date}: {e}", file=sys.stderr)
    # Loud: a REPORTING workspace missing from the result reads 0 in §1 AND §4 (a wrong-but-plausible
    # zero, not a genuine no-send) — surface it so a dead/rotated key is caught, never silent (M3).
    missing = [s for s in REPORT_SLUGS if s not in out]
    if missing:
        print(f"WARN instantly_daily {date}: NO DATA for reporting workspace(s) "
              f"{missing} — these render 0 sent/opps (dead key? rename?). NOT a genuine zero.",
              file=sys.stderr)
    _inst_cache[date] = out
    return out

_sendivo_cache = {}
def sendivo_sms(date):
    """{sub_account_id: sms_quantity} from sendivo billing_report (the calibrated source)."""
    if date in _sendivo_cache:
        return _sendivo_cache[date]
    out = {}
    if _CREDS:
        try:
            from sources.sendivo import SendivoClient
            with SendivoClient(_CREDS.require("SENDIVO_API_KEY")) as s:
                for r in _retry(lambda: s.billing_report(date, date), label="sendivo"):
                    sf = r.get("sms_fees") or {}
                    qty = sf.get("quantity") if isinstance(sf, dict) else sf
                    out[int(r.get("sub_account_id"))] = int(qty or 0)
        except Exception as e:
            print(f"WARN sendivo_sms {date}: {e}", file=sys.stderr)
    _sendivo_cache[date] = out
    return out

# ---------------------------- google sheet reader ----------------------------
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
_g_creds = Credentials.from_authorized_user_file(TOK)
try:
    _g_creds.refresh(Request())  # populate .expiry so .valid is meaningful (M1)
except Exception as _e:
    print(f"WARN initial Google token refresh failed ({_e}); will retry lazily", file=sys.stderr)
def _gtok():
    if not _g_creds.valid:
        _g_creds.refresh(Request())
    return _g_creds.token
def gget(sid, rng):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{urllib.parse.quote(rng)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_gtok()}"})
    return json.load(urllib.request.urlopen(req, timeout=60)).get("values", [])

def _date_variants(d):
    # Pre-IPO desk sheets are human/automation-touched — tolerate every format we've seen so a rendering
    # change can't silently zero a desk (M4): "Jun 29, 2026", "June 29, 2026", "2026-06-29",
    # "6/29/2026", "06/29/2026".
    return {d.strftime("%b %-d, %Y"), d.strftime("%B %-d, %Y"), d.isoformat(),
            d.strftime("%-m/%-d/%Y"), d.strftime("%m/%d/%Y")}

# Booking source (Channel + Workspace + Partner per booking) = the portal `raw_im_bookings` [cutover
# 2026-06-30]. Feeds ALL meeting columns: §1 (email, by workspace), §2 (SMS + WhatsApp), §3 (Call), §5
# (by partner). The Funding-Form 'Data' sheet (REG.bookings_sheet()) was frozen/retired after 06-29 —
# reading it silently zeroed every meeting column for 06-30 (0 rows). im_bookings is the live portal
# writer, deduped by email|phone (one booking per person; `id` churns, so never key on it — see
# reference_portal_im_bookings_darcy_form_live_writer_20260630).
_book_cache = {}
def consolidated_bookings(date):
    """deduped (email|phone) Funding bookings for `date` from the portal mirror `main.raw_im_bookings`
    (latest snapshot, non-deleted): list of {channel, ws, partner, key, email, phone}. Fail-soft -> []
    so a warehouse hiccup zeros meetings for the day but never crashes the render."""
    if date in _book_cache:
        return _book_cache[date]
    try:
        rows = wq(f"""
          WITH b AS (
            SELECT lower(coalesce(nullif(email,''), phone)) AS k,
                   max(channel)   AS channel,
                   max(workspace) AS workspace,
                   max(partner)   AS partner,
                   max(email)     AS email,
                   max(phone)     AS phone
            FROM main.raw_im_bookings
            -- Key on `date` = the ET BUSINESS date (a clean 'YYYY-MM-DD' string the portal writes),
            -- NOT `created_at` (a UTC ingest/reseed timestamp: keying on it splits ~28 rows across
            -- UTC-midnight into the wrong day and lands every reseeded row on its load date). [Sam 06-30]
            WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
              AND offer='Funding' AND substr(coalesce(date,''),1,10)=DATE '{date}'::VARCHAR
              AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')
            GROUP BY 1)
          SELECT k, channel, workspace, partner, email, phone FROM b WHERE k IS NOT NULL AND k<>''""")
    except Exception as e:
        print(f"WARN consolidated_bookings im_bookings read failed {date}: {e}", file=sys.stderr)
        _book_cache[date] = []; return []
    out = []
    for k, channel, workspace, partner, email, phone in rows:
        out.append({"channel": channel or "", "ws": ws_alias(workspace),
                    "partner": (partner or "").strip() or "(unknown)", "key": k,
                    "email": (email or "").lower(), "phone": phone or ""})
    if not out:
        print(f"WARN consolidated_bookings: 0 im_bookings rows for {date} (portal snapshot missing the "
              f"day? all meeting columns will read 0)", file=sys.stderr)
    _book_cache[date] = out
    return out

# Pre-IPO (Ren2 / "SMS IPO") meetings = the active Pre-IPO booking DESKS (Collins + Summit) — resolved
# FROM the registry (config/daily_report_sources.json -> metrics.preipo_meetings.desks), NOT hardcoded
# here, so a new/renamed desk is a one-line registry edit. Counted by booking-made Date, deduped within
# each desk by email|phone. The desks are ADDITIVE (a lead books one OR the other — Collins overflow ->
# Summit per Grace), so do NOT cross-dedup. (Jun 29 = 23 Collins + 11 Summit = 34.)
PREIPO_SHEETS = [(d["spreadsheet_id"], d["tab"], d["desk"]) for d in REG.preipo_desks()]
_preipo_cache = {}  # date -> {"by_desk": {desk:n}, "health": {desk:"OK"|reason}}
def preipo_by_desk(date):
    """{desk: meetings} for `date` + per-desk source health, for the reconciliation/verify gate.
    A desk whose sheet 404s or is missing its Date column reports health != 'OK' AND count 0 — that is
    drift (flagged by --verify), never a silent genuine zero."""
    if date in _preipo_cache:
        return _preipo_cache[date]
    d = datetime.date.fromisoformat(date); want = _date_variants(d)
    by_desk = {}; health = {}
    for sid, tab, desk in PREIPO_SHEETS:
        by_desk[desk] = 0
        try:
            rows = gget(sid, f"{tab}!A1:Z")
        except Exception as e:
            print(f"WARN preipo {desk} {sid}: {e}", file=sys.stderr)
            health[desk] = f"UNREACHABLE: {e}"; continue
        if not rows:
            health[desk] = "EMPTY: no rows returned"; continue
        hdr = rows[0]; ix = {h.strip().lower(): i for i, h in enumerate(hdr)}
        di = ix.get("date"); ei = ix.get("email"); pi = ix.get("phone")
        if di is None:
            health[desk] = "NO_DATE_COLUMN: 'Date' header missing (renamed?)"; continue
        seen = set(); n = 0
        for r in rows[1:]:
            if di < len(r) and r[di] and str(r[di]).strip() in want:
                key = (r[ei].strip().lower() if ei is not None and ei < len(r) and r[ei] else "")
                if not key and pi is not None and pi < len(r) and r[pi]:
                    key = r[pi].strip()
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                n += 1
        by_desk[desk] = n; health[desk] = "OK"
    _preipo_cache[date] = {"by_desk": by_desk, "health": health}
    return _preipo_cache[date]

def preipo_meetings(date):
    """Total Pre-IPO meetings = SUM of the additive desks (Collins + Summit)."""
    return sum(preipo_by_desk(date)["by_desk"].values())

# im_bookings lead_type (cheap/regular) by email|phone, for the report day (warehouse SoT)
def imbookings_meetings(date):
    """{key(email|phone): lead_type} from the portal mirror (offer=Funding). Fail-soft -> {} so a
    warehouse hiccup costs only the cheap/regular split, not §1's Instantly sent/opps."""
    try:
        rows = wq(f"""
        WITH b AS (
          SELECT lower(coalesce(nullif(email,''), phone)) AS k,
                 max(lower(coalesce(lead_type,''))) AS lead_type
          FROM main.raw_im_bookings
          WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
            AND offer='Funding' AND substr(coalesce(date,''),1,10)=DATE '{date}'::VARCHAR
            AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')
          GROUP BY 1)
        SELECT k, lead_type FROM b WHERE k IS NOT NULL AND k<>''""")
    except Exception as e:
        print(f"WARN imbookings_meetings failed {date}: {e}", file=sys.stderr)
        return {}
    return {r[0]: (r[1] or "") for r in rows}

# ============================ SECTION DATA ============================
def get_email():
    inst = instantly_daily(DAILY)
    book = consolidated_bookings(DAILY)
    ltmap = imbookings_meetings(DAILY)
    # email-channel meetings per workspace; cheap/regular from im_bookings lead_type
    mtg = collections.defaultdict(lambda: [0, 0, 0])  # name -> [meetings, cheap, regular]
    for b in book:
        if b["channel"].lower() != "email" or not b["ws"]:
            continue
        lt = ltmap.get(b["key"], "")
        cheap = 1 if lt == "cheap" else 0
        reg = 1 if lt and lt != "cheap" else 0
        m = mtg[b["ws"]]; m[0] += 1; m[1] += cheap; m[2] += reg
    out = []
    for slug, name in WS:
        sent, opps = inst.get(slug, (0, 0))
        mm, c, r = mtg.get(name, [0, 0, 0])
        out.append((name, sent, 0, opps, mm, c, r))  # (ws, sent, hr(unused), opps, meetings, cheap, regular)
    return out

# sendivo sub-account ids -> labels, FROM the registry (metrics.sms_sent.api.sub_accounts).
SENDIVO_SUB = REG.sendivo_subs()                  # {id: label}
SUB_REN1 = REG.sendivo_sub_id("Funding")          # 12720 — Ren1 Funding SMS
SUB_REN2 = REG.sendivo_sub_id("Pre-IPO")          # 13922 — Ren2 Pre-IPO SMS
SUB_REN3 = REG.sendivo_sub_id("webform")          # 14603 — Ren3 RG3 webform SMS (app-link AIM)
def get_sms_wa():
    sms = sendivo_sms(DAILY)
    inb = {r[0]: (int(r[1]), int(r[2])) for r in wq(
        f"""SELECT sub_account_name, count(*), count(*) FILTER (WHERE NOT is_opt_out)
            FROM main.raw_sendivo_inbound WHERE CAST(received_at AS DATE)=DATE '{DAILY}' GROUP BY 1""")}
    book = consolidated_bookings(DAILY)
    sms_mtg = sum(1 for b in book if b["channel"].lower() == "sms")
    wa_mtg = sum(1 for b in book if b["channel"].lower() == "whatsapp")
    # Row shape: (label, sent, delivered, failed|None, human_replies, meetings).  WhatsApp surfaces
    # DELIVERED + FAIL% (ISKRA "sent" runs ~30-37% failed -> attempted is misleading); KPI=deliv/mtg
    # (folds in #116). SMS has no failed column here -> failed=None -> Fail% "—", delivered≈sent (the
    # sendivo billed/sent count; SMS delivery is ~100%). Funding SMS meetings on Ren1; Pre-IPO on Ren2.
    # Ren3 (webform) shows sent + replies but meetings=0: im_bookings' SMS channel carries no sub-account,
    # so ALL SMS-sourced meetings stay on the Ren1 row rather than being double-counted across sub-accounts.
    # ORDER MATTERS: Ren1 must stay index 0 and Ren2 index 1 (write_summary_block reads SMS_D[0]/[1]).
    rows = []
    s1 = sms.get(SUB_REN1, 0); rows.append(("Renaissance 1 (SMS)", s1, s1, None, inb.get("Renaissance 1", (0, 0))[1], sms_mtg))
    s2 = sms.get(SUB_REN2, 0); rows.append(("Renaissance 2 (SMS · Pre-IPO)", s2, s2, None, inb.get("Renaissance 2", (0, 0))[1], preipo_meetings(DAILY)))
    s3 = sms.get(SUB_REN3, 0); rows.append(("Renaissance 3 (SMS · webform)", s3, s3, None, inb.get("Renaissance 3", (0, 0))[1], 0))
    wa = wq(f"""SELECT sent, delivered, failed, replies_total FROM main.v_sms_dash_wa_daily
                WHERE channel='whatsapp' AND metric_date=DATE '{DAILY}'""")
    if wa:
        ws_, wd, wf, wr = (int(wa[0][i] or 0) for i in range(4))
    else:
        ws_, wd, wf, wr = 0, 0, 0, 0
    rows.append(("WhatsApp (ISKRA)", ws_, wd, wf, wr, wa_mtg))
    return rows

def get_close():
    c = wq(f"""SELECT COUNT(*) dials, COUNT(DISTINCT close_lead_id) leads,
                 COUNT(*) FILTER (WHERE duration_seconds >= 60) connects
               FROM core.call WHERE (occurred_at AT TIME ZONE 'America/New_York')::DATE = DATE '{DAILY}'""")
    d, l, cn = (int(c[0][0]), int(c[0][1]), int(c[0][2])) if c else (0, 0, 0)
    book = consolidated_bookings(DAILY)
    m = sum(1 for b in book if b["channel"].lower() == "call")
    return {"dials": d, "leads": l, "connects": cn, "meetings": m}

def get_truth():
    """§4 no-lag: Expected = active accounts' configured daily_limit by ws x infra (latest census,
    Outlook excluded); Actual = Instantly daily per workspace (== §1 sent). Per-infra actual is not
    cheaply measurable same-day (30k+ accounts/ws) so Actual is shown at workspace Total only."""
    cap = {(r[0], r[1]): float(r[2] or 0) for r in wq(
        f"""SELECT workspace_slug, infra, sum(daily_limit)
            FROM core.account_label
            WHERE census_date=(SELECT max(census_date) FROM core.account_label)
              AND lifecycle='Active' AND infra IN ('OTD','Google')
              AND workspace_slug IN ({SLUGS_SQL}) GROUP BY 1,2""")}
    inst = instantly_daily(DAILY)
    out = []
    for slug, name in WS:
        otd = cap.get((slug, "OTD"), 0.0); goog = cap.get((slug, "Google"), 0.0)
        actual = inst.get(slug, (0, 0))[0]
        out.append((name, otd, goog, otd + goog, actual))
    cr = wq("SELECT max(census_date) FROM core.account_label")
    census = cr[0][0] if cr and cr[0] else None
    return census, out

def get_partner():
    book = consolidated_bookings(DAILY)
    cnt = collections.Counter(b["partner"] for b in book)
    pr = sorted(cnt.items(), key=lambda x: -x[1])
    return pr, sum(n for _, n in pr)

def get_im_reply():
    """§6 IM reply-time from the native core.email_message: first prospect reply (ue_type 2) ->
    first our reply (ue_type 3) in the same thread; median/avg per workspace, daily/weekly/monthly.

    BUSINESS-HOURS WINDOW [Grace, 06-30 funnel sync]: only prospect replies that ARRIVE
    12pm-8pm ET, Mon-Fri are counted. IMs log in at 11am ET (first hour = catch-up) and don't
    work nights/weekends, so an 11pm or Saturday arrival is not answerable and would massively
    inflate latency. After-hours/weekend arrivals are EXCLUDED from the calc entirely (not clamped)
    -- the difference between a real ~3-8 min median and a garbage one. The window is applied to the
    prospect-reply ARRIVAL (p_et = message_at in ET); the response time itself is measured as-is, and
    the day/week/month buckets use the ET arrival date. message_at is TIMESTAMP WITH TIME ZONE (UTC);
    `AT TIME ZONE 'America/New_York'` yields the ET wall-clock. isodow 1-5 = Mon-Fri; hour 12..19 =
    12pm-8pm. NOTE: the most-recent day's DAILY column is inherently thin -- same-day prospect replies
    are often not yet answered (or the native our-reply sync lags); weekly/monthly carry the signal.
    The window is applied AFTER ranking (seq=1), so a thread whose FIRST reply arrived off-hours is
    excluded entirely (it is not re-pointed to a later in-window reply) -- the literal 'first reply
    only' reading of the spec."""
    wk = (_d - datetime.timedelta(days=6)).isoformat()
    mo = _d.replace(day=1).isoformat()
    sql = f"""
      WITH inbound AS (
        SELECT thread_id, workspace_id AS ws, message_at AS p_ts,
               (message_at AT TIME ZONE 'America/New_York') AS p_et,
               row_number() OVER (PARTITION BY thread_id ORDER BY message_at, message_id) AS seq
        FROM core.email_message
        WHERE ue_type=2 AND thread_id IS NOT NULL AND message_at >= DATE '{mo}'),
      ours AS (SELECT thread_id, message_at AS r_ts FROM core.email_message
               WHERE ue_type=3 AND thread_id IS NOT NULL AND message_at >= DATE '{mo}'),
      paired AS (
        SELECT i.ws, CAST(i.p_et AS DATE) d,
               date_diff('minute', i.p_ts,
                 (SELECT min(o.r_ts) FROM ours o WHERE o.thread_id=i.thread_id AND o.r_ts > i.p_ts)) AS lat
        FROM inbound i
        WHERE i.seq=1
          AND extract(isodow FROM i.p_et) BETWEEN 1 AND 5
          AND extract(hour   FROM i.p_et) BETWEEN 12 AND 19)
      SELECT ws,
        count(*) FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        median(lat) FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        avg(lat)    FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        count(*) FILTER (WHERE d>=DATE '{wk}' AND d<=DATE '{DAILY}' AND lat IS NOT NULL),
        median(lat) FILTER (WHERE d>=DATE '{wk}' AND d<=DATE '{DAILY}' AND lat IS NOT NULL),
        avg(lat)    FILTER (WHERE d>=DATE '{wk}' AND d<=DATE '{DAILY}' AND lat IS NOT NULL),
        count(*) FILTER (WHERE d<=DATE '{DAILY}' AND lat IS NOT NULL),
        median(lat) FILTER (WHERE d<=DATE '{DAILY}' AND lat IS NOT NULL),
        avg(lat)    FILTER (WHERE d<=DATE '{DAILY}' AND lat IS NOT NULL)
      FROM paired WHERE ws IN ({SLUGS_SQL}) GROUP BY 1"""
    rows = {r[0]: r for r in wq(sql)}
    out = []
    for slug, name in WS:
        r = rows.get(slug)
        if r:
            out.append((name, r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]))
        else:
            out.append((name, 0, None, None, 0, None, None, 0, None, None))
    return (DAILY, wk, mo), out

# ============================ source self-verify (the DATA-TICKET fix) ============================
# A chat / the SLA watchdog can confirm "am I sourcing the right thing for every metric?" with ZERO
# human hand-feeding: probe each registered source is reachable + correctly-shaped, and reconcile
# Pre-IPO per-desk counts to the team's #pre-ipo-success counter. Drift is FLAGGED, never silently
# rendered.  handoffs/2026-06-30-DATA-TICKET-preipo-source-mapping-incompleteness.md
def _rel_ok(rel):
    try:
        wq(f"SELECT 1 FROM {rel} LIMIT 1"); return True, "OK"
    except Exception as e:
        return False, str(e).strip()[:160]

def reconcile_preipo(report_date):
    """Reconcile the rendered per-desk Pre-IPO counts to the #pre-ipo-success counter (the team's own
    tally). Returns (status, drift_lines, sheet_by_desk, anchor_by_desk).
    status: OK | DRIFT | ANCHOR_UNAVAILABLE.  Per-desk source health (unreachable / renamed Date col)
    is drift on its own, regardless of whether the Slack anchor is reachable."""
    info = preipo_by_desk(report_date); sheet = info["by_desk"]; health = info["health"]
    drift = [f"{d} desk source unhealthy: {h}" for d, h in health.items() if h != "OK"]
    slack = None
    try:
        slack = REG.fetch_preipo_slack_tally(report_date)
    except Exception as e:
        print(f"WARN preipo slack tally failed: {e}", file=sys.stderr)
    if slack and slack.get("by_desk"):
        sd = slack["by_desk"]
        for desk, n_sheet in sheet.items():
            n_slack = sd.get(desk, 0)
            if n_sheet != n_slack:
                drift.append(f"{desk}: sheet={n_sheet} vs #pre-ipo-success counter={n_slack}")
        if slack.get("unknown_desks"):
            drift.append(f"UNKNOWN desk(s) posting in #pre-ipo-success but NOT in the registry: "
                         f"{slack['unknown_desks']} -> add to config/daily_report_sources.json")
        return ("DRIFT" if drift else "OK"), drift, sheet, sd
    # degrade 1: reconcile against the registry's known_good if it is for this date
    kg = REG.preipo_known_good()
    if kg.get("date") == report_date and kg.get("by_desk"):
        for desk, n_sheet in sheet.items():
            n_kg = kg["by_desk"].get(desk)
            if n_kg is not None and n_sheet != n_kg:
                drift.append(f"{desk}: sheet={n_sheet} vs known_good={n_kg}")
        return ("DRIFT" if drift else "OK"), drift, sheet, kg["by_desk"]
    # degrade 2: no live anchor and not a known_good date -> structural health only
    return ("DRIFT" if drift else "ANCHOR_UNAVAILABLE"), drift, sheet, None

def verify_sources(report_date):
    """Print a per-metric CONFIRM/WARN/DRIFT report and return True iff no source DRIFTED. Used by
    `render_daily.py <date> --verify` (a self-check a chat runs to trust the report without a human)."""
    reg = REG.load_registry()
    out = []  # (status, msg)
    def L(st, msg): out.append((st, msg))
    errs = REG.validate_registry(reg)
    L("OK" if not errs else "DRIFT", "registry schema valid" if not errs else "registry INVALID: " + "; ".join(errs))
    # §1 Instantly per-workspace
    try:
        inst = instantly_daily(report_date); missing = [s for s, _ in WS if s not in inst]
        L("OK" if not missing else "WARN",
          f"§1 Instantly: {len(inst)}/{len(WS)} workspaces returned" + (f"; MISSING {missing}" if missing else ""))
    except Exception as e:
        L("WARN", f"§1 Instantly probe failed: {e}")
    # booking source (§1 meetings / §2 / §3 / §5) = the portal im_bookings [cutover 2026-06-30] — probe
    # the ACTUAL source (raw_im_bookings has Funding rows for the report day), NOT the retired FF sheet
    # (probing a decommissioned source would false-flag). 0 rows for the day = the portal snapshot is
    # missing it -> every meeting column would read 0 (a real, flaggable problem).
    try:
        bn = consolidated_bookings(report_date)
        L("OK" if bn else "WARN",
          f"booking source main.raw_im_bookings (Funding, {report_date}): {len(bn)} deduped bookings"
          + ("" if bn else " — 0 rows; meetings will render 0 (portal snapshot missing the day?)"))
    except Exception as e:
        L("DRIFT", f"booking source main.raw_im_bookings UNREACHABLE: {e}")
    # §2 sendivo billing reachable
    try:
        sm = sendivo_sms(report_date)
        L("OK" if sm else "WARN", f"§2 sendivo billing: {len(sm)} sub-account(s) returned; registry subs {REG.sendivo_subs()}")
    except Exception as e:
        L("WARN", f"§2 sendivo probe failed: {e}")
    # every warehouse-sourced metric: relation reachable (right name?)
    for mid, m in reg["metrics"].items():
        if m.get("source_type") == "warehouse":
            ok, why = _rel_ok(m["warehouse"]["relation"])
            L("OK" if ok else "DRIFT", f"{mid}: warehouse {m['warehouse']['relation']} " + ("reachable" if ok else f"UNREACHABLE: {why}"))
    # Pre-IPO reconciliation (the ticket's core metric)
    st, drift, sheet, anchor = reconcile_preipo(report_date)
    if st == "OK":
        L("OK", f"Pre-IPO desks {sheet} reconcile to anchor {anchor}")
    elif st == "ANCHOR_UNAVAILABLE":
        L("WARN", f"Pre-IPO desks {sheet} reachable; reconciliation anchor unavailable "
                  f"(no Slack read creds and {report_date} != known_good date) — structural check only")
    else:
        L("DRIFT", "Pre-IPO RECONCILIATION DRIFT — " + " | ".join(drift) + f"  (rendered {sheet}, anchor {anchor})")
    # print
    print(f"\n=== daily-report SOURCE VERIFY · {report_date} ===")
    for st_, msg in out:
        print(f"  [{ {'OK': '✓ OK   ', 'WARN': '! WARN ', 'DRIFT': '✗ DRIFT'}.get(st_, '? ?    ') }] {msg}")
    n_drift = sum(1 for st_, _ in out if st_ == "DRIFT")
    print("=== " + ("PASS — every source confirmed against the registry" if not n_drift
                     else f"DRIFT — {n_drift} source(s) need attention (see config/daily_report_sources.json)") + " ===\n")
    return n_drift == 0

if VERIFY:
    sys.exit(0 if verify_sources(DAILY) else 2)

# ============================ collect (per-section failure isolation, C1/C2) ============================
# One external failure (warehouse 500, sheet error, API timeout) must degrade ONLY its section to a
# safe sentinel, never take down the whole nightly render. Fallback tuple arities MUST match the
# table builders + write_summary_block unpacking.
def _safe(label, fn, default):
    try:
        return fn()
    except Exception as e:
        print(f"WARN section '{label}' FAILED ({e}); rendering it empty", file=sys.stderr)
        return default
_EMAIL_FB = [(name, 0, 0, 0, 0, 0, 0) for _, name in WS]
_SMS_FB = [("Renaissance 1 (SMS)", 0, 0, None, 0, 0), ("Renaissance 2 (SMS · Pre-IPO)", 0, 0, None, 0, 0), ("Renaissance 3 (SMS · webform)", 0, 0, None, 0, 0), ("WhatsApp (ISKRA)", 0, 0, 0, 0, 0)]
_CLOSE_FB = {"dials": 0, "leads": 0, "connects": 0, "meetings": 0}
_TRUTH_FB = (None, [(name, 0, 0, 0, 0) for _, name in WS])
_IMREPLY_FB = ((DAILY, DAILY, DAILY), [(name, 0, None, None, 0, None, None, 0, None, None) for _, name in WS])

EMAIL_D = _safe("email", get_email, _EMAIL_FB)
SMS_D = _safe("sms_wa", get_sms_wa, _SMS_FB)
CLOSE_D = _safe("close", get_close, _CLOSE_FB)
SENDING_CENSUS, SENDING_TRUTH = _safe("truth", get_truth, _TRUTH_FB)
PARTNER_D, PARTNER_D_TOTAL = _safe("partner", get_partner, ([], 0))
IMREPLY_PERIODS, IMREPLY_D = _safe("imreply", get_im_reply, _IMREPLY_FB)
PREIPO_MTG = _safe("preipo", lambda: preipo_meetings(DAILY), 0)

if DRY:
    print(f"REPORT_DATE={DAILY}  TAB={DAILY_TAB}  census(§4)={SENDING_CENSUS}  periods(§6)={IMREPLY_PERIODS}")
    print("§1 EMAIL:");    [print("  ", x) for x in EMAIL_D]
    print("§2 SMS/WA:");   [print("  ", x) for x in SMS_D]
    print("§3 CLOSE:", CLOSE_D)
    print("§4 TRUTH (ws, otd_exp, goog_exp, tot_exp, actual):"); [print("  ", x) for x in SENDING_TRUTH]
    print("§5 PARTNER:", PARTNER_D, "total", PARTNER_D_TOTAL)
    print("§6 IM-REPLY (ws, d_n,d_med,d_avg, w_n,w_med,w_avg, m_n,m_med,m_avg):"); [print("  ", x) for x in IMREPLY_D]
    print("Pre-IPO meetings:", PREIPO_MTG)
    sys.exit(0)

# ============================ formatting engine ============================
def api(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Authorization": f"Bearer {_gtok()}", "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))
# defensive coercion: the read API may serialize DuckDB DECIMAL/avg/median as strings (M2)
def _f(x):
    try: return float(x)
    except (TypeError, ValueError): return 0.0
def rr(hr, sent): return (_f(hr) / _f(sent)) if sent else "—"
def pctstr(n, m): return f"'{round(100.0*_f(n)/_f(m))}%" if m else "'0%"
def kpi(sent, m): return round(_f(sent) / _f(m)) if m else "—"
def fpct(actual, expected): return (_f(actual) / _f(expected)) if expected else "—"
def mins(v): return "—" if v is None else round(_f(v))
def rgb(r, g, b): return {"red": r, "green": g, "blue": b}

def build_and_write(tab, build_fn):
    rows = []; sec = []; th = []; tot = []; rrrows = []; merges = []; strows = []; data = []
    th_ncol = {}; row_ncol = {}
    def add(r=None): rows.append(r or []); return len(rows) - 1
    EMAIL_HDR_TOP = ["Workspace", "Sent", "Opportunities", "Meetings", "KPI (sent/mtg)", "Cheap", "", "Regular", ""]
    SUB_HDR = ["", "", "", "", "", "#", "%", "#", "%"]
    def email_table(data_rows, header_label):
        W = 9
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr_top = add(EMAIL_HDR_TOP); th.append(hr_top); hr_sub = add(SUB_HDR); th.append(hr_sub)
        merges.append((hr_top, 5, 7)); merges.append((hr_top, 7, 9)); th_ncol[hr_top] = th_ncol[hr_sub] = W
        ts = topp = tm = tc_ = tr_ = 0
        for ws, sent, hr, opps, m, c, r in data_rows:
            ri = add([ws, sent, opps, m, kpi(sent, m), c, pctstr(c, m), r, pctstr(r, m)]); data.append(ri); row_ncol[ri] = W
            ts += sent; topp += opps; tm += m; tc_ += c; tr_ += r
        ti = add(["TOTAL", ts, topp, tm, kpi(ts, tm), tc_, pctstr(tc_, tm), tr_, pctstr(tr_, tm)]); tot.append(ti); row_ncol[ti] = W
    def sms_wa_table(rows_data, header_label):
        # WhatsApp shows DELIVERED + FAIL% (ISKRA "sent" runs ~30-37% failed -> attempted misleads);
        # KPI = delivered/mtg. SMS: failed=None -> Fail% "—", delivered≈sent. (folds in #116)
        W = 7
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Channel / workspace", "Sent", "Delivered", "Fail %", "Human RR", "Meetings", "KPI (deliv/mtg)"]); th.append(hr); th_ncol[hr] = W
        def failpct(failed, sent): return (f"'{round(100.0*failed/sent)}%" if sent else "'0%") if failed is not None else "—"
        ts = td = tf = thr = tm = 0; any_fail = False
        for label, sent, deliv, failed, reps, m in rows_data:
            ri = add([label, sent, deliv, failpct(failed, sent), rr(reps, deliv), m, kpi(deliv, m)]); rrrows.append(ri); data.append(ri); row_ncol[ri] = W
            ts += sent; td += deliv; thr += reps; tm += m
            if failed is not None: tf += failed; any_fail = True
        ti = add(["TOTAL", ts, td, (f"'{round(100.0*tf/ts)}%" if (any_fail and ts) else "—"), rr(thr, td), tm, kpi(td, tm)]); tot.append(ti); rrrows.append(ti); row_ncol[ti] = W
    def truth_table(header_label):
        W = 6
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Workspace", "Expected OTD", "Expected Google", "Expected Total", "Actual (sent)", "Fulfillment %"])
        th.append(hr); th_ncol[hr] = W
        oe = ge = te = ta = 0
        for ws, otd, goog, totexp, actual in SENDING_TRUTH:
            ri = add([ws, round(otd), round(goog), round(totexp), actual, fpct(actual, totexp)])
            data.append(ri); strows.append(ri); row_ncol[ri] = W
            oe += otd; ge += goog; te += totexp; ta += actual
        sti = add(["TOTAL", round(oe), round(ge), round(te), ta, fpct(ta, te)]); tot.append(sti); strows.append(sti); row_ncol[sti] = W
    def close_table(close, label):
        W = 2
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Close CRM — metric", "Value"]); th.append(hr); th_ncol[hr] = W
        d2m = f"'{(100.0*close['meetings']/close['dials']):.2f}%" if close['dials'] else "—"
        for r2 in [["Dials", close["dials"]], ["Distinct leads dialed", close["leads"]],
                   ["Connects (≥60s real convo)", close["connects"]],
                   ["Meetings booked (call-sourced)", close["meetings"]], ["Dial → meeting %", d2m]]:
            ri = add(r2); data.append(ri); row_ncol[ri] = W
    def partner_table(rows_data, total, label):
        W = 2
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Partner", "Meetings booked"]); th.append(hr); th_ncol[hr] = W
        for p, n in rows_data:
            ri = add([p, n]); data.append(ri); row_ncol[ri] = W
        ti = add(["TOTAL", total]); tot.append(ti); row_ncol[ti] = W
    def imreply_table(rows_data, label):
        W = 10
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr_top = add(["Workspace", "Daily", "", "", "Weekly (7d)", "", "", "Monthly (MTD)", "", ""]); th.append(hr_top)
        hr_sub = add(["", "n", "Median min", "Avg min", "n", "Median min", "Avg min", "n", "Median min", "Avg min"]); th.append(hr_sub)
        merges.append((hr_top, 1, 4)); merges.append((hr_top, 4, 7)); merges.append((hr_top, 7, 10)); th_ncol[hr_top] = th_ncol[hr_sub] = W
        for name, dn, dmed, davg, wn, wmed, wavg, mn, mmed, mavg in rows_data:
            ri = add([name, dn, mins(dmed), mins(davg), wn, mins(wmed), mins(wavg), mn, mins(mmed), mins(mavg)])
            data.append(ri); row_ncol[ri] = W
        # Explicit empty-state so a legitimately-quiet in-window day (or the ~24h email_message sync lag)
        # never reads as a clobbered/missing §6 — the header + a per-workspace shell ALWAYS render, and
        # this note names WHY the daily column is thin (the watchdog anchors on this section existing).
        if not any((r[1] or 0) for r in rows_data):
            note = add(["(no in-window prospect email replies for %s — 12-8pm ET Mon-Fri arrivals only; "
                        "same-day replies often unanswered yet + email_message sync lags ~24h — weekly/"
                        "monthly carry the signal)" % DAILY]); data.append(note); row_ncol[note] = W
        pend = add(["SMS · WhatsApp first-reply time", "pending", "pending", "pending", "pending", "pending", "pending", "pending", "pending", "pending"])
        data.append(pend); row_ncol[pend] = W
    build_fn(dict(add=add, email_table=email_table, sms_wa_table=sms_wa_table, truth_table=truth_table,
                  close_table=close_table, partner_table=partner_table, imreply_table=imreply_table))

    meta = api("GET", BASE + "?fields=sheets(properties(sheetId,title),bandedRanges(bandedRangeId),conditionalFormats)")
    sh = next((s for s in meta["sheets"] if s["properties"]["title"] == tab), None)
    if sh is None:
        r = api("POST", BASE + ":batchUpdate", {"requests": [{"addSheet": {"properties": {"title": tab, "gridProperties": {"rowCount": max(len(rows) + 20, 200), "columnCount": 26}}}}]})
        sid = r["replies"][0]["addSheet"]["properties"]["sheetId"]; sh = {"properties": {"sheetId": sid}, "bandedRanges": [], "conditionalFormats": []}
    else:
        sid = sh["properties"]["sheetId"]
    api("POST", f"{BASE}/values/{urllib.parse.quote(tab)}!A1:Z400:clear", {})
    api("POST", BASE + ":batchUpdate", {"requests": [{"unmergeCells": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 400, "startColumnIndex": 0, "endColumnIndex": 26}}}]})
    api("PUT", f"{BASE}/values/{urllib.parse.quote(tab)}!A1?valueInputOption=USER_ENTERED", {"values": rows})
    NCOL = 10; NROW = len(rows); WIDE = max(NROW + 5, 200)
    def rng(r0, r1, c0=0, c1=NCOL): return {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r1, "startColumnIndex": c0, "endColumnIndex": c1}
    def fill(i, color, **tf): return {"repeatCell": {"range": rng(i, i + 1, 0, row_ncol.get(i, NCOL)), "cell": {"userEnteredFormat": {"backgroundColor": rgb(*color), "textFormat": tf}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}}
    reqs = []
    for br in sh.get("bandedRanges", []) or []: reqs.append({"deleteBanding": {"bandedRangeId": br["bandedRangeId"]}})
    for idx in range(len(sh.get("conditionalFormats", []) or []) - 1, -1, -1): reqs.append({"deleteConditionalFormatRule": {"sheetId": sid, "index": idx}})
    reqs.append({"unmergeCells": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": WIDE, "startColumnIndex": 0, "endColumnIndex": 26}}})
    reqs.append({"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": WIDE, "startColumnIndex": 0, "endColumnIndex": 26}, "cell": {"userEnteredFormat": {"backgroundColor": rgb(1, 1, 1), "horizontalAlignment": "LEFT", "verticalAlignment": "BOTTOM", "wrapStrategy": "OVERFLOW_CELL", "numberFormat": {"type": "TEXT"}, "textFormat": {"bold": False, "italic": False, "fontSize": 10, "foregroundColor": rgb(0, 0, 0)}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,numberFormat,textFormat)"}})
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": WIDE}, "properties": {"pixelSize": 21}, "fields": "pixelSize"}})
    reqs.append({"repeatCell": {"range": rng(0, NROW, 1, NCOL), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}, "fields": "userEnteredFormat.numberFormat"}})
    reqs.append({"repeatCell": {"range": rng(0, NROW, 0, 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in rrrows:  # §2 Human RR (col 4) as a percent
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 4, 5), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in strows:
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 5, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in sorted(set(data) | set(tot) | set(strows)):
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 1, NCOL), "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}}, "fields": "userEnteredFormat.horizontalAlignment"}})
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 0, 1), "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}}, "fields": "userEnteredFormat.horizontalAlignment"}})
    reqs.append({"repeatCell": {"range": rng(0, 1), "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 14}}}, "fields": "userEnteredFormat.textFormat"}})
    reqs.append({"repeatCell": {"range": rng(1, 2), "cell": {"userEnteredFormat": {"textFormat": {"italic": True, "foregroundColor": rgb(0.4, 0.4, 0.4), "fontSize": 9}}}, "fields": "userEnteredFormat.textFormat"}})
    for i in sec: reqs.append(fill(i, (0.85, 0.89, 0.95), bold=True, fontSize=11))
    for i in th:
        cw = th_ncol.get(i, NCOL)
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 0, cw), "cell": {"userEnteredFormat": {"backgroundColor": rgb(0.20, 0.23, 0.27), "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "CLIP", "textFormat": {"bold": True, "foregroundColor": rgb(1, 1, 1), "fontSize": 10}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,textFormat)"}})
    for i in data: reqs.append(fill(i, (1, 1, 1), bold=False, italic=False, foregroundColor=rgb(0, 0, 0), fontSize=10))
    for i in tot: reqs.append(fill(i, (0.90, 0.92, 0.95), bold=True))
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 230}, "fields": "pixelSize"}})
    LAST_COL = max([th_ncol.get(i, NCOL) for i in th] + [NCOL])
    for c in range(1, LAST_COL):
        reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": c, "endIndex": c + 1}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}})
    # red gradient on §4 fulfillment % (col 5)
    if len(strows) >= 2:
        d0 = strows[0]; d1 = strows[-2] + 1
        reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {"ranges": [{"sheetId": sid, "startRowIndex": d0, "endRowIndex": d1, "startColumnIndex": 5, "endColumnIndex": 6}], "gradientRule": {"minpoint": {"color": rgb(0.91, 0.40, 0.40), "type": "NUMBER", "value": "0"}, "maxpoint": {"color": rgb(1, 1, 1), "type": "NUMBER", "value": "1"}}}}})
    api("POST", BASE + ":batchUpdate", {"requests": reqs})
    merge_reqs = [{"mergeCells": {"range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": c0, "endColumnIndex": c1}, "mergeType": "MERGE_ALL"}} for r0, c0, c1 in merges]
    if merge_reqs: api("POST", BASE + ":batchUpdate", {"requests": merge_reqs})
    write_summary_block(sid)
    print(f"  {tab}: {NROW} rows, sections={len(sec)} headers={len(th)} totals={len(tot)} + summary block")

def write_summary_block(sid):
    """Cream right-side 'Business / Funding' summary block (cols L-O), GENERATED from section data."""
    def short(ws): return ws.split(" (")[0]
    def ratio(a, b): return round(a / b) if b else ""
    # SMS_D rows: (label, sent, delivered, failed, replies, meetings)
    sms1 = SMS_D[0] if len(SMS_D) > 0 else ("", 0, 0, None, 0, 0)
    sms2 = SMS_D[1] if len(SMS_D) > 1 else ("", 0, 0, None, 0, 0)
    wa = next((x for x in SMS_D if "WhatsApp" in x[0]), ("", 0, 0, 0, 0, 0))
    warm = next((r for r in EMAIL_D if r[0].lower().startswith("warm")), ("Warm leads", 0, 0, 0, 0, 0, 0))
    wsrows = [r for r in EMAIL_D if not r[0].lower().startswith("warm")]
    blk = [[f"{DAILY_TAB} — Business / Funding · {DAILY}", "", "", ""],
           ["WORKSPACE", "Email Sent", "Meeting Booked", "Meeting to Booked"]]
    ts = tm = 0
    for ws, sent, hr, opps, m, c, r in wsrows:
        blk.append([short(ws), sent, m, ratio(sent, m)]); ts += sent; tm += m
    blk.append(["Total", ts, tm, ratio(ts, tm)]); blk.append(["", "", "", ""])
    for lbl, s, m in [("SMS Funding", sms1[1], sms1[5]), ("SDR (Close)", CLOSE_D["dials"], CLOSE_D["meetings"]),
                      ("Warm Leads", warm[1], warm[4]), ("WhatsApp Funding (delivered)", wa[2], wa[5])]:
        blk.append([lbl, s, m, ratio(s, m)])
    blk.append(["", "", "", ""])
    blk.append(["SMS IPO", sms2[1], PREIPO_MTG, ratio(sms2[1], PREIPO_MTG)])
    for lbl in ["WhatsApp PRE-IPO", "SEC 125", "Tariffs", "R&D Credit"]:
        blk.append([lbl, 0, 0, ""])
    api("PUT", f"{BASE}/values/{urllib.parse.quote(DAILY_TAB)}!L4?valueInputOption=USER_ENTERED", {"values": blk})
    n = len(blk); r0 = 3; r1 = r0 + n; CREAM = rgb(0.988, 0.953, 0.804)
    def br(a, b, c=11, d=15): return {"sheetId": sid, "startRowIndex": a, "endRowIndex": b, "startColumnIndex": c, "endColumnIndex": d}
    thin = {"style": "SOLID", "width": 1, "color": rgb(0.55, 0.45, 0.15)}
    reqs = [
        {"repeatCell": {"range": br(r0, r1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "textFormat": {"fontSize": 10}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
        {"mergeCells": {"range": br(r0, r0 + 1), "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": br(r0, r0 + 1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True, "fontSize": 12}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": br(r0 + 1, r0 + 2), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": br(r0 + 9, r0 + 10), "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat"}},
        {"repeatCell": {"range": br(r0 + 2, r1, 12, 15), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"}}, "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"}},
        {"updateBorders": {"range": br(r0, r1), "top": thin, "bottom": thin, "left": thin, "right": thin, "innerHorizontal": thin, "innerVertical": thin}}]
    api("POST", BASE + ":batchUpdate", {"requests": reqs})

def daily(ctx):
    add = ctx["add"]
    add([f"DAILY REVOPS REPORT — Business Funding · {DAILY}"])
    add([os.environ.get("DAILY_SUBTITLE", f"Daily · {DAILY} · single-source-of-truth (warehouse + Instantly/sendivo live)")]); add()
    ctx["email_table"](EMAIL_D, f"1 · EMAIL + WARM LEADS — by workspace · day {DAILY}"); add()
    ctx["sms_wa_table"](SMS_D, f"2 · SMS + WHATSAPP — by channel · day {DAILY}"); add()
    ctx["close_table"](CLOSE_D, f"3 · CLOSE CRM — warm calling · day {DAILY}"); add()
    ctx["truth_table"](f"4 · SENDING VOLUME TRUTH — expected (active capacity) vs actual sends · no-lag · census {SENDING_CENSUS}"); add()
    ctx["partner_table"](PARTNER_D, PARTNER_D_TOTAL, f"5 · BOOKINGS BY PARTNER · day {DAILY}"); add()
    ctx["imreply_table"](IMREPLY_D, f"6 · IM REPLY-TIME — first-reply latency by workspace · 12-8pm ET Mon-Fri arrivals only · daily / weekly / monthly · email (SMS+WA pending) · Grace & Sam")

def _alert(text):
    """Best-effort drift alert via the established scripts/alert_slack.py path; never raises (the tab
    is already written, so a Slack hiccup must not fail the render)."""
    try:
        import subprocess
        subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_slack.py"), text],
                       timeout=30, check=False)
    except Exception as e:
        print(f"WARN _alert failed: {e}", file=sys.stderr)

print(f"Rendering DAILY tab '{DAILY_TAB}' for {DAILY} ...")
build_and_write(DAILY_TAB, daily)
print(f"OK — wrote tab '{DAILY_TAB}'.")

# Post-render drift guard: reconcile Pre-IPO to the team counter AFTER writing (never blocks the tab).
# "flag drift instead of silently rendering" — DATA-TICKET 2026-06-30. Fully guarded; a Slack/recon
# failure only costs the alert, not the render.
try:
    _st, _drift, _sheet, _anchor = reconcile_preipo(DAILY)
    if _st == "DRIFT":
        _msg = (f":warning: *daily-report Pre-IPO source DRIFT* ({DAILY}, tab '{DAILY_TAB}'): "
                + " | ".join(_drift) + f"  (rendered {_sheet} vs anchor {_anchor}). "
                "Fix config/daily_report_sources.json -> preipo_meetings (a new/renamed desk?).")
        print("WARN " + _msg, file=sys.stderr)
        _alert(_msg)
    else:
        print(f"Pre-IPO reconcile: {_st} (desks {_sheet})")
except Exception as _e:
    print(f"WARN post-render Pre-IPO reconcile failed: {_e}", file=sys.stderr)
