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
 §2 SMS Cost $ (actual)-> WAREHOUSE main.raw_sendivo_billing_daily.total_spend (nightly upsert; the
                          ALL-IN Sendivo $/day per sub: SMS fees + carrier fees + any setup/renewal/
                          brand/phone fees — sms_fee_usd ALONE understated ~3x by excluding carrier
                          fees, which bill per segment) + Cost/mtg·form = cost ÷ that row's meetings
                          (Ren3: web-form fills). Partial/missing day-row -> '—' + WARN
                          (100%-or-wipe), back-fills post-nightly. WhatsApp: no cost feed -> '—'.
 §2b SMS cost (window) -> raw_sendivo_billing_daily.total_spend (all-in $, SAME basis as §2) summed
                          over the SAME trailing fully-classified window as §2b's opps; Cost/opp =
                          cost ÷ opps. (Was funnel campaign-feed cost_usd = the carrier-fee component
                          only — understated all-in spend ~1.6x; fixed 2026-07-01.)
 §1 Opp→mtg %          -> meetings ÷ opportunities, same same-day frame as §1's columns ('—' when opps=0).
 §2 SMS replies (human)-> raw_sendivo_inbound non-opt-out.
 §2 WhatsApp           -> v_sms_dash_wa_daily (sent/delivered/failed/replies); meetings -> im_bookings.
 §3 Close             -> core.call (dials/leads/connects @>=60s, ET day) — Close API SoR, under-captures.
 §4 Sending truth      -> Expected = active accounts' configured daily_limit by infra (core.account_label,
                          latest census, no lag); Actual = Instantly daily per workspace (== §1 sent).
 §5 Bookings/partner   -> im_bookings partner.
 §6 IM reply-time      -> core.email_message (native, NIGHTLY-synced — D-1 at best, so the report-day
                          DAILY column is structurally thin/empty and back-fills on later renders):
                          BUSINESS MINUTES (clock runs 12-8pm ET Mon-Fri only) from each thread's first
                          prospect reply (ue_type 2) to our first reply (ue_type 3); med/avg per
                          workspace daily/weekly. Spec locked with Grace 06-30. NO MTD on a daily tab
                          [DR-10, Sam 07-01] — §3's MTD rows + §6's Monthly block removed; trailing
                          windows (§2b 7d, §6 weekly) stay.
 §1b EMAIL KPIs BY INFRA   -> live (re-added after the 06-30 removal); renders INFRA_RENDER_ROWS only
                          (Google/OTD — Milkbox row wiped 2026-07-01 per the 100%-or-wipe rule,
                          pending the centralize-pass rebuild; nonzero Milkbox sends WARN loud).

Usage:  render_daily.py 2026-06-29 ["Jun 29"]      (tab name defaults to "%b %-d" = "Jun 29")
        render_daily.py 2026-06-29 --dry            (print the data, do not write the sheet)
"""
import json, os, sys, datetime, statistics, urllib.request, urllib.parse, collections, concurrent.futures

# ---------- repo imports (box: run via .venv/bin/python from REPO_DIR) ----------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from core.credentials import load_credentials
    _CREDS = load_credentials()
except Exception as _e:  # dry/off-box fallback: external fetchers degrade, warehouse still works
    _CREDS = None
    print(f"WARN: credentials unavailable ({_e}); Instantly/sendivo sections will be empty", file=sys.stderr)
try:
    from sources.instantly import InstantlyClient   # §1b infra KPIs (campaign-grain + tag map)
except Exception as _e:
    InstantlyClient = None
    print(f"WARN: InstantlyClient import failed ({_e}); §1b infra section will be empty", file=sys.stderr)

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
# ET_TZ = THE one ET timezone object for this file (§6 clock math + today-default share it — never
# re-derive ET per function). None => zoneinfo unavailable: today-default degrades to UTC date and
# §6 raises into _safe (renders empty + WARN) rather than compute SLA math in a wrong fixed offset.
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
    _today_et = datetime.datetime.now(ET_TZ).date().isoformat()
except Exception:
    ET_TZ = None
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

# Campaign operator-tag -> workspace override [Sam 2026-06-30]. The booking desk sometimes mis-picks
# the free-text `workspace` (a (SAMUEL) booking was landing under "Renaissance 1"), but the campaign
# name carries the operator tag, which for a 1:1 operator is AUTHORITATIVE and overrides the label.
# SAMUEL runs ONLY Funding 1 -> any (SAMUEL) campaign is Funding 1, whatever the workspace field says.
# NB: "SAM" is deliberately NOT here — SAM runs BOTH F2 and F4 (campaigns "F2 - … (SAM)" / "F4 - … (SAM)"),
# so it's ambiguous and MUST be resolved by the workspace label; and matching "SAMUEL" (not "SAM") can't
# false-hit a "(SAM)" campaign. Add another operator only once confirmed unique to one workspace.
_CAMPAIGN_OPERATOR = {"SAMUEL": "Funding 1 (Samuel)"}
def ws_from_booking(workspace, campaign):
    """Resolve a booking's workspace: a 1:1 operator tag in the campaign wins, else the free-text label."""
    c = (campaign or "").upper()
    for op, ws in _CAMPAIGN_OPERATOR.items():
        if op in c:
            return ws
    return ws_alias(workspace)

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
    resp = json.load(urllib.request.urlopen(req, timeout=180))
    # A truncated result would silently understate whatever aggregates from it (§6 pulls row-level
    # pairs); fail LOUD -> _safe renders that section empty + WARN, never a wrong-but-plausible number.
    if resp.get("truncated"):
        raise RuntimeError(f"warehouse query TRUNCATED at {resp.get('row_count')} rows — refusing partial data")
    return resp["rows"]

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

_billing_cache = {}
def sendivo_billing_cost(date):
    """{sub_account_id: (sms_fee_qty, total_spend_usd)} for `date` from the WAREHOUSE
    main.raw_sendivo_billing_daily (nightly 'sendivo billing_daily' phase, upsert-keyed
    (metric_date, sub_account_id)) — the §2 actual-$ SMS cost feed [backlog A1, 2026-07-01].
    NEW columns read the warehouse (the centralize direction); §2 Sent deliberately STAYS on the
    live billing API (no existing-metric repoint — that flip is gated). $ = total_spend, the API's
    own ALL-IN figure (sms_fee_usd + carrier_fee_usd + setup/renewal/brand/phone fees; sms_fee_usd
    ALONE understated ~3x — carrier fees bill per SEGMENT, so carrier_fee_qty > message count.
    July-1 sanity: sub 12720 sms_fee $754.03 + carrier $1,489.83 = total_spend $2,243.87).
    sms_fee_qty still returned as the freshness tripwire vs live billed sent. Fail-soft -> {}
    (cost cells render '—' + WARN), never crashes §2."""
    if date in _billing_cache:
        return _billing_cache[date]
    out = {}
    try:
        for r in wq(f"""SELECT sub_account_id, sms_fee_qty, total_spend
                        FROM main.raw_sendivo_billing_daily WHERE metric_date=DATE '{date}'"""):
            out[int(r[0])] = (int(r[1] or 0), float(r[2] or 0.0))
    except Exception as e:
        print(f"WARN sendivo_billing_cost failed {date}: {e}; §2 Cost $ renders '—'", file=sys.stderr)
    _billing_cache[date] = out
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
                   max(campaign)  AS campaign,
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
          SELECT k, channel, workspace, partner, campaign, email, phone FROM b WHERE k IS NOT NULL AND k<>''""")
    except Exception as e:
        print(f"WARN consolidated_bookings im_bookings read failed {date}: {e}", file=sys.stderr)
        _book_cache[date] = []; return []
    out = []
    for k, channel, workspace, partner, campaign, email, phone in rows:
        out.append({"channel": channel or "", "ws": ws_from_booking(workspace, campaign),
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

# WhatsApp PRE-IPO meetings (yellow-block row) — registry DR-9. Reads the SAME portal source with
# the SAME conventions as every other booking read (latest raw_im_bookings snapshot, email|phone
# dedup, non-deleted, keyed on the ET business `date` string — see consolidated_bookings): just
# offer='Pre-IPO' + channel='WhatsApp' instead of offer='Funding'. consolidated_bookings() is
# deliberately Funding-locked (it feeds the §1/§2/§3 meeting columns), so it cannot serve this row —
# which is why the row sat hardcoded to 0 until 2026-07-02.
def wa_preipo_meetings(date):
    """Deduped (email|phone) WhatsApp Pre-IPO bookings for `date` from the portal mirror.
    Fail-soft via the _safe() collect wrapper -> 0 (never crashes the summary block)."""
    rows = wq(f"""
      SELECT count(DISTINCT lower(coalesce(nullif(email,''), phone)))
      FROM main.raw_im_bookings
      WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
        AND offer='Pre-IPO' AND channel='WhatsApp'
        AND substr(coalesce(date,''),1,10)=DATE '{date}'::VARCHAR
        AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')
        AND coalesce(nullif(email,''), phone) <> ''""")
    return int(rows[0][0] or 0) if rows else 0

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

def webform_submissions(date):
    """Ren3's conversion metric = completed Lumara apply-now web-form fills for the ET day [Sam 06-30].
    Source = the LIVE comms Supabase `comms.lead_application` ("the money table", one row per fill),
    deduped by prospect (email|phone). Queried LIVE — NOT the warehouse mirror main.raw_comms_lead_application,
    which lags (last-loaded hours behind → 0 same-day) AND carries duplicate loads — so the current-day
    number is real, same principle as §1/§2 reading Instantly/sendivo live. Fail-soft -> 0 (a comms
    hiccup zeros only Ren3's webform count, never the render). Conn = COMMS_SUPABASE_DB_URL (.env)."""
    url = _CREDS.optional("COMMS_SUPABASE_DB_URL") if _CREDS else None
    if not url:
        print("WARN webform_submissions: COMMS_SUPABASE_DB_URL unavailable -> 0", file=sys.stderr)
        return 0
    try:
        import psycopg2
        conn = psycopg2.connect(url, connect_timeout=15)
        try:
            conn.set_session(readonly=True)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT count(DISTINCT lower(coalesce(nullif(email,''), prospect_number)))
                    FROM comms.lead_application
                    WHERE (created_at AT TIME ZONE 'America/New_York')::date = %s""", (date,))
                return int(cur.fetchone()[0] or 0)
        finally:
            conn.close()
    except Exception as e:
        print(f"WARN webform_submissions failed {date}: {e}", file=sys.stderr)
        return 0

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
    # (folds in #116). SMS delivered/failed come from main.v_sms_campaign_performance (raw sendivo
    # campaign-daily rollup) per sub-account [Jun-30 accuracy pass 07-01]: SMS delivery is NOT ~100%
    # (06-30 Ren1 ran 14% failed), so delivered=sent + Fail% "—" understated real failures. Sent stays
    # the sendivo BILLING quantity (the calibrated §2 source; the view's own sent tracks it within
    # ~0.01%). Fail-soft: no view row for a sub/day (same-day sync lag) -> legacy (delivered≈sent,
    # failed=None -> Fail% "—"), never a crash. Funding SMS meetings on Ren1; Pre-IPO on Ren2.
    # Ren3 (webform): the "meetings" column shows completed WEB-FORM FILLS (Lumara apply-now), NOT
    # meetings — Ren3 is a form-conversion line [Sam 06-30]. KPI = sent / webforms (same sent/x formula
    # as the other rows). SMS-sourced MEETINGS stay on the Ren1 row (im_bookings' SMS channel carries no
    # sub-account) — Ren3 does not double-count them. The shared column is headed "Meetings/Webform".
    # ORDER MATTERS: Ren1 must stay index 0 and Ren2 index 1 (write_summary_block reads SMS_D[0]/[1]).
    perf = {}
    try:
        for r in wq(f"""SELECT sub_account_name, sum(sent), sum(delivered), sum(failed)
                        FROM main.v_sms_campaign_performance
                        WHERE metric_date = DATE '{DAILY}' AND sub_account_name IS NOT NULL
                        GROUP BY 1"""):
            perf[r[0]] = (int(r[1] or 0), int(r[2] or 0), int(r[3] or 0))
    except Exception as e:
        print(f"WARN get_sms_wa: v_sms_campaign_performance unavailable for {DAILY} ({e}); "
              "SMS rows fall back to delivered≈sent, Fail% '—'", file=sys.stderr)
    def deliv_failed(sub_name, billed_sent):
        p = perf.get(sub_name)
        if p is None:
            if billed_sent:
                print(f"WARN get_sms_wa: no v_sms_campaign_performance row for '{sub_name}' on {DAILY} "
                      f"(billed {billed_sent}); rendering delivered≈sent, Fail% '—'", file=sys.stderr)
            return billed_sent, None
        vsent, vdeliv, vfail = p
        # Tripwire [code-review 07-01]: on a FULLY-loaded day the view's own sent tracks billing
        # within ~0.03%. A mid-load partial day would silently UNDERSTATE delivered/failed behind a
        # plausible Fail% — worse than the '—' it replaces (100%-or-wipe rule). >5% divergence ->
        # treat the view's day as partial and fall back to legacy.
        if billed_sent and abs(vsent - billed_sent) > 0.05 * billed_sent:
            print(f"WARN get_sms_wa: v_sms_campaign_performance sent={vsent} diverges >5% from billed "
                  f"{billed_sent} for '{sub_name}' on {DAILY} (partial day-load?); rendering "
                  "delivered≈sent, Fail% '—'", file=sys.stderr)
            return billed_sent, None
        return vdeliv, vfail
    # §2 Cost $ (actual) [backlog A1] = warehouse raw_sendivo_billing_daily.total_spend for the day
    # (all-in Sendivo $: SMS fees + carrier fees + setup/renewal/brand/phone fees — NOT sms_fee_usd,
    # which excluded carrier fees and understated spend ~3x).
    # Same 100%-or-wipe tripwire class as deliv_failed: the table is nightly-upserted (7d window), so
    # on an evening render the report day's row can be a PARTIAL-day snapshot — its sms_fee_qty would
    # lag the LIVE billed quantity §2 Sent already fetched. >5% qty divergence (or a missing row while
    # live billed >0) -> cost renders '—' + loud WARN and back-fills on the post-nightly re-render;
    # never a silently-understated $.
    bill = sendivo_billing_cost(DAILY)
    def cost_usd(sub_id, sub_label, billed_sent):
        row = bill.get(sub_id)
        if row is None:
            if billed_sent:
                print(f"WARN get_sms_wa: no raw_sendivo_billing_daily row for sub {sub_id} "
                      f"('{sub_label}') on {DAILY} (live billed {billed_sent}); Cost $ renders '—' "
                      "(back-fills after the nightly)", file=sys.stderr)
            return None
        qty, usd = row
        if billed_sent and abs(qty - billed_sent) > 0.05 * billed_sent:
            print(f"WARN get_sms_wa: raw_sendivo_billing_daily sms_fee_qty={qty} diverges >5% from "
                  f"live billed {billed_sent} for sub {sub_id} ('{sub_label}') on {DAILY} — partial "
                  "day-load; Cost $ renders '—' (100%-or-wipe) and back-fills after the nightly",
                  file=sys.stderr)
            return None
        return usd
    rows = []
    s1 = sms.get(SUB_REN1, 0); d1, f1 = deliv_failed("Renaissance 1", s1); rows.append(("Renaissance 1 (SMS)", s1, d1, f1, inb.get("Renaissance 1", (0, 0))[1], sms_mtg, cost_usd(SUB_REN1, "Renaissance 1", s1)))
    s2 = sms.get(SUB_REN2, 0); d2, f2 = deliv_failed("Renaissance 2", s2); rows.append(("Renaissance 2 (SMS · Pre-IPO)", s2, d2, f2, inb.get("Renaissance 2", (0, 0))[1], preipo_meetings(DAILY), cost_usd(SUB_REN2, "Renaissance 2", s2)))
    s3 = sms.get(SUB_REN3, 0); d3, f3 = deliv_failed("Renaissance 3", s3); rows.append(("Renaissance 3 (SMS · webform)", s3, d3, f3, inb.get("Renaissance 3", (0, 0))[1], webform_submissions(DAILY), cost_usd(SUB_REN3, "Renaissance 3", s3)))
    wa = wq(f"""SELECT sent, delivered, failed, replies_total FROM main.v_sms_dash_wa_daily
                WHERE channel='whatsapp' AND metric_date=DATE '{DAILY}'""")
    if wa:
        ws_, wd, wf, wr = (int(wa[0][i] or 0) for i in range(4))
    else:
        ws_, wd, wf, wr = 0, 0, 0, 0
    # WhatsApp cost = None ALWAYS: no actual WA cost feed exists (cost_usd 100% NULL) and the $0.07/msg
    # model is explicitly GATED (no modeled numbers on the report) [backlog B3/A6, Sam 07-01].
    rows.append(("WhatsApp (ISKRA)", ws_, wd, wf, wr, wa_mtg, None))
    return rows

# ---------------------------- §2b SMS KPI-to-opp ----------------------------
# The SMS "opp" (opportunity) = a POSITIVE-INTENT inbound reply, per the CURRENT-native notion:
# derived.sms_reply_is_positive_qwen.is_positive (the Qwen classifier already shipped as the warehouse's
# SMS positive-reply signal — we surface it, we do NOT build a bespoke reclassifier [Sam 2026-06-30]).
# WHY these are a TRAILING WINDOW, not the render-day: that classifier runs on a ~2-3d LAG (cron
# /root/sms-sentiment-bi/incremental.sh @09:00 UTC -> appended to the seed -> loaded on the next
# nightly), so the render day's opps are NOT yet classified at 10pm-ET report time. A partial
# classification massively UNDER-counts opps (06-29 read 4% classified -> ~10x too few opps), which
# would poison the KPI-to-opp gate — so we show the number ONLY over days that are ~fully classified
# and mark same-day as pending (the 100%-or-wipe rule, feedback_partial_data_100pct_or_wipe_20260614).
# This is exactly why §2 historically carried NO opp column ("excluded as inaccurate").
_sms_win_cache = {}
def sms_complete_window(max_days=7, cov_threshold=0.9, lookback_days=21):
    """The most-recent up-to-`max_days` SMS-reply days (<= DAILY) that are ~fully classified
    (>= cov_threshold of that day's human/non-opt-out inbound replies carry a Qwen verdict). Returns
    a sorted list of ISO date strings (may be []). Memoized; fail-soft -> []."""
    if "w" in _sms_win_cache:
        return _sms_win_cache["w"]
    dates = []
    try:
        rows = wq(f"""
          WITH cov AS (
            SELECT CAST(received_at AS DATE) d, count(*) FILTER (WHERE NOT is_opt_out) human
            FROM main.raw_sendivo_inbound
            WHERE received_at >= (DATE '{DAILY}' - INTERVAL {int(lookback_days)} DAY)
              AND CAST(received_at AS DATE) <= DATE '{DAILY}'
            GROUP BY 1),
          cls AS (
            SELECT CAST(received_at AS DATE) d, count(*) classified
            FROM derived.sms_reply_is_positive_qwen GROUP BY 1)
          SELECT cov.d
          FROM cov LEFT JOIN cls USING (d)
          WHERE cov.human > 0 AND COALESCE(cls.classified, 0) >= {float(cov_threshold)} * cov.human
          ORDER BY cov.d DESC LIMIT {int(max_days)}""")
        dates = sorted(str(r[0])[:10] for r in rows)
    except Exception as e:
        print(f"WARN sms_complete_window failed: {e}", file=sys.stderr)
    _sms_win_cache["w"] = dates
    return dates

def _sms_win_inlist():
    dates = sms_complete_window()
    return dates, (", ".join("DATE '%s'" % d for d in dates) if dates else "")

def get_sms_kpi_to_opp():
    """Per-workspace SMS sent / opps / sent-per-opp / cost / cost-per-opp over the trailing
    fully-classified window. Sent/Opps = the current-native funnel main.v_sms_workspace_funnel
    (opps = Qwen positive replies attributed to the sub-account by reply number); sent_per_opp =
    the KPI-to-opp gate. Ren3 (webform) sends route mostly via the AIM API and are NOT in the
    campaign feed -> its funnel sent undercounts -> Sent/opp unreliable there (opps still shown).
    Cost $ (window) [2026-07-01 fix] = warehouse raw_sendivo_billing_daily.total_spend summed over
    the SAME window dates per sub-account — the ALL-IN Sendivo $ (SMS fees + carrier fees + any
    setup/renewal/brand/phone fees that day), the SAME basis as §2's Cost $ (actual), just windowed.
    The previous basis (v_sms_workspace_funnel.cost_usd = campaign-feed per-message $) priced only
    the CARRIER-FEE component of campaign-feed sends (per-segment rate matches billing
    carrier_fee_usd/qty to ~2%) — it excluded the SMS fee and every AIM-API send, understating
    all-in spend ~1.6x (June audit: Ren1 $14,337 feed vs $23,097 billed) — replaced, not kept.
    Cost gate (100%-or-wipe): every window date must lie inside raw_sendivo_billing_daily's loaded
    [min,max] range; outside (or on lookup failure) cost is None -> renders '—' + WARN, never a
    partial sum. Inside the range a missing (date,sub) billing row = a genuine $0 day (Sendivo's
    billing report only emits rows for subs with activity)."""
    dates, inlist = _sms_win_inlist()
    if not inlist:
        return {"dates": [], "rows": []}
    rows = wq(f"""SELECT sub_account, sum(sent), sum(opps)
                  FROM main.v_sms_workspace_funnel
                  WHERE metric_date IN ({inlist}) AND sub_account LIKE 'Renaissance%'
                  GROUP BY 1 ORDER BY 1""")
    cost_by_ws = None   # None = cost gate tripped -> every §2b cost cell renders '—'
    try:
        rng = wq("""SELECT CAST(min(metric_date) AS VARCHAR), CAST(max(metric_date) AS VARCHAR)
                    FROM main.raw_sendivo_billing_daily""")
        lo, hi = (rng[0][0], rng[0][1]) if (rng and rng[0][0]) else (None, None)
        if not lo or dates[0] < lo or dates[-1] > hi:
            raise RuntimeError(f"window {dates[0]}..{dates[-1]} outside billing coverage {lo}..{hi}")
        sub2ws = {SUB_REN1: "Renaissance 1", SUB_REN2: "Renaissance 2", SUB_REN3: "Renaissance 3"}
        cost_by_ws = {ws: 0.0 for ws in sub2ws.values()}   # in-coverage missing rows = genuine $0
        for r in wq(f"""SELECT sub_account_id, sum(total_spend)
                        FROM main.raw_sendivo_billing_daily
                        WHERE metric_date IN ({inlist}) GROUP BY 1"""):
            ws = sub2ws.get(int(r[0] or 0))
            if ws:
                cost_by_ws[ws] = float(r[1] or 0.0)
    except Exception as e:
        print(f"WARN get_sms_kpi_to_opp billing window cost failed: {e}; §2b Cost $ renders '—'",
              file=sys.stderr)
        cost_by_ws = None
    out_rows = []
    for r in rows:
        ws = r[0]
        if cost_by_ws is not None and ws not in cost_by_ws:
            # name-drift tripwire: the funnel's sub_account label no longer matches the sub2ws map —
            # cost renders '—' (never a wrong-row $), but say so LOUD instead of failing silently.
            print(f"WARN get_sms_kpi_to_opp: funnel workspace '{ws}' has no billing cost mapping "
                  f"(sub2ws name drift?); its §2b Cost $ renders '—'", file=sys.stderr)
        out_rows.append((ws, int(r[1] or 0), int(r[2] or 0),
                         (cost_by_ws.get(ws) if cost_by_ws is not None else None)))
    return {"dates": dates, "rows": out_rows}

# (§2c campaign leaderboard REMOVED from the report 2026-07-01 per the Jun-30 accuracy pass — the
# underlying warehouse view main.v_sms_campaign_performance stays and now feeds §2 delivered/failed.)

def get_close():
    c = wq(f"""SELECT COUNT(*) dials, COUNT(DISTINCT close_lead_id) leads,
                 COUNT(*) FILTER (WHERE duration_seconds >= 60) connects
               FROM core.call WHERE (occurred_at AT TIME ZONE 'America/New_York')::DATE = DATE '{DAILY}'""")
    d, l, cn = (int(c[0][0]), int(c[0][1]), int(c[0][2])) if c else (0, 0, 0)
    book = consolidated_bookings(DAILY)
    m = sum(1 for b in book if b["channel"].lower() == "call")
    # NO MTD here [DR-10, Sam 07-01]: "we don't want MTD anything in a daily tab" — the warm-caller
    # MTD rows added 07-01 (backlog A7) were removed same-day; monthly belongs on a dedicated MTD tab.
    return {"dials": d, "leads": l, "connects": cn, "meetings": m}

def get_truth(infra_d=None):
    """§4 no-lag: Expected = active accounts' configured daily_limit by ws x infra (per-workspace
    LAST-GOOD census, Outlook excluded); Actual = Instantly daily per workspace (== §1 sent), now ALSO
    split OTD-vs-Google via account-level sends x the same last-good census (Outlook excluded from the
    split, so Actual OTD + Actual Google == Actual total minus Outlook; unmapped surfaced loud).

    Expected uses each workspace's most-recent census that actually CONTAINS it, not the global
    max(census_date). The hourly /accounts poller drops a whole workspace when Instantly 500s on its
    /accounts page (e.g. Funding 1 / renaissance-4, 2026-07-01: fleet 460k->319k, F1 vanished), which
    would zero that workspace's Expected while Actual keeps flowing -> §4 rendered F1 Expected 0 vs
    Actual 529,670. This read-side carry-forward mirrors the build-side fix in entities/account_census.py
    (#133) so §4 Expected never collapses to 0 for a still-active workspace on a partial census. When the
    census is complete this is identical to the old global-max behavior (per-ws max == global max for
    every workspace), so it is a strict safety net, not a behavior change on healthy days.

    Expected counts ONLY census-connected accounts (core.account_census.status=1) [2026-07-01, Sam]:
    a disconnected/paused inbox keeps lifecycle='Active' (ever-sent-cold is history, not state), so
    without the status predicate §4 reported capacity that physically cannot send — e.g. Renaissance 1
    on 2026-07-01: Expected 101,465 while 43,455 of it sat on 2,897 connection_error accounts (the
    cancelled-reseller wave); true connected capacity 57,950 matched the 56,472 actually sent. The
    excluded capacity is printed loud (NOTE below) so a disconnect wave is visible, not silently
    dropped. The status filter applies to the capacity SUM only — the census PICK stays presence-based
    (any Active OTD/Google label rows), so the #133 carry-forward still engages only on a genuine
    poller drop, while a workspace whose entire fleet disconnects correctly shows Expected 0 at its
    true newest census (a state event, not a capture gap) with the full wave in the NOTE. The census
    join is LEFT + COALESCE(status,1)=1, so a missing/NULL census status counts as connected — a data
    gap can only ever OVERSTATE Expected (fail toward the old behavior), never silently shrink it."""
    # `elig` = eligible (Active OTD/Google) capacity per (ws, census, infra); the filter lives INSIDE
    # the CTE so a workspace's "last-good" census is the latest one that actually HAS Active OTD/Google
    # rows — not merely the latest census it appears in (which could hold only Outlook/retired rows and
    # re-zero Expected after the outer filter).
    cap = {(r[0], r[1]): float(r[2] or 0) for r in wq(
        f"""WITH elig AS (
              SELECT al.workspace_slug, al.census_date, al.infra,
                     sum(CASE WHEN COALESCE(c.status, 1) = 1 THEN al.daily_limit END) AS cap
              FROM core.account_label al LEFT JOIN core.account_census c
                ON c.census_date=al.census_date AND c.workspace_slug=al.workspace_slug
               AND c.email=al.email
              WHERE al.workspace_slug IN ({SLUGS_SQL})
                AND al.lifecycle='Active' AND al.infra IN ('OTD','Google')
              GROUP BY 1,2,3),
            ws_latest AS (SELECT workspace_slug, max(census_date) AS census_date FROM elig GROUP BY 1)
            SELECT e.workspace_slug, e.infra, e.cap
            FROM elig e JOIN ws_latest USING (workspace_slug, census_date)""")}
    # Per-workspace census actually used (same PRESENCE-based pick as `cap`'s ws_latest) — for the
    # header + carry-forward flag.
    used = {r[0]: r[1] for r in wq(
        f"""SELECT workspace_slug, max(census_date)
            FROM core.account_label
            WHERE workspace_slug IN ({SLUGS_SQL})
              AND lifecycle='Active' AND infra IN ('OTD','Google')
            GROUP BY 1""")}
    # Capacity EXCLUDED by the status=1 sum filter at each workspace's used census — printed loud so a
    # disconnect wave shows up in the log as phantom capacity, not as a silent Expected shrink.
    disc = {r[0]: float(r[1] or 0) for r in wq(
        f"""WITH ws_latest AS (
              SELECT workspace_slug, max(census_date) AS census_date
              FROM core.account_label
              WHERE workspace_slug IN ({SLUGS_SQL})
                AND lifecycle='Active' AND infra IN ('OTD','Google')
              GROUP BY 1)
            SELECT al.workspace_slug, sum(al.daily_limit)
            FROM core.account_label al
            JOIN ws_latest w ON w.workspace_slug=al.workspace_slug AND w.census_date=al.census_date
            LEFT JOIN core.account_census c
              ON c.census_date=al.census_date AND c.workspace_slug=al.workspace_slug
             AND c.email=al.email
            WHERE al.lifecycle='Active' AND al.infra IN ('OTD','Google')
              AND COALESCE(c.status, 1) <> 1
            GROUP BY 1""")}
    if disc:
        print(f"NOTE §4 Expected excludes {sum(disc.values()):,.0f}/day configured capacity sitting on "
              f"disconnected/paused Active accounts (census status<>1): "
              + ", ".join(f"{s}={v:,.0f}" for s, v in sorted(disc.items(), key=lambda x: -x[1])),
              file=sys.stderr)
    inst = instantly_daily(DAILY)
    # Per-infra ACTUAL split (OTD vs Google) — account-level sends (core.sending_account_daily) mapped
    # to infra via EACH workspace's last-good census (the SAME per-workspace carry-forward used above
    # for Expected), so a partial global census never mis-buckets a workspace: e.g. F1/renaissance-4
    # vanished from the 2026-07-01 census, so its report-day sends map via its 06-29 label -> 0 unmapped.
    # sending_account_daily reconciles to instantly_daily per workspace (both Instantly-sourced), so
    # OTD + Google + Outlook == Actual(total); Outlook is real send volume but EXCLUDED from truth
    # (mirrors the Expected side), so the Actual OTD/Google columns sum to Actual(total) minus Outlook.
    # A sender on an account with no label row falls to 'unmapped' and is surfaced LOUD (never a silent
    # zero) — currently 0 for every reporting workspace on the last-good census.
    asplit = {}   # slug -> (otd_actual, google_actual, unmapped_actual)
    for r in wq(f"""
        WITH ws_cens AS (   -- IDENTICAL census pick to Expected's `used`/`cap` (max census that
                            -- actually HOLDS Active OTD/Google rows — PRESENCE-based, status-blind),
                            -- so the split and Expected always read a workspace at the SAME census —
                            -- never diverge on a partial-capture day whose newest census holds only
                            -- Outlook/retired rows.
              SELECT workspace_slug, max(census_date) AS cd FROM core.account_label
              WHERE workspace_slug IN ({SLUGS_SQL})
                AND lifecycle='Active' AND infra IN ('OTD','Google') GROUP BY 1),
             lbl AS (   -- ALL infra rows (incl. Outlook) at that census, to bucket every sender
              SELECT al.workspace_slug, al.email, al.infra
              FROM core.account_label al JOIN ws_cens w
                ON w.workspace_slug=al.workspace_slug AND w.cd=al.census_date),
             sad AS (
              SELECT workspace_slug, account_id, sum(actual_sends) AS a
              FROM core.sending_account_daily
              WHERE date=DATE '{DAILY}' AND workspace_slug IN ({SLUGS_SQL})
              GROUP BY 1,2)
        SELECT s.workspace_slug,
               sum(CASE WHEN l.infra='OTD'    THEN s.a ELSE 0 END),
               sum(CASE WHEN l.infra='Google' THEN s.a ELSE 0 END),
               sum(CASE WHEN l.infra IS NULL  THEN s.a ELSE 0 END)
        FROM sad s LEFT JOIN lbl l
          ON l.workspace_slug=s.workspace_slug AND l.email=s.account_id
        GROUP BY 1"""):
        asplit[r[0]] = (float(r[1] or 0), float(r[2] or 0), float(r[3] or 0))
    unmapped_ws = sorted({slug for slug, v in asplit.items() if v[2] > 0})
    if unmapped_ws:
        print(f"WARN §4 per-infra actual: sends on accounts absent from their last-good census "
              f"(unmapped>0) for {unmapped_ws} — Actual OTD/Google understate those rows until the "
              f"census heals (Actual total is unaffected).", file=sys.stderr)
    # SPLIT SOURCE WATERFALL (fix 2026-07-02, DR-1/DATA-9): the Actual OTD/Google split must NOT read
    # ONLY core.sending_account_daily. That table is a NIGHTLY (D-1) account-grain entity, so on the
    # EVENING render (day D, ~10pm ET) it has NO report-day rows and the split zeroed while Actual total
    # (live Instantly) populated — and on feed-gap/churn days recent days can be absent (2026-07-02 had
    # 0 rows; 07-01 only ~42% loaded; 06-29 missing entirely). Actual TOTAL and §1 read a LIVE Instantly
    # fetch; §1b resolves the infra split LIVE (per-campaign -> tag -> infra, same Instantly source), so
    # it is complete same-day. We therefore waterfall per workspace:
    #   1. core.sending_account_daily has a COMPLETE day-D load -> use it (canonical account grain;
    #      captures subsequence sends; survives deleted campaigns; this is what backfill re-renders heal
    #      onto). "Complete" is NOT "has any row": the nightly writes this table account-by-account, so
    #      mid-load a workspace can hold a tiny fraction of its sends (2026-07-01 was ~42% loaded), which
    #      would render e.g. OTD~5 next to Actual total 500,000. We therefore require the sad split to
    #      RECONCILE to most of the workspace's live actual total before trusting it (sad excludes Outlook
    #      so it is legitimately <= actual; a genuine partial load is orders of magnitude below any Outlook
    #      share). A partial/absent load falls through to branch 2, exactly like a churn day.
    #   2. else the live §1b infra split (infra_d.by_ws) if it resolved >0 -> reconciles to Actual total by
    #      construction (shared instantly_daily source).
    #   3. else UNRESOLVED (None -> rendered "—") — NEVER a fake 0 [partial_data_100pct_or_wipe].
    def _sad_complete(slug):
        v = asplit.get(slug)
        if not v:
            return False
        split_total = v[0] + v[1] + v[2]
        act = inst.get(slug, (0, 0))[0] or 0
        return split_total > 0 and (act == 0 or split_total >= 0.5 * act)
    live = {}   # slug -> (otd_live, goog_live) from the §1b live per-infra resolution
    for _s, _n, agg, *_rest in (infra_d or {}).get("by_ws", []):
        live[_s] = (float(agg.get("OTD", [0, 0, 0, 0])[0]), float(agg.get("Google", [0, 0, 0, 0])[0]))
    out = []
    used_live = []
    for slug, name in WS:
        otd = cap.get((slug, "OTD"), 0.0); goog = cap.get((slug, "Google"), 0.0)
        actual = inst.get(slug, (0, 0))[0]
        if _sad_complete(slug):
            otd_a, goog_a, _un = asplit.get(slug, (0.0, 0.0, 0.0))
        elif slug in live and (live[slug][0] or live[slug][1]):
            otd_a, goog_a = live[slug]; used_live.append(name)   # live §1b split resolved >0
        else:
            # sending_account_daily has no usable day-D load AND §1b resolved neither infra (e.g. Ren1/Warm,
            # whose sends don't tag-classify to OTD/Google) -> UNRESOLVED "—", never a fake 0. Heals to
            # the real account-grain split on the next backfill re-render.
            otd_a, goog_a = None, None
        out.append((name, otd, goog, otd + goog, actual, otd_a, goog_a))
    if used_live:
        print(f"NOTE §4 per-infra actual: no usable report-day ({DAILY}) account split (absent or partial "
              f"nightly load) for {used_live} — used the LIVE §1b infra split (reconciles to §1); the "
              f"account-grain split heals on the next backfill re-render.", file=sys.stderr)
    census = max(used.values()) if used else None
    stale = sorted({str(d) for d in used.values() if d != census})
    if stale:
        print(f"WARN §4 census carry-forward: some workspaces on older census {stale} "
              f"(latest={census}) — Instantly /accounts poll dropped them; using last-good.",
              file=sys.stderr)
    return census, out

def get_partner():
    """§5 BOOKINGS BY PARTNER = ALL bookings for the day, EVERY time [Sam 2026-07-01]. It is the
    partner lens that must equal the portal's offer-agnostic "BY PARTNER" view (what Grace reads).
    Sources, in priority order:
      1. the portal `main.raw_im_bookings` — ALL offers (Funding + Pre-IPO), per-partner email|phone
         dedup. This is the primary/authoritative source and covers every channel going forward.
      2. the Pre-IPO desk SHEETS (Collins/Summit) — used ONLY for a desk the portal does not yet
         carry that day. The Pre-IPO desks migrated INTO the portal on 2026-06-30 (offer='Pre-IPO');
         BEFORE that (e.g. 06-29) they lived only in the Google sheets, so a portal-only §5 would miss
         them. Once the portal carries a desk it is authoritative (06-30: portal Collins 18 / Summit 10
         reconciled to the sheets exactly), so we do NOT also add the sheet -> no double-count.
    NB on the 06-29 source switch: the Funding bookings that were in the retired Business-Funding form
    sheet were already imported into the portal that day (06-29 portal = 123), so they need no separate
    union here; only the Pre-IPO desks (still sheet-only on 06-29) do. consolidated_bookings() stays
    offer='Funding'-locked for the §1/§2/§3 MEETING columns; §5 must NOT reuse it. Dedup one booking
    per person WITHIN each partner (email|phone); do NOT cross-dedup across partners (desks are additive).
    Null-key rows (no email AND no phone) drop, same as consolidated_bookings."""
    try:
        rows = wq(f"""
          SELECT coalesce(nullif(trim(partner), ''), '(unknown)')         AS partner,
                 count(DISTINCT lower(coalesce(nullif(email, ''), phone))) AS n
          FROM main.raw_im_bookings
          WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
            AND substr(coalesce(date, ''), 1, 10)=DATE '{DAILY}'::VARCHAR
            AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')
          GROUP BY 1""")
    except Exception as e:
        print(f"WARN get_partner im_bookings read failed {DAILY}: {e}", file=sys.stderr)
        rows = []
    # int(n) in the filter too: the query API can serialize a BIGINT count as a JSON string, and a
    # truthy "0" would otherwise leak a spurious '(unknown) 0' row.
    cnt = {p: int(n) for p, n in rows if int(n)}
    # Union the Pre-IPO desk sheets, but ONLY for a desk the portal doesn't already carry this day
    # (else 06-30+ would double-count the desks the portal now owns). preipo_by_desk is cached, so
    # this reuses the read §2 already did. Fail-soft: a sheet hiccup never breaks §5. Presence check is
    # case/whitespace-insensitive so a desk vs partner casing mismatch can't leak a duplicate row.
    present = {str(k).strip().casefold() for k in cnt}
    try:
        for desk, dn in preipo_by_desk(DAILY)["by_desk"].items():
            if int(dn) and str(desk).strip().casefold() not in present:
                cnt[desk] = int(dn)
    except Exception as e:
        print(f"WARN get_partner preipo-desk union failed {DAILY}: {e}", file=sys.stderr)
    # Secondary sort key = partner name so tied counts render deterministically.
    pr = sorted(cnt.items(), key=lambda x: (-x[1], x[0]))
    return pr, sum(n for _, n in pr)

# §6 CLOCK MIGRATED [DR-7, 2026-07-03]: the business-minute clamp (_biz_minutes) + clock-open
# bucket (_clock_open_date) now live in the WAREHOUSE — scripts/build_sla_reply_time.py materializes
# core.sla_reply_time.biz_latency_minutes + .clock_open_date (the SAME validated §6 clamp, PR #151).
# get_im_reply() below READS those columns (seq_in_thread=1) instead of recomputing, so the warehouse
# and this report share ONE definition and can never drift. Verified equal to the digit at cutover.

_IMREPLY_WARN = None  # (frontier_date, n_missing_window_biz_days) set by get_im_reply; read by imreply_table

def get_im_reply():
    """§6 IM reply-time — reads the CANONICAL warehouse fact core.sla_reply_time (DR-7, 2026-07-03).
    Average/median BUSINESS MINUTES from each thread's FIRST prospect reply to our first reply, per
    workspace, trailing-7d weekly. (Daily/MTD blocks removed — #180/DR-10; §6 is WEEKLY-only.)

    ONE DEFINITION: the business-minute clamp + clock-open bucket used to be computed inline here in
    Python; DR-7 migrated them into the warehouse. scripts/build_sla_reply_time.py materializes
    core.sla_reply_time.biz_latency_minutes (the 12:00-20:00 ET Mon-Fri clock, DST-correct) and
    .clock_open_date (the ET date the SLA clock opens) using the IDENTICAL validated §6 clamp (PR
    #151). This function now READS those columns (seq_in_thread=1 = the thread's first prospect reply),
    so a warehouse consumer and the report can never diverge. Reconciled to the digit at cutover
    (deliverables/2026-07-02-sla-scrutiny/FINDINGS.md). All first prospect replies count (an off-hours
    arrival's clock opens at the next window); unanswered threads carry NULL latency and are excluded.

    FRESHNESS: core.sla_reply_time is nightly-rebuilt from core.email_message (itself D-1-synced), so
    the weekly window back-fills as the SYNC-7 email-reply drain advances — the #177 WARN below flags
    a window still eroded by the drain. Do NOT 'fix' that by switching sources.

    FAIL-SAFE: any read error (e.g. the fact not yet promoted, or empty) raises -> _safe renders §6
    empty + WARN, never a wrong-but-plausible number (same protection as the old truncation guard)."""
    wk = (_d - datetime.timedelta(days=6)).isoformat()
    day0, wk0 = _d, datetime.date.fromisoformat(wk)
    # Read 4 days before wk0 so the sync frontier is detectable even when the WHOLE 7d window is
    # eroded (the #177 WARN needs to see a frontier that can sit below wk0). We bucket on the
    # materialized clock_open_date, so filtering on it is exact — the fact already carries the
    # first-reply (seq=1) grain and the workspace-scoped pairing (no cross-workspace clock-close).
    lo = (wk0 - datetime.timedelta(days=4)).isoformat()
    rows = wq(f"""
      SELECT workspace_slug AS ws, clock_open_date AS d, biz_latency_minutes AS lat
      FROM core.sla_reply_time
      WHERE seq_in_thread = 1
        AND biz_latency_minutes IS NOT NULL
        AND clock_open_date >= DATE '{lo}'
        AND clock_open_date <= DATE '{day0}'
        AND workspace_slug IN ({SLUGS_SQL})""")
    lats = {}  # ws -> {"d"/"w": [business-minute latencies]}
    frontier = None  # max clock-open date that carries ANY answered pair = the sync frontier
    for ws, d_val, lat in rows:
        if lat is None:
            continue  # belt+braces (SQL already filters answered)
        d = d_val if isinstance(d_val, datetime.date) else datetime.date.fromisoformat(str(d_val)[:10])
        if d > day0:
            continue  # clock opens after the report day
        if frontier is None or d > frontier:
            frontier = d
        b = lats.setdefault(ws, {"d": [], "w": []})
        if d == day0: b["d"].append(float(lat))
        if d >= wk0:  b["w"].append(float(lat))
    # WEEKLY-INCOMPLETENESS GUARD (2026-07-02): the weekly block silently HALVES when the
    # core.email_message sync (SYNC-7 drain) lags into the 7d window — a pair needs BOTH the
    # inbound first-reply (ue_type=2) AND our reply (ue_type=3), so any window business-day past
    # the sync frontier contributes ZERO pairs, deflating n and reshuffling the median toward
    # whatever older days survived (measured 07-02: R1 262→129, median 28→0 purely from dropping
    # a synced high-volume day while the 3 newest window-days were unsynced). The old empty-state
    # note below only inspects the DAILY column, so it NEVER caught this — a materially-incomplete
    # weekly rendered as if it were the signal. Count how many window business-days sit past the
    # frontier; ≥2 empty days = the weekly is eroded (1 empty = the normal D-1 daily lag, weekly
    # still intact). Surfaced as a WARN row so the number is never taken at face value.
    global _IMREPLY_WARN
    _IMREPLY_WARN = None
    if frontier is not None:
        missing = [dd for dd in (wk0 + datetime.timedelta(n) for n in range((day0 - wk0).days + 1))
                   if dd.isoweekday() <= 5 and dd > frontier]
        if len(missing) >= 2:
            _IMREPLY_WARN = (frontier, len(missing))
    def _stats(v):
        return (len(v), statistics.median(v) if v else None, sum(v) / len(v) if v else None)
    out = []
    for slug, name in WS:
        b = lats.get(slug, {"d": [], "w": []})
        out.append((name,) + _stats(b["d"]) + _stats(b["w"]))
    return (DAILY, wk), out

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

# ---------------------------- §1b: KPIs by infra type (Google / OTD / Milkbox) ----------------------------
# Infra is the campaign's SENDING TAG (email_tag_list), NEVER the campaign name. The sending tag is the
# literal infra determinant (it gates which sending-account pool the campaign uses) and is a bounded
# per-campaign field on GET /campaigns -- so it avoids the runaway /custom-tag-mappings edge-list pull
# (the all-history account-tag pull that #122 had to cap). Map: 'Reseller Active'->Google,
# 'Outreach Today Active'->OTD, 'Milkbox Active*'->Milkbox, no infra tag->other, >=2 infra tags->FLAG.
INFRA_ORDER = ["Google", "OTD", "Milkbox", "other"]   # classifier buckets (_infra_of) — unchanged
# The §1b RENDERED rows — THE single sync point for the sheet row set, the DRY print, the TOTAL
# label and the §1b header. Milkbox REMOVED [Sam 2026-07-01]: its attribution renders a wrong 0
# today, and per the 100%-or-wipe rule a wrong row comes OFF the report until the centralize-pass
# rebuild. Restore = add it back HERE (labels/total/header all derive). The classifier still buckets
# Milkbox; nonzero Milkbox sends WARN loud in infra_table — never silently swallowed.
INFRA_RENDER_ROWS = ["Google", "OTD"]
INFRA_RENDER_LABEL = {"Google": "Google (reseller)", "OTD": "OTD", "Milkbox": "Milkbox"}

def _infra_of(tag_labels):
    """campaign sending-tag labels -> ('Google'|'OTD'|'Milkbox'|'other', flagged_tags_or_None)."""
    hits = []
    for L in tag_labels:
        if not L:
            continue
        if "Reseller" in L:                hits.append("Google")
        elif "Outreach Today" in L:        hits.append("OTD")
        elif L.startswith("Milkbox Active"): hits.append("Milkbox")
    hits = list(dict.fromkeys(hits))
    if not hits:
        return "other", None
    if len(hits) > 1:
        return hits[0], hits           # >=2 infra tags -> first wins, but flag it (never double-count)
    return hits[0], None

def _campaign_day_metrics(client, campaign_id, date):
    """one campaign's metrics for `date` from /campaigns/analytics/daily (single-day -> _unique fields
    are valid; the 'do not sum _unique across days' caveat only bites multi-day sums). Returns
    (sent, opp, human_replies, auto_replies).

    Routed through InstantlyClient._get (NOT raw httpx) so it inherits the browser User-Agent that
    Instantly's Cloudflare WAF requires (default python-httpx UA is 403'd -- troubleshooting note
    2026-06-08) AND the 429/5xx-aware backoff. httpx.Client is thread-safe, so one client per
    workspace shared across the pool is fine.

    SEMANTICS (verified live + matches the warehouse convention in build_campaign_daily.py:10-11):
    `unique_replies` is the HUMAN reply count (already EXCLUDES automatic), `unique_replies_automatic`
    is the SEPARATE automatic (OOO/auto-responder) count -- they are NOT subset/superset (live F4 Google
    showed human=1447 < auto=2970, impossible if auto were a subset of a 'total'). So total replies =
    human + auto. The handoff's stated 'RR=reply_count/sent, HumanRR=(reply_count-reply_count_automatic)
    /sent' assumed reply_count was the all-in total; the API does the opposite, so we compute
    RR(total)=(human+auto)/sent and HumanRR=human/sent (same INTENT, correct against the real fields)."""
    payload = client._get("/campaigns/analytics/daily",
                          params={"campaign_id": campaign_id, "start_date": date, "end_date": date})
    rows = payload if isinstance(payload, list) else (payload.get("items") if isinstance(payload, dict) else []) or []
    day = next((x for x in rows if str(x.get("date", ""))[:10] == date), None)
    if not day:
        return (0, 0, 0, 0)
    return (int(day.get("sent") or 0), int(day.get("unique_opportunities") or 0),
            int(day.get("unique_replies") or 0), int(day.get("unique_replies_automatic") or 0))

def get_infra_kpis():
    """Per (infra x workspace) KPIs for the report day, summed over that infra's campaigns (campaign
    grain). RR/HumanRR = % of sent; PositiveRR = % of total replies (opp/(human+auto)); Email->opp / Email->meeting = emails-per-X ratios.
    Sends are reconciled to §1: each workspace's infra rows + an 'Unattributed (subseq/other)' row
    sum to the §1 workspace sent (the §1-vs-Σcampaign gap is subsequence sends, which inherit the
    parent's infra but are not rolled into the per-campaign analytics endpoint; folding them in would
    cost ~1.7k calls/night, so we surface the residual as an honest reconciliation row -- the handoff's
    'other'/'unattributed' bucket). Meetings come from im_bookings.campaign -> Instantly campaign ->
    infra; most bookings are SMS/script/portal rows with no email campaign -> 'unattributed' (expected).
    """
    if not (_CREDS and InstantlyClient):
        raise RuntimeError("credentials/InstantlyClient unavailable")
    keys = _CREDS.instantly_workspace_keys()
    inst_ws = instantly_daily(DAILY)  # §1 per-workspace totals (sent) -- memoized, reused for reconcile
    name2infra = {}                   # lower(instantly campaign name) -> (slug, infra) for meeting attribution
    flagged = []                      # campaigns carrying >=2 infra tags
    by_ws = []                        # [(slug, name, {infra: [sent,opp,human,auto]}, ws_total_sent, unattr_sent, n_failed)]
    for slug, name in WS:
        key = keys.get(slug)
        if not key:
            continue
        client = InstantlyClient(key)
        tags = {t["id"]: (t.get("label") or t.get("name")) for t in client.list_tags()}
        camps = list(client.list_campaigns())
        cinf = {}  # campaign_id -> infra
        for cm in camps:
            cid = cm.get("id")
            labels = [tags.get(t, "") for t in (cm.get("email_tag_list") or [])]
            infra, flag = _infra_of(labels)
            cinf[cid] = infra
            if flag:
                flagged.append((cm.get("name"), flag, name))
            cn = (cm.get("name") or "").strip().lower()
            if cn:
                name2infra[cn] = (slug, infra)
        agg = {inf: [0, 0, 0, 0] for inf in INFRA_ORDER}
        n_failed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_campaign_day_metrics, client, cm.get("id"), DAILY): cm.get("id") for cm in camps}
            for f in concurrent.futures.as_completed(futs):
                cid = futs[f]
                try:
                    s, o, rt, ra = f.result()
                except Exception as e:
                    n_failed += 1
                    print(f"WARN infra daily {slug} {cid}: {e}", file=sys.stderr); continue
                a = agg[cinf.get(cid, "other")]
                a[0] += s; a[1] += o; a[2] += rt; a[3] += ra
        # A campaign whose fetch failed (3 retries exhausted) contributes 0 sent -> its real sends fall
        # into unattr_sent below (still reconciles to §1) but get MIS-labelled as subseq/other. Carry
        # n_failed so the Unattributed row can flag it instead of reading as a silent subsequence surge.
        ws_total_sent = inst_ws.get(slug, (0, 0))[0]
        attr_sent = sum(a[0] for a in agg.values())
        unattr_sent = max(0, ws_total_sent - attr_sent)
        by_ws.append((slug, name, agg, ws_total_sent, unattr_sent, n_failed))

    # meetings by infra: im_bookings.campaign (Funding, report day) -> instantly campaign -> infra
    mtg_by = collections.defaultdict(int)   # (slug, infra) -> meetings
    unattr_mtg = 0
    try:
        # Dedup by email|phone (one meeting per person), same as imbookings_meetings / consolidated_bookings,
        # so §1b's meeting counts can't disagree with §1/§5 by double-counting a person across bookings.
        # A person with bookings under >1 campaign is collapsed to one (max(campaign)) -> attributed once.
        rows = wq(f"""
          WITH b AS (
            SELECT lower(coalesce(nullif(email,''), phone)) AS k,
                   max(lower(trim(coalesce(campaign,'')))) AS c
            FROM main.raw_im_bookings
            WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
              AND offer='Funding' AND substr(coalesce(date,''),1,10)=DATE '{DAILY}'::VARCHAR
              AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')
            GROUP BY 1)
          SELECT c, count(*) n FROM b WHERE k IS NOT NULL AND k<>'' GROUP BY 1""")
        for c, n in rows:
            hit = name2infra.get(c)
            if hit:
                mtg_by[(hit[0], hit[1])] += int(n)
            else:
                unattr_mtg += int(n)
    except Exception as e:
        print(f"WARN infra meetings failed {DAILY}: {e}", file=sys.stderr)
    return {"by_ws": by_ws, "mtg_by": dict(mtg_by), "unattr_mtg": unattr_mtg, "flagged": flagged}

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
_SMS_FB = [("Renaissance 1 (SMS)", 0, 0, None, 0, 0, None), ("Renaissance 2 (SMS · Pre-IPO)", 0, 0, None, 0, 0, None), ("Renaissance 3 (SMS · webform)", 0, 0, None, 0, 0, None), ("WhatsApp (ISKRA)", 0, 0, 0, 0, 0, None)]
_CLOSE_FB = {"dials": 0, "leads": 0, "connects": 0, "meetings": 0}
_TRUTH_FB = (None, [(name, 0, 0, 0, 0, None, None) for _, name in WS])  # split None -> "—", never fake 0
_IMREPLY_FB = ((DAILY, DAILY), [(name, 0, None, None, 0, None, None) for _, name in WS])

EMAIL_D = _safe("email", get_email, _EMAIL_FB)
SMS_D = _safe("sms_wa", get_sms_wa, _SMS_FB)
SMS_KPI_D = _safe("sms_kpi_to_opp", get_sms_kpi_to_opp, {"dates": [], "rows": []})
CLOSE_D = _safe("close", get_close, _CLOSE_FB)
# §1b infra resolution is collected BEFORE §4 so §4's Actual OTD/Google split can fall back to the LIVE
# per-infra numbers when core.sending_account_daily has no report-day rows (see get_truth waterfall).
# Both memoize instantly_daily, so this only reorders — no extra Instantly cost. If §1b fails, INFRA_D
# is the empty fallback and §4 degrades to sending_account_daily / unresolved (—), never a fake 0.
INFRA_D = _safe("infra", get_infra_kpis, {"by_ws": [], "mtg_by": {}, "unattr_mtg": 0, "flagged": []})
SENDING_CENSUS, SENDING_TRUTH = _safe("truth", lambda: get_truth(INFRA_D), _TRUTH_FB)
PARTNER_D, PARTNER_D_TOTAL = _safe("partner", get_partner, ([], 0))
IMREPLY_PERIODS, IMREPLY_D = _safe("imreply", get_im_reply, _IMREPLY_FB)
PREIPO_MTG = _safe("preipo", lambda: preipo_meetings(DAILY), 0)
WA_PREIPO_MTG = _safe("wa_preipo", lambda: wa_preipo_meetings(DAILY), 0)

# Milkbox row is wiped from §1b (see INFRA_RENDER_ROWS) — if the classifier still attributed
# volume to it, WARN LOUD here, in BOTH --dry and write paths (never silently swallow). Covers
# sends AND meetings: meetings lag sends by days, so a send-only guard would miss a booking
# landing after its Milkbox campaign went quiet.
_mb_sent = sum(a.get("Milkbox", [0, 0, 0, 0])[0] for _s, _n, a, *_ in INFRA_D.get("by_ws", []))
_mb_mtg = sum(n for (_s, _inf), n in INFRA_D.get("mtg_by", {}).items() if _inf == "Milkbox")
if _mb_sent or _mb_mtg:
    print(f"WARN §1b: Milkbox-classified volume on {DAILY} is NOT rendered (row wiped 2026-07-01 "
          f"pending rebuild): sends={_mb_sent}, meetings={_mb_mtg} — still in §1/§4/§5 totals.",
          file=sys.stderr)

if DRY:
    print(f"REPORT_DATE={DAILY}  TAB={DAILY_TAB}  census(§4)={SENDING_CENSUS}  periods(§6)={IMREPLY_PERIODS}")
    if INFRA_D.get("by_ws"):
        _ia = {inf: [0,0,0,0] for inf in INFRA_RENDER_ROWS}; _im = {inf: 0 for inf in INFRA_RENDER_ROWS}
        for _s,_n,_a,_t,_u,_f in INFRA_D["by_ws"]:
            for inf in _ia:
                for i in range(4): _ia[inf][i] += _a.get(inf,[0,0,0,0])[i]
        for (_sl,inf),n in INFRA_D["mtg_by"].items():
            if inf in _im: _im[inf] += n
        print("§1b INFRA (sent,opp,human,auto | mtg):"); [print("  ", inf, _ia[inf], "|", _im[inf]) for inf in _ia]
    print("§1 EMAIL:");    [print("  ", x) for x in EMAIL_D]
    print("§2 SMS/WA:");   [print("  ", x) for x in SMS_D]
    print(f"§2b SMS KPI-to-opp (window {SMS_KPI_D.get('dates')}):"); [print("   ", r, "sent/opp=", (round(r[1]/r[2]) if r[1] and r[2] else '—')) for r in SMS_KPI_D.get("rows", [])]
    print("§3 CLOSE:", CLOSE_D)
    print("§4 TRUTH (ws, otd_exp, goog_exp, tot_exp, actual, otd_actual, goog_actual):"); [print("  ", x) for x in SENDING_TRUTH]
    print("§5 PARTNER:", PARTNER_D, "total", PARTNER_D_TOTAL)
    print("§6 IM-REPLY (ws, d_n,d_med,d_avg, w_n,w_med,w_avg):"); [print("  ", x) for x in IMREPLY_D]
    print("Pre-IPO meetings:", PREIPO_MTG)
    print("WhatsApp Pre-IPO meetings (yellow block):", WA_PREIPO_MTG)
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
    rows = []; sec = []; th = []; tot = []; rrrows = []; merges = []; strows = []; data = []; infrows = []
    usdcells = []   # (row_idx, col_start, col_end) -> CURRENCY "$#,##0.00" (§2 / §2b cost columns)
    th_ncol = {}; row_ncol = {}
    def add(r=None): rows.append(r or []); return len(rows) - 1
    # Opp→mtg % [backlog A8 / opp→meeting conversion]: meetings ÷ opportunities in the SAME same-day
    # frame §1 already uses (Instantly opps × im_bookings email meetings). APPENDED as the 10th column
    # (col J — the last column inside the A1:J health window) so the Cheap/Regular merge indices
    # (5-7, 7-9) stay untouched. '—' when opps=0 (never '0%' over an empty denominator).
    EMAIL_HDR_TOP = ["Workspace", "Sent", "Opportunities", "Meetings", "KPI (sent/mtg)", "Cheap", "", "Regular", "", "Opp→mtg %"]
    SUB_HDR = ["", "", "", "", "", "#", "%", "#", "%", ""]
    def email_table(data_rows, header_label):
        W = 10
        def o2m(m, opps): return pctstr(m, opps) if opps else "—"
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr_top = add(EMAIL_HDR_TOP); th.append(hr_top); hr_sub = add(SUB_HDR); th.append(hr_sub)
        merges.append((hr_top, 5, 7)); merges.append((hr_top, 7, 9)); th_ncol[hr_top] = th_ncol[hr_sub] = W
        ts = topp = tm = tc_ = tr_ = 0
        for ws, sent, hr, opps, m, c, r in data_rows:
            ri = add([ws, sent, opps, m, kpi(sent, m), c, pctstr(c, m), r, pctstr(r, m), o2m(m, opps)]); data.append(ri); row_ncol[ri] = W
            ts += sent; topp += opps; tm += m; tc_ += c; tr_ += r
        ti = add(["TOTAL", ts, topp, tm, kpi(ts, tm), tc_, pctstr(tc_, tm), tr_, pctstr(tr_, tm), o2m(tm, topp)]); tot.append(ti); row_ncol[ti] = W
    def sms_wa_table(rows_data, header_label):
        # WhatsApp shows DELIVERED + FAIL% (ISKRA "sent" runs ~30-37% failed -> attempted misleads);
        # KPI = delivered/mtg. SMS: failed=None -> Fail% "—", delivered≈sent. (folds in #116)
        # Cost columns [backlog A1/A3/A4, 2026-07-01]: Cost $ (actual) = the day's Sendivo BILLING
        # total_spend from warehouse raw_sendivo_billing_daily (ALL-IN $: SMS fees + carrier fees +
        # any setup/renewal/brand/phone fees that day); Cost/mtg·form = that cost ÷ the SAME row's
        # Meetings/Webform value (Ren1/Ren2: meetings; Ren3: web-form fills) — '—' when the unit
        # count is 0 (never divide-by-zero) or when cost is None (missing/partial billing row, already
        # WARNed loud in get_sms_wa). WhatsApp cost is ALWAYS '—' (no actual feed; $0.07 model gated).
        # TOTAL cost renders ONLY when every SMS row has a cost (partial sum would silently
        # understate — 100%-or-wipe); TOTAL Cost/mtg·form stays '—' (meetings+webforms don't blend).
        W = 9
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Channel / workspace", "Sent", "Delivered", "Fail %", "Human RR", "Meetings/Webform", "KPI", "Cost $ (actual)", "Cost / mtg·form"]); th.append(hr); th_ncol[hr] = W
        def failpct(failed, sent): return (f"'{round(100.0*failed/sent)}%" if sent else "'0%") if failed is not None else "—"
        def costcell(c): return round(c, 2) if c is not None else "—"
        def costper(c, m): return round(c / m, 2) if (c is not None and m) else "—"
        ts = td = tf = thr = tm = 0; any_fail = False
        sms_costs = []   # cost of every NON-WhatsApp row (None = missing) -> gates the TOTAL
        for label, sent, deliv, failed, reps, m, cost in rows_data:
            ri = add([label, sent, deliv, failpct(failed, sent), rr(reps, deliv), m, kpi(deliv, m),
                      costcell(cost), costper(cost, m)]); rrrows.append(ri); data.append(ri); row_ncol[ri] = W
            usdcells.append((ri, 7, 9))
            ts += sent; td += deliv; thr += reps; tm += m
            if failed is not None: tf += failed; any_fail = True
            if "WhatsApp" not in label: sms_costs.append(cost)
        tcost = round(sum(sms_costs), 2) if (sms_costs and all(c is not None for c in sms_costs)) else "—"
        ti = add(["TOTAL", ts, td, (f"'{round(100.0*tf/ts)}%" if (any_fail and ts) else "—"), rr(thr, td), tm, kpi(td, tm), tcost, "—"]); tot.append(ti); rrrows.append(ti); row_ncol[ti] = W
        usdcells.append((ti, 7, 9))
        ni = add(["Cost $ (actual) = ALL-IN Sendivo spend for the day (warehouse raw_sendivo_billing_daily total_spend: SMS fees + carrier fees + any setup/renewal/brand/phone fees that day; carrier fees bill per segment, so carrier-fee qty > message count; WhatsApp has no cost feed). Cost/mtg·form = cost ÷ that row's meetings (Ren3: web-form fills). '—' cost = no fully-loaded billing row yet (back-fills after the nightly)."])
        data.append(ni); row_ncol[ni] = W
    def sms_kpi_table(kpi_d, header_label):
        # §2b — SMS KPI-to-opp (texts per opportunity) per workspace over the trailing fully-classified
        # window. opp = Qwen positive-intent reply (current-native). Same-day opps lag ~2-3d -> shown as a
        # trailing window, NOT the render day (see get_sms_kpi_to_opp / feedback_partial_data_100pct_or_wipe).
        # Cost $ (window) / Cost/opp [backlog A2; basis fixed 2026-07-01]: billing total_spend (all-in
        # Sendivo $, same basis as §2) summed over the SAME window dates ÷ the SAME Opps column —
        # window-consistent by construction. cost None (coverage gate tripped) -> '—'; Cost/opp '—'
        # when opps=0. TOTAL cost only when every row has one (partial sum understates; 100%-or-wipe).
        W = 6
        LBL = {"Renaissance 1": "Renaissance 1 (Funding SMS)", "Renaissance 2": "Renaissance 2 (Pre-IPO SMS)",
               "Renaissance 3": "Renaissance 3 (webform SMS)"}
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Workspace", "Sent", "Opps", "Sent/opp", "Cost $ (window)", "Cost/opp"]); th.append(hr); th_ncol[hr] = W
        d = kpi_d.get("dates") or []
        if not kpi_d.get("rows"):
            ni = add(["(pending — no fully-classified SMS-reply day in the trailing window; the Qwen positive-intent classifier lags ~2-3d so same-day opps aren't ready)"]); data.append(ni); row_ncol[ni] = W
            return
        ts = topp = 0; costs = []
        for ws, sent, opps, cost in kpi_d["rows"]:
            spo = round(sent / opps) if (opps and sent) else "—"
            ccell = round(cost, 2) if cost is not None else "—"
            cpo = round(cost / opps, 2) if (cost is not None and opps) else "—"
            ri = add([LBL.get(ws, ws), sent, opps, spo, ccell, cpo]); data.append(ri); row_ncol[ri] = W
            usdcells.append((ri, 4, 6))
            ts += sent; topp += opps; costs.append(cost)
        tcost = round(sum(costs), 2) if (costs and all(c is not None for c in costs)) else None
        tspo = round(ts / topp) if (ts and topp) else "—"
        tcpo = round(tcost / topp, 2) if (tcost is not None and topp) else "—"
        ti = add(["TOTAL (SMS)", ts, topp, tspo, (tcost if tcost is not None else "—"), tcpo]); tot.append(ti); row_ncol[ti] = W
        usdcells.append((ti, 4, 6))
        note = (f"opp = positive-intent inbound reply (Qwen classifier — the current-native SMS opp). "
                f"Sent/opp = the KPI-to-opp gate (lower = better). Window = {len(d)} fully-classified day(s) "
                f"{d[0]}..{d[-1]} (≥90% of that day's human replies classified); same-day opps lag ~2-3d so are "
                f"not shown live. Ren3 webform sends route via the AIM API (not in the campaign feed) -> its Sent "
                f"undercounts, Sent/opp unreliable. Cost $ (window) = ALL-IN Sendivo spend over the SAME window "
                f"(warehouse raw_sendivo_billing_daily total_spend: SMS fees + carrier fees + any setup/renewal/"
                f"brand/phone fees; carrier fees bill per segment) — the same basis as §2's Cost $ (actual), "
                f"windowed; Cost/opp = window cost ÷ window opps.")
        ni = add([note]); data.append(ni); row_ncol[ni] = W
    def truth_table(header_label):
        W = 8
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Workspace", "Expected OTD", "Expected Google", "Expected Total",
                  "Actual OTD", "Actual Google", "Actual (total)", "Fulfillment %"])
        th.append(hr); th_ncol[hr] = W
        # otd_a/goog_a may be None (split UNRESOLVED for that workspace on that day) -> render "—", never
        # a fake 0 [partial_data_100pct_or_wipe]; the TOTAL sums only resolved rows and shows "—" if NONE
        # resolved (all-unresolved day) rather than a misleading 0.
        _cell = lambda v: "—" if v is None else round(v)
        oe = ge = te = ta = oa = ga = 0
        any_split = False
        for ws, otd, goog, totexp, actual, otd_a, goog_a in SENDING_TRUTH:
            ri = add([ws, round(otd), round(goog), round(totexp),
                      _cell(otd_a), _cell(goog_a), actual, fpct(actual, totexp)])
            data.append(ri); strows.append(ri); row_ncol[ri] = W
            oe += otd; ge += goog; te += totexp; ta += actual
            oa += otd_a or 0; ga += goog_a or 0
            any_split = any_split or otd_a is not None or goog_a is not None
        sti = add(["TOTAL", round(oe), round(ge), round(te),
                   (round(oa) if any_split else "—"), (round(ga) if any_split else "—"),
                   ta, fpct(ta, te)]); tot.append(sti); strows.append(sti); row_ncol[sti] = W
    def close_table(close, label):
        W = 2
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Close CRM — metric", "Value"]); th.append(hr); th_ncol[hr] = W
        d2m = f"'{(100.0*close['meetings']/close['dials']):.2f}%" if close['dials'] else "—"
        # DAILY metrics only [DR-10, Sam 07-01]: no MTD rows on a daily tab.
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
        # WEEKLY-7d ONLY [Sam 2026-07-02]: the Daily (trailing) block is REMOVED. §6's source
        # (core.email_message) is nightly-fed (D-1 at best), so day-D's daily cell is STRUCTURALLY empty
        # on day D — an always-empty block that reads as noise. The weekly-7d window carries the signal.
        # (The Monthly/MTD block was already dropped [DR-10, Sam 07-01] — no MTD on a daily tab.)
        W = 4
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Workspace", "Weekly (7d) — n", "Median min", "Avg min"]); th.append(hr); th_ncol[hr] = W
        for name, dn, dmed, davg, wn, wmed, wavg in rows_data:
            ri = add([name, wn, mins(wmed), mins(wavg)])
            data.append(ri); row_ncol[ri] = W
        # Empty-state: the weekly window should essentially always carry pairs; if it's empty the pull
        # FAILED (_safe fallback) or returned nothing — say THAT (the old benign D-1 lag line only ever
        # explained the now-removed daily cell, so it's gone with the daily block).
        if not any((r[4] or 0) for r in rows_data):
            note = add(["(!) §6 rendered EMPTY for the trailing week — the warehouse pull failed or "
                        "returned no first-reply pairs at all (see render stderr)"])
            data.append(note); row_ncol[note] = W
        # WEEKLY-INCOMPLETENESS WARN (SYNC-7, #177/#178, retained through the weekly-only rewrite): fires
        # when the email_message sync frontier lags ≥2 business-days into the trailing-7d window, so the
        # weekly n/median are structurally deflated (not a real speed change). This is about the WEEKLY
        # window (the only §6 block now), so it stays load-bearing even with the daily block gone.
        if _IMREPLY_WARN:
            fr, nmiss = _IMREPLY_WARN
            warn = add(["(⚠ WEEKLY UNDERSTATED — email reply-history synced only through %s (SYNC-7 "
                        "drain, D-1+); %d of the 7 window business-days have zero synced pairs, so n "
                        "is deflated and the median reflects only the older synced days — NOT a "
                        "desk-speed change. Self-corrects as the drain advances.)" % (fr.isoformat(), nmiss)])
            data.append(warn); row_ncol[warn] = W
        pend = add(["SMS · WhatsApp first-reply time", "pending", "pending", "pending"])
        data.append(pend); row_ncol[pend] = W
    def infra_table(infra, header_label):
        # §1b — one row PER INFRASTRUCTURE, summed across ALL workspaces (Sam 2026-06-30; Google == reseller).
        # RR=(human+auto)/sent, HumanRR=human/sent, PositiveRR=opp/replies [opp/(human+auto)], Email->opp=sent/opp, Email->meeting=sent/mtg.
        # Row set/labels derive from the module-level INFRA_RENDER_ROWS (single sync point — see
        # its comment for the Milkbox wipe rationale).
        INFRA_ROWS = INFRA_RENDER_ROWS
        INFRA_LABEL = INFRA_RENDER_LABEL
        W = 7
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Infrastructure", "Sent", "RR", "Human RR", "Positive RR", "Email\u2192opp", "Email\u2192meeting"])
        th.append(hr); th_ncol[hr] = W
        def pct(n, d): return (float(n) / float(d)) if d else "\u2014"
        def ratio(n, d): return round(float(n) / float(d)) if d else "\u2014"
        agg = {inf: [0, 0, 0, 0] for inf in INFRA_ROWS}
        mtg = {inf: 0 for inf in INFRA_ROWS}
        for slug, name, a, ws_total_sent, unattr_sent, n_failed in infra.get("by_ws", []):
            for inf in INFRA_ROWS:
                av = a.get(inf, [0, 0, 0, 0])
                for i in range(4): agg[inf][i] += av[i]
        for (slug, inf), n in infra.get("mtg_by", {}).items():
            if inf in mtg: mtg[inf] += n
        gs = gh = ga = go = gm = 0
        for inf in INFRA_ROWS:
            s, o, h, a = agg[inf]; m = mtg[inf]
            ri = add([INFRA_LABEL[inf], s, pct(h + a, s), pct(h, s), pct(o, h + a), ratio(s, o), ratio(s, m)])
            data.append(ri); infrows.append(ri); row_ncol[ri] = W
            gs += s; gh += h; ga += a; go += o; gm += m
        gi = add([f"TOTAL ({len(INFRA_ROWS)} infra)", gs, pct(gh + ga, gs), pct(gh, gs), pct(go, gh + ga), ratio(gs, go), ratio(gs, gm)])
        tot.append(gi); infrows.append(gi); row_ncol[gi] = W
        um = infra.get("unattr_mtg", 0)
        if um:
            ni = add(["(note: %s Funding meetings on %s had no resolvable email campaign — MOSTLY non-email "
                      "channels (SMS / Call / WhatsApp), correctly excluded from these EMAIL infra rows; on "
                      "OTD-churn days a few real email meetings also land here (campaign created/deleted "
                      "intraday → no live tagged campaign), so Email→meeting understates slightly — see §7 "
                      "DATA CAVEATS)" % (um, DAILY)])
            data.append(ni); row_ncol[ni] = W
    # §7 DATA CAVEATS — load-bearing "how to read this number" notes promoted OUT of per-section footnotes
    # so nothing that changes a number's meaning stays buried [Sam 2026-07-02]. Text-only; one row/caveat.
    def caveats_table(label, items):
        # Section header spans the full tab width (like every other §) so its blue band matches; the
        # caveat rows themselves are single long text cells (W=1) that overflow visually to the right.
        si = add([label]); sec.append(si); row_ncol[si] = 8
        for t in items:
            ci = add(["• " + t]); data.append(ci); row_ncol[ci] = 1
    build_fn(dict(add=add, email_table=email_table, sms_wa_table=sms_wa_table, truth_table=truth_table,
                  close_table=close_table, partner_table=partner_table, imreply_table=imreply_table,
                  infra_table=infra_table, sms_kpi_table=sms_kpi_table, caveats_table=caveats_table))

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
    for i in strows:  # §4 Fulfillment % — col 7 (shifted +2 by the Actual OTD/Google columns)
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 7, 8), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in infrows:  # §1b RR / Human RR / Positive RR (cols 2,3,4) as percents
        for c in (2, 3, 4):
            reqs.append({"repeatCell": {"range": rng(i, i + 1, c, c + 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i, c0, c1 in usdcells:  # §2 / §2b cost columns as USD (strings like '—' are unaffected)
        reqs.append({"repeatCell": {"range": rng(i, i + 1, c0, c1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}})
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
    # red gradient on §4 fulfillment % (col 7 — shifted +2 by the Actual OTD/Google columns)
    if len(strows) >= 2:
        d0 = strows[0]; d1 = strows[-2] + 1
        reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {"ranges": [{"sheetId": sid, "startRowIndex": d0, "endRowIndex": d1, "startColumnIndex": 7, "endColumnIndex": 8}], "gradientRule": {"minpoint": {"color": rgb(0.91, 0.40, 0.40), "type": "NUMBER", "value": "0"}, "maxpoint": {"color": rgb(1, 1, 1), "type": "NUMBER", "value": "1"}}}}})
    api("POST", BASE + ":batchUpdate", {"requests": reqs})
    merge_reqs = [{"mergeCells": {"range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": c0, "endColumnIndex": c1}, "mergeType": "MERGE_ALL"}} for r0, c0, c1 in merges]
    if merge_reqs: api("POST", BASE + ":batchUpdate", {"requests": merge_reqs})
    write_summary_block(sid)
    print(f"  {tab}: {NROW} rows, sections={len(sec)} headers={len(th)} totals={len(tot)} + summary block")

def write_summary_block(sid):
    """Cream right-side 'Business / Funding' summary block (cols L-O), GENERATED from section data."""
    def ratio(a, b): return round(a / b) if b else ""
    # SMS_D rows: (label, sent, delivered, failed, replies, meetings, cost_usd|None)
    sms1 = SMS_D[0] if len(SMS_D) > 0 else ("", 0, 0, None, 0, 0, None)
    sms2 = SMS_D[1] if len(SMS_D) > 1 else ("", 0, 0, None, 0, 0, None)
    wa = next((x for x in SMS_D if "WhatsApp" in x[0]), ("", 0, 0, 0, 0, 0, None))
    warm = next((r for r in EMAIL_D if r[0].lower().startswith("warm")), ("Warm leads", 0, 0, 0, 0, 0, 0))
    wsrows = [r for r in EMAIL_D if not r[0].lower().startswith("warm")]
    blk = [[f"{DAILY_TAB} — Business / Funding · {DAILY}", "", "", ""],
           ["WORKSPACE", "Email Sent", "Meeting Booked", "Meeting to Booked"]]
    # Total = §1's TOTAL BY CONSTRUCTION: summed over ALL §1 rows (same list email_table totals),
    # INCLUDING Warm leads — Warm is displayed in the channel group below but was silently missing
    # from this total (06-30 read 2,419,145 vs §1's 2,433,955; the 14,810 gap was Warm). Summing
    # EMAIL_D directly means the yellow total and §1's TOTAL can never disagree again.
    ts = sum(x[1] for x in EMAIL_D); tm = sum(x[4] for x in EMAIL_D)
    for ws, sent, hr, opps, m, c, r in wsrows:
        blk.append([ws, sent, m, ratio(sent, m)])  # full name incl CM suffix "(Sam)" [Sam 06-30]
    blk.append(["Total (incl. Warm)", ts, tm, ratio(ts, tm)]); blk.append(["", "", "", ""])
    # NO "SDR (Close)" row here: Close's 388 were DIALS, not emails — an Email-Sent block must not
    # carry dial counts [Jun-30 accuracy pass 07-01]. Close lives in §3 only.
    for lbl, s, m in [("SMS Funding", sms1[1], sms1[5]),
                      ("Warm Leads", warm[1], warm[4]), ("WhatsApp Funding (delivered)", wa[2], wa[5])]:
        blk.append([lbl, s, m, ratio(s, m)])
    blk.append(["", "", "", ""])
    blk.append(["SMS IPO", sms2[1], PREIPO_MTG, ratio(sms2[1], PREIPO_MTG)])
    # WhatsApp PRE-IPO meetings now READ from the portal (wa_preipo_meetings — registry DR-9; was
    # hardcoded 0). Sent stays 0: no WA Pre-IPO send feed exists (v_sms_dash_wa_daily is the Funding
    # ISKRA line), and the ratio stays '' — a KPI over a structurally-0 sent would be fake.
    blk.append(["WhatsApp PRE-IPO", 0, WA_PREIPO_MTG, ""])
    # SEC 125 / Tariffs / R&D Credit stay HARDCODED 0: those offers do not exist yet (no booking
    # source, no send feed anywhere). Wire each one like WhatsApp PRE-IPO above when it launches.
    for lbl in ["SEC 125", "Tariffs", "R&D Credit"]:
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
        # Column widths for the summary block (cols L,M,N,O = 11..14). The main-table width loop only
        # sizes cols A..J, so without this L stays ~100px and the labels truncate ("Max's workspac",
        # "WhatsApp Fundi", "WhatsApp PRE-"). Persisted every render. L wide (long labels like "WhatsApp
        # Funding (delivered)"); M/N/O sized for their numbers + the "Meeting to Booked" header.
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 11, "endIndex": 12}, "properties": {"pixelSize": 220}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 12, "endIndex": 13}, "properties": {"pixelSize": 110}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 13, "endIndex": 14}, "properties": {"pixelSize": 125}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 15}, "properties": {"pixelSize": 140}, "fields": "pixelSize"}},
        {"updateBorders": {"range": br(r0, r1), "top": thin, "bottom": thin, "left": thin, "right": thin, "innerHorizontal": thin, "innerVertical": thin}}]
    api("POST", BASE + ":batchUpdate", {"requests": reqs})

def daily(ctx):
    add = ctx["add"]
    add([f"DAILY REVOPS REPORT — Business Funding · {DAILY}"])
    add([os.environ.get("DAILY_SUBTITLE", f"Daily · {DAILY} · single-source-of-truth (warehouse + Instantly/sendivo live) · ⚠ read §7 DATA CAVEATS before citing")]); add()
    ctx["email_table"](EMAIL_D, f"1 · EMAIL + WARM LEADS — by workspace · day {DAILY}"); add()
    ctx["infra_table"](INFRA_D, f"1b · EMAIL KPIs BY INFRASTRUCTURE — {' / '.join(INFRA_RENDER_LABEL[i] for i in INFRA_RENDER_ROWS)} · all workspaces · day {DAILY} (Milkbox row wiped 2026-07-01 pending rebuild)"); add()
    ctx["sms_wa_table"](SMS_D, f"2 · SMS + WHATSAPP — by channel · day {DAILY}"); add()
    # §2b (7-day trailing SMS KPI-to-opp) REMOVED from the daily tab [Sam 2026-07-02]: nothing non-daily
    # belongs on a daily report — a trailing window next to single-day rows invites the exact misread the
    # window-label patch was fighting. The underlying views (v_sms_campaign_performance / KPI-to-opp) and
    # get_sms_kpi_to_opp stay for other consumers; only this tab section is dropped.
    ctx["close_table"](CLOSE_D, f"3 · CLOSE CRM — warm calling · day {DAILY}"); add()
    ctx["truth_table"](f"4 · SENDING VOLUME TRUTH — expected (CONNECTED active capacity; disconnected/paused inboxes excluded) vs actual sends, split OTD/Google · Actual total = §1 Instantly (incl. Outlook, excluded from the OTD/Google split & from Expected) · split = account-grain sending_account_daily when the report day has loaded, else the live §1b infra split (unresolved shows '—', never 0) · no-lag · census {SENDING_CENSUS}"); add()
    ctx["partner_table"](PARTNER_D, PARTNER_D_TOTAL, f"5 · BOOKINGS BY PARTNER · day {DAILY}"); add()
    ctx["imreply_table"](IMREPLY_D, f"6 · IM REPLY-TIME — business minutes to first reply, by workspace · clock runs 12-8pm ET Mon-Fri only (all arrivals count; off-hours clock opens next window) · WEEKLY (7d) only — daily block removed 2026-07-02 (source is nightly-fed D-1, so day-D daily is structurally empty) · email (SMS+WA pending) · BLENDED IM+AIM: AIM (AI-drafted) replies ship through the same Instantly inboxes and carry NO distinguishing flag in the data, so a fast median reflects AI-assisted answering, not desk speed (a workspace with little/no AIM — e.g. Renaissance 1 DFY — reads slower for that reason, not because the desk is broken) · Grace & Sam"); add()
    ctx["caveats_table"]("7 · DATA CAVEATS — how to read these numbers (load-bearing; promoted from footnotes)", [
        "§1b EMAIL infra rows exclude non-email bookings: SMS / Call / WhatsApp Funding meetings (the bulk of §1b's 'no resolvable email campaign' note) are correctly NOT in the OTD/Google rows. On OTD-churn days a few genuine EMAIL meetings also fall into 'unattributed' — their campaign was created or deleted intraday, so it doesn't map to a live tagged campaign — so §1b Email→meeting understates slightly that day. Not a data error.",
        f"§4 Actual OTD/Google split: on the ~10pm-ET evening render the report-day account feed (core.sending_account_daily) has not loaded yet, so the split is the LIVE §1b infra resolution (same Instantly source as Actual total). Subsequence + deleted-campaign sends fall to an unsplit residual, so OTD+Google can be LESS than Actual total; '—' means the split was unresolvable for that workspace on {DAILY} (never a fake 0). It heals to the complete account-grain split at the 12:45Z backfill re-render. Actual total includes Outlook, which is excluded from the split and from Expected.",
        "§6 reply-time is BLENDED human + AIM (AI-drafted): AIM replies ship through the same inboxes with no distinguishing flag, so a fast median reflects AI-assisted answering, not desk speed — a workspace with little/no AIM (e.g. Renaissance 1 DFY) reads slower for that reason. WEEKLY-only: the daily window is nightly-fed (D-1) and structurally empty on day D. When the email-reply sync frontier lags into the 7-day window, the weekly n/median are structurally deflated (SYNC-7 drain) — a '⚠ WEEKLY UNDERSTATED' row flags that when it happens; it is not a desk-speed change.",
        "§2 SMS: Renaissance 3 (webform) sends route via the AIM API, which is NOT in the campaign feed, so Ren3 Sent (and any Sent-derived ratio) undercounts. A '—' cost cell means the all-in Sendivo billing row back-fills after the nightly.",
        "Evening-render timing: meetings read LOW on the ~10pm-ET render — the IM booking backlog clears ~midnight and bookings after the ~9pm snapshot miss it. The tab trues up at the 12:45Z backfill re-render the next day.",
    ])

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
