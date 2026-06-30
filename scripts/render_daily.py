#!/usr/bin/env python3
"""Daily RevOps Report — DAILY tab, SINGLE-SOURCE-OF-TRUTH per metric.

Every metric resolves to ONE canonical source used by EVERY section + the yellow block, so a
workspace's number can never disagree section-to-section (the class of bug that produced
"§1 Samuel 513k vs §4 150k"). See handoffs/2026-06-30-BUILD-daily-report-full-automation-and-im-reply-time.md
and deliverables/2026-06-29-daily-report-10pm-sync/RENDERER-SPEC.md.

Canonical sources (verified 2026-06-30):
 §1 Email sent + opps  -> Instantly API /campaigns/analytics/daily per workspace (memoized; the SAME
                          object feeds §4 Actual, so §1 sent and §4 Actual are literally one fetch).
 §1 meetings/cheap-reg -> consolidated booking sheet (channel+workspace) + im_bookings.lead_type.
 §2 SMS sent           -> sendivo billing_report.sms_fees.quantity (12720=Ren1, 13922=Ren2, 14603=webform).
 §2 SMS replies (human)-> raw_sendivo_inbound non-opt-out.
 §2 WhatsApp           -> v_sms_dash_wa_daily (sent/delivered/failed/replies); meetings -> booking sheet.
 §3 Close             -> core.call (dials/leads/connects @>=60s, ET day) — Close API SoR, under-captures.
 §4 Sending truth      -> Expected = active accounts' configured daily_limit by infra (core.account_label,
                          latest census, no lag); Actual = Instantly daily per workspace (== §1 sent).
 §5 Bookings/partner   -> consolidated booking sheet partner.
 §6 IM reply-time      -> core.email_message (native, fresh): first prospect reply (ue_type 2) -> first
                          our reply (ue_type 3) per thread; median/avg per workspace daily/weekly/monthly.

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

SID = "1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
TOK = os.environ.get("GOOGLE_TOKEN", "/root/.config/mcp-google-sheets/token.json")
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SID}"

args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in sys.argv
REPORT_DATE = args[0] if args else "2026-06-29"
_d = datetime.date.fromisoformat(REPORT_DATE)
DAILY = REPORT_DATE
DAILY_TAB = args[1] if len(args) > 1 else _d.strftime("%b %-d")  # "Jun 29"

# ---------------------------- workspace identity ----------------------------
# warehouse slug (== Instantly key slug == account_label.workspace_slug == email_message.workspace_id)
WS = [  # (slug, display name)  — render order
    ("renaissance-4",   "Funding 1 (Samuel)"),
    ("renaissance-5",   "Funding 2 (Ido)"),
    ("prospects-power", "Funding 3 (Leo)"),
    ("koi-and-destroy", "Funding 4 (Sam)"),
    ("renaissance-2",   "Funding 5 (Eyver)"),
    ("renaissance-1",   "Renaissance 1 (Instantly)"),
    ("the-gatekeepers", "Max's workspace"),
    ("warm-leads",      "Warm leads"),
]
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
import httpx
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
            r = httpx.get(_INST_BASE + "/campaigns/analytics/daily",
                          params={"start_date": date, "end_date": date},
                          headers={"Authorization": f"Bearer {k}"}, timeout=60.0)
            r.raise_for_status()
            rows = r.json() or []
            day = next((x for x in rows if str(x.get("date", ""))[:10] == date), None)
            if day:
                out[slug] = (int(day.get("sent") or 0), int(day.get("opportunities") or 0))
        except Exception as e:
            print(f"WARN instantly_daily {slug} {date}: {e}", file=sys.stderr)
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
                for r in s.billing_report(date, date):
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
_g_creds = Credentials.from_authorized_user_file(TOK); _g_creds.refresh(Request())
def _gtok():
    if not _g_creds.valid:
        _g_creds.refresh(Request())
    return _g_creds.token
def gget(sid, rng):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{urllib.parse.quote(rng)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_gtok()}"})
    return json.load(urllib.request.urlopen(req, timeout=60)).get("values", [])

# consolidated booking sheet (Channel + Workspace + Partner per booking; portal-fed)
BOOKINGS_SID = "1vaExhxu319o2CSoQtjWRV49lq5GowOeJzo1_GHXIIqM"
def _date_variants(d):
    # the booking sheet writes "Jun 29, 2026"; tolerate "June 29, 2026" too
    return {d.strftime("%b %-d, %Y"), d.strftime("%B %-d, %Y")}
_book_cache = {}
def consolidated_bookings(date):
    """deduped (email|phone) bookings for `date`: list of {channel, ws, partner, key}."""
    if date in _book_cache:
        return _book_cache[date]
    d = datetime.date.fromisoformat(date); want = _date_variants(d)
    rows = gget(BOOKINGS_SID, "Data!A1:R")
    if not rows:
        _book_cache[date] = []; return []
    hdr = rows[0]; ix = {h: i for i, h in enumerate(hdr)}
    def c(r, n):
        i = ix.get(n); return (r[i].strip() if i is not None and i < len(r) and r[i] else "")
    seen, out = set(), []
    for r in rows[1:]:
        if c(r, "Date") not in want:
            continue
        key = (c(r, "Email").lower() or c(r, "Phone"))
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append({"channel": c(r, "Channel"), "ws": ws_alias(c(r, "Workspace")),
                    "partner": c(r, "Partner") or "(unknown)", "key": key,
                    "email": c(r, "Email").lower(), "phone": c(r, "Phone")})
    _book_cache[date] = out
    return out

# Pre-IPO meeting sheets (count by booking-made Date) -> yellow-block "SMS IPO"
PREIPO_SHEETS = [
    ("1oKlY_2qI-p0oH4d8UAOE3GpceiFU4OIWx1aDAM5RzRY", "Sheet1"),   # Pre-IPO SMS (Craig Diana)
    ("1IZzmCXtbtrpZYbxuU1qkxoOkITL4zmP2In6glpffmYw", "Collins"),  # Collins desk
]
_preipo_cache = {}
def preipo_meetings(date):
    if date in _preipo_cache:
        return _preipo_cache[date]
    d = datetime.date.fromisoformat(date); want = _date_variants(d) | {date, d.strftime("%-m/%-d/%Y"), d.strftime("%m/%d/%Y")}
    total = 0
    for sid, tab in PREIPO_SHEETS:
        try:
            rows = gget(sid, f"{tab}!A1:Z")
        except Exception as e:
            print(f"WARN preipo {sid}: {e}", file=sys.stderr); continue
        if not rows:
            continue
        hdr = rows[0]; ix = {h.strip().lower(): i for i, h in enumerate(hdr)}
        di = ix.get("date")
        if di is None:
            continue
        for r in rows[1:]:
            if di < len(r) and r[di] and str(r[di]).strip() in want:
                total += 1
    _preipo_cache[date] = total
    return total

# im_bookings lead_type (cheap/regular) by email|phone, for the report day (warehouse SoT)
def imbookings_meetings(date):
    """{key(email|phone): lead_type} + raw deduped count, from the portal mirror (offer=Funding)."""
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

# sendivo sub-account -> (label, channel-bucket)
SENDIVO_SUB = {12720: "Renaissance 1", 13922: "Renaissance 2", 14603: "RG3 webform"}
def get_sms_wa():
    sms = sendivo_sms(DAILY)
    inb = {r[0]: (int(r[1]), int(r[2])) for r in wq(
        f"""SELECT sub_account_name, count(*), count(*) FILTER (WHERE NOT is_opt_out)
            FROM main.raw_sendivo_inbound WHERE CAST(received_at AS DATE)=DATE '{DAILY}' GROUP BY 1""")}
    book = consolidated_bookings(DAILY)
    sms_mtg = sum(1 for b in book if b["channel"].lower() == "sms")
    wa_mtg = sum(1 for b in book if b["channel"].lower() == "whatsapp")
    # SMS meetings split Ren1(Funding)/Ren2(Pre-IPO) not channel-tagged in the sheet -> report combined on Ren1 row,
    # Pre-IPO SMS meetings come from the Pre-IPO sheets (yellow block). Here: Ren1 row carries funding SMS meetings.
    rows = []
    rows.append(("Renaissance 1 (SMS)", sms.get(12720, 0), inb.get("Renaissance 1", (0, 0))[1], sms_mtg))
    rows.append(("Renaissance 2 (SMS · Pre-IPO)", sms.get(13922, 0), inb.get("Renaissance 2", (0, 0))[1], preipo_meetings(DAILY)))
    wa = wq(f"""SELECT sent, delivered, failed, replies_total FROM main.v_sms_dash_wa_daily
                WHERE channel='whatsapp' AND metric_date=DATE '{DAILY}'""")
    wa_sent = int(wa[0][0] or 0) if wa else 0
    wa_rep = int(wa[0][3] or 0) if wa else 0
    rows.append(("WhatsApp (ISKRA)", wa_sent, wa_rep, wa_mtg))
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
    census = wq("SELECT max(census_date) FROM core.account_label")[0][0]
    return census, out

def get_partner():
    book = consolidated_bookings(DAILY)
    cnt = collections.Counter(b["partner"] for b in book)
    pr = sorted(cnt.items(), key=lambda x: -x[1])
    return pr, sum(n for _, n in pr)

def get_im_reply():
    """§6 IM reply-time from the native core.email_message: first prospect reply (ue_type 2) ->
    first our reply (ue_type 3) in the same thread; median/avg per workspace, daily/weekly/monthly."""
    wk = (_d - datetime.timedelta(days=6)).isoformat()
    mo = _d.replace(day=1).isoformat()
    sql = f"""
      WITH inbound AS (
        SELECT thread_id, workspace_id AS ws, message_at AS p_ts,
               row_number() OVER (PARTITION BY thread_id ORDER BY message_at, message_id) AS seq
        FROM core.email_message
        WHERE ue_type=2 AND thread_id IS NOT NULL AND message_at >= DATE '{mo}'),
      ours AS (SELECT thread_id, message_at AS r_ts FROM core.email_message
               WHERE ue_type=3 AND thread_id IS NOT NULL AND message_at >= DATE '{mo}'),
      paired AS (
        SELECT i.ws, CAST(i.p_ts AS DATE) d,
               date_diff('minute', i.p_ts,
                 (SELECT min(o.r_ts) FROM ours o WHERE o.thread_id=i.thread_id AND o.r_ts > i.p_ts)) AS lat
        FROM inbound i WHERE i.seq=1)
      SELECT ws,
        count(*) FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        median(lat) FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        avg(lat)    FILTER (WHERE d=DATE '{DAILY}' AND lat IS NOT NULL),
        count(*) FILTER (WHERE d>=DATE '{wk}' AND lat IS NOT NULL),
        median(lat) FILTER (WHERE d>=DATE '{wk}' AND lat IS NOT NULL),
        avg(lat)    FILTER (WHERE d>=DATE '{wk}' AND lat IS NOT NULL),
        count(*) FILTER (WHERE lat IS NOT NULL),
        median(lat) FILTER (WHERE lat IS NOT NULL),
        avg(lat)    FILTER (WHERE lat IS NOT NULL)
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

# ============================ collect ============================
EMAIL_D = get_email()
SMS_D = get_sms_wa()
CLOSE_D = get_close()
SENDING_CENSUS, SENDING_TRUTH = get_truth()
PARTNER_D, PARTNER_D_TOTAL = get_partner()
IMREPLY_PERIODS, IMREPLY_D = get_im_reply()
PREIPO_MTG = preipo_meetings(DAILY)

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
def rr(hr, sent): return (hr / sent) if sent else "—"
def pctstr(n, m): return f"'{round(100.0*n/m)}%" if m else "'0%"
def kpi(sent, m): return round(sent / m) if m else "—"
def fpct(actual, expected): return (actual / expected) if expected else "—"
def mins(v): return "—" if v is None else round(v)
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
        W = 5
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Channel / workspace", "Sent", "Human replies", "Meetings", "KPI (sent/mtg)"]); th.append(hr); th_ncol[hr] = W
        ts = th_ = tm = 0
        for label, sent, reps, m in rows_data:
            ri = add([label, sent, reps, m, kpi(sent, m)]); data.append(ri); row_ncol[ri] = W; ts += sent; th_ += reps; tm += m
        ti = add(["TOTAL", ts, th_, tm, kpi(ts, tm)]); tot.append(ti); row_ncol[ti] = W
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
    sms1 = SMS_D[0] if len(SMS_D) > 0 else ("", 0, 0, 0)
    sms2 = SMS_D[1] if len(SMS_D) > 1 else ("", 0, 0, 0)
    wa = next((x for x in SMS_D if "WhatsApp" in x[0]), ("", 0, 0, 0))
    warm = next((r for r in EMAIL_D if r[0].lower().startswith("warm")), ("Warm leads", 0, 0, 0, 0, 0, 0))
    wsrows = [r for r in EMAIL_D if not r[0].lower().startswith("warm")]
    blk = [[f"{DAILY_TAB} — Business / Funding · {DAILY}", "", "", ""],
           ["WORKSPACE", "Email Sent", "Meeting Booked", "Meeting to Booked"]]
    ts = tm = 0
    for ws, sent, hr, opps, m, c, r in wsrows:
        blk.append([short(ws), sent, m, ratio(sent, m)]); ts += sent; tm += m
    blk.append(["Total", ts, tm, ratio(ts, tm)]); blk.append(["", "", "", ""])
    for lbl, s, m in [("SMS Funding", sms1[1], sms1[3]), ("SDR (Close)", CLOSE_D["dials"], CLOSE_D["meetings"]),
                      ("Warm Leads", warm[1], warm[4]), ("WhatsApp Funding", wa[1], wa[3])]:
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
    ctx["imreply_table"](IMREPLY_D, f"6 · IM REPLY-TIME — first-reply latency by workspace · daily / weekly / monthly · email (SMS+WA pending)")

print(f"Rendering DAILY tab '{DAILY_TAB}' for {DAILY} ...")
build_and_write(DAILY_TAB, daily)
print(f"OK — wrote tab '{DAILY_TAB}'.")
