#!/usr/bin/env python3
"""Month-to-date RevOps Report — MTD tab (DR-12). Re-renderable sibling of scripts/render_daily.py.

Parameterized by report-date:  render_mtd.py YYYY-MM-DD ["Jun MTD"]
Renders ONE month-to-date tab aggregating month-1 -> report-day (ET business dates) for the
Business-Funding RevOps report. Sections (matching the existing "Jun MTD" tab layout):
  §1  EMAIL + WARM LEADS by workspace  (13 cols: Sent/Opps/Opp rate/Meetings/Book rate/KPI/
       Cheap KPI/Regular KPI/Cheap #%/Regular #%)
  §2  SMS + WHATSAPP by channel        (Sent/Human RR/Meetings/KPI)
  §3  CLOSE CRM warm calling           (Dials/Leads/Connects/Meetings/Dial->mtg %)
  §4  BOOKINGS BY PARTNER
  yellow "Business / Funding" summary block (cols L-O)
NO Sending-Volume-Truth / §1b infra / §6 reply-time (those are daily-tab-only).

CANONICAL SOURCES — every value warehouse-native (NO live Instantly/Sendivo API call; MTD sums read
the nightly mirrors, so the render is deterministic + re-runnable):
  §1 Sent/Opps  -> main.raw_instantly_workspace_analytics_daily (workspace-level Instantly mirror,
                   summed MTD by slug; NOT per-campaign — the per-campaign fact loses same-day-deleted
                   campaigns' sends. Mirror reconciles digit-for-digit to the live daily tabs).
  Meetings      -> THE BLEND RULE (label honestly):
                   • report month >= 2026-07  -> portal main.raw_im_bookings ONLY (offer='Funding',
                     latest snapshot, non-deleted, email|phone dedup) — the clean post-cutover case.
                   • report month == 2026-06 (or earlier) -> core.meeting, which already stitches
                     Slack(<06-01) / Funding-Form-sheet([06-01,06-29)) / im_bookings(>=06-29) /
                     Pre-IPO desks, so the pre-cutover June days aren't lost (offer='Business Funding',
                     dedup by lead_email — core.meeting carries no phone). IMB_CUTOVER=2026-06-29.
  Cheap/Regular meeting split -> lead_type (July im_bookings) / campaign-name keyword
                   (June core.meeting.campaign_name_raw — lead_type not retained pre-portal). regular =
                   meetings - cheap (always sums to Meetings).
  Cheap KPI/Reg KPI sends -> cheap_sends = MTD sends of campaigns whose NAME matches
                   isaac|mca|gbc|btc|gq|cheap (raw_pipeline_campaign_daily_metrics JOIN core.campaign);
                   regular_sends = mirror total - cheap_sends. Sam-approved method (2026-06-27).
  §2 SMS sent   -> main.raw_sendivo_billing_daily.sms_fee_qty summed MTD per sub (12720 Ren1 / 13922
                   Ren2 Pre-IPO). Human RR -> raw_sendivo_inbound non-opt-out / sent. Meetings by channel.
  §2 WhatsApp   -> main.v_sms_dash_wa_daily (channel='whatsapp') summed MTD; meetings = Funding WhatsApp.
  §3 Close      -> core.call (occurred_at ET); meetings channel='Call'.
  §4 Partner    -> grouped by partner over the meeting source (all offers), deduped.
"""
import json, os, sys, re, datetime, urllib.request, urllib.parse

# ---------------- args / window ----------------
args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in sys.argv
try:
    from zoneinfo import ZoneInfo
    _today = datetime.datetime.now(ZoneInfo("America/New_York")).date().isoformat()
except Exception:
    _today = datetime.date.today().isoformat()
REPORT_DATE = args[0] if args else _today
_d = datetime.date.fromisoformat(REPORT_DATE)
MONTH_START = _d.replace(day=1).isoformat()
RANGE = f"{_d.strftime('%B')} 1 → {REPORT_DATE}"
DEFAULT_TAB = _d.strftime("%b MTD")  # "Jul MTD" / "Jun MTD"
TAB = args[1] if len(args) > 1 else DEFAULT_TAB

# BLEND boundary: June-and-earlier reads core.meeting (Funding-Form-era + portal blend); July-onward
# reads the portal im_bookings only. IMB_CUTOVER = 2026-06-29 (meeting.py).
USE_CORE_MEETING = (_d.year, _d.month) <= (2026, 6)
BLEND_LABEL = ("June MTD = Funding-Form-era (≢06-28) + portal (06-29→) blend"
               if USE_CORE_MEETING else "portal-only (im_bookings)")

# ---------------- workspace identity (registry roster) ----------------
WS = [
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
NAMES = [n for _, n in WS]

def ws_alias(raw):
    t = (raw or "").strip().lower()
    if not t: return None
    if t.startswith("funding 1") or t == "f1": return "Funding 1 (Samuel)"
    if t.startswith("funding 2") or t == "f2": return "Funding 2 (Ido)"
    if t.startswith("funding 3") or t == "f3": return "Funding 3 (Leo)"
    if t.startswith("funding 4") or t == "f4": return "Funding 4 (Sam)"
    if t.startswith("funding 5") or t == "f5": return "Funding 5 (Eyver)"
    if t.startswith("warm"): return "Warm leads"
    if t.startswith("max") or "gatekeeper" in t: return "Max's workspace"
    if "renaissance 1" in t or t in ("r1", "instantly") or "sendivo" in t: return "Renaissance 1 (Instantly)"
    return None

_CAMPAIGN_OPERATOR = {"SAMUEL": "Funding 1 (Samuel)"}
def ws_from_booking(workspace, campaign):
    c = (campaign or "").upper()
    for op, ws in _CAMPAIGN_OPERATOR.items():
        if op in c: return ws
    return ws_alias(workspace)

# ---------------- warehouse read API (verbatim from render_daily) ----------------
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
    if resp.get("truncated"):
        raise RuntimeError(f"warehouse query TRUNCATED at {resp.get('row_count')} rows — refusing partial data")
    return resp["rows"]

# ---------------- cheap classifier ----------------
CHEAP_RE = re.compile(r"isaac|mca|gbc|btc|gq|cheap", re.I)
CHEAP_SQL = ("(c.name ILIKE '%isaac%' OR c.name ILIKE '%mca%' OR c.name ILIKE '%gbc%' "
             "OR c.name ILIKE '%btc%' OR c.name ILIKE '%gq%' OR c.name ILIKE '%cheap%')")

S, E = MONTH_START, REPORT_DATE  # window bounds (ET business dates)

# ==================================================================== §1 EMAIL
def get_email():
    """-> (rows, cr) where rows = [(ws, sent, hr(0), opps, meetings, cheap_mtg, reg_mtg)] and
    cr = {ws: (cheap_sends, cheap_mtg, reg_mtg)}. Sent/opps from the workspace mirror (MTD sum)."""
    # sent/opps MTD from the workspace-level Instantly mirror
    mir = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in wq(
        f"""SELECT workspace_slug, sum(sent), sum(opportunities)
            FROM main.raw_instantly_workspace_analytics_daily
            WHERE date BETWEEN DATE '{S}' AND DATE '{E}' GROUP BY 1""")}
    # cheap sends MTD (campaign-name keyword) per slug
    cheap_sent = {r[0]: int(r[1] or 0) for r in wq(
        f"""SELECT m.workspace_id, sum(m.sent)
            FROM raw_pipeline_campaign_daily_metrics m JOIN core.campaign c ON c.campaign_id=m.campaign_id
            WHERE m.date BETWEEN DATE '{S}' AND DATE '{E}' AND {CHEAP_SQL} GROUP BY 1""")}
    # email meetings per workspace + cheap split
    mtg = {n: 0 for n in NAMES}; cheap_m = {n: 0 for n in NAMES}
    if USE_CORE_MEETING:
        rows = wq(f"""SELECT workspace_slug, lower(nullif(lead_email,'')) AS k,
                             coalesce(campaign_name_raw,'') AS camp
                      FROM core.meeting
                      WHERE offer='Business Funding' AND channel='Email'
                        AND meeting_date BETWEEN DATE '{S}' AND DATE '{E}'""")
        seen = set()
        for i, (slug, k, camp) in enumerate(rows):
            name = SLUG2NAME.get(slug)
            if not name: continue
            key = (name, k) if k else (name, f"__r{i}")
            if key in seen: continue
            seen.add(key)
            mtg[name] += 1
            if CHEAP_RE.search(camp or ""): cheap_m[name] += 1
    else:
        rows = wq(f"""SELECT workspace, campaign, lower(coalesce(nullif(email,''), phone)) AS k,
                             lower(coalesce(lead_type,'')) AS lt
                      FROM main.raw_im_bookings
                      WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
                        AND offer='Funding' AND channel='Email'
                        AND substr(coalesce(date,''),1,10) BETWEEN '{S}' AND '{E}'
                        AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null')""")
        seen = set()
        for i, (workspace, campaign, k, lt) in enumerate(rows):
            name = ws_from_booking(workspace, campaign)
            if name not in mtg: continue
            key = (name, k) if k else (name, f"__r{i}")
            if key in seen: continue
            seen.add(key)
            mtg[name] += 1
            if lt == "cheap": cheap_m[name] += 1
    out = []; cr = {}
    for slug, name in WS:
        sent, opps = mir.get(slug, (0, 0))
        m = mtg[name]; cm = cheap_m[name]; rm = m - cm
        out.append((name, sent, 0, opps, m, cm, rm))
        cr[name] = (cheap_sent.get(slug, 0), cm, rm)
    return out, cr

# ==================================================================== meeting scalars (§2 / summary)
def _mtg_core(offer, channel):
    r = wq(f"""WITH b AS (SELECT lower(nullif(lead_email,'')) k FROM core.meeting
                 WHERE offer='{offer}' AND channel='{channel}'
                   AND meeting_date BETWEEN DATE '{S}' AND DATE '{E}')
               SELECT count(DISTINCT k) FILTER (WHERE k IS NOT NULL)
                    + count(*) FILTER (WHERE k IS NULL) FROM b""")
    return int(r[0][0] or 0)

def _mtg_imb(offer, channel):
    r = wq(f"""WITH b AS (SELECT lower(coalesce(nullif(email,''), phone)) k FROM main.raw_im_bookings
                 WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
                   AND offer='{offer}' AND channel='{channel}'
                   AND substr(coalesce(date,''),1,10) BETWEEN '{S}' AND '{E}'
                   AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null'))
               SELECT count(DISTINCT k) FILTER (WHERE k IS NOT NULL AND k<>'')
                    + count(*) FILTER (WHERE k IS NULL OR k='') FROM b""")
    return int(r[0][0] or 0)

# core.meeting uses offer label 'Business Funding'; im_bookings uses 'Funding'. Pre-IPO shared.
def mtg(channel, preipo=False):
    if USE_CORE_MEETING:
        return _mtg_core("Pre-IPO" if preipo else "Business Funding", channel)
    return _mtg_imb("Pre-IPO" if preipo else "Funding", channel)

# ==================================================================== §2 SMS + WA
SUB_REN1, SUB_REN2 = 12720, 13922
def get_sms_wa():
    bill = {int(r[0]): int(r[1] or 0) for r in wq(
        f"""SELECT sub_account_id, sum(sms_fee_qty) FROM main.raw_sendivo_billing_daily
            WHERE metric_date BETWEEN DATE '{S}' AND DATE '{E}' GROUP BY 1""")}
    inb = {r[0]: int(r[1] or 0) for r in wq(
        f"""SELECT sub_account_name, count(*) FILTER (WHERE NOT is_opt_out)
            FROM main.raw_sendivo_inbound
            WHERE CAST(received_at AS DATE) BETWEEN DATE '{S}' AND DATE '{E}' GROUP BY 1""")}
    wa = wq(f"""SELECT sum(sent), sum(replies_total) FROM main.v_sms_dash_wa_daily
                WHERE channel='whatsapp' AND metric_date BETWEEN DATE '{S}' AND DATE '{E}'""")
    wa_sent, wa_rep = (int(wa[0][0] or 0), int(wa[0][1] or 0)) if wa else (0, 0)
    # rows: (label, sent, human_replies, meetings)
    return [
        ("Renaissance 1", bill.get(SUB_REN1, 0), inb.get("Renaissance 1", 0), mtg("SMS")),
        ("Renaissance 2", bill.get(SUB_REN2, 0), inb.get("Renaissance 2", 0), mtg("SMS", preipo=True)),
        ("WhatsApp (ISKRA)", wa_sent, wa_rep, mtg("WhatsApp")),
    ]

# ==================================================================== §3 Close
def get_close():
    c = wq(f"""SELECT COUNT(*), COUNT(DISTINCT close_lead_id),
                 COUNT(*) FILTER (WHERE duration_seconds >= 60)
               FROM core.call
               WHERE (occurred_at AT TIME ZONE 'America/New_York')::DATE BETWEEN DATE '{S}' AND DATE '{E}'""")
    d, l, cn = (int(c[0][0]), int(c[0][1]), int(c[0][2])) if c else (0, 0, 0)
    return {"dials": d, "leads": l, "connects": cn, "meetings": mtg("Call")}

# ==================================================================== §4 Partner
def get_partner():
    if USE_CORE_MEETING:
        rows = wq(f"""WITH b AS (
                        SELECT coalesce(nullif(trim(partner),''),'(unknown)') p,
                               lower(nullif(lead_email,'')) k
                        FROM core.meeting WHERE meeting_date BETWEEN DATE '{S}' AND DATE '{E}')
                      SELECT p, count(DISTINCT k) FILTER (WHERE k IS NOT NULL)
                                + count(*) FILTER (WHERE k IS NULL) FROM b GROUP BY 1""")
    else:
        rows = wq(f"""WITH b AS (
                        SELECT coalesce(nullif(trim(partner),''),'(unknown)') p,
                               lower(coalesce(nullif(email,''), phone)) k
                        FROM main.raw_im_bookings
                        WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings)
                          AND substr(coalesce(date,''),1,10) BETWEEN '{S}' AND '{E}'
                          AND (deleted_at IS NULL OR deleted_at='' OR lower(deleted_at)='null'))
                      SELECT p, count(DISTINCT k) FILTER (WHERE k IS NOT NULL AND k<>'')
                                + count(*) FILTER (WHERE k IS NULL OR k='') FROM b GROUP BY 1""")
    pr = sorted(((p, int(n)) for p, n in rows if int(n)), key=lambda x: (-x[1], x[0]))
    return pr, sum(n for _, n in pr)

# ==================================================================== collect
EMAIL_D, EMAIL_CR = get_email()
SMS_D = get_sms_wa()
CLOSE_D = get_close()
PARTNER_D, PARTNER_TOTAL = get_partner()
PREIPO_MTG = SMS_D[1][3]                      # Ren2 Pre-IPO SMS meetings
WA_PREIPO_MTG = mtg("WhatsApp", preipo=True)  # yellow-block WhatsApp PRE-IPO

if DRY:
    print(f"REPORT_DATE={REPORT_DATE} TAB={TAB} window {S}..{E} blend={BLEND_LABEL}")
    print("§1 EMAIL (ws,sent,_,opps,mtg,cheap_m,reg_m):"); [print("  ", x) for x in EMAIL_D]
    print("§1 CR (ws->cheap_sends,cheap_m,reg_m):"); [print("  ", k, v) for k, v in EMAIL_CR.items()]
    print("§2 SMS/WA (label,sent,human,mtg):"); [print("  ", x) for x in SMS_D]
    print("§3 CLOSE:", CLOSE_D)
    print("§4 PARTNER:", PARTNER_D, "total", PARTNER_TOTAL)
    print("Pre-IPO SMS mtg:", PREIPO_MTG, " WA Pre-IPO mtg:", WA_PREIPO_MTG)
    sys.exit(0)

# ==================================================================== sheet write engine
SID = "1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
TOK = os.environ.get("GOOGLE_SA_KEY", "/root/.config/gcp-sa/droplet-sheets-sync.json")  # [2026-07-14 creds-rebuild] service-account key
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SID}"
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account as _sa  # [2026-07-14 creds-rebuild]
_g = _sa.Credentials.from_service_account_file(TOK, scopes=["https://www.googleapis.com/auth/spreadsheets"]); _g.refresh(Request())
def _gtok():
    if not _g.valid: _g.refresh(Request())
    return _g.token
def api(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Authorization": f"Bearer {_gtok()}", "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))

def rr(hr, sent): return (hr / sent) if sent else "—"
def cnt(n): return n
def pctstr(n, m): return f"'{round(100.0*n/m)}%" if m else "'0%"
def kpi(sent, m): return round(sent / m) if m else "—"
def opp_rate(sent, opps): return round(sent / opps) if opps else "—"
def book_rate(opps, m): return round(opps / m, 1) if m else "—"
def rgb(r, g, b): return {"red": r, "green": g, "blue": b}

def build_and_write(tab, build_fn):
    rows = []; sec = []; th = []; tot = []; rrrows = []; merges = []; data = []
    th_ncol = {}; s5_rows = []; s5_oppr = []; s5_bookr = []
    def add(r=None): rows.append(r or []); return len(rows) - 1
    S5_HDR_TOP = ["Workspace", "Sent", "Opportunities", "Opp rate (sent/opp)", "Meetings",
                  "Book rate (opps/mtg)", "KPI (sent/mtg)", "Cheap KPI", "Regular KPI",
                  "Cheap", "", "Regular", ""]
    S5_SUB_HDR = ["", "", "", "", "", "", "", "", "", "#", "%", "#", "%"]
    def email_table_s5(data_rows, header_label, cr):
        sec.append(add([header_label])); hr_top = add(S5_HDR_TOP); th.append(hr_top)
        hr_sub = add(S5_SUB_HDR); th.append(hr_sub)
        merges.append((hr_top, 9, 11)); merges.append((hr_top, 11, 13)); th_ncol[hr_top] = th_ncol[hr_sub] = 13
        ts = topp = tm = tc_ = tr_ = 0; tcs = trs = tcm = trm = 0
        for ws, sent, hr, opps, m, c, r in data_rows:
            cs, cmn, rmn = cr[ws]; rs = sent - cs
            ri = add([ws, sent, opps, opp_rate(sent, opps), m, book_rate(opps, m), kpi(sent, m),
                      kpi(cs, cmn), kpi(rs, rmn), cnt(c), pctstr(c, m), cnt(r), pctstr(r, m)])
            data.append(ri); s5_rows.append(ri); s5_oppr.append(ri); s5_bookr.append(ri)
            ts += sent; topp += opps; tm += m; tc_ += c; tr_ += r; tcs += cs; trs += rs; tcm += cmn; trm += rmn
        ti = add(["TOTAL", ts, topp, opp_rate(ts, topp), tm, book_rate(topp, tm), kpi(ts, tm),
                  kpi(tcs, tcm), kpi(trs, trm), cnt(tc_), pctstr(tc_, tm), cnt(tr_), pctstr(tr_, tm)])
        tot.append(ti); s5_rows.append(ti); s5_oppr.append(ti); s5_bookr.append(ti)
    def sms_wa_table(rows_data, header_label):
        sec.append(add([header_label]))
        hr = add(["Channel / workspace", "Sent", "Human RR", "Meetings", "KPI (sent/mtg)"]); th.append(hr); th_ncol[hr] = 5
        ts = th_ = tm = 0
        for label, sent, reps, m in rows_data:
            ri = add([label, sent, rr(reps, sent), m, kpi(sent, m)]); rrrows.append(ri); data.append(ri)
            ts += sent; th_ += reps; tm += m
        ti = add(["TOTAL", ts, rr(th_, ts), tm, kpi(ts, tm)]); tot.append(ti); rrrows.append(ti)
    def close_table(close, label):
        sec.append(add([label])); th.append(add(["Close CRM — metric", "Value"]))
        d2m = f"'{(100.0*close['meetings']/close['dials']):.2f}%" if close['dials'] else "—"
        for r2 in [["Dials", close["dials"]], ["Distinct leads dialed", close["leads"]],
                   ["Connects (≥60s real convo)", close["connects"]],
                   ["Meetings booked (call-sourced)", close["meetings"]], ["Dial → meeting %", d2m]]:
            data.append(add(r2))
    def partner_table(rows_data, total, label, valcol="MTD bookings"):
        sec.append(add([label])); th.append(add(["Partner", valcol]))
        for p, n in rows_data: data.append(add([p, n]))
        tot.append(add(["TOTAL", total]))
    build_fn(dict(add=add, email_table_s5=email_table_s5, sms_wa_table=sms_wa_table,
                  close_table=close_table, partner_table=partner_table))

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
    def fill(i, color, **tf): return {"repeatCell": {"range": rng(i, i + 1), "cell": {"userEnteredFormat": {"backgroundColor": rgb(*color), "textFormat": tf}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}}
    reqs = []
    for br in sh.get("bandedRanges", []) or []: reqs.append({"deleteBanding": {"bandedRangeId": br["bandedRangeId"]}})
    for idx in range(len(sh.get("conditionalFormats", []) or []) - 1, -1, -1): reqs.append({"deleteConditionalFormatRule": {"sheetId": sid, "index": idx}})
    reqs.append({"unmergeCells": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": WIDE, "startColumnIndex": 0, "endColumnIndex": 26}}})
    reqs.append({"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": WIDE, "startColumnIndex": 0, "endColumnIndex": 26}, "cell": {"userEnteredFormat": {"backgroundColor": rgb(1, 1, 1), "horizontalAlignment": "LEFT", "verticalAlignment": "BOTTOM", "wrapStrategy": "OVERFLOW_CELL", "numberFormat": {"type": "TEXT"}, "textFormat": {"bold": False, "italic": False, "fontSize": 10, "foregroundColor": rgb(0, 0, 0)}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,numberFormat,textFormat)"}})
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": WIDE}, "properties": {"pixelSize": 21}, "fields": "pixelSize"}})
    reqs.append({"repeatCell": {"range": rng(0, NROW, 1, NCOL), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}, "fields": "userEnteredFormat.numberFormat"}})
    reqs.append({"repeatCell": {"range": rng(0, NROW, 0, 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in rrrows: reqs.append({"repeatCell": {"range": rng(i, i + 1, 2, 3), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in sorted(set(data) | set(tot)):
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
    S5_NCOL = 13
    for i in s5_rows:
        is_total = i in tot
        if is_total: reqs.append({"repeatCell": {"range": rng(i, i + 1, 0, S5_NCOL), "cell": {"userEnteredFormat": {"backgroundColor": rgb(0.90, 0.92, 0.95), "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
        else: reqs.append({"repeatCell": {"range": rng(i, i + 1, 0, S5_NCOL), "cell": {"userEnteredFormat": {"backgroundColor": rgb(1, 1, 1), "textFormat": {"bold": False, "italic": False, "foregroundColor": rgb(0, 0, 0), "fontSize": 10}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 1, S5_NCOL), "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}}, "fields": "userEnteredFormat.horizontalAlignment"}})
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 0, 1), "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}}, "fields": "userEnteredFormat.horizontalAlignment"}})
        reqs.append({"repeatCell": {"range": rng(i, i + 1, 1, S5_NCOL), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in s5_oppr: reqs.append({"repeatCell": {"range": rng(i, i + 1, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i in s5_bookr: reqs.append({"repeatCell": {"range": rng(i, i + 1, 5, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}}}, "fields": "userEnteredFormat.numberFormat"}})
    LAST_COL = max([th_ncol.get(i, NCOL) for i in th] + [NCOL])
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 230}, "fields": "pixelSize"}})
    for c in range(1, LAST_COL):
        reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": c, "endIndex": c + 1}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}})
    api("POST", BASE + ":batchUpdate", {"requests": reqs})
    merge_reqs = [{"mergeCells": {"range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": c0, "endColumnIndex": c1}, "mergeType": "MERGE_ALL"}} for r0, c0, c1 in merges]
    if merge_reqs: api("POST", BASE + ":batchUpdate", {"requests": merge_reqs})
    write_summary_block(sid)
    print(f"  {tab}: {NROW} rows, sections={len(sec)} + summary block")

def write_summary_block(sid):
    """Cream 'Business / Funding' summary block (cols L-O), generated from section data. Lists the
    Funding email workspaces (excl. Warm) + Total, then the channel rows (incl. SDR/Close)."""
    def ratio(a, b): return round(a / b) if b else ""
    sms1 = SMS_D[0]; sms2 = SMS_D[1]
    wa = SMS_D[2]
    warm = next((r for r in EMAIL_D if r[0].lower().startswith("warm")), ("Warm leads", 0, 0, 0, 0, 0, 0))
    wsrows = [r for r in EMAIL_D if not r[0].lower().startswith("warm")]
    def short(n): return n.split(" (")[0]
    blk = [[f"{TAB} — Business / Funding · {RANGE}", "", "", ""],
           ["WORKSPACE", "Email Sent", "Meeting Booked", "Meeting to Booked"]]
    ts = sum(x[1] for x in wsrows); tm = sum(x[4] for x in wsrows)
    for ws, sent, hr, opps, m, c, r in wsrows:
        blk.append([short(ws), sent, m, ratio(sent, m)])
    blk.append(["Total", ts, tm, ratio(ts, tm)]); blk.append(["", "", "", ""])
    for lbl, s, m in [("SMS Funding", sms1[1], sms1[3]),
                      ("SDR (Close)", CLOSE_D["dials"], CLOSE_D["meetings"]),
                      ("Warm Leads", warm[1], warm[4]),
                      ("WhatsApp Funding", wa[1], wa[3])]:
        blk.append([lbl, s, m, ratio(s, m)])
    blk.append(["", "", "", ""])
    blk.append(["SMS IPO", sms2[1], PREIPO_MTG, ratio(sms2[1], PREIPO_MTG)])
    blk.append(["WhatsApp PRE-IPO", 0, WA_PREIPO_MTG, ""])
    # SEC 125 / Tariffs / R&D Credit: offers not wired to a send/booking source yet -> hardcoded 0.
    for lbl in ["SEC 125", "Tariffs", "R&D Credit"]:
        blk.append([lbl, 0, 0, ""])
    api("PUT", f"{BASE}/values/{urllib.parse.quote(TAB)}!L4?valueInputOption=USER_ENTERED", {"values": blk})
    n = len(blk); r0 = 3; r1 = r0 + n; CREAM = rgb(0.988, 0.953, 0.804)
    def brr(a, b, c=11, d=15): return {"sheetId": sid, "startRowIndex": a, "endRowIndex": b, "startColumnIndex": c, "endColumnIndex": d}
    thin = {"style": "SOLID", "width": 1, "color": rgb(0.55, 0.45, 0.15)}
    total_row = r0 + 2 + len(wsrows)  # the "Total" row index
    reqs = [
        {"repeatCell": {"range": brr(r0, r1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "textFormat": {"fontSize": 10}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
        {"mergeCells": {"range": brr(r0, r0 + 1), "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": brr(r0, r0 + 1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True, "fontSize": 12}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": brr(r0 + 1, r0 + 2), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": brr(total_row, total_row + 1), "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat"}},
        {"repeatCell": {"range": brr(r0 + 2, r1, 12, 15), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"}}, "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 11, "endIndex": 12}, "properties": {"pixelSize": 220}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 12, "endIndex": 13}, "properties": {"pixelSize": 110}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 13, "endIndex": 14}, "properties": {"pixelSize": 125}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 15}, "properties": {"pixelSize": 140}, "fields": "pixelSize"}},
        {"updateBorders": {"range": brr(r0, r1), "top": thin, "bottom": thin, "left": thin, "right": thin, "innerHorizontal": thin, "innerVertical": thin}}]
    api("POST", BASE + ":batchUpdate", {"requests": reqs})

def mtd_page(ctx):
    add = ctx["add"]
    add([f"MONTH-TO-DATE REVOPS REPORT — Business Funding · {RANGE}"])
    add([f"MTD · {RANGE} · {BLEND_LABEL} · refreshes daily"]); add()
    ctx["email_table_s5"](EMAIL_D, f"1 · EMAIL + WARM LEADS — by workspace · MTD {RANGE}", EMAIL_CR); add()
    ctx["sms_wa_table"](SMS_D, f"2 · SMS + WHATSAPP — by channel · MTD {RANGE}"); add()
    ctx["close_table"](CLOSE_D, f"3 · CLOSE CRM — warm calling · MTD {RANGE}"); add()
    ctx["partner_table"](PARTNER_D, PARTNER_TOTAL, f"4 · BOOKINGS BY PARTNER · MTD {RANGE}")

print(f"Rendering MTD tab '{TAB}' for {REPORT_DATE} (window {S}..{E}, {BLEND_LABEL}) ...")
build_and_write(TAB, mtd_page)
print(f"OK — wrote tab '{TAB}'.")
