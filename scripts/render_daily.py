#!/usr/bin/env python3
"""Daily RevOps Report — DAILY tab, fully WAREHOUSE-DRIVEN (no hardcoded constants).

Reuses the proven formatting engine from deliverables/2026-06-26-revops-funnel-deep-dive/write_report_v2.py
but live-queries every section from the warehouse read API for a given REPORT_DATE, so it is the
renderer the `daily_report_sync` (10 PM ET) job calls. Writes ONE tab named for the date.

Usage:  python3 render_daily.py 2026-06-29 ["Tab Name"]   (tab name defaults to "Mon DD")
        python3 render_daily.py 2026-06-29 --dry          (print the data, do not write the sheet)

Sources (canonical, per /data-warehouse B.3):
 §1 Email = raw_pipeline_campaign_daily_metrics (sent/opps, COALESCE workspace attribution) +
            core.meeting (email meetings by workspace) + core.v_meeting_lead_type (cheap=cheap_mca).
 §2 SMS/WA = v_sms_workspace_funnel (sent/delivered/RR) + core.meeting SMS (meetings) + v_sms_dash_wa_daily
            (WA: sent/DELIVERED/FAILED/replies/meetings — WhatsApp shows delivered + fail%, not raw attempted,
            since ISKRA "sent" runs ~30-37% failed) + v_sms_sends_by_offer (Funding/IPO split, summary).
 §3 Close = core.call (dials/leads/connects, ET day) + core.meeting channel=Call (meetings).
 §4 Sending truth = core.v_sending_truth_pit (eligible_capacity=expected, actual_sends=actual),
            LAST COMPLETE field day (lagged ~2d by design — not on the 10 PM critical path).
 §5 Bookings by partner = core.meeting grouped by partner.
"""
import json, os, sys, subprocess, datetime, urllib.request, urllib.parse

SID = "1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
TOK = os.environ.get("GOOGLE_TOKEN", "/root/.config/mcp-google-sheets/token.json")
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SID}"

args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in sys.argv
REPORT_DATE = args[0] if args else "2026-06-29"
_d = datetime.date.fromisoformat(REPORT_DATE)
DAILY = REPORT_DATE
DAILY_TAB = args[1] if len(args) > 1 else _d.strftime("%B %-d")  # e.g. "June 29"

# ---- warehouse read API ----
WH_BASE = "https://renaissance-droplet.tailae5c80.ts.net"
def _wh_token():
    # BOX: read the read-API reader token directly from local allowed_tokens.txt (no ssh).
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

WS_ORDER = ["Funding 1 (Samuel)","Funding 2 (Ido)","Funding 3 (Leo)","Funding 4 (Sam)",
            "Funding 5 (Eyver)","Renaissance 1 (Instantly)","Max's workspace","Warm leads"]
SLUGS = "'renaissance-4','renaissance-5','prospects-power','koi-and-destroy','renaissance-2','renaissance-1','the-gatekeepers','warm-leads'"

# ============================ LIVE DATA ============================
def get_email():
    sent = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in wq(
        f"""WITH dims AS (SELECT DISTINCT ON (campaign_id) campaign_id, workspace_id
              FROM main.raw_pipeline_campaigns ORDER BY campaign_id, _loaded_at DESC)
            SELECT w.name, SUM(cd.sent), SUM(cd.unique_opportunities)
            FROM main.raw_pipeline_campaign_daily_metrics cd
            LEFT JOIN dims d USING(campaign_id)
            JOIN core.workspace w ON w.slug = COALESCE(d.workspace_id, cd.workspace_id)
            WHERE cd.date = DATE '{DAILY}' AND COALESCE(d.workspace_id, cd.workspace_id) IN ({SLUGS})
            GROUP BY 1""")}
    mtg = {r[0]: (int(r[1]), int(r[2]), int(r[3])) for r in wq(
        f"""SELECT m.workspace_canonical, COUNT(*),
              COUNT(*) FILTER (WHERE lt.lead_type='cheap_mca'),
              COUNT(*) FILTER (WHERE COALESCE(lt.lead_type,'normal_cold') <> 'cheap_mca')
            FROM core.meeting m LEFT JOIN core.v_meeting_lead_type lt USING(meeting_id)
            WHERE m.source='sheet' AND m.channel='Email' AND m.meeting_date = DATE '{DAILY}'
              AND m.is_duplicate_of IS NULL AND m.workspace_canonical IS NOT NULL
            GROUP BY 1""")}
    out = []
    for ws in WS_ORDER:
        s, o = sent.get(ws, (0, 0)); mm, c, r = mtg.get(ws, (0, 0, 0))
        out.append((ws, s, 0, o, mm, c, r))   # (ws, sent, hr(unused), opps, meetings, cheap, regular)
    return out

def get_sms_wa():
    # row shape: (label, sent, delivered, failed|None, replies, meetings). failed=None -> Fail% shows '—'
    # (SMS has no per-funnel failed column here; SMS deliverability lives in main.v_sms_deliverability_*).
    smsf = {r[0]: r for r in wq(f"SELECT sub_account, sent, delivered, replies FROM main.v_sms_workspace_funnel WHERE metric_date = DATE '{DAILY}'")}
    smsm = {r[0]: int(r[1]) for r in wq(
        f"SELECT sendivo_sub_account, COUNT(*) FROM core.meeting WHERE channel='SMS' AND meeting_date = DATE '{DAILY}' AND sendivo_sub_account IS NOT NULL AND is_duplicate_of IS NULL GROUP BY 1")}
    rows = []
    for sa in ["Renaissance 1", "Renaissance 2"]:
        f = smsf.get(sa)
        rows.append((sa, int(f[1]) if f else 0, int(f[2]) if f else 0, None, int(f[3]) if f else 0, smsm.get(sa, 0)))
    # WhatsApp: surface DELIVERED + FAIL% (not raw attempted). ISKRA "sent" runs ~30-37% failed.
    wa = wq(f"SELECT sent, delivered, failed, replies_total, meetings_booked FROM main.v_sms_dash_wa_daily WHERE metric_date = DATE '{DAILY}'")
    if wa:
        s,dl,fl,rp,mt = (int(wa[0][i] or 0) for i in range(5))
        rows.append(("WhatsApp (ISKRA)", s, dl, fl, rp, mt))
    else:
        rows.append(("WhatsApp (ISKRA)", 0, 0, 0, 0, 0))
    return rows

def get_close():
    c = wq(f"""SELECT COUNT(*) dials, COUNT(DISTINCT close_lead_id) leads,
                 COUNT(*) FILTER (WHERE duration_seconds >= 60) connects
               FROM core.call WHERE (occurred_at AT TIME ZONE 'America/New_York')::DATE = DATE '{DAILY}'""")
    d, l, cn = (int(c[0][0]), int(c[0][1]), int(c[0][2])) if c else (0,0,0)
    m = wq(f"SELECT COUNT(*) FROM core.meeting WHERE channel='Call' AND meeting_date = DATE '{DAILY}' AND is_duplicate_of IS NULL")
    return {"dials": d, "leads": l, "connects": cn, "meetings": int(m[0][0]) if m else 0}

def get_truth():
    # §4 sources from raw_account_truth_daily_actuals (has ALL infra incl Google; v_sending_truth_pit
    # is OTD-only). expected_sends ~ eligible capacity, actual_sends = real sends. 2-day lag by design.
    fd = wq("SELECT max(date) FROM main.raw_account_truth_daily_actuals WHERE date < current_date AND infra_type = 'OTD' GROUP BY date HAVING SUM(actual_sends) > 1000000 ORDER BY 1 DESC LIMIT 1")
    field_day = fd[0][0] if fd else (str(_d - datetime.timedelta(days=2)))
    rows = {(r[0], r[1]): (int(r[2] or 0), int(r[3] or 0)) for r in wq(
        f"""SELECT workspace_slug, infra_type, SUM(expected_sends), SUM(actual_sends)
            FROM main.raw_account_truth_daily_actuals WHERE date = DATE '{field_day}' AND infra_type IN ('OTD','Google')
              AND workspace_slug IN ({SLUGS}) GROUP BY 1,2""")}
    name = {r[0]: r[1] for r in wq(f"SELECT slug, name FROM core.workspace WHERE slug IN ({SLUGS})")}
    out = []
    for slug in ["koi-and-destroy","renaissance-2","prospects-power","renaissance-4","renaissance-5","the-gatekeepers","renaissance-1","warm-leads"]:
        oe, oa = rows.get((slug,"OTD"), (0,0)); ge, ga = rows.get((slug,"Google"), (0,0))
        out.append((name.get(slug, slug), oe, oa, ge, ga))
    return field_day, out

def get_partner():
    rows = wq(f"""SELECT COALESCE(partner_key, partner) p, COUNT(*) n FROM core.meeting
                  WHERE meeting_date = DATE '{DAILY}' AND is_duplicate_of IS NULL
                    AND COALESCE(partner_key, partner) IS NOT NULL GROUP BY 1 ORDER BY n DESC""")
    pr = [(r[0], int(r[1])) for r in rows]
    return pr, sum(n for _, n in pr)

def get_offer_summary():
    rows = {r[0]: int(r[1] or 0) for r in wq(f"SELECT offer, sent FROM main.v_sms_sends_by_offer WHERE metric_date = DATE '{DAILY}'")}
    return rows

_dj = os.environ.get("DAILY_DATA_JSON")
if _dj:
    _o = json.load(open(_dj))
    EMAIL_D = [tuple(x) for x in _o["EMAIL_D"]]
    SMS_D = [tuple(x) for x in _o["SMS_D"]]
    CLOSE_D = _o["CLOSE_D"]
    PARTNER_D = [tuple(x) for x in _o["PARTNER_D"]]; PARTNER_D_TOTAL = _o["PARTNER_D_TOTAL"]
    OFFER = _o.get("OFFER", {})
    SENDING_TRUTH_DAY, SENDING_TRUTH = get_truth()   # §4 from serving (2-day lagged; current)
else:
    EMAIL_D = get_email()
    SMS_D = get_sms_wa()
    CLOSE_D = get_close()
    SENDING_TRUTH_DAY, SENDING_TRUTH = get_truth()
    PARTNER_D, PARTNER_D_TOTAL = get_partner()
    OFFER = get_offer_summary()

if DRY:
    print(f"REPORT_DATE={DAILY}  TAB={DAILY_TAB}  field_day(§4)={SENDING_TRUTH_DAY}")
    print("§1 EMAIL:", json.dumps(EMAIL_D, indent=0))
    print("§2 SMS/WA:", SMS_D)
    print("§3 CLOSE:", CLOSE_D)
    print("§4 TRUTH:", SENDING_TRUTH)
    print("§5 PARTNER:", PARTNER_D, "total", PARTNER_D_TOTAL)
    print("offer split:", OFFER)
    sys.exit(0)

# ============================ formatting engine (verbatim from write_report_v2.py) ============================
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
creds = Credentials.from_authorized_user_file(TOK); creds.refresh(Request()); TOKEN = creds.token
def api(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))
def rr(hr, sent): return (hr/sent) if sent else "—"
def pctstr(n, m): return f"'{round(100.0*n/m)}%" if m else "'0%"
def kpi(sent, m): return round(sent/m) if m else "—"
def fpct(actual, expected): return (actual/expected) if expected else "—"
def rgb(r, g, b): return {"red": r, "green": g, "blue": b}

def build_and_write(tab, build_fn, gradient_truth=True):
    rows=[]; sec=[]; th=[]; tot=[]; rrrows=[]; merges=[]; strows=[]; data=[]
    th_ncol={}; row_ncol={}   # row_ncol = the ACTUAL column width of each section/data/total row, so
                              # background fills never stretch past a table (June-26 cleanliness).
    def add(r=None): rows.append(r or []); return len(rows)-1
    EMAIL_HDR_TOP=["Workspace","Sent","Opportunities","Meetings","KPI (sent/mtg)","Cheap","","Regular",""]
    SUB_HDR=["","","","","","#","%","#","%"]
    def email_table(data_rows, header_label):
        W=9
        si=add([header_label]); sec.append(si); row_ncol[si]=W
        hr_top=add(EMAIL_HDR_TOP); th.append(hr_top); hr_sub=add(SUB_HDR); th.append(hr_sub)
        merges.append((hr_top,5,7)); merges.append((hr_top,7,9)); th_ncol[hr_top]=th_ncol[hr_sub]=W
        ts=topp=tm=tc_=tr_=0
        for ws,sent,hr,opps,m,c,r in data_rows:
            ri=add([ws,sent,opps,m,kpi(sent,m),c,pctstr(c,m),r,pctstr(r,m)]); data.append(ri); row_ncol[ri]=W
            ts+=sent; topp+=opps; tm+=m; tc_+=c; tr_+=r
        ti=add(["TOTAL",ts,topp,tm,kpi(ts,tm),tc_,pctstr(tc_,tm),tr_,pctstr(tr_,tm)]); tot.append(ti); row_ncol[ti]=W
    def sms_wa_table(rows_data, header_label):
        # Show DELIVERED + FAIL% (not raw attempted). KPI = delivered/mtg (raw 'sent' includes ~30% failures
        # for WhatsApp -> sent/mtg was implausibly efficient). Fail% = failed/sent; '—' when failed unknown (SMS).
        W=7
        si=add([header_label]); sec.append(si); row_ncol[si]=W
        hr=add(["Channel / workspace","Sent","Delivered","Fail %","Human RR","Meetings","KPI (deliv/mtg)"]); th.append(hr); th_ncol[hr]=W
        ts=td=tf=th_=tm=0; any_fail=False
        def failpct(failed,sent): return (f"'{round(100.0*failed/sent)}%" if sent else "'0%") if failed is not None else "—"
        for label,sent,deliv,failed,reps,m in rows_data:
            ri=add([label,sent,deliv,failpct(failed,sent),rr(reps,deliv),m,kpi(deliv,m)]); rrrows.append(ri); data.append(ri); row_ncol[ri]=W
            ts+=sent; td+=deliv; th_+=reps; tm+=m
            if failed is not None: tf+=failed; any_fail=True
        ti=add(["TOTAL",ts,td,(f"'{round(100.0*tf/ts)}%" if (any_fail and ts) else "—"),rr(th_,td),tm,kpi(td,tm)]); tot.append(ti); rrrows.append(ti); row_ncol[ti]=W
    def truth_table(header_label):
        W=10
        si=add([header_label]); sec.append(si); row_ncol[si]=W
        hr_top=add(["Workspace","OTD","","","Google","","","Total","",""]); th.append(hr_top)
        hr_sub=add(["","Expected","Actual","%","Expected","Actual","%","Expected","Actual","%"]); th.append(hr_sub)
        merges.append((hr_top,1,4)); merges.append((hr_top,4,7)); merges.append((hr_top,7,10)); th_ncol[hr_top]=th_ncol[hr_sub]=W
        oe_t=oa_t=ge_t=ga_t=0
        def grp(e,a): return ("—","—","—") if (e==0 and a==0) else (e,a,fpct(a,e))
        for ws,oe,oa,ge,ga in SENDING_TRUTH:
            o1,o2,o3=grp(oe,oa); g1,g2,g3=grp(ge,ga); t1,t2,t3=grp(oe+ge,oa+ga)
            ri=add([ws,o1,o2,o3,g1,g2,g3,t1,t2,t3]); data.append(ri); strows.append(ri); row_ncol[ri]=W
            oe_t+=oe; oa_t+=oa; ge_t+=ge; ga_t+=ga
        sti=add(["TOTAL",oe_t,oa_t,fpct(oa_t,oe_t),ge_t,ga_t,fpct(ga_t,ge_t),oe_t+ge_t,oa_t+ga_t,fpct(oa_t+ga_t,oe_t+ge_t)])
        tot.append(sti); strows.append(sti); row_ncol[sti]=W
    def close_table(close, label):
        W=2
        si=add([label]); sec.append(si); row_ncol[si]=W
        hr=add(["Close CRM — metric","Value"]); th.append(hr); th_ncol[hr]=W
        d2m=f"'{(100.0*close['meetings']/close['dials']):.2f}%" if close['dials'] else "—"
        for r2 in [["Dials",close["dials"]],["Distinct leads dialed",close["leads"]],["Connects (≥60s real convo)",close["connects"]],
                   ["Meetings booked (call-sourced)",close["meetings"]],["Dial → meeting %",d2m]]:
            ri=add(r2); data.append(ri); row_ncol[ri]=W
    def partner_table(rows_data, total, label):
        W=2
        si=add([label]); sec.append(si); row_ncol[si]=W
        hr=add(["Partner","Meetings booked"]); th.append(hr); th_ncol[hr]=W
        for p,n in rows_data: ri=add([p,n]); data.append(ri); row_ncol[ri]=W
        ti=add(["TOTAL",total]); tot.append(ti); row_ncol[ti]=W
    build_fn(dict(add=add,email_table=email_table,sms_wa_table=sms_wa_table,truth_table=truth_table,close_table=close_table,partner_table=partner_table))

    meta=api("GET",BASE+"?fields=sheets(properties(sheetId,title),bandedRanges(bandedRangeId),conditionalFormats)")
    sh=next((s for s in meta["sheets"] if s["properties"]["title"]==tab), None)
    if sh is None:
        r=api("POST",BASE+":batchUpdate",{"requests":[{"addSheet":{"properties":{"title":tab,"gridProperties":{"rowCount":max(len(rows)+20,200),"columnCount":26}}}}]})
        sid=r["replies"][0]["addSheet"]["properties"]["sheetId"]; sh={"properties":{"sheetId":sid},"bandedRanges":[],"conditionalFormats":[]}
    else:
        sid=sh["properties"]["sheetId"]
    api("POST",f"{BASE}/values/{urllib.parse.quote(tab)}!A1:Z400:clear",{})
    api("POST",BASE+":batchUpdate",{"requests":[{"unmergeCells":{"range":{"sheetId":sid,"startRowIndex":0,"endRowIndex":400,"startColumnIndex":0,"endColumnIndex":26}}}]})
    api("PUT",f"{BASE}/values/{urllib.parse.quote(tab)}!A1?valueInputOption=USER_ENTERED",{"values":rows})
    NCOL=10; NROW=len(rows); WIDE=max(NROW+5,200)
    def rng(r0,r1,c0=0,c1=NCOL): return {"sheetId":sid,"startRowIndex":r0,"endRowIndex":r1,"startColumnIndex":c0,"endColumnIndex":c1}
    def fill(i,color,**tf): return {"repeatCell":{"range":rng(i,i+1,0,row_ncol.get(i,NCOL)),"cell":{"userEnteredFormat":{"backgroundColor":rgb(*color),"textFormat":tf}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}}
    reqs=[]
    for br in sh.get("bandedRanges",[]) or []: reqs.append({"deleteBanding":{"bandedRangeId":br["bandedRangeId"]}})
    for idx in range(len(sh.get("conditionalFormats",[]) or [])-1,-1,-1): reqs.append({"deleteConditionalFormatRule":{"sheetId":sid,"index":idx}})
    reqs.append({"unmergeCells":{"range":{"sheetId":sid,"startRowIndex":0,"endRowIndex":WIDE,"startColumnIndex":0,"endColumnIndex":26}}})
    reqs.append({"repeatCell":{"range":{"sheetId":sid,"startRowIndex":0,"endRowIndex":WIDE,"startColumnIndex":0,"endColumnIndex":26},"cell":{"userEnteredFormat":{"backgroundColor":rgb(1,1,1),"horizontalAlignment":"LEFT","verticalAlignment":"BOTTOM","wrapStrategy":"OVERFLOW_CELL","numberFormat":{"type":"TEXT"},"textFormat":{"bold":False,"italic":False,"fontSize":10,"foregroundColor":rgb(0,0,0)}}},"fields":"userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,numberFormat,textFormat)"}})
    reqs.append({"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"ROWS","startIndex":0,"endIndex":WIDE},"properties":{"pixelSize":21},"fields":"pixelSize"}})
    reqs.append({"repeatCell":{"range":rng(0,NROW,1,NCOL),"cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"#,##0"}}},"fields":"userEnteredFormat.numberFormat"}})
    reqs.append({"repeatCell":{"range":rng(0,NROW,0,1),"cell":{"userEnteredFormat":{"numberFormat":{"type":"TEXT"}}},"fields":"userEnteredFormat.numberFormat"}})
    for i in rrrows: reqs.append({"repeatCell":{"range":rng(i,i+1,4,5),"cell":{"userEnteredFormat":{"numberFormat":{"type":"PERCENT","pattern":"0.00%"}}},"fields":"userEnteredFormat.numberFormat"}})  # Human RR now col 4 (was 2; +Delivered,+Fail%)
    for i in strows:
        for c in (3,6,9): reqs.append({"repeatCell":{"range":rng(i,i+1,c,c+1),"cell":{"userEnteredFormat":{"numberFormat":{"type":"PERCENT","pattern":"0.0%"}}},"fields":"userEnteredFormat.numberFormat"}})
    for i in sorted(set(data)|set(tot)|set(strows)):
        reqs.append({"repeatCell":{"range":rng(i,i+1,1,NCOL),"cell":{"userEnteredFormat":{"horizontalAlignment":"RIGHT"}},"fields":"userEnteredFormat.horizontalAlignment"}})
        reqs.append({"repeatCell":{"range":rng(i,i+1,0,1),"cell":{"userEnteredFormat":{"horizontalAlignment":"LEFT"}},"fields":"userEnteredFormat.horizontalAlignment"}})
    reqs.append({"repeatCell":{"range":rng(0,1),"cell":{"userEnteredFormat":{"textFormat":{"bold":True,"fontSize":14}}},"fields":"userEnteredFormat.textFormat"}})
    reqs.append({"repeatCell":{"range":rng(1,2),"cell":{"userEnteredFormat":{"textFormat":{"italic":True,"foregroundColor":rgb(0.4,0.4,0.4),"fontSize":9}}},"fields":"userEnteredFormat.textFormat"}})
    for i in sec: reqs.append(fill(i,(0.85,0.89,0.95),bold=True,fontSize=11))
    for i in th:
        cw=th_ncol.get(i,NCOL)
        reqs.append({"repeatCell":{"range":rng(i,i+1,0,cw),"cell":{"userEnteredFormat":{"backgroundColor":rgb(0.20,0.23,0.27),"horizontalAlignment":"CENTER","verticalAlignment":"MIDDLE","wrapStrategy":"CLIP","textFormat":{"bold":True,"foregroundColor":rgb(1,1,1),"fontSize":10}}},"fields":"userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,wrapStrategy,textFormat)"}})
    for i in data: reqs.append(fill(i,(1,1,1),bold=False,italic=False,foregroundColor=rgb(0,0,0),fontSize=10))
    for i in tot: reqs.append(fill(i,(0.90,0.92,0.95),bold=True))
    reqs.append({"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":0,"endIndex":1},"properties":{"pixelSize":230},"fields":"pixelSize"}})
    LAST_COL=max([th_ncol.get(i,NCOL) for i in th]+[NCOL])
    for c in range(1,LAST_COL):
        reqs.append({"updateDimensionProperties":{"range":{"sheetId":sid,"dimension":"COLUMNS","startIndex":c,"endIndex":c+1},"properties":{"pixelSize":120},"fields":"pixelSize"}})
    if gradient_truth and len(strows)>=2:
        d0=strows[0]; d1=strows[-2]+1
        def Rg(c): return {"sheetId":sid,"startRowIndex":d0,"endRowIndex":d1,"startColumnIndex":c,"endColumnIndex":c+1}
        reqs.append({"addConditionalFormatRule":{"index":0,"rule":{"ranges":[Rg(3),Rg(6),Rg(9)],"gradientRule":{"minpoint":{"color":rgb(0.91,0.40,0.40),"type":"NUMBER","value":"0"},"maxpoint":{"color":rgb(1,1,1),"type":"NUMBER","value":"1"}}}}})
    api("POST",BASE+":batchUpdate",{"requests":reqs})
    merge_reqs=[{"mergeCells":{"range":{"sheetId":sid,"startRowIndex":r0,"endRowIndex":r0+1,"startColumnIndex":c0,"endColumnIndex":c1},"mergeType":"MERGE_ALL"}} for r0,c0,c1 in merges]
    if merge_reqs: api("POST",BASE+":batchUpdate",{"requests":merge_reqs})
    write_summary_block(sid)   # the cream right-side "Business / Funding" block (cols L-O), generated
    print(f"  {tab}: {NROW} rows, sections={len(sec)} headers={len(th)} totals={len(tot)} + summary block")

def write_summary_block(sid):
    """The cream right-side 'Business / Funding' summary block (cols L-O), GENERATED from the section
    data so it propagates to every tab (was hand-made on June 26 → never propagated)."""
    def short(ws): return ws.split(" (")[0]
    def ratio(a,b): return round(a/b) if b else ""
    sms1=next((x for x in SMS_D if x[0]=="Renaissance 1"),("",0,0,0))
    sms2=next((x for x in SMS_D if x[0]=="Renaissance 2"),("",0,0,0))
    wa=next((x for x in SMS_D if "WhatsApp" in x[0]),("",0,0,0))
    warm=next((r for r in EMAIL_D if r[0].lower().startswith("warm")),("Warm leads",0,0,0,0,0,0))
    wsrows=[r for r in EMAIL_D if not r[0].lower().startswith("warm")]
    blk=[[f"{DAILY_TAB} — Business / Funding · {DAILY}","","",""],
         ["WORKSPACE","Email Sent","Meeting Booked","Meeting to Booked"]]
    ts=tm=0
    for ws,sent,hr,opps,m,c,r in wsrows:
        blk.append([short(ws),sent,m,ratio(sent,m)]); ts+=sent; tm+=m
    blk.append(["Total",ts,tm,ratio(ts,tm)]); blk.append(["","","",""])
    for lbl,s,m in [("SMS Funding",sms1[1],sms1[3]),("SDR (Close)",CLOSE_D["dials"],CLOSE_D["meetings"]),
                    ("Warm Leads",warm[1],warm[4]),("WhatsApp Funding",wa[1],wa[3])]:
        blk.append([lbl,s,m,ratio(s,m)])
    blk.append(["","","",""])
    blk.append(["SMS IPO",sms2[1],sms2[3],ratio(sms2[1],sms2[3])])
    for lbl in ["WhatsApp PRE-IPO","SEC 125","Tariffs","R&D Credit"]:
        blk.append([lbl,0,0,""])
    api("PUT",f"{BASE}/values/{urllib.parse.quote(DAILY_TAB)}!L4?valueInputOption=USER_ENTERED",{"values":blk})
    n=len(blk); r0=3; r1=r0+n; CREAM=rgb(0.988,0.953,0.804)
    def br(a,b,c=11,d=15): return {"sheetId":sid,"startRowIndex":a,"endRowIndex":b,"startColumnIndex":c,"endColumnIndex":d}
    thin={"style":"SOLID","width":1,"color":rgb(0.55,0.45,0.15)}
    reqs=[
     {"repeatCell":{"range":br(r0,r1),"cell":{"userEnteredFormat":{"backgroundColor":CREAM,"textFormat":{"fontSize":10}}},"fields":"userEnteredFormat(backgroundColor,textFormat)"}},
     {"mergeCells":{"range":br(r0,r0+1),"mergeType":"MERGE_ALL"}},
     {"repeatCell":{"range":br(r0,r0+1),"cell":{"userEnteredFormat":{"backgroundColor":CREAM,"horizontalAlignment":"CENTER","textFormat":{"bold":True,"fontSize":12}}},"fields":"userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
     {"repeatCell":{"range":br(r0+1,r0+2),"cell":{"userEnteredFormat":{"backgroundColor":CREAM,"horizontalAlignment":"CENTER","textFormat":{"bold":True}}},"fields":"userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
     {"repeatCell":{"range":br(r0+9,r0+10),"cell":{"userEnteredFormat":{"textFormat":{"bold":True}}},"fields":"userEnteredFormat.textFormat"}},
     {"repeatCell":{"range":br(r0+2,r1,12,15),"cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"#,##0"},"horizontalAlignment":"RIGHT"}},"fields":"userEnteredFormat(numberFormat,horizontalAlignment)"}},
     {"updateBorders":{"range":br(r0,r1),"top":thin,"bottom":thin,"left":thin,"right":thin,"innerHorizontal":thin,"innerVertical":thin}}]
    api("POST",BASE+":batchUpdate",{"requests":reqs})

def daily(ctx):
    add=ctx["add"]
    add([f"DAILY REVOPS REPORT — Business Funding · {DAILY}"]); add([os.environ.get("DAILY_SUBTITLE", f"Daily · {DAILY} · PILOT (warehouse-driven)")]); add()
    ctx["email_table"](EMAIL_D, f"1 · EMAIL + WARM LEADS — by workspace · day {DAILY}"); add()
    ctx["sms_wa_table"](SMS_D, f"2 · SMS + WHATSAPP — by channel · day {DAILY}"); add()
    ctx["close_table"](CLOSE_D, f"3 · CLOSE CRM — warm calling · day {DAILY}"); add()
    ctx["truth_table"](f"4 · SENDING VOLUME TRUTH — expected vs actual sends by workspace × infra · OTD + Google · last complete field day {SENDING_TRUTH_DAY}"); add()
    ctx["partner_table"](PARTNER_D, PARTNER_D_TOTAL, f"5 · BOOKINGS BY PARTNER · day {DAILY}")

print(f"Rendering DAILY tab '{DAILY_TAB}' for {DAILY} ...")
build_and_write(DAILY_TAB, daily)
print(f"OK — wrote tab '{DAILY_TAB}'.")
