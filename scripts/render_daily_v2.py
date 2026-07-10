#!/usr/bin/env python3
"""Daily RevOps Report v2 — the deep RevOps data layer. D-1-FINAL, WAREHOUSE-ONLY, freshness-gated.

Renders YESTERDAY (the previous complete ET day) ONCE, after the nightly promote is confirmed by
DATA FRESHNESS (never a clock, never a snapshot-id timestamp). Every cell reads the warehouse query
API — no live Instantly / Sendivo / comms calls, no heal cron, no `—` placeholders, no caveat wall.

WHY (audit deliverables/2026-07-08-mof-orchestrator/DAILY-REPORT-AUDIT.md + handoff 2026-07-09-daily-
report-v2-build.md): the old renderer rendered day D *on* day D from a D-1 warehouse + live APIs, then
tried to heal — the heal raced the nightly promote and silently skipped, freezing ~30 empties + a
9-bullet caveat wall + 4 wrong cells. v2 fixes the DECISION: render D-1 once, gated, warehouse-only.

FRESHNESS GATE (flip-transparent — survives the MotherDuck migration): asserts the ACTUAL sources for
day D are present + non-degenerate in the served snapshot, not that a timestamp advanced. See gate_ready().

SOURCE MAP (all verified EXACT for 2026-07-08 — see deliverables/2026-07-09-daily-report-v2/DESIGN-AND-
SOURCE-MAP.md):
  §1 Email sent/opps/replies  -> main.raw_instantly_workspace_analytics_daily  (WORKSPACE grain,
                                 Instantly-native — carries opps=1,320 EXACT, survives deleted campaigns;
                                 the campaign-grain fact undercounts, flag #24 — NOT used here).
  §1 meetings + cheap/regular -> core.v_meeting_canonical (channel=Email, workspace_name = the ONE
                                 mapper via ws_alias) LEFT JOIN main.raw_im_bookings.lead_type.
  §1b infra SENDS (Google/OTD)-> core.sending_account_daily GROUP BY esp (zero residual at D-1).
  §2 SMS sent/cost            -> main.raw_sendivo_billing_daily (sms_fee_qty / total_spend).
  §2 SMS delivered/failed     -> main.v_sms_campaign_performance.  Replies/opt-outs -> raw_sendivo_inbound.
  §2 SMS opps (Qwen)          -> derived.sms_reply_is_positive_qwen.  WA -> main.v_sms_dash_wa_daily.
  §3 Close                    -> core.call + core.v_meeting_canonical channel=Call.
  §4 Actual sends by ws x ESP -> core.sending_account_daily.  (Expected/fulfillment WIPED — census
                                 mutates retroactively; returns when the census is frozen, gap #4.)
  §5 partner                  -> core.v_meeting_canonical GROUP BY partner (offer-agnostic).
  §6 reply-time SLA           -> core.sla_reply_time / core.sla_reply_time_smswa (canonical facts).
  Pre-IPO meetings            -> core.v_meeting_canonical offer='Pre-IPO'.
  Offer summary               -> core.v_meeting_canonical GROUP BY offer/lane (meetings only for now).

WIPED (100%-or-wipe, both gated on additive gaps, never a caveat): §1b per-infra RATES (no clean
campaign->infra bridge, gap #6 — per-infra SENDS kept, 100%); §4 Expected/fulfillment (census-freeze
gap #4 — Actual-by-infra kept, 100%).

Usage:
  render_daily_v2.py                         render YESTERDAY-ET's tab, gated (the production path)
  render_daily_v2.py 2026-07-08 ["Jul 8"]    explicit date / tab
  render_daily_v2.py 2026-07-08 --dry        print the data, do not write the sheet (gate still checked)
  render_daily_v2.py 2026-07-08 --gate-only  evaluate the freshness gate; exit 0=READY / 3=NOT-READY
  render_daily_v2.py 2026-07-08 --shadow     write to a shadow tab '<tab> ·v2' (verification, no clobber)
  render_daily_v2.py 2026-07-08 --force      render even if the gate is NOT ready (manual override)
"""
import json, os, sys, datetime, statistics, urllib.request, urllib.parse, collections

# ------------------------------ config ------------------------------
SID = "1vL77hVTY3P5_0e-K34qjWWpes_hymx4XiC630llyF34"
TOK = os.environ.get("GOOGLE_TOKEN", "/root/.config/mcp-google-sheets/token.json")
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SID}"

_flags = {a for a in sys.argv[1:] if a.startswith("--")}
args = [a for a in sys.argv[1:] if not a.startswith("--")]
DRY = "--dry" in _flags
GATE_ONLY = "--gate-only" in _flags
SHADOW = "--shadow" in _flags
FORCE = "--force" in _flags

# ET timezone (one object for the whole file — §6 clock math + the D-1 default share it).
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
    _today_et = datetime.datetime.now(ET_TZ).date()
except Exception:
    ET_TZ = None
    _today_et = datetime.date.today()

# DEFAULT = YESTERDAY ET (D-1-FINAL). The report is the previous COMPLETE day; same-day belongs to the
# booking page. An explicit arg (manual/pilot/backfill) overrides.
REPORT_DATE = args[0] if args else (_today_et - datetime.timedelta(days=1)).isoformat()
_d = datetime.date.fromisoformat(REPORT_DATE)
DAILY = REPORT_DATE
DAILY_TAB = args[1] if len(args) > 1 else _d.strftime("%b %-d")   # "Jul 8"
if SHADOW:
    DAILY_TAB = DAILY_TAB + " ·v2"

# ------------------------------ workspace identity ------------------------------
# Roster from the canonical registry (config/daily_report_sources.json) when on-box; else a hardcoded
# fallback identical to it (8 report workspaces, render order). slug == Instantly key slug ==
# sending_account_daily.workspace_slug == sla_reply_time.workspace_slug — ONE slug end-to-end.
_ROSTER_FALLBACK = [
    ("renaissance-4",   "Funding 1 (Samuel)"), ("renaissance-5",   "Funding 2 (Ido)"),
    ("prospects-power", "Funding 3 (Leo)"),    ("koi-and-destroy", "Funding 4 (Sam)"),
    ("renaissance-2",   "Funding 5 (Eyver)"),  ("renaissance-1",   "Renaissance 1 (Instantly)"),
    ("the-gatekeepers", "Max's workspace"),    ("warm-leads",      "Warm leads"),
]
try:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from core import daily_report_sources as REG
    WS = REG.workspace_roster()
    if not WS or len(WS) < 8:
        raise RuntimeError("registry roster degenerate")
except Exception as _e:
    print(f"WARN registry roster unavailable ({_e}); using hardcoded fallback roster", file=sys.stderr)
    WS = list(_ROSTER_FALLBACK)
SLUG2NAME = dict(WS)
REPORT_SLUGS = [s for s, _ in WS]
SLUGS_SQL = ",".join("'" + s + "'" for s in REPORT_SLUGS)

# Sendivo sub-accounts (stable): 12720 Funding SMS · 13922 Pre-IPO SMS · 14603 RG3 webform SMS.
SUB_REN1, SUB_REN2, SUB_REN3 = 12720, 13922, 14603

# channel_status registry [handoff §5]: SMS is deliberately PAUSED (TCPA/DNC scrub) — near-zero SMS is
# REAL, not a broken feed. Rendered as a header label so a paused lane never reads as an empty/error.
# (Promote to a warehouse table when ops flips are wired — gap #8.)
CHANNEL_STATUS = {"sms": "PAUSED since 2026-07-07 (TCPA/DNC scrub) — near-zero volume is expected, not a broken feed"}

# free-text booking workspace_name -> canonical roster display name. Applied to the ALREADY-CORRECT
# core.v_meeting_canonical.workspace_name (the single mapper) — NOT the old ws_from_booking campaign-
# operator override that drifted from canonical (F2 49 vs 47 / Warm 18 vs 20). 'Tariffs' -> None
# (handled as its own lane, not a Funding desk).
def ws_alias(raw):
    t = (raw or "").strip().lower()
    if not t:
        return None
    if t.startswith("funding 1") or t == "f1":                        return "Funding 1 (Samuel)"
    if t.startswith("funding 2") or t == "f2":                        return "Funding 2 (Ido)"
    if t.startswith("funding 3") or t == "f3":                        return "Funding 3 (Leo)"
    if t.startswith("funding 4") or t == "f4":                        return "Funding 4 (Sam)"
    if t.startswith("funding 5") or t == "f5":                        return "Funding 5 (Eyver)"
    if t.startswith("warm"):                                          return "Warm leads"
    if t.startswith("max") or "gatekeeper" in t:                      return "Max's workspace"
    if ("renaissance 1" in t or t in ("r1", "instantly") or "sendivo" in t): return "Renaissance 1 (Instantly)"
    return None

# ------------------------------ warehouse read API ------------------------------
WH_BASE = os.environ.get("WAREHOUSE_API_BASE", "https://renaissance-droplet.tailae5c80.ts.net")
def _wh_token():
    path = os.environ.get("WAREHOUSE_TOKENS_FILE", "/opt/duckdb/allowed_tokens.txt")
    if os.path.exists(path):
        for line in open(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[1] == "cc-service-reader":
                return parts[0].strip()
    raise RuntimeError("cc-service-reader token not found in " + path)
WH_TOKEN = _wh_token()
_SNAPSHOT_ID = None   # captured from the first query response; stamped on the tab for provenance
def wq(sql):
    global _SNAPSHOT_ID
    req = urllib.request.Request(WH_BASE + "/query", data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": f"Bearer {WH_TOKEN}", "Content-Type": "application/json"}, method="POST")
    resp = json.load(urllib.request.urlopen(req, timeout=180))
    if resp.get("truncated"):
        raise RuntimeError(f"warehouse query TRUNCATED at {resp.get('row_count')} rows — refusing partial data")
    if _SNAPSHOT_ID is None:
        _SNAPSHOT_ID = resp.get("snapshot_id")
    return resp["rows"]

def _scalar(sql, default=0):
    r = wq(sql)
    return r[0][0] if (r and r[0] and r[0][0] is not None) else default

# ------------------------------ google sheets api ------------------------------
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as _GReq
_g_creds = Credentials.from_authorized_user_file(TOK)
try:
    _g_creds.refresh(_GReq())
except Exception as _e:
    print(f"WARN initial Google token refresh failed ({_e}); will retry lazily", file=sys.stderr)
def _gtok():
    if not _g_creds.valid:
        _g_creds.refresh(_GReq())
    return _g_creds.token
def api(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Authorization": f"Bearer {_gtok()}", "Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req))

# ============================ FRESHNESS GATE ============================
def gate_ready(date):
    """Return (ok, reasons[]). The report renders day D ONLY when every nightly-fed source it reads is
    COMPLETE + non-degenerate for D in the served snapshot. This is the OUTCOME (the actual data),
    never a clock/snapshot-id timestamp — so it is transparent across the MotherDuck flip (post-07-12).
    A today-in-progress day fails (email sends=0); a genuinely complete D-1 passes."""
    reasons = []
    def chk(label, ok, detail=""):
        reasons.append(("OK " if ok else "MISS") + f" {label}" + (f" — {detail}" if detail else ""))
        return ok
    ok = True
    # 1) Email workspace analytics: all 8 report workspaces present for D AND non-zero sends (the
    #    in-progress current day reads 0 sends until its nightly lands).
    try:
        r = wq(f"""SELECT count(*) ws, coalesce(sum(sent),0) sent
                   FROM main.raw_instantly_workspace_analytics_daily
                   WHERE date=DATE '{date}' AND workspace_slug IN ({SLUGS_SQL})""")
        nws, sent = (int(r[0][0] or 0), int(r[0][1] or 0)) if r else (0, 0)
        ok &= chk("email workspace analytics", nws >= len(WS) and sent > 0, f"{nws}/{len(WS)} ws, sent={sent:,}")
    except Exception as e:
        ok &= chk("email workspace analytics", False, str(e))
    # 2) Account-grain send truth (feeds §1b infra split + §4 actual).
    try:
        n = int(_scalar(f"SELECT count(*) FROM core.sending_account_daily WHERE date=DATE '{date}'"))
        ok &= chk("sending_account_daily", n > 0, f"{n} account-rows")
    except Exception as e:
        ok &= chk("sending_account_daily", False, str(e))
    # 3) SMS delivered (D-1 /sms/logs) — proves the nightly per-message carrier pull landed.
    try:
        n = int(_scalar(f"SELECT count(*) FROM main.v_sms_campaign_performance WHERE metric_date=DATE '{date}'"))
        ok &= chk("v_sms_campaign_performance (delivered)", n > 0, f"{n} rows")
    except Exception as e:
        ok &= chk("v_sms_campaign_performance (delivered)", False, str(e))
    # 4) SLA fact rebuilt through D (nightly SLA build ran on the promoted snapshot).
    try:
        mx = _scalar("SELECT CAST(max(clock_open_date) AS VARCHAR) FROM core.sla_reply_time", "")
        ok &= chk("sla_reply_time freshness", bool(mx) and str(mx)[:10] >= date, f"max clock_open={mx}")
    except Exception as e:
        ok &= chk("sla_reply_time freshness", False, str(e))
    return ok, reasons

# ============================ DATA (warehouse-only) ============================
def get_email(date):
    """§1 per-ws (name, sent, hr_unused, opps, meetings, cheap, regular). Sends/opps/human-replies from
    the workspace-grain Instantly-native store (opps EXACT to Instantly final, survives deleted
    campaigns); meetings + cheap/regular from the canonical meeting view (one mapper) joined to
    raw_im_bookings.lead_type. Also returns a reconciliation delta so a mapper miss can never silently
    drop meetings."""
    # sent / opps / human-replies per workspace
    an = {}
    for r in wq(f"""SELECT workspace_slug, coalesce(sent,0), coalesce(opportunities,0),
                           coalesce(unique_replies,0)
                    FROM main.raw_instantly_workspace_analytics_daily
                    WHERE date=DATE '{date}' AND workspace_slug IN ({SLUGS_SQL})"""):
        an[r[0]] = (int(r[1]), int(r[2]), int(r[3]))
    # meetings + cheap/regular per workspace_name (canonical view), lead_type from im_bookings
    mtg = collections.defaultdict(lambda: [0, 0, 0])   # roster_name -> [meetings, cheap, regular]
    rows = wq(f"""
      WITH m AS (SELECT workspace_name, lower(lead_email) k
                 FROM core.v_meeting_canonical WHERE meeting_date=DATE '{date}' AND channel='Email'),
      lt AS (SELECT lower(coalesce(nullif(email,''), phone)) k, max(lower(coalesce(lead_type,''))) lead_type
             FROM main.raw_im_bookings
             WHERE _snapshot_date=(SELECT max(_snapshot_date) FROM main.raw_im_bookings) AND offer='Funding'
             GROUP BY 1)
      SELECT m.workspace_name,
             count(*) meetings,
             count(*) FILTER (WHERE lt.lead_type='cheap') cheap,
             count(*) FILTER (WHERE lt.lead_type IS NOT NULL AND lt.lead_type<>'' AND lt.lead_type<>'cheap') regular
      FROM m LEFT JOIN lt ON lt.k=m.k GROUP BY 1""")
    tariffs_m = 0; total_email_m = 0; unmapped = []
    for wsname, m, c, r in rows:
        m, c, r = int(m or 0), int(c or 0), int(r or 0)
        total_email_m += m
        nm = ws_alias(wsname)
        if nm is None:
            if (wsname or "").strip().lower() == "tariffs":
                tariffs_m += m
            else:
                unmapped.append((wsname, m))
            continue
        agg = mtg[nm]; agg[0] += m; agg[1] += c; agg[2] += r
    if unmapped:
        print(f"WARN get_email: {sum(m for _,m in unmapped)} email meetings on {date} did not map to a "
              f"roster workspace and are NOT in §1 rows: {unmapped}", file=sys.stderr)
    out = []
    for slug, name in WS:
        sent, opps, hr = an.get(slug, (0, 0, 0))
        mm, c, r = mtg.get(name, [0, 0, 0])
        out.append((name, sent, hr, opps, mm, c, r))
    return {"rows": out, "tariffs_m": tariffs_m, "total_email_m": total_email_m, "unmapped": unmapped}

def get_infra_sends(date):
    """§1b per-INFRASTRUCTURE sends (Google/OTD), summed across workspaces. RATES wiped (no 100% campaign
    ->infra bridge — gap #6); sends are 100% at D-1 (account grain resolves the full Instantly total). An
    'Other' row appears ONLY if a non-OTD/Google ESP (e.g. Outlook) actually sent, so §1b TOTAL always
    reconciles to §1 total sent — never silently omits volume (today Outlook sends = 0)."""
    out = collections.OrderedDict([("Google (reseller)", 0), ("OTD", 0)])
    espmap = {"google": "Google (reseller)", "otd": "OTD"}
    other = 0
    # Scope to the 8 REPORT workspaces (like §1/§4) — sending_account_daily also carries non-roster
    # infra slugs (tariffs / section-125 / erc / equinox / renaissance-3 …); unscoped, any of them
    # sending email would silently inflate §1b past §1's total, breaking the §1b==§1 contract.
    for r in wq(f"""SELECT esp, coalesce(sum(actual_sends),0) FROM core.sending_account_daily
                    WHERE date=DATE '{date}' AND workspace_slug IN ({SLUGS_SQL}) GROUP BY 1"""):
        lbl = espmap.get((r[0] or "").lower())
        if lbl:
            out[lbl] += int(r[1] or 0)
        else:
            other += int(r[1] or 0)
    rows = list(out.items())
    if other:
        rows.append(("Other (Outlook / unsplit)", other))
    return rows

def get_offer_summary(date):
    """Offer/lane split of MEETINGS (the offer dimension is 100% for meetings today; sends/opps offer-
    split activates when campaign names carry offer tokens — gap #7). Business Funding · Pre-IPO ·
    Tariffs (a workspace_name lane), all channels; only lanes with activity render."""
    rows = wq(f"""
      SELECT CASE WHEN workspace_name='Tariffs' THEN 'Tariffs'
                  WHEN offer IS NULL OR offer='' THEN '(unmapped)' ELSE offer END lane,
             channel, count(*) n
      FROM core.v_meeting_canonical WHERE meeting_date=DATE '{date}' GROUP BY 1,2""")
    lanes = collections.defaultdict(lambda: collections.defaultdict(int))
    order = []
    for lane, ch, n in rows:
        if lane not in lanes:
            order.append(lane)
        lanes[lane][ch] += int(n or 0)
    # stable, meaningful order
    pref = ["Business Funding", "Pre-IPO", "Tariffs"]
    order = [l for l in pref if l in lanes] + [l for l in order if l not in pref]
    out = []
    for lane in order:
        chd = lanes[lane]
        out.append((lane, sum(chd.values()), chd.get("Email", 0), chd.get("SMS", 0),
                    chd.get("Call", 0), chd.get("WhatsApp", 0)))
    return out

def sms_opps_by_sub(date):
    """{sub_account_name: opps|None}. Opps = Qwen positive-intent over human (non-opt-out) replies.
    HONEST-LAG GATE (100%-or-wipe): a sub's opps render only when >=90% of its day's human replies carry
    a verdict; else None. At D-1-FINAL the classifier is typically caught up, so this is normally a
    number, not '—'."""
    out = {}
    try:
        rows = wq(f"""
          WITH human AS (
            SELECT sub_account_name AS sub, count(*) FILTER (WHERE NOT is_opt_out) AS human
            FROM main.raw_sendivo_inbound WHERE CAST(received_at AS DATE)=DATE '{date}' GROUP BY 1),
          cls AS (
            SELECT i.sub_account_name AS sub,
                   count(*) FILTER (WHERE NOT i.is_opt_out) AS classified,
                   count(*) FILTER (WHERE q.is_positive AND NOT i.is_opt_out) AS opps
            FROM derived.sms_reply_is_positive_qwen q
            JOIN main.raw_sendivo_inbound i ON i.inbound_message_id=q.reply_id
            WHERE CAST(i.received_at AS DATE)=DATE '{date}' GROUP BY 1)
          SELECT h.sub, h.human, COALESCE(c.classified,0), COALESCE(c.opps,0)
          FROM human h LEFT JOIN cls c ON c.sub=h.sub""")
        for sub, human, classified, opps in rows:
            human, classified, opps = int(human or 0), int(classified or 0), int(opps or 0)
            if human == 0:
                out[sub] = 0
            elif classified >= 0.9 * human:
                out[sub] = opps
            else:
                out[sub] = None
                print(f"WARN sms_opps_by_sub: '{sub}' {date} only {classified}/{human} classified (<90%) "
                      "— Opps renders '—'", file=sys.stderr)
    except Exception as e:
        print(f"WARN sms_opps_by_sub failed {date}: {e}", file=sys.stderr)
    return out

def get_meetings_co(date):
    """{(channel_lower, is_preipo 0/1): n} from the canonical view — the ONE meeting split feeding §2
    (SMS Funding vs Pre-IPO), §3 (call), and the Pre-IPO yellow rows, so the SAME meeting is never
    counted in two lanes (the bug where all SMS-channel meetings, incl. the 3 Pre-IPO, landed on the
    Ren1 Funding row). is_preipo=1 -> offer='Pre-IPO'; 0 -> everything else (Business Funding, incl. the
    Tariffs workspace lane)."""
    out = collections.defaultdict(int)
    for ch, pre, n in wq(f"""SELECT lower(channel) ch, CASE WHEN offer='Pre-IPO' THEN 1 ELSE 0 END pre,
                                    count(*) n
                             FROM core.v_meeting_canonical WHERE meeting_date=DATE '{date}' GROUP BY 1,2"""):
        out[((ch or ""), int(pre or 0))] = int(n or 0)
    return dict(out)

def webform_fills(date):
    """RG3 (Ren3) completed apply-now web-form fills, ET day — the D-1 warehouse mirror (complete at
    D-1, unlike same-day). Deduped by prospect."""
    try:
        return int(_scalar(f"""SELECT count(DISTINCT lower(coalesce(nullif(email,''), prospect_number)))
            FROM main.raw_comms_lead_application
            WHERE (created_at AT TIME ZONE 'America/New_York')::date = DATE '{date}'"""))
    except Exception as e:
        print(f"WARN webform_fills failed {date}: {e}", file=sys.stderr)
        return 0

def get_sms_wa(date, co):
    """§2 rows (label, sent, delivered|None, failed|None, replies_total, opt_outs|None, opps|None,
    meetings, cost|None). All warehouse-only. At D-1-FINAL every source is complete, so delivered/opps
    are numbers (100%-or-wipe still returns None on a genuinely partial day, never a guess). `co` =
    get_meetings_co() so the SMS Funding / Pre-IPO / WA meeting counts partition cleanly."""
    # SMS sent + cost (billing)
    bill = {}
    for r in wq(f"""SELECT sub_account_id, sms_fee_qty, total_spend FROM main.raw_sendivo_billing_daily
                    WHERE metric_date=DATE '{date}'"""):
        bill[int(r[0])] = (int(r[1] or 0), float(r[2] or 0.0))
    # delivered/failed
    perf = {}
    for r in wq(f"""SELECT sub_account_name, sum(sent), sum(delivered), sum(failed)
                    FROM main.v_sms_campaign_performance
                    WHERE metric_date=DATE '{date}' AND sub_account_name IS NOT NULL GROUP BY 1"""):
        perf[r[0]] = (int(r[1] or 0), int(r[2] or 0), int(r[3] or 0))
    # replies / opt-outs
    inb = {r[0]: (int(r[1]), int(r[2])) for r in wq(
        f"""SELECT sub_account_name, count(*), count(*) FILTER (WHERE is_opt_out)
            FROM main.raw_sendivo_inbound WHERE CAST(received_at AS DATE)=DATE '{date}' GROUP BY 1""")}
    opps = sms_opps_by_sub(date)
    sms_funding_mtg = co.get(("sms", 0), 0)      # Ren1 Funding SMS meetings (excludes Pre-IPO)
    sms_preipo_mtg = co.get(("sms", 1), 0)       # Ren2 Pre-IPO SMS meetings
    wa_mtg = co.get(("whatsapp", 0), 0)          # WA Funding meetings
    def deliv_failed(sub_name, billed):
        p = perf.get(sub_name)
        if p is None:
            if billed:
                print(f"WARN get_sms_wa: no delivered row for '{sub_name}' {date} (billed {billed})", file=sys.stderr)
            return None, None
        vsent, vdeliv, vfail = p
        if billed and abs(vsent - billed) > 0.05 * billed:   # partial-load tripwire (100%-or-wipe)
            print(f"WARN get_sms_wa: delivered sent={vsent} diverges >5% from billed {billed} for "
                  f"'{sub_name}' {date}; Delivered/Fail render '—'", file=sys.stderr)
            return None, None
        return vdeliv, vfail
    def cost_of(sub_id, billed):
        row = bill.get(sub_id)
        if row is None:
            return None
        qty, usd = row
        if billed and abs(qty - billed) > 0.05 * billed:
            return None
        return usd
    def sub_row(label, sub_name, sub_id, mtgs):
        s = bill.get(sub_id, (0, 0.0))[0]
        d, f = deliv_failed(sub_name, s)
        rt, ro = inb.get(sub_name, (0, 0))
        o = opps.get(sub_name, 0 if rt == 0 else None)
        return (label, s, d, f, rt, ro, o, mtgs, cost_of(sub_id, s))
    rows = [
        sub_row("Renaissance 1 (SMS)", "Renaissance 1", SUB_REN1, sms_funding_mtg),
        sub_row("Renaissance 2 (SMS · Pre-IPO)", "Renaissance 2", SUB_REN2, sms_preipo_mtg),
        sub_row("Renaissance 3 (SMS · webform)", "Renaissance 3", SUB_REN3, webform_fills(date)),
    ]
    wa = wq(f"""SELECT sent, delivered, failed, replies_total FROM main.v_sms_dash_wa_daily
                WHERE channel='whatsapp' AND metric_date=DATE '{date}'""")
    if wa:
        ws_, wd, wf, wr = (int(wa[0][i] or 0) for i in range(4))
    else:
        ws_, wd, wf, wr = 0, 0, 0, 0
    rows.append(("WhatsApp (ISKRA)", ws_, wd, wf, wr, None, None, wa_mtg,
                 (round(0.072 * wd, 2) if wd else None)))
    return rows

def get_close(date, co):
    c = wq(f"""SELECT count(*) dials, count(DISTINCT close_lead_id) leads,
                 count(*) FILTER (WHERE duration_seconds >= 60) connects
               FROM core.call WHERE (occurred_at AT TIME ZONE 'America/New_York')::date = DATE '{date}'""")
    d, l, cn = (int(c[0][0] or 0), int(c[0][1] or 0), int(c[0][2] or 0)) if c else (0, 0, 0)
    m = co.get(("call", 0), 0)   # Business Funding call-sourced meetings (Pre-IPO calls, if any, excluded)
    return {"dials": d, "leads": l, "connects": cn, "meetings": m}

def get_actual_by_infra(date):
    """§4 Actual sends per workspace x ESP (OTD/Google/Total). Expected/fulfillment WIPED (census-freeze
    gap #4). 100% exact at D-1 (account grain == Instantly)."""
    by = {slug: {"otd": 0, "google": 0, "total": 0} for slug in REPORT_SLUGS}
    for r in wq(f"""SELECT workspace_slug, esp, coalesce(sum(actual_sends),0)
                    FROM core.sending_account_daily WHERE date=DATE '{date}'
                      AND workspace_slug IN ({SLUGS_SQL}) GROUP BY 1,2"""):
        slug, esp, n = r[0], (r[1] or "").lower(), int(r[2] or 0)
        if slug not in by:
            continue
        by[slug]["total"] += n
        if esp in ("otd", "google"):
            by[slug][esp] += n
    out = []
    for slug, name in WS:
        b = by.get(slug, {"otd": 0, "google": 0, "total": 0})
        out.append((name, b["otd"], b["google"], b["total"]))
    return out

def get_partner(date):
    """§5 bookings by partner — canonical view, offer-agnostic, all channels (== the portal 'BY PARTNER'
    lens Grace reads). Deduped in the view already."""
    rows = wq(f"""SELECT coalesce(nullif(trim(partner),''),'(unknown)') partner, count(*) n
                  FROM core.v_meeting_canonical WHERE meeting_date=DATE '{date}' GROUP BY 1""")
    cnt = {p: int(n) for p, n in rows if int(n or 0)}
    pr = sorted(cnt.items(), key=lambda x: (-x[1], x[0]))
    return pr, sum(n for _, n in pr)

# ============================ §6 reply-time SLA (canonical fact; warehouse-only) ============================
def _biz_minutes(p, r, tz):
    if r <= p:
        return 0.0
    d = p.astimezone(tz).date(); end = r.astimezone(tz).date(); tot = 0.0
    while d <= end:
        if d.isoweekday() <= 5:
            o = datetime.datetime.combine(d, datetime.time(12), tzinfo=tz)
            c = datetime.datetime.combine(d, datetime.time(20), tzinfo=tz)
            lo = p if p > o else o; hi = r if r < c else c
            if hi > lo:
                tot += (hi - lo).total_seconds() / 60.0
        d += datetime.timedelta(days=1)
    return tot

def _clock_open_date(p, tz):
    e = p.astimezone(tz); d = e.date()
    if e.isoweekday() <= 5 and e < datetime.datetime.combine(d, datetime.time(20), tzinfo=tz):
        return d
    d += datetime.timedelta(days=1)
    while d.isoweekday() > 5:
        d += datetime.timedelta(days=1)
    return d

_IMREPLY_WARN = None
_IMREPLY_SOURCE = None

def _im_pairs_from_fact(wk0, day0):
    lo = (wk0 - datetime.timedelta(days=4)).isoformat()
    rows = wq(f"""SELECT workspace_slug ws, clock_open_date d, biz_latency_minutes lat
      FROM core.sla_reply_time
      WHERE seq_in_thread=1 AND biz_latency_minutes IS NOT NULL
        AND clock_open_date >= DATE '{lo}' AND clock_open_date <= DATE '{day0.isoformat()}'
        AND workspace_slug IN ({SLUGS_SQL})""")
    out = []
    for ws, d_val, lat in rows:
        if lat is None:
            continue
        d = d_val if isinstance(d_val, datetime.date) else datetime.date.fromisoformat(str(d_val)[:10])
        out.append((ws, d, float(lat)))
    return out

def _im_pairs_from_email(wk0, day0):
    tz = ET_TZ; utc = datetime.timezone.utc
    pull = (wk0 - datetime.timedelta(days=4)).isoformat()
    rows = wq(f"""
      WITH inbound AS (
        SELECT thread_id, workspace_id ws, message_at p_ts,
               row_number() OVER (PARTITION BY thread_id, workspace_id ORDER BY message_at, message_id) seq
        FROM core.email_message WHERE ue_type=2 AND thread_id IS NOT NULL),
      ours AS (SELECT thread_id, workspace_id ws, message_at r_ts FROM core.email_message
               WHERE ue_type=3 AND thread_id IS NOT NULL AND message_at >= DATE '{pull}')
      SELECT * FROM (
        SELECT i.ws ws, epoch_ms(i.p_ts) p,
               epoch_ms((SELECT min(o.r_ts) FROM ours o WHERE o.thread_id=i.thread_id AND o.ws=i.ws AND o.r_ts > i.p_ts)) r
        FROM inbound i WHERE i.seq=1 AND i.p_ts >= DATE '{pull}' AND i.ws IN ({SLUGS_SQL}))
      WHERE r IS NOT NULL""")
    out = []
    for ws, p_ms, r_ms in rows:
        if r_ms is None:
            continue
        p = datetime.datetime.fromtimestamp(float(p_ms) / 1000.0, tz=utc)
        r = datetime.datetime.fromtimestamp(float(r_ms) / 1000.0, tz=utc)
        out.append((ws, _clock_open_date(p, tz), _biz_minutes(p, r, tz)))
    return out

_IM_OPTOUT_LIKE = " OR ".join(
    f"lower(f.body_text) LIKE '%{p}%'" for p in
    ("unsubscribe", "remove me", "remove from", "not interested", "opt out", "opt-out",
     "take me off", "do not contact", "do not email", "please remove", "unsub", "leave me alone"))

def _im_answerable_by_ws(wk0, day0):
    try:
        rows = wq(f"""
          WITH firsts AS (
            SELECT thread_id, body_text,
                   row_number() OVER (PARTITION BY thread_id ORDER BY message_at, message_id) rn
            FROM core.email_message WHERE ue_type=2 AND thread_id IS NOT NULL)
          SELECT s.workspace_slug,
                 count(*) - count(*) FILTER (WHERE ({_IM_OPTOUT_LIKE}) AND s.biz_latency_minutes IS NULL) answerable
          FROM core.sla_reply_time s JOIN firsts f ON f.thread_id=s.thread_id AND f.rn=1
          WHERE s.seq_in_thread=1 AND s.clock_open_date >= DATE '{wk0.isoformat()}'
            AND s.clock_open_date <= DATE '{day0.isoformat()}' AND s.workspace_slug IN ({SLUGS_SQL})
          GROUP BY 1""")
        return {r[0]: int(r[1] or 0) for r in rows}
    except Exception as e:
        print(f"WARN §6 answerable count failed ({e})", file=sys.stderr)
        return {}

def get_im_reply(date):
    """§6 email reply-time, weekly-7d, from the canonical fact (fallback to inline email_message clamp,
    both warehouse-only). Reconciled to the digit (PR #151 clamp)."""
    global _IMREPLY_WARN, _IMREPLY_SOURCE
    day0 = datetime.date.fromisoformat(date)
    wk0 = day0 - datetime.timedelta(days=6)
    try:
        pairs = _im_pairs_from_fact(wk0, day0); _IMREPLY_SOURCE = "warehouse fact core.sla_reply_time"
    except Exception as e:
        print(f"WARN §6 fact read failed ({e}); falling back to inline email_message clamp", file=sys.stderr)
        if ET_TZ is None:
            raise RuntimeError("zoneinfo unavailable — refusing §6 SLA math in a wrong fixed offset")
        pairs = _im_pairs_from_email(wk0, day0)
        _IMREPLY_SOURCE = "inline email_message (transitional fallback)"
    lats = {}; frontier = None
    for ws, d, lat in pairs:
        if lat is None or d > day0:
            continue
        if frontier is None or d > frontier:
            frontier = d
        b = lats.setdefault(ws, {"d": [], "w": []})
        if d == day0: b["d"].append(float(lat))
        if d >= wk0:  b["w"].append(float(lat))
    _IMREPLY_WARN = None
    if frontier is not None:
        missing = [dd for dd in (wk0 + datetime.timedelta(n) for n in range((day0 - wk0).days + 1))
                   if dd.isoweekday() <= 5 and dd > frontier]
        if len(missing) >= 2:
            _IMREPLY_WARN = (frontier, len(missing))
    def _stats(v):
        return (len(v), statistics.median(v) if v else None, sum(v) / len(v) if v else None)
    answerable = _im_answerable_by_ws(wk0, day0)
    out = []
    for slug, name in WS:
        b = lats.get(slug, {"d": [], "w": []})
        out.append((name,) + _stats(b["d"]) + _stats(b["w"]) + (answerable.get(slug),))
    return (date, wk0.isoformat()), out

_SMSWA_WARNS = []
_SMSWA_DESKS = [("Renaissance 1", "Renaissance 1 (SMS)", "sms"),
                ("Renaissance 2", "Renaissance 2 (SMS · Pre-IPO)", "sms"),
                ("Renaissance 3", "Renaissance 3 (SMS · webform)", "sms"),
                ("WhatsApp (ISKRA)", "WhatsApp (ISKRA)", "whatsapp")]

def get_smswa_reply(date):
    """§6 SMS/WA first-reply time — canonical fact core.sla_reply_time_smswa, weekly-7d, same clock."""
    global _SMSWA_WARNS
    _SMSWA_WARNS = []
    d0 = datetime.date.fromisoformat(date)
    wk0 = d0 - datetime.timedelta(days=6)
    lo = (wk0 - datetime.timedelta(days=4)).isoformat()
    rows = wq(f"""
      SELECT desk,
             count(*) FILTER (WHERE clock_open_date >= DATE '{wk0.isoformat()}') pop,
             count(biz_latency_minutes) FILTER (WHERE clock_open_date >= DATE '{wk0.isoformat()}') n_ans,
             median(biz_latency_minutes) FILTER (WHERE clock_open_date >= DATE '{wk0.isoformat()}') med_biz,
             avg(biz_latency_minutes) FILTER (WHERE clock_open_date >= DATE '{wk0.isoformat()}') avg_biz,
             median(raw_latency_minutes) FILTER (WHERE clock_open_date >= DATE '{wk0.isoformat()}') med_raw,
             max(clock_open_date) FILTER (WHERE biz_latency_minutes IS NOT NULL) frontier
      FROM core.sla_reply_time_smswa
      WHERE clock_open_date >= DATE '{lo}' AND clock_open_date <= DATE '{d0.isoformat()}'
      GROUP BY desk""")
    per = {r[0]: r[1:] for r in rows}
    desk_channel = {desk: ch for desk, _label, ch in _SMSWA_DESKS}
    frontier = {}; haspop = {}
    for desk, vals in per.items():
        ch = desk_channel.get(desk)
        if ch is None:
            continue
        if (vals[0] or 0) > 0:
            haspop[ch] = True
        f_val = vals[5]
        if f_val is None:
            continue
        d = f_val if isinstance(f_val, datetime.date) else datetime.date.fromisoformat(str(f_val)[:10])
        if ch not in frontier or d > frontier[ch]:
            frontier[ch] = d
    biz_days = [dd for dd in (wk0 + datetime.timedelta(n) for n in range((d0 - wk0).days + 1)) if dd.isoweekday() <= 5]
    for channel, label in (("sms", "SMS"), ("whatsapp", "WhatsApp")):
        fr = frontier.get(channel)
        if fr is None:
            if haspop.get(channel):
                _SMSWA_WARNS.append((label, None, len(biz_days)))
            continue
        missing = [dd for dd in biz_days if dd > fr]
        if len(missing) >= 3:
            _SMSWA_WARNS.append((label, fr, len(missing)))
    out = []
    for desk, label, _ch in _SMSWA_DESKS:
        pop, n_ans, med_b, avg_b, med_r, _f = per.get(desk, (0, 0, None, None, None, None))
        out.append((label, int(pop or 0), int(n_ans or 0), med_b, avg_b, med_r))
    return out

# ============================ formatting helpers ============================
def _f(x):
    try: return float(x)
    except (TypeError, ValueError): return 0.0
def pctstr(n, m): return f"'{round(100.0*_f(n)/_f(m))}%" if m else "'0%"
def kpi(sent, m): return round(_f(sent) / _f(m)) if m else "—"
def mins(v): return "—" if v is None else round(_f(v))
def rgb(r, g, b): return {"red": r, "green": g, "blue": b}

# ============================ render engine ============================
def build_and_write(tab, build_fn):
    rows = []; sec = []; th = []; tot = []; pctcells = []; merges = []; data = []
    usdcells = []
    th_ncol = {}; row_ncol = {}
    def add(r=None): rows.append(r or []); return len(rows) - 1

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
            ri = add([ws, sent, opps, m, kpi(sent, m), c, pctstr(c, m), r, pctstr(r, m), o2m(m, opps)])
            data.append(ri); row_ncol[ri] = W
            ts += sent; topp += opps; tm += m; tc_ += c; tr_ += r
        ti = add(["TOTAL", ts, topp, tm, kpi(ts, tm), tc_, pctstr(tc_, tm), tr_, pctstr(tr_, tm), o2m(tm, topp)])
        tot.append(ti); row_ncol[ti] = W
    def infra_sends_table(rows_data, header_label):
        W = 2
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Infrastructure", "Sent"]); th.append(hr); th_ncol[hr] = W
        t = 0
        for lbl, s in rows_data:
            ri = add([lbl, s]); data.append(ri); row_ncol[ri] = W; t += s
        ti = add(["TOTAL", t]); tot.append(ti); row_ncol[ti] = W
    def offer_table(rows_data, header_label):
        W = 6
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Offer / lane", "Meetings", "Email", "SMS", "Call", "WhatsApp"]); th.append(hr); th_ncol[hr] = W
        tt = te = tsx = tc = tw = 0
        for lane, m, e, s, c, w in rows_data:
            ri = add([lane, m, e, s, c, w]); data.append(ri); row_ncol[ri] = W
            tt += m; te += e; tsx += s; tc += c; tw += w
        ti = add(["TOTAL", tt, te, tsx, tc, tw]); tot.append(ti); row_ncol[ti] = W
    def sms_wa_table(rows_data, header_label):
        # Funnel COUNTS (all 100%): Sent → Delivered → Replies → Opps → Meetings, with the COHORT-HONEST
        # conversions only. Deliv% (deliv/sent) + Opp% (opp/reply, both measured on the same received
        # population) + Opp→mtg% are cohort-clean. "Reply %" (replies/delivered) is DROPPED [audit §4.3]:
        # replies lag delivery, so it's non-cohort and reads >100% on a paused/low-send day.
        W = 9
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Channel / workspace", "Sent", "Delivered", "Deliv %", "Replies (total)",
                  "Opps (Qwen)", "Opp %", "Mtgs / Webform", "Opp→mtg %"]); th.append(hr); th_ncol[hr] = W
        dash = "—"
        def cell(v): return dash if v is None else v
        def div(n, d): return (float(n) / float(d)) if (n is not None and d) else dash
        ts = tr = tm = 0; deliv_vals = []; opps_vals = []
        for label, sent, deliv, failed, reps, oo, opp, m, cost in rows_data:
            ri = add([label, sent, cell(deliv), div(deliv, sent), reps,
                      cell(opp), div(opp, reps), m, div(m, opp)])
            data.append(ri); row_ncol[ri] = W
            for c in (3, 6, 8): pctcells.append((ri, c))
            ts += sent; tr += reps; tm += m; deliv_vals.append(deliv)
            if "WhatsApp" not in label: opps_vals.append(opp)
        tdeliv = sum(deliv_vals) if (deliv_vals and all(v is not None for v in deliv_vals)) else None
        topp = sum(opps_vals) if (opps_vals and all(v is not None for v in opps_vals)) else None
        ti = add(["TOTAL", ts, cell(tdeliv), div(tdeliv, ts), tr,
                  cell(topp), dash, tm, dash]); tot.append(ti); row_ncol[ti] = W
        pctcells.append((ti, 3))
    def sms_cost_table(rows_data, header_label):
        W = 7
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Channel / workspace", "Opt-outs", "Opt-out %", "KPI (deliv/mtg·form)",
                  "Cost $", "Cost basis", "Cost / mtg·form"]); th.append(hr); th_ncol[hr] = W
        dash = "—"
        toptout = 0; treps = 0; seen_oo = False; sms_costs = []
        for label, sent, deliv, failed, reps, oo, opp, m, cost in rows_data:
            is_wa = "WhatsApp" in label
            basis = ("model $0.072×delivered" if is_wa else "actual (Sendivo billing, all-in)")
            ri = add([label, (oo if oo is not None else dash),
                      ((float(oo) / float(reps)) if (oo is not None and reps) else dash),
                      (kpi(deliv, m) if deliv is not None else dash),
                      (round(cost, 2) if cost is not None else dash),
                      (basis if cost is not None else dash),
                      (round(cost / m, 2) if (cost is not None and m) else dash)])
            data.append(ri); row_ncol[ri] = W
            pctcells.append((ri, 2)); usdcells.append((ri, 4, 5)); usdcells.append((ri, 6, 7))
            if oo is not None: toptout += oo; treps += reps; seen_oo = True
            if not is_wa: sms_costs.append(cost)
        tcost = round(sum(sms_costs), 2) if (sms_costs and all(c is not None for c in sms_costs)) else dash
        ti = add(["TOTAL", (toptout if seen_oo else dash),
                  ((float(toptout) / float(treps)) if treps else dash), dash,
                  tcost, ("actual, SMS rows only (WA model kept separate)" if tcost != dash else dash),
                  dash]); tot.append(ti); row_ncol[ti] = W
        pctcells.append((ti, 2)); usdcells.append((ti, 4, 5))
    def close_table(close, label):
        W = 2
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Close CRM — metric", "Value"]); th.append(hr); th_ncol[hr] = W
        d2m = f"'{(100.0*close['meetings']/close['dials']):.2f}%" if close['dials'] else "—"
        for r2 in [["Dials", close["dials"]], ["Distinct leads dialed", close["leads"]],
                   ["Connects (≥60s real convo)", close["connects"]],
                   ["Meetings booked (call-sourced)", close["meetings"]], ["Dial → meeting %", d2m]]:
            ri = add(r2); data.append(ri); row_ncol[ri] = W
    def actual_table(rows_data, header_label):
        W = 4
        si = add([header_label]); sec.append(si); row_ncol[si] = W
        hr = add(["Workspace", "Actual OTD", "Actual Google", "Actual (total)"]); th.append(hr); th_ncol[hr] = W
        to = tg = tt = 0
        for ws, otd, goog, total in rows_data:
            ri = add([ws, otd, goog, total]); data.append(ri); row_ncol[ri] = W
            to += otd; tg += goog; tt += total
        ti = add(["TOTAL", to, tg, tt]); tot.append(ti); row_ncol[ti] = W
    def partner_table(rows_data, total, label):
        W = 2
        si = add([label]); sec.append(si); row_ncol[si] = W
        hr = add(["Partner", "Meetings booked"]); th.append(hr); th_ncol[hr] = W
        for p, n in rows_data:
            ri = add([p, n]); data.append(ri); row_ncol[ri] = W
        ti = add(["TOTAL", total]); tot.append(ti); row_ncol[ti] = W
    def imreply_table(rows_data, smswa_rows, label):
        W = 6
        si = add([label]); sec.append(si); row_ncol[si] = 7
        hr = add(["Workspace", "Weekly (7d) — n answered", "of answerable", "Answered %", "Median min", "Avg min"]); th.append(hr); th_ncol[hr] = W
        for name, dn, dmed, davg, wn, wmed, wavg, wpop in rows_data:
            pct = (float(wn) / float(wpop)) if wpop else "—"
            ri = add([name, wn, (wpop if wpop is not None else "—"), pct, mins(wmed), mins(wavg)])
            data.append(ri); row_ncol[ri] = W; pctcells.append((ri, 3))
        if not any((r[4] or 0) for r in rows_data):
            note = add(["(!) §6 rendered EMPTY for the trailing week — the warehouse pull failed or returned no first-reply pairs (see render stderr)"])
            data.append(note); row_ncol[note] = W
        if _IMREPLY_WARN:
            fr, nmiss = _IMREPLY_WARN
            warn = add(["(⚠ WEEKLY UNDERSTATED — email reply-history synced only through %s; %d of the 7 window business-days have zero synced pairs, so n is deflated — a sync-drain artifact, NOT a desk-speed change. Self-corrects as the drain advances.)" % (fr.isoformat(), nmiss)])
            data.append(warn); row_ncol[warn] = W
        if _IMREPLY_SOURCE and "fallback" in _IMREPLY_SOURCE:
            src = add(["(i §6 on transitional INLINE fallback — canonical fact not in serving yet; numbers are the reconciled clamp, identical to the fact.)"])
            data.append(src); row_ncol[src] = W
        W2 = 7
        hr2 = add(["SMS · WhatsApp (same clock; non-opt-out first replies)", "Weekly (7d) — n answered",
                   "of first replies", "Answered %", "Median biz-min", "Avg biz-min", "Median RAW min"])
        th.append(hr2); th_ncol[hr2] = W2
        for label2, pop, n_ans, med_b, avg_b, med_r in smswa_rows:
            pct = (float(n_ans) / float(pop)) if pop else "—"
            ri = add([label2, n_ans, pop, pct, mins(med_b), mins(avg_b), mins(med_r)])
            data.append(ri); row_ncol[ri] = W2; pctcells.append((ri, 3))
        if not any((r[1] or 0) for r in smswa_rows):
            note = add(["(!) SMS/WA reply-time rendered EMPTY — core.sla_reply_time_smswa is absent or the pull failed (see render stderr)"])
            data.append(note); row_ncol[note] = W2
        for ch_label, fr, nmiss in _SMSWA_WARNS:
            if fr is None:
                warn = add(["(⚠ %s WEEKLY UNDERSTATED — first replies exist but NO answered pair synced in the pull range: the response sync looks stalled. Check the recovered-log / Iskra sync before reading these rows.)" % ch_label])
            else:
                warn = add(["(⚠ %s WEEKLY UNDERSTATED — responses synced only through %s; %d window business-days have zero synced responses (structural sync lag, NOT a desk-speed change). Back-fills on the next nightly.)" % (ch_label, fr.isoformat(), nmiss)])
            data.append(warn); row_ncol[warn] = W2

    build_fn(dict(add=add, email_table=email_table, infra_sends_table=infra_sends_table,
                  offer_table=offer_table, sms_wa_table=sms_wa_table, sms_cost_table=sms_cost_table,
                  actual_table=actual_table, close_table=close_table, partner_table=partner_table,
                  imreply_table=imreply_table))

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
    for i, c in pctcells:
        reqs.append({"repeatCell": {"range": rng(i, i + 1, c, c + 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    for i, c0, c1 in usdcells:
        reqs.append({"repeatCell": {"range": rng(i, i + 1, c0, c1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}})
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
    reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 230}, "fields": "pixelSize"}})
    LAST_COL = max([th_ncol.get(i, NCOL) for i in th] + [NCOL])
    for c in range(1, LAST_COL):
        reqs.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": c, "endIndex": c + 1}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}})
    api("POST", BASE + ":batchUpdate", {"requests": reqs})
    merge_reqs = [{"mergeCells": {"range": {"sheetId": sid, "startRowIndex": r0, "endRowIndex": r0 + 1, "startColumnIndex": c0, "endColumnIndex": c1}, "mergeType": "MERGE_ALL"}} for r0, c0, c1 in merges]
    if merge_reqs: api("POST", BASE + ":batchUpdate", {"requests": merge_reqs})
    write_summary_block(sid)
    print(f"  {tab}: {NROW} rows, sections={len(sec)} headers={len(th)} totals={len(tot)} + summary block")

def write_summary_block(sid):
    """Cream right-side overview (cols L-O). Every meeting counted ONCE; the GRAND TOTAL reconciles to
    §5 partner total (the true day total / booking-page number). Fixes the audit's L-O findings:
    channel-correct headers, an SDR/Call row (12 call meetings were invisible), Tariffs reads its real
    count, dead placeholder rows (SEC 125 / R&D Credit) dropped."""
    def ratio(a, b): return round(a / b) if (b and a) else ""
    _SMS_EMPTY = ("", 0, None, None, 0, None, None, 0, None)
    sms1 = SMS_D[0] if len(SMS_D) > 0 else _SMS_EMPTY   # Ren1 Funding SMS (index 0)
    sms2 = SMS_D[1] if len(SMS_D) > 1 else _SMS_EMPTY   # Ren2 Pre-IPO SMS (index 1)
    wa = next((x for x in SMS_D if "WhatsApp" in x[0]), _SMS_EMPTY)
    blk = [[f"{DAILY_TAB} — Business / Funding overview · {DAILY}", "", "", ""],
           ["LANE", "Sent", "Meeting Booked", "Sent / meeting"]]
    ts = sum(x[1] for x in EMAIL_D); tm = sum(x[4] for x in EMAIL_D)   # all 8 email workspaces
    for ws, sent, hr, opps, m, c, r in EMAIL_D:   # F1-F5, R1, Max, Warm — each email desk once
        blk.append([ws, sent, m, ratio(sent, m)])
    blk.append(["Email — all funding desks", ts, tm, ratio(ts, tm)])
    blk.append(["", "", "", ""])
    grand = tm
    wa_deliv = wa[2] if wa[2] is not None else 0
    for lbl, s, m in [("SMS Funding", sms1[1], sms1[7]),
                      ("WhatsApp Funding (delivered)", wa_deliv, wa[7]),
                      ("SDR (Close · call-sourced)", "", CLOSE_D.get("meetings", 0))]:
        blk.append([lbl, s, m, ratio(s, m) if isinstance(s, int) else ""])
        grand += (m or 0)
    blk.append(["", "", "", ""])
    blk.append(["SMS IPO (Pre-IPO)", sms2[1], PREIPO_MTG, ratio(sms2[1], PREIPO_MTG)])
    grand += (PREIPO_MTG or 0)
    if WA_PREIPO_MTG:
        blk.append(["WhatsApp PRE-IPO", 0, WA_PREIPO_MTG, ""]); grand += WA_PREIPO_MTG
    if TARIFFS_M:
        blk.append(["Tariffs", "", TARIFFS_M, ""]); grand += TARIFFS_M
    blk.append(["", "", "", ""])
    blk.append(["GRAND TOTAL (all lanes = §5 partner total)", "", grand, ""])
    # self-check: the overview grand total must equal §5 partner total (the true day count)
    if PARTNER_D_TOTAL and grand != PARTNER_D_TOTAL:
        print(f"WARN summary grand total {grand} != §5 partner total {PARTNER_D_TOTAL} on {DAILY} "
              f"(a meeting lane is mis-summed or a channel is missing)", file=sys.stderr)
    api("PUT", f"{BASE}/values/{urllib.parse.quote(DAILY_TAB)}!L4?valueInputOption=USER_ENTERED", {"values": blk})
    n = len(blk); r0 = 3; r1 = r0 + n; CREAM = rgb(0.988, 0.953, 0.804)
    def br(a, b, c=11, d=15): return {"sheetId": sid, "startRowIndex": a, "endRowIndex": b, "startColumnIndex": c, "endColumnIndex": d}
    thin = {"style": "SOLID", "width": 1, "color": rgb(0.55, 0.45, 0.15)}
    reqs = [
        {"repeatCell": {"range": br(r0, r1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "textFormat": {"fontSize": 10}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
        {"mergeCells": {"range": br(r0, r0 + 1), "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": br(r0, r0 + 1), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True, "fontSize": 12}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": br(r0 + 1, r0 + 2), "cell": {"userEnteredFormat": {"backgroundColor": CREAM, "horizontalAlignment": "CENTER", "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)"}},
        {"repeatCell": {"range": br(r0 + 2 + len(EMAIL_D), r0 + 3 + len(EMAIL_D)), "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat"}},
        {"repeatCell": {"range": br(r1 - 1, r1), "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}}, "fields": "userEnteredFormat.textFormat"}},
        {"repeatCell": {"range": br(r0 + 2, r1, 12, 15), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}, "horizontalAlignment": "RIGHT"}}, "fields": "userEnteredFormat(numberFormat,horizontalAlignment)"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 11, "endIndex": 12}, "properties": {"pixelSize": 260}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 12, "endIndex": 13}, "properties": {"pixelSize": 110}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 13, "endIndex": 14}, "properties": {"pixelSize": 125}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 15}, "properties": {"pixelSize": 130}, "fields": "pixelSize"}},
        {"updateBorders": {"range": br(r0, r1), "top": thin, "bottom": thin, "left": thin, "right": thin, "innerHorizontal": thin, "innerVertical": thin}}]
    api("POST", BASE + ":batchUpdate", {"requests": reqs})

def daily(ctx):
    add = ctx["add"]
    sms_status = CHANNEL_STATUS.get("sms", "")
    add([f"DAILY REVOPS REPORT — Business Funding · {DAILY} (D-1 FINAL)"])
    add([f"Warehouse-only · rendered once after the nightly promote · snapshot {_SNAPSHOT_ID or '?'} · every cell reproducible from the served snapshot"]); add()
    ctx["email_table"](EMAIL_D, f"1 · EMAIL + WARM LEADS — by workspace · day {DAILY}"); add()
    ctx["infra_sends_table"](INFRA_SENDS_D, f"1b · EMAIL SENDS BY INFRASTRUCTURE — Google / OTD · all workspaces · day {DAILY} (per-infra RATES pending the campaign→infra bridge)"); add()
    ctx["offer_table"](OFFER_D, f"1c · MEETINGS BY OFFER / LANE — all channels · day {DAILY}"); add()
    ctx["sms_wa_table"](SMS_D, f"2 · SMS + WHATSAPP — funnel by sub-account · sent → delivered → replies → opps → meetings · day {DAILY}  [SMS {sms_status}]"); add()
    ctx["sms_cost_table"](SMS_D, f"2b · SMS + WHATSAPP — opt-outs · KPI · cost · day {DAILY}"); add()
    ctx["close_table"](CLOSE_D, f"3 · CLOSE CRM — warm calling · day {DAILY}"); add()
    ctx["actual_table"](ACTUAL_D, f"4 · SENDING VOLUME — actual sends by workspace × infrastructure · day {DAILY} (Expected/fulfillment pending the census freeze)"); add()
    ctx["partner_table"](PARTNER_D, PARTNER_D_TOTAL, f"5 · BOOKINGS BY PARTNER — all channels · day {DAILY}"); add()
    ctx["imreply_table"](IMREPLY_D, SMSWA_D, f"6 · REPLY-TIME — business minutes to first reply · 12-8pm ET Mon-Fri clock · WEEKLY (7d) · EMAIL by workspace + SMS/WA by desk (blended human+AIM)"); add()

def _alert(text):
    try:
        import subprocess
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alert_slack.py")
        if os.path.exists(p):
            subprocess.run([sys.executable, p, text], timeout=30, check=False)
    except Exception as e:
        print(f"WARN _alert failed: {e}", file=sys.stderr)

# ============================ main ============================
def _safe(label, fn, default):
    try:
        return fn()
    except Exception as e:
        print(f"WARN section '{label}' FAILED ({e}); rendering it empty", file=sys.stderr)
        return default

_GATE_OK, _GATE_REASONS = gate_ready(DAILY)
print(f"[gate] {DAILY}: {'READY' if _GATE_OK else 'NOT READY'}")
for r in _GATE_REASONS:
    print("  " + r)
if GATE_ONLY:
    sys.exit(0 if _GATE_OK else 3)
if not _GATE_OK and not (FORCE or DRY):
    msg = (f":hourglass_flowing_sand: daily-report v2: gate NOT ready for {DAILY} — "
           + "; ".join(x for x in _GATE_REASONS if x.startswith("MISS")) + ". Not rendering; will retry.")
    print("SKIP: " + msg, file=sys.stderr)
    if os.environ.get("DAILY_V2_ALERT_ON_SKIP") == "1":
        _alert(msg)
    sys.exit(3)

_EMAIL_FB = {"rows": [(name, 0, 0, 0, 0, 0, 0) for _, name in WS], "tariffs_m": 0, "total_email_m": 0, "unmapped": []}
_SMS_FB = [("Renaissance 1 (SMS)", 0, None, None, 0, None, None, 0, None),
           ("Renaissance 2 (SMS · Pre-IPO)", 0, None, None, 0, None, None, 0, None),
           ("Renaissance 3 (SMS · webform)", 0, None, None, 0, None, None, 0, None),
           ("WhatsApp (ISKRA)", 0, None, None, 0, None, None, 0, None)]
_CLOSE_FB = {"dials": 0, "leads": 0, "connects": 0, "meetings": 0}
_IMREPLY_FB = ((DAILY, DAILY), [(name, 0, None, None, 0, None, None, None) for _, name in WS])
_SMSWA_FB = [(label, 0, 0, None, None, None) for _desk, label, _ch in _SMSWA_DESKS]

MEET_CO = _safe("meetings_co", lambda: get_meetings_co(DAILY), {})
_EM = _safe("email", lambda: get_email(DAILY), _EMAIL_FB)
EMAIL_D = _EM["rows"]; TARIFFS_M = _EM["tariffs_m"]
INFRA_SENDS_D = _safe("infra_sends", lambda: get_infra_sends(DAILY), [("Google (reseller)", 0), ("OTD", 0)])
OFFER_D = _safe("offer", lambda: get_offer_summary(DAILY), [])
SMS_D = _safe("sms_wa", lambda: get_sms_wa(DAILY, MEET_CO), _SMS_FB)
CLOSE_D = _safe("close", lambda: get_close(DAILY, MEET_CO), _CLOSE_FB)
ACTUAL_D = _safe("actual", lambda: get_actual_by_infra(DAILY), [(name, 0, 0, 0) for _, name in WS])
PARTNER_D, PARTNER_D_TOTAL = _safe("partner", lambda: get_partner(DAILY), ([], 0))
IMREPLY_PERIODS, IMREPLY_D = _safe("imreply", lambda: get_im_reply(DAILY), _IMREPLY_FB)
SMSWA_D = _safe("smswa_reply", lambda: get_smswa_reply(DAILY), _SMSWA_FB)
# Pre-IPO yellow-block rows, split by channel from the SAME partition (no double-count with the SMS rows).
PREIPO_MTG = MEET_CO.get(("sms", 1), 0)          # 'SMS IPO' = Pre-IPO SMS meetings
WA_PREIPO_MTG = MEET_CO.get(("whatsapp", 1), 0)  # Pre-IPO WhatsApp meetings

# reconciliation: §1 email meetings + Tariffs must equal the canonical email meeting total
_email_row_m = sum(x[4] for x in EMAIL_D) + TARIFFS_M
if _EM["total_email_m"] and _email_row_m != _EM["total_email_m"]:
    print(f"WARN §1 reconciliation: rows+tariffs={_email_row_m} != canonical email total {_EM['total_email_m']} "
          f"on {DAILY} (unmapped: {_EM['unmapped']})", file=sys.stderr)

if DRY:
    print(f"REPORT_DATE={DAILY} TAB={DAILY_TAB} snapshot={_SNAPSHOT_ID}")
    print("§1 EMAIL (ws, sent, _, opps, mtg, cheap, reg):"); [print("  ", x) for x in EMAIL_D]
    print(f"  Tariffs meetings: {TARIFFS_M}  · canonical email total: {_EM['total_email_m']}")
    print("§1b INFRA SENDS:", INFRA_SENDS_D)
    print("§1c OFFER (lane, mtg, email, sms, call, wa):"); [print("  ", x) for x in OFFER_D]
    print("§2 SMS/WA (label, sent, deliv, fail, replies, optouts, opps, mtg, cost):"); [print("  ", x) for x in SMS_D]
    print("§3 CLOSE:", CLOSE_D)
    print("§4 ACTUAL (ws, otd, google, total):"); [print("  ", x) for x in ACTUAL_D]
    print("§5 PARTNER:", PARTNER_D, "TOTAL", PARTNER_D_TOTAL)
    print("§6 IM-REPLY (ws, d_n,d_med,d_avg, w_n,w_med,w_avg, w_answerable):"); [print("  ", x) for x in IMREPLY_D]
    print("§6 SMS/WA REPLY:"); [print("  ", x) for x in SMSWA_D]
    print("Pre-IPO meetings:", PREIPO_MTG, "· WA Pre-IPO:", WA_PREIPO_MTG)
    print("§6 warns:", _IMREPLY_WARN, _SMSWA_WARNS, "| src:", _IMREPLY_SOURCE)
    sys.exit(0)

print(f"Rendering DAILY tab '{DAILY_TAB}' for {DAILY} (D-1 FINAL, warehouse-only) ...")
build_and_write(DAILY_TAB, daily)
print(f"OK — wrote tab '{DAILY_TAB}' from snapshot {_SNAPSHOT_ID}.")
