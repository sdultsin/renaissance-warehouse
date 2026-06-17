"""Generate the Renaissance Portal data feed from the warehouse (read-only).

The portal (renaissanceportal.com) currently reads its numbers from a separate
Supabase project (pxrdmjjaxtqycuxhxmgi) plus a hand-maintained im_bookings_data.js
and hardcoded JSX constants. This script makes the DuckDB warehouse the SINGLE
SOURCE OF TRUTH instead: it queries the canonical views/facts read-only and emits
ONE JS file the portal loads (`portal_data.js`, which sets `window.PORTAL_DATA`).
It SUPERSEDES the Supabase runtime fetch and the hand-edited constants.

Every metric below is pinned to the canonical source in
`deliverables/warehouse-query-prompt.md`, the warehouse's own dashboard_data.py /
kpi_dashboard_data.py, and reconciled in
`deliverables/2026-06-14-portal-consolidation/PHASE0-SOURCE-RECONCILE.md` +
`PORTAL-FULL-DASHBOARD-INVENTORY.md`. Read-only — safe anytime except the
03:30-05:45 UTC write window.

PHASE-0 EDGE CASES HANDLED HERE (see GENERATOR-NOTES.md for the full rationale):
  1. EMAIL FILTER is HYBRID, not the raw_text regex alone. For source='sheet' rows
     (>=Jun-1) the explicit `channel` column is authoritative (channel='Email');
     the raw_text regex over-counts sheet email meetings by +47% because sheet rows
     have raw_text=NULL so 728 non-Email rows leak through. For source<>'sheet'
     (Slack era, <Jun-1) raw_text is the only channel signal, so the regex stays.
     -> `EMAIL_IS` below encodes this hybrid; `EMAIL_WHERE` is the WHERE-clause form.
  2. PARTNER LABELS are normalized across eras (Slack short labels vs sheet full
     labels): BTC|Big Think Capital -> "Big Think"; Qualifi|GoQualifi -> "GoQualifi";
     GreenBridge|GreenBridge Capital -> "GreenBridge"; Llama|Llama Funding -> "Llama".
     -> `PARTNER_NORM` (a SQL CASE) + `PARTNER_KEY_OF` (a python map) keep the
     warehouse counts under ONE label per partner. The whName keys the portal uses
     are produced verbatim so its partner cards light up with no JS change.
  3. CM LEADERBOARDS are FACT-DRIVEN per time-window (no static active-CM allowlist):
     a CM shows in a window IFF they booked meetings in it. All-time = every real CM ever
     (incl. let-go CMs Tomi/Carlos/Shaan/Brendan); MTD/range = only CMs with meetings in
     that window (naturally the 5 current CMs). CM is resolved (core.meeting.cm, else the
     raw_text '(CM)' parenthetical where let-go CMs' names live) and noise-filtered to the
     real-CM set (raw_pipeline_campaigns.cm_name minus INSTANTLY/MAX/non-person tokens).
     -> `CM_RESOLVED` + `REAL_CMS_CTE` + `CM_NOISE` below.
  4. ACTIVE INBOXES uses OUR warehouse definition, not the portal's legacy 127,662.
     Chosen filter = `status='active'` (see ACTIVE_INBOX_WHERE + GENERATOR-NOTES).
  5. INSTANTLY CREDITS have NO warehouse table -> pulled by the sibling
     scripts/portal_credits.py (Instantly billing plan-details API, read-only) and
     merged in here when its JSON is present (PORTAL_CREDITS_JSON env / default path).

Usage (run on the droplet where warehouse.duckdb + duckdb live):
    cd /root/renaissance-warehouse && .venv/bin/python scripts/portal_data.py > portal_data.js

Nightly cron (after the 07:00 UTC meetings refresh, mirrors kpi_dashboard_data.py):
    20 7 * * * cd /root/renaissance-warehouse && .venv/bin/python scripts/portal_credits.py \
        > /root/portal/portal_credits.json 2>>/root/portal/portal_credits.err ; true
    25 7 * * * cd /root/renaissance-warehouse && .venv/bin/python scripts/portal_data.py \
        > /root/portal/portal_data.js.tmp && mv -f /root/portal/portal_data.js.tmp /root/portal/portal_data.js \
        && (cd /root/portal && git add portal_data.js && git commit -m "portal data $(date -u +%FT%TZ)" && git push)
    # (publish step is whatever ships the portal repo — see PLAN.md "Publishing")

CONFIG knobs (env):
    CORE_DB_PATH         path to warehouse.duckdb (default /root/core/warehouse.duckdb)
    PORTAL_GOAL_MEETINGS monthly meetings goal shown on the MTD tile (default 4000)
    PORTAL_GOAL_SB       S/B-ratio goal shown on the MTD tile (default 4500)
    PORTAL_TREND_DAYS    business-funding trend window in days (default 180)
    PORTAL_CREDITS_JSON  path to portal_credits.json from portal_credits.py
                         (default: ./portal_credits.json next to this output)
    PORTAL_ACTIVE_INBOX_FILTER  override the active-inbox SQL predicate (default
                         "status='active'"); kept as a knob so the definition can be
                         re-pinned without a code change (see GENERATOR-NOTES §Active).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import duckdb

DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
GOAL_MEETINGS = int(os.environ.get("PORTAL_GOAL_MEETINGS", "4000"))
GOAL_SB = int(os.environ.get("PORTAL_GOAL_SB", "4500"))
TREND_DAYS = int(os.environ.get("PORTAL_TREND_DAYS", "180"))
CREDITS_JSON = os.environ.get("PORTAL_CREDITS_JSON", "portal_credits.json")

# ── Attribution-clean WINDOW (Sam, 2026-06-15: "window it") ────────────────────────
# The attribution-dependent leaderboards (meetings-by-campaign / by-CM / advisor / IM)
# are clean (94-100% attributed) ONLY from 2026-06-01 — the Funding-Form-sheet era. Pre-
# Jun-1 (Slack era) attribution is partial and is HELD/HIDDEN by the portal per the
# 100%-or-wipe rule. So every leaderboard the portal DISPLAYS is windowed to >=this date
# and labelled "since Jun 1". (All-time totals stay emitted for provenance, but the
# portal does not surface the partial all-time cuts.)
WINDOW_START = os.environ.get("PORTAL_WINDOW_START", "2026-06-01")
_wm = datetime.strptime(WINDOW_START, "%Y-%m-%d")
WINDOW_LABEL = f"since {_wm.strftime('%b')} {_wm.day}"  # e.g. "since Jun 1"
# SQL fragment: meeting m is in the clean window. `m` must be the table alias.
WINDOW_WHERE = f"m.posted_at >= DATE '{WINDOW_START}'"

# ── Org sends/replies WINDOW (Instantly-native reply scorecard) ────────────────────
# The org-wide sends/replies scorecard is sourced from the SAME Instantly-native daily
# fact as the per-workspace cut (raw_pipeline_campaign_daily_metrics) and windowed from
# 2026-05-15 — the date from which native reply/positive coverage is sound
# (reference_warehouse_reply_and_tag_truth_20260614: Instantly NATIVE is the SOLE truth
# for human/auto/total reply + positive(=opps÷human, >= May-15)). Kept as an env knob.
SENDS_REPLIES_WINDOW_START = os.environ.get("PORTAL_SENDS_REPLIES_WINDOW_START", "2026-05-15")

# ── CM leaderboard logic: FACT-DRIVEN per time-window (not a static allowlist) ─────
# RULE (Sam, 2026-06-14): a CM appears in a leaderboard window IFF they have meetings
# in that window — same principle as the deleted-workspace lifecycle. So:
#   • ALL-TIME leaderboard  = EVERY real CM who ever booked a meeting (incl. let-go CMs
#                             with pre-May-11 history: Tomi/Carlos/Shaan/Brendan/etc.).
#   • Time-windowed cuts (MTD / last-week / any range) = only CMs with meetings in that
#     window. Since all but 5 were let go ~2026-05-11, a June-MTD cut NATURALLY shows
#     only the 5 current CMs (Samuel/Sam/Ido/Leo/Eyver) — driven by the data, not a list.
# The old `ACTIVE_CMS` / `_CM_IN` allowlist gate is REMOVED; it silently dropped every
# let-go CM from the all-time leaderboard. It is replaced by:
#   (1) CM_RESOLVED — resolve the CM per meeting, and
#   (2) a NOISE FILTER that keeps only real CMs (excludes INSTANTLY / MAX / non-person tokens).
#
# WHY a resolver is needed: core.meeting.cm is only populated for the active-5 (+ the
# noise tokens INSTANTLY / MAX). Every let-go CM's booking has cm=NULL — their name lives
# only as a trailing parenthetical in raw_text, e.g.
#   "Meeting booked 110 - KDN - Contractors 641 (TOMI)"  /  "... (BRENDAN)".
# So CM_RESOLVED = COALESCE(m.cm, <trailing (TOKEN) from raw_text>). Verified 2026-06-14
# this recovers TOMI/SHAAN/CARLOS/BRENDAN/ALEX/MARCOS/ANDRES/LAUTARO into the all-time roster.
#
# NOISE FILTER (data-driven, not a hardcoded person-list): the raw_text parenthetical is
# noisy — it also carries partner/tool/segment tokens (GBC, GQ, ZOOMINFO, GMAIL, COPY,
# GENERAL, "ALREADY BOOKED IN GQ", segment names, employee-count ranges, ...). The set of
# REAL CM names is taken from the warehouse's own CM dimension
# (raw_pipeline_campaigns.cm_name: EYVER/SAM/LEO/TOMI/LAUTARO/IDO/MARCOS/BRENDAN/SHAAN/
# ANDRES/CARLOS/SAMUEL/ALEX), minus the obvious non-CM tokens below. A resolved CM is
# kept IFF it is in that real-CM set → INSTANTLY / MAX / partner / segment noise never
# rolls up, at ANY window, with NO static active-CM allowlist.
CM_NOISE = ("INSTANTLY", "INSTANTLY VIP", "MAX", "MAX (OUTREACH TODAY)")
_CM_NOISE_SQL = ", ".join("'" + n.replace("'", "''") + "'" for n in CM_NOISE)

# The real-CM roster, derived from the warehouse CM dimension (NOT hardcoded names).
# Drives the noise filter below. Built once; reused as a CTE in every CM query.
REAL_CMS_CTE = f"""
    real_cms AS (
      SELECT DISTINCT UPPER(TRIM(cm_name)) AS cm
      FROM raw_pipeline_campaigns
      WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
        AND UPPER(TRIM(cm_name)) NOT IN ({_CM_NOISE_SQL})
    )"""

# CM_RESOLVED: the canonical CM of a meeting. core.meeting.cm when present, else the
# trailing "(TOKEN)" parenthetical in raw_text (where every let-go CM's name actually is).
# `m` must be the table alias in the surrounding query.
CM_RESOLVED = (
    "COALESCE("
    "NULLIF(UPPER(TRIM(m.cm)),''),"
    r"NULLIF(UPPER(TRIM(regexp_extract(m.raw_text, '\(([^()]*)\)\s*$', 1))),'')"
    ")"
)

# ── Active-inbox definition (Phase-0 PARTIAL #4 — the number Sam flagged) ──────────
# Sam: "use OUR data, ignore the portal's legacy 127,662, but document the filter."
# Candidates measured 2026-06-14 on core.sending_account (1,359,514 rows total):
#   is_active=true ............. 1,299,431  (current/old generator — TOO LOOSE: includes
#                                            535,340 connection_error + 36,061 paused
#                                            + 2,967 sending error)
#   status='active' ............   725,063  ◀ CHOSEN. Clean "currently healthy & sending":
#                                            excludes connection_error / paused / sending
#                                            error / missing. lifecycle_state breaks down
#                                            active 713,640 / warming 10,834 / warmed 589.
#   lifecycle_state='active' ...   713,640  (≈status='active' minus the warming bucket)
# Rationale: status='active' is the tightest defensible "active inbox" — it is what an
# operator means by "an inbox that can send right now". Kept as an env knob so the
# definition can be re-pinned against the live inbox count post-swap without a code edit.
ACTIVE_INBOX_WHERE = os.environ.get("PORTAL_ACTIVE_INBOX_FILTER", "status = 'active'")
# In-warmup = active inboxes still in the warming lifecycle phase.
WARMUP_WHERE = f"({ACTIVE_INBOX_WHERE}) AND lower(COALESCE(lifecycle_state,'')) LIKE '%warm%' AND lower(COALESCE(lifecycle_state,'')) <> 'warmed'"

# ── HYBRID email filter (Phase-0 edge case #1 — the +47% over-count fix) ──────────
# For source='sheet' rows the explicit channel column is the truth (channel='Email').
# For Slack-era rows (no channel column) fall back to the SMS-exclusion raw_text regex
# (byte-identical to v_kpi_email / warehouse-query-prompt.md). `m` must be the alias.
_REGEX_EMAIL = (
    r"NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),"
    r"'sendivo|\bsms\b|whatsapp|iskra')"
)
EMAIL_IS = (
    "(CASE WHEN m.source = 'sheet' THEN m.channel = 'Email' "
    f"ELSE {_REGEX_EMAIL} END)"
)
EMAIL_WHERE = EMAIL_IS  # alias for readability in WHERE clauses

# ── Partner-label normalization across eras (Phase-0 edge case #2) ────────────────
# One canonical label per partner so all-time rollups don't split across Slack/sheet
# spellings. The OUTPUT labels match the portal's whName keys verbatim:
#   "GoQualifi" / "Big Think Capital" / "GreenBridge Capital" / "Llama"
# (the index.html partner cards key on whName — see the staged index.html.diff).
PARTNER_NORM = (
    "CASE "
    "WHEN m.partner IN ('GreenBridge','GreenBridge Capital') THEN 'GreenBridge Capital' "
    "WHEN m.partner IN ('BTC','Big Think Capital') THEN 'Big Think Capital' "
    "WHEN m.partner IN ('Qualifi','GoQualifi') THEN 'GoQualifi' "
    "WHEN m.partner IN ('Llama','Llama Funding') THEN 'Llama' "
    "WHEN m.partner IS NULL OR m.partner = '' THEN '(unattributed)' "
    "ELSE m.partner END"
)

# Channel classification from the Slack/sheet booking post — used ONLY for the
# Slack-era trend fallback. For sheet rows we prefer the explicit channel column.
CHANNEL_HYBRID = (
    "CASE WHEN m.source = 'sheet' THEN lower(COALESCE(m.channel,'email')) "
    "WHEN lower(COALESCE(m.raw_text,'')) LIKE '%whatsapp%' THEN 'whatsapp' "
    "WHEN lower(COALESCE(m.raw_text,'')) LIKE '%sendivo%' OR lower(COALESCE(m.raw_text,'')) LIKE '%sms%' THEN 'sms' "
    "WHEN lower(COALESCE(m.raw_text,'')) LIKE '%linkedin%' THEN 'linkedin' "
    "WHEN lower(COALESCE(m.raw_text,'')) LIKE '%sdr%' THEN 'sdr' ELSE 'email' END"
)

conn = duckdb.connect(DB, read_only=True)


def q(sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def one(sql: str):
    r = conn.execute(sql).fetchone()
    return r[0] if r else None


def table_exists(name: str) -> bool:
    schema = "%" if "." not in name else name.split(".")[0]
    tbl = name.split(".")[-1]
    return (one(
        f"SELECT count(*) FROM information_schema.tables "
        f"WHERE table_name='{tbl}' AND table_schema LIKE '{schema}'"
    ) or 0) > 0


def safe(label: str, fn):
    """Never let one missing view nuke the whole feed; emit a null + warn to stderr."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        print(f"[portal_data] WARN {label}: {e}", file=sys.stderr)
        return None


data: dict = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "source": "renaissance-warehouse/warehouse.duckdb (read-only) via scripts/portal_data.py",
    "goals": {"meetings": GOAL_MEETINGS, "sb_ratio": GOAL_SB},
    # Attribution-clean window the portal DISPLAYS its leaderboards over.
    "window": {"start": WINDOW_START, "label": WINDOW_LABEL},
    # Self-document the load-bearing choices so the portal / a reviewer can see them.
    "definitions": {
        "attribution_window": (
            f"leaderboards the portal SHOWS (meetings-by-campaign / by-CM / advisor / IM) "
            f"are windowed to >= {WINDOW_START} ('{WINDOW_LABEL}') where attribution is "
            f"94-100% clean (Funding-Form-sheet era). Pre-window all-time cuts are emitted "
            f"for provenance but HELD/HIDDEN by the portal (partial attribution)."
        ),
        "active_inbox_filter": ACTIVE_INBOX_WHERE,
        "email_filter": "hybrid: sheet rows -> channel='Email'; slack rows -> SMS-exclusion regex",
        "partner_label_norm": "BTC/Big Think Capital, Qualifi/GoQualifi, GreenBridge/GreenBridge Capital, Llama/Llama Funding merged",
        "cm_leaderboard": (
            "fact-driven per window: a CM shows in a window IFF they have meetings in it. "
            "all-time = every real CM ever (incl. let-go CMs); MTD/range = only CMs with "
            "meetings in that window. CM resolved from core.meeting.cm, else raw_text '(CM)'. "
            "noise filter = keep only real CMs (per raw_pipeline_campaigns.cm_name); "
            "INSTANTLY / MAX / non-person tokens excluded at every window."
        ),
        "cm_noise_excluded": list(CM_NOISE),
        "advisor_im_leaderboard": (
            f"WINDOWED to >= {WINDOW_START} ('{WINDOW_LABEL}') and served from core.meeting "
            "(advisor_name/advisor/advisor_partner/inbox_manager on sheet-era rows), "
            "email-filtered — the attribution-clean cut the portal DISPLAYS. The ALL-TIME "
            "union leaderboards (DDL 71 derived.v_advisor_alltime / v_inbox_manager_alltime) "
            "are 7.7% attributed pre-window and HELD/HIDDEN by the portal (provenance only)."
        ),
    },
}

# ───────────────────────────────────────────────────────────── Freshness banner
# Surface max data dates so the portal can caveat a stale nightly (mirrors §2 of the
# warehouse query prompt). The portal can show a small "data as of" pill from this.
data["freshness"] = safe("freshness", lambda: {
    "campaign_daily_metrics": str(one("SELECT max(date) FROM raw_pipeline_campaign_daily_metrics")),
    "meeting": str(one("SELECT max(posted_at)::DATE FROM core.meeting")),
    "sending_account_daily": str(one("SELECT max(date) FROM core.sending_account_daily")),
})

# ─────────────────────────────────────────────────── Active Inboxes / warmup tile
# CANONICAL: core.sending_account, filtered by OUR active-inbox definition (status='active').
# Tiles: O1 (Overview), Accounts ▸ Overview/Account Status hero tiles, ESP split.
data["inboxes"] = safe("inboxes", lambda: {
    "totals": q(f"""
        SELECT COUNT(*) FILTER (WHERE {ACTIVE_INBOX_WHERE})              AS active_inboxes,
               COUNT(*)                                                  AS total_ever,
               SUM(daily_limit) FILTER (WHERE {ACTIVE_INBOX_WHERE})      AS daily_capacity
        FROM core.sending_account""")[0],
    "warmup": one(f"SELECT COUNT(*) FROM core.sending_account WHERE {WARMUP_WHERE}"),
    "by_status": q("""
        SELECT COALESCE(status,'(unknown)') AS status, COUNT(*) AS inboxes,
               SUM(daily_limit) AS daily_capacity, COUNT(DISTINCT domain) AS domains
        FROM core.sending_account GROUP BY 1 ORDER BY inboxes DESC"""),
    "by_lifecycle": q(f"""
        SELECT COALESCE(lifecycle_state,'(unknown)') AS state, COUNT(*) AS inboxes
        FROM core.sending_account WHERE {ACTIVE_INBOX_WHERE} GROUP BY 1 ORDER BY inboxes DESC"""),
    # ESP / inbox-by-provider split (Accounts ▸ Email Provider). esp at account grain
    # is the only OTD-splittable surface (campaign infra_type lumps OTD into google).
    "by_esp": q(f"""
        SELECT esp, COUNT(*) AS inboxes, SUM(daily_limit) AS daily_capacity,
               COUNT(DISTINCT domain) AS domains
        FROM core.sending_account WHERE {ACTIVE_INBOX_WHERE} AND esp IS NOT NULL
        GROUP BY esp ORDER BY inboxes DESC"""),
    # Workspace split (Accounts ▸ Workspaces). Volume ≈ daily sending capacity.
    "by_workspace": q(f"""
        SELECT COALESCE(w.name, sa.workspace_slug, '(unknown)') AS workspace,
               COUNT(*) AS inboxes, SUM(sa.daily_limit) AS daily_capacity,
               COUNT(DISTINCT sa.domain) AS domains
        FROM core.sending_account sa
        LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
        WHERE sa.{ACTIVE_INBOX_WHERE} GROUP BY 1 ORDER BY inboxes DESC"""),
    "total_domains": one(f"SELECT COUNT(DISTINCT domain) FROM core.sending_account WHERE {ACTIVE_INBOX_WHERE}"),
})

# ───────────────────────────────────────────── Meetings: MTD, all-time, record day
# CANONICAL: core.meeting (NOT the orphan meeting_campaign_attribution).
# EMAIL filter = HYBRID (sheet:channel='Email' / slack:regex) — Phase-0 edge case #1.
data["meetings"] = safe("meetings", lambda: {
    "mtd": one(f"""
        SELECT COUNT(*) FROM core.meeting m
        WHERE m.posted_at >= date_trunc('month', current_date)
          AND m.posted_at < current_date + 1 AND {EMAIL_WHERE}"""),
    "all_time": one(f"SELECT COUNT(*) FROM core.meeting m WHERE {EMAIL_WHERE}"),
    "record_day": q(f"""
        SELECT posted_at::DATE::VARCHAR AS d, COUNT(*) AS meetings
        FROM core.meeting m WHERE {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC LIMIT 1""")[0],
    # Daily series (email) for the MTD pace + the bar/line trend.
    "daily_mtd": q(f"""
        SELECT posted_at::DATE::VARCHAR AS d, COUNT(*) AS meetings
        FROM core.meeting m
        WHERE m.posted_at >= date_trunc('month', current_date) AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY 1"""),
    # YTD (calendar-year) email meetings — Business ▸ Overview "YTD Mtgs '26" stat.
    "ytd": one(f"""
        SELECT COUNT(*) FROM core.meeting m
        WHERE m.posted_at >= date_trunc('year', current_date) AND {EMAIL_WHERE}"""),
    # WINDOWED total (>= WINDOW_START) — the attribution-clean count the portal displays
    # for the windowed trend. Labelled "since Jun 1".
    "since_window": one(f"""
        SELECT COUNT(*) FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE}"""),
    "window_label": WINDOW_LABEL,
})

# ───────────────────────────────────────────────────────── MTD S/B (sent ÷ booked)
# CANONICAL sends: raw_pipeline_campaign_daily_metrics.sent (never core.campaign_daily,
# never SUM(unique_*)). Booked = email meetings (above). Period ratio = Σsent ÷ Σbooked
# (recomputed from summed measures, never an average of daily ratios).
data["sb_ratio"] = safe("sb_ratio", lambda: (lambda sent, booked: {
    "mtd_sent": sent,
    "mtd_booked": booked,
    "mtd_sb": round(sent / booked) if booked else None,
})(
    one("""SELECT SUM(sent) FROM raw_pipeline_campaign_daily_metrics
           WHERE date >= date_trunc('month', current_date)"""),
    one(f"""SELECT COUNT(*) FROM core.meeting m
            WHERE m.posted_at >= date_trunc('month', current_date) AND {EMAIL_WHERE}"""),
))

# ───────────────────────────────────────────────────────── Partner summary
# Partner of a booked meeting from core.meeting.partner, label-NORMALIZED across eras
# (Phase-0 edge case #2). MTD + all-time per partner. Email-only (hybrid filter).
# Revenue MODELS (PPA / PPA+10% / 50-50) are business constants kept client-side; the
# warehouse commercial_model from core.funding_partner is included as a reference.
data["partners"] = safe("partners", lambda: {
    "all_time": q(f"""
        SELECT {PARTNER_NORM} AS partner, COUNT(*) AS meetings
        FROM core.meeting m WHERE {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    "mtd": q(f"""
        SELECT {PARTNER_NORM} AS partner, COUNT(*) AS meetings
        FROM core.meeting m
        WHERE m.posted_at >= date_trunc('month', current_date) AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    # WINDOWED per-partner (>= WINDOW_START) — the attribution-clean cut the portal cards
    # display ("All-Time Mtgs" card relabelled "since Jun 1" by index.html).
    "since_window": q(f"""
        SELECT {PARTNER_NORM} AS partner, COUNT(*) AS meetings
        FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    # Monthly per-partner booked (Business ▸ Partners "Bookings Trend by Partner").
    "monthly": q(f"""
        SELECT strftime(date_trunc('month', m.posted_at), '%b ''%y') AS month,
               {PARTNER_NORM} AS partner, COUNT(*) AS meetings
        FROM core.meeting m
        WHERE {EMAIL_WHERE} AND m.posted_at >= current_date - INTERVAL '14 months'
        GROUP BY date_trunc('month', m.posted_at), 2
        ORDER BY date_trunc('month', m.posted_at), meetings DESC"""),
    # Reference: commercial models / tiers from the warehouse dim (NOT the source of
    # the portal's rev-model labels — those stay hardcoded — just provenance).
    "models_ref": safe("partner_models", lambda: q("""
        SELECT display_name AS partner, commercial_model, tier
        FROM core.funding_partner ORDER BY 1""")),
})

# ───────────────────────────────────────────────────── Top CMs (all-time meetings)
# Fact-driven: every real CM who ever booked, ranked by all-time email meetings; top 3.
# CM resolved (core.meeting.cm → raw_text '(CM)'), noise-filtered to real CMs. Email-only
# (hybrid). Overview O3 + CM-tab leaderboards.
data["top_cms"] = safe("top_cms", lambda: q(f"""
    WITH {REAL_CMS_CTE}
    SELECT {CM_RESOLVED} AS cm, COUNT(*) AS meetings
    FROM core.meeting m
    JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
    WHERE {EMAIL_WHERE}
    GROUP BY 1 ORDER BY meetings DESC LIMIT 3"""))

# WINDOWED Top-3 CMs (>= WINDOW_START) — the attribution-clean cut the Overview displays.
data["top_cms_window"] = safe("top_cms_window", lambda: q(f"""
    WITH {REAL_CMS_CTE}
    SELECT {CM_RESOLVED} AS cm, COUNT(*) AS meetings
    FROM core.meeting m
    JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
    WHERE {WINDOW_WHERE} AND {EMAIL_WHERE}
    GROUP BY 1 ORDER BY meetings DESC LIMIT 3"""))

# Full per-CM all-time + MTD (CM tab leaderboards / By-CM). FACT-DRIVEN per window:
#   all_time = every real CM who ever booked (incl. let-go CMs Tomi/Carlos/Shaan/Brendan);
#   mtd      = only CMs with meetings this month (naturally the 5 current CMs).
# CM resolved (core.meeting.cm → raw_text '(CM)'), noise-filtered to real CMs. Email-only.
# NOTE: AI-vs-non-AI split is DEFERRED (no clean warehouse AI flag) — see GENERATOR-NOTES.
data["cms"] = safe("cms", lambda: {
    "all_time": q(f"""
        WITH {REAL_CMS_CTE}
        SELECT {CM_RESOLVED} AS cm, COUNT(*) AS meetings
        FROM core.meeting m
        JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
        WHERE {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    # WINDOWED per-CM (>= WINDOW_START) — the attribution-clean leaderboard the portal shows.
    "since_window": q(f"""
        WITH {REAL_CMS_CTE}
        SELECT {CM_RESOLVED} AS cm, COUNT(*) AS meetings
        FROM core.meeting m
        JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    "mtd": q(f"""
        WITH {REAL_CMS_CTE}
        SELECT {CM_RESOLVED} AS cm, COUNT(*) AS meetings
        FROM core.meeting m
        JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
        WHERE m.posted_at >= date_trunc('month', current_date)
          AND m.posted_at < current_date + 1
          AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY meetings DESC"""),
    # Per-CM inbox count (approx) — account→CM via workspace_slug. PARTIAL: workspace
    # grain, not a true per-CM map (Phase-0 PARTIAL #6). Documented as approximate.
    "inboxes_by_workspace": safe("cm_inboxes", lambda: q(f"""
        SELECT COALESCE(workspace_slug,'(unknown)') AS workspace_slug, COUNT(*) AS inboxes
        FROM core.sending_account WHERE {ACTIVE_INBOX_WHERE}
        GROUP BY 1 ORDER BY inboxes DESC""")),
    # B1 — per-CM email SENDS (the missing half of S/B-by-CM; meetings are above).
    # Source = raw_pipeline_campaign_daily_metrics.sent joined to the campaign dim by
    # campaign_id; CM = UPPER(TRIM(cm_name)) (NAME-derived, never tags). Same REAL_CMS_CTE
    # roster as the meeting cuts -> labels join 1:1 -> client computes S/B-per-CM = sends/meetings.
    # COVERAGE: per-CM sends do NOT sum to org sends (~0-13% orphan campaign-days); coverage block exposed.
    "sends_by_cm": safe("cms_sends_by_cm", lambda: {
        "since_window": q(f"""
            WITH {REAL_CMS_CTE},
            dims AS (
              SELECT DISTINCT ON (campaign_id) campaign_id, UPPER(TRIM(cm_name)) AS cm
              FROM raw_pipeline_campaigns
              WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
              ORDER BY campaign_id, _loaded_at DESC)
            SELECT d.cm AS cm, SUM(cd.sent) AS sends
            FROM raw_pipeline_campaign_daily_metrics cd
            JOIN dims d USING (campaign_id)
            JOIN real_cms rc ON rc.cm = d.cm
            WHERE cd.date >= DATE '{WINDOW_START}'
            GROUP BY 1 ORDER BY sends DESC"""),
        "mtd": q(f"""
            WITH {REAL_CMS_CTE},
            dims AS (
              SELECT DISTINCT ON (campaign_id) campaign_id, UPPER(TRIM(cm_name)) AS cm
              FROM raw_pipeline_campaigns
              WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
              ORDER BY campaign_id, _loaded_at DESC)
            SELECT d.cm AS cm, SUM(cd.sent) AS sends
            FROM raw_pipeline_campaign_daily_metrics cd
            JOIN dims d USING (campaign_id)
            JOIN real_cms rc ON rc.cm = d.cm
            WHERE cd.date >= date_trunc('month', current_date)
            GROUP BY 1 ORDER BY sends DESC"""),
        "monthly": q(f"""
            WITH {REAL_CMS_CTE},
            dims AS (
              SELECT DISTINCT ON (campaign_id) campaign_id, UPPER(TRIM(cm_name)) AS cm
              FROM raw_pipeline_campaigns
              WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
              ORDER BY campaign_id, _loaded_at DESC)
            SELECT strftime(date_trunc('month', cd.date), '%b ''%y') AS month,
                   d.cm AS cm, SUM(cd.sent) AS sends
            FROM raw_pipeline_campaign_daily_metrics cd
            JOIN dims d USING (campaign_id)
            JOIN real_cms rc ON rc.cm = d.cm
            WHERE cd.date >= current_date - INTERVAL '14 months'
            GROUP BY date_trunc('month', cd.date), 2
            ORDER BY date_trunc('month', cd.date), sends DESC"""),
        "coverage": (lambda org, matched: {
            "org_total_mtd": org, "cm_matched_mtd": matched,
            "matched_pct": (round(100.0 * matched / org, 1) if org else None),
            "note": ("per-CM sends are dim-matched (campaign_id -> raw_pipeline_campaigns.cm_name); "
                     "orphan campaign-days + dimension drift leave a ~0-13% gap vs the org total. "
                     "Do NOT expect sum(sends_by_cm) == org sent."),
        })(
            one("""SELECT SUM(sent) FROM raw_pipeline_campaign_daily_metrics
                   WHERE date >= date_trunc('month', current_date)"""),
            one("""WITH dims AS (
                     SELECT DISTINCT ON (campaign_id) campaign_id
                     FROM raw_pipeline_campaigns
                     WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
                     ORDER BY campaign_id, _loaded_at DESC)
                   SELECT SUM(cd.sent) FROM raw_pipeline_campaign_daily_metrics cd
                   JOIN dims d USING (campaign_id)
                   WHERE cd.date >= date_trunc('month', current_date)"""),
        ),
    }),
    # Per-CM per-month sends + meetings (the MONTHLY_CM replacement: CM trend, by-CM,
    # S/B-by-CM). FULL OUTER JOIN of per-CM monthly sends (campaign-name->CM) + per-CM
    # monthly meetings (CM_RESOLVED). Last 14 months; let-go CMs taper out. NO AI split.
    "by_cm_monthly": safe("cms_by_cm_monthly", lambda: q(f"""
        WITH {REAL_CMS_CTE},
        dims AS (
          SELECT DISTINCT ON (campaign_id) campaign_id, UPPER(TRIM(cm_name)) AS cm
          FROM raw_pipeline_campaigns
          WHERE cm_name IS NOT NULL AND TRIM(cm_name) <> ''
          ORDER BY campaign_id, _loaded_at DESC),
        se AS (
          SELECT d.cm AS cm, date_trunc('month', cd.date) AS mo, SUM(cd.sent) AS sends
          FROM raw_pipeline_campaign_daily_metrics cd
          JOIN dims d USING (campaign_id) JOIN real_cms rc ON rc.cm = d.cm
          WHERE cd.date >= current_date - INTERVAL '14 months' GROUP BY 1,2),
        mt AS (
          SELECT {CM_RESOLVED} AS cm, date_trunc('month', m.posted_at) AS mo, COUNT(*) AS meetings
          FROM core.meeting m JOIN real_cms rc ON rc.cm = {CM_RESOLVED}
          WHERE {EMAIL_WHERE} AND m.posted_at >= current_date - INTERVAL '14 months'
          GROUP BY 1, date_trunc('month', m.posted_at))
        SELECT strftime(COALESCE(se.mo, mt.mo), '%b ''%y') AS month,
               COALESCE(se.cm, mt.cm) AS cm,
               COALESCE(se.sends, 0) AS sends, COALESCE(mt.meetings, 0) AS meetings
        FROM se FULL OUTER JOIN mt ON se.cm = mt.cm AND se.mo = mt.mo
        ORDER BY COALESCE(se.mo, mt.mo), sends DESC""")),
})

# ──────────────────────────────────────── Accounts tab (inbox inventory) ───────────
# Source: core.v_account_campaign_offer (account x campaign x offer, sv=78) + core.sending_account
# (domain, daily_limit=capacity). READ-ONLY. The view's `offer` resolves to {Funding, NULL} only
# (NO Section125/ERC in the data), and `cm` is ~93% NULL -> so we emit the ENTIRE attached fleet
# (~494k accounts) as the single real offer the portal surfaces ("Funding"); Section125/ERC fall
# back to the portal's existing Supabase data, and the per-CM tile stays on Supabase too. DEDUP:
# 1 row/account, dimension = mode(); domain/capacity via lower(email)=account_email. Groups +
# per-account Inbox Managers have NO warehouse dimension -> dropped client-side.
_INBOX_ACCT_CTE = """
WITH acct AS (
  SELECT v.account_email,
         mode(v.vendor_category) AS vendor_category,
         mode(v.esp)             AS esp,
         mode(v.account_status)  AS account_status,
         mode(v.workspace_slug)  AS workspace_slug,
         ANY_VALUE(sa.domain)      AS domain,
         ANY_VALUE(sa.daily_limit) AS cap
  FROM core.v_account_campaign_offer v
  LEFT JOIN core.sending_account sa ON lower(sa.email) = v.account_email
  GROUP BY v.account_email
)"""

def _inbox_dim(col):
    return q(f"""{_INBOX_ACCT_CTE}
        SELECT COALESCE({col}, '(unknown)') AS key,
               COUNT(DISTINCT account_email)         AS accounts,
               COUNT(DISTINCT domain)                AS domains,
               CAST(SUM(COALESCE(cap, 0)) AS BIGINT) AS sending_volume
        FROM acct GROUP BY 1 ORDER BY accounts DESC""")

def _inboxes_by_offer():
    totals = q(f"""{_INBOX_ACCT_CTE}
        SELECT COUNT(DISTINCT account_email) AS accounts, COUNT(DISTINCT domain) AS domains,
               CAST(SUM(COALESCE(cap,0)) AS BIGINT) AS sending_volume FROM acct""")[0]
    return {
        "Funding": {
            "accountStatus":  _inbox_dim("account_status"),
            "emailTypes":     _inbox_dim("vendor_category"),
            "emailProviders": _inbox_dim("esp"),
            "workspaces":     _inbox_dim("workspace_slug"),
        },
        "_meta": {
            "distinct_accounts": totals["accounts"], "distinct_domains": totals["domains"],
            "sending_volume_is": "sum(core.sending_account.daily_limit) = capacity",
            "offers_present": ["Funding"], "offers_absent": ["Section 125", "ERC"],
            "note": ("Funding = entire attached fleet (the only real offer in the warehouse). "
                     "campaignManagers/Section125/ERC stay on Supabase; Groups + per-account "
                     "Inbox Managers have no warehouse dimension. Full mapping (not a 30d sample)."),
        },
    }

data["inboxes_by_offer"] = safe("inboxes_by_offer", _inboxes_by_offer)

# ─────────────────────────────────────── Business-Funding meetings trend (bar/line)
# Email-channel meetings/day over the trend window — the headline Overview chart (O4).
# Uses the hybrid channel classifier so SMS/WhatsApp don't pollute it.
data["trend"] = safe("trend", lambda: q(f"""
    SELECT posted_at::DATE::VARCHAR AS d, COUNT(*) AS meetings
    FROM core.meeting m
    WHERE m.posted_at >= current_date - {TREND_DAYS} AND {EMAIL_WHERE}
    GROUP BY 1 ORDER BY 1"""))

# Monthly rollup (the portal's monthlyData replacement: month, booked, sent, ratio).
# Drives Overview O4 + Business ▸ Overview/Volume/Insights. Period ratio per month.
data["monthly"] = safe("monthly", lambda: q(f"""
    WITH mt AS (
      SELECT date_trunc('month', m.posted_at) AS mo, COUNT(*) AS booked
      FROM core.meeting m WHERE {EMAIL_WHERE} GROUP BY 1),
    se AS (
      SELECT date_trunc('month', date) AS mo, SUM(sent) AS sent
      FROM raw_pipeline_campaign_daily_metrics GROUP BY 1)
    SELECT strftime(COALESCE(mt.mo, se.mo), '%b ''%y') AS month,
           COALESCE(mt.booked,0) AS booked, COALESCE(se.sent,0) AS sent,
           CASE WHEN COALESCE(mt.booked,0)>0 THEN round(se.sent::DOUBLE/mt.booked) ELSE 0 END AS ratio
    FROM mt FULL OUTER JOIN se USING (mo)
    WHERE COALESCE(mt.mo, se.mo) >= current_date - INTERVAL '18 months'
    ORDER BY COALESCE(mt.mo, se.mo)"""))

# ───────────────────────── C2gen — monthly[] enrichments + day-of-week (dow[]) ─────
# Reuses the SAME daily booked (email meetings, hybrid filter) + daily sent
# (raw_pipeline_campaign_daily_metrics.sent) the feed already computes — at full history —
# to add per-month enrichments + a top-level dow[]. ALL ratios are PERIOD ratios
# (Sum sent / Sum booked), never averages of daily ratios. 6 S/B tiers = portal KPI_BANDS
# verbatim: a(>4500) b(4000-4500) c(3600-4000) d(3200-3600) e(2800-3200) f(0-2800).
# monthly_enriched rows key on the SAME "%b '%y" label as data["monthly"] (client merges by month).
_C2_BAND_CASE = (
    "CASE WHEN booked=0 THEN NULL "
    "WHEN sent::DOUBLE/booked >= 4500 THEN 'a' "
    "WHEN sent::DOUBLE/booked >= 4000 THEN 'b' "
    "WHEN sent::DOUBLE/booked >= 3600 THEN 'c' "
    "WHEN sent::DOUBLE/booked >= 3200 THEN 'd' "
    "WHEN sent::DOUBLE/booked >= 2800 THEN 'e' "
    "ELSE 'f' END"
)
_C2_DAILY_CTE = f"""
    bk AS (
      SELECT m.posted_at::DATE AS d, COUNT(*) AS booked
      FROM core.meeting m WHERE {EMAIL_WHERE} GROUP BY 1),
    se AS (
      SELECT date AS d, SUM(sent) AS sent
      FROM raw_pipeline_campaign_daily_metrics GROUP BY 1),
    day AS (
      SELECT COALESCE(bk.d, se.d) AS d, COALESCE(bk.booked,0) AS booked,
             COALESCE(se.sent,0) AS sent, isodow(COALESCE(bk.d, se.d)) AS iso,
             dayname(COALESCE(bk.d, se.d)) AS dname
      FROM bk FULL OUTER JOIN se ON bk.d = se.d)"""
_C2_DAILY_CTE_WINDOWED = f"""
    bk AS (
      SELECT m.posted_at::DATE AS d, COUNT(*) AS booked
      FROM core.meeting m WHERE {EMAIL_WHERE} AND m.posted_at >= current_date - {TREND_DAYS} GROUP BY 1),
    se AS (
      SELECT date AS d, SUM(sent) AS sent
      FROM raw_pipeline_campaign_daily_metrics WHERE date >= current_date - {TREND_DAYS} GROUP BY 1),
    day AS (
      SELECT COALESCE(bk.d, se.d) AS d, COALESCE(bk.booked,0) AS booked,
             COALESCE(se.sent,0) AS sent, isodow(COALESCE(bk.d, se.d)) AS iso,
             dayname(COALESCE(bk.d, se.d)) AS dname
      FROM bk FULL OUTER JOIN se ON bk.d = se.d)"""
data["monthly_enriched"] = safe("monthly_enriched", lambda: q(f"""
    WITH {_C2_DAILY_CTE},
    banded AS (SELECT *, {_C2_BAND_CASE} AS band FROM day
               WHERE d >= current_date - INTERVAL '18 months')
    SELECT strftime(date_trunc('month', d), '%b ''%y')               AS month,
           COUNT(*)                                                  AS days_with_data,
           SUM(booked)                                               AS booked,
           SUM(sent)                                                 AS sent,
           ROUND(SUM(booked)::DOUBLE / NULLIF(COUNT(*),0))           AS avg_daily_meetings,
           CASE WHEN SUM(booked) FILTER (WHERE iso<=5)>0
                THEN ROUND(SUM(sent) FILTER (WHERE iso<=5)::DOUBLE
                           / SUM(booked) FILTER (WHERE iso<=5)) ELSE 0 END AS weekday_ratio,
           CASE WHEN SUM(booked) FILTER (WHERE iso>=6)>0
                THEN ROUND(SUM(sent) FILTER (WHERE iso>=6)::DOUBLE
                           / SUM(booked) FILTER (WHERE iso>=6)) ELSE 0 END AS weekend_ratio,
           COUNT(*) FILTER (WHERE band IS NOT NULL)                  AS kpi_days_rated,
           COUNT(*) FILTER (WHERE band='a') AS kpi_days_a, COUNT(*) FILTER (WHERE band='b') AS kpi_days_b,
           COUNT(*) FILTER (WHERE band='c') AS kpi_days_c, COUNT(*) FILTER (WHERE band='d') AS kpi_days_d,
           COUNT(*) FILTER (WHERE band='e') AS kpi_days_e, COUNT(*) FILTER (WHERE band='f') AS kpi_days_f,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='a')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_a,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='b')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_b,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='c')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_c,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='d')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_d,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='e')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_e,
           ROUND(100.0*COUNT(*) FILTER (WHERE band='f')/NULLIF(COUNT(*) FILTER (WHERE band IS NOT NULL),0),2) AS kpi_pct_f
    FROM banded GROUP BY date_trunc('month', d) ORDER BY date_trunc('month', d)"""))
data["dow"] = safe("dow", lambda: q(f"""
    WITH {_C2_DAILY_CTE_WINDOWED}
    SELECT iso AS iso_dow, dname AS day, COUNT(*) AS days,
           SUM(booked) AS total_booked, SUM(sent) AS total_sent,
           ROUND(SUM(booked)::DOUBLE / NULLIF(COUNT(*),0))      AS avg_booked,
           CASE WHEN SUM(booked)>0 THEN ROUND(SUM(sent)::DOUBLE / SUM(booked)) ELSE 0 END AS avg_ratio
    FROM day GROUP BY iso, dname ORDER BY iso"""))

# ───────────────────────────────────── Daily booked + sent (Business ▸ Daily Trends)
# Daily email meetings + daily sends, for the dual-axis composed chart + Day-of-Week.
data["daily"] = safe("daily", lambda: (lambda booked, sent: {"booked": booked, "sent": sent})(
    q(f"""
        SELECT posted_at::DATE::VARCHAR AS d, COUNT(*) AS meetings
        FROM core.meeting m WHERE {EMAIL_WHERE} AND m.posted_at >= current_date - {TREND_DAYS}
        GROUP BY 1 ORDER BY 1"""),
    q(f"""
        SELECT date::VARCHAR AS d, SUM(sent) AS sent
        FROM raw_pipeline_campaign_daily_metrics WHERE date >= current_date - {TREND_DAYS}
        GROUP BY 1 ORDER BY 1"""),
))

# ─────────────────────────────────── Inbox Management bookings (im tab — by channel)
# The IM tab counts ALL bookings (every channel), grouped by workspace / campaign /
# partner / date. We expose the all-channel meeting rollups the warehouse CAN serve.
# CAVEAT: warehouse core.meeting dates on SUBMISSION time; the portal's im_bookings
# dates on SCHEDULED time (Phase-0 §1.3) — these are different populations, do NOT
# equate the core.meeting totals. What core.meeting CAN serve = by-channel / by-partner
# / monthly. (Advisor + Inbox-Manager leaderboards are served separately, WINDOWED to
# >= Jun 1, from core.meeting below — the attribution-clean cut the portal displays.)
data["im_bookings"] = safe("im_bookings", lambda: {
    "by_channel": q("""
        SELECT COALESCE(channel, 'email_or_unknown') AS channel, COUNT(*) AS bookings
        FROM core.meeting m GROUP BY 1 ORDER BY bookings DESC"""),
    "by_partner_all_channel": q(f"""
        SELECT {PARTNER_NORM} AS partner, COUNT(*) AS bookings
        FROM core.meeting m GROUP BY 1 ORDER BY bookings DESC"""),
    "monthly_all_channel": q("""
        SELECT strftime(date_trunc('month', posted_at), '%Y-%m') AS month, COUNT(*) AS bookings
        FROM core.meeting m
        WHERE posted_at >= current_date - INTERVAL '24 months'
        GROUP BY 1 ORDER BY 1"""),
    # WINDOWED meetings-by-campaign (>= WINDOW_START, email) — the attribution-clean
    # per-campaign cut the portal SHOWS (IM ▸ Campaigns / Workspaces). The ALL-TIME
    # per-campaign cut (32.6% attributed) is HELD/HIDDEN by the portal — not emitted here.
    "by_campaign_window": q(f"""
        SELECT COALESCE(NULLIF(TRIM(m.campaign_name_raw),''),'(unattributed)') AS campaign,
               COUNT(*) AS bookings
        FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE}
        GROUP BY 1 ORDER BY bookings DESC LIMIT 50"""),
    "window_label": WINDOW_LABEL,
})

# ───────────────────────────── Org + Workspace sends & replies (Instantly-NATIVE) ──
# Instantly-native counts are the SOLE truth for replies (reference_warehouse_reply_and_
# tag_truth_20260614). BOTH cuts below now read the SAME native daily fact
# (raw_pipeline_campaign_daily_metrics) so they reconcile by construction:
#   • org-wide rollup: human (unique_replies) / auto (unique_replies_automatic) /
#     total (= human + auto) / positive (unique_opportunities = opps), windowed from
#     SENDS_REPLIES_WINDOW_START (2026-05-15, where native reply coverage is sound).
#     positive_rate = positive / human (per Sam's "positive = opps ÷ human").
#     The org block is the un-grouped aggregate of the SAME query the per-workspace cut
#     uses (just without the per-workspace GROUP BY) — so it reconciles by construction.
#   • per-workspace MTD sends + replies + opps (derived.v_workspace_send_mtd, which itself
#     sums the same native columns).
# NOTE: the org block is NOT currently rendered by index.html — kept native+correct so it
# cannot mislead if ever wired. PRIOR BUG: it was built from mv_esp_send_matrix (the
# DROPPED home-grown classifier), 8-13x wrong on replies.
data["sends_replies"] = safe("sends_replies", lambda: {
    "org": (lambda r: {
        **r,
        "positive_rate": (round(r["positive"] / r["human"], 4)
                          if r and r.get("human") else None),
    })(q(f"""
        SELECT SUM(sent)                     AS sends,
               SUM(unique_replies)           AS human,
               SUM(unique_replies_automatic) AS auto,
               SUM(unique_replies) + SUM(unique_replies_automatic) AS total,
               SUM(unique_opportunities)     AS positive
        FROM raw_pipeline_campaign_daily_metrics
        WHERE date >= DATE '{SENDS_REPLIES_WINDOW_START}'""")[0]),
    "by_workspace_mtd": q("""
        SELECT COALESCE(workspace_label, workspace_id, '(unknown)') AS workspace,
               sent_mtd AS sent, replies_mtd AS replies, opps_mtd_trend AS opps
        FROM derived.v_workspace_send_mtd
        WHERE COALESCE(workspace_deleted, FALSE) = FALSE
        ORDER BY sent_mtd DESC"""),
    "note": (f"Instantly-native (100%, not attribution-dependent). org = "
             f"raw_pipeline_campaign_daily_metrics aggregate windowed from "
             f"{SENDS_REPLIES_WINDOW_START} (human=unique_replies, auto=unique_replies_"
             f"automatic, total=human+auto, positive=unique_opportunities); "
             f"positive_rate = positive / human. by_workspace_mtd is June MTD from the "
             f"same native columns (derived.v_workspace_send_mtd)."),
})

# ───────────────────── Advisor + Inbox-Manager leaderboards — WINDOWED (>= Jun 1)
# Sam (2026-06-15): "window it". The portal DISPLAYS advisor + IM leaderboards over the
# attribution-clean window only (>= WINDOW_START, 94-100% attributed) — labelled
# "since Jun 1". Served DIRECTLY from core.meeting (which carries advisor_name / advisor /
# advisor_partner / inbox_manager on the sheet-era rows), email-filtered. This both
# fulfils the windowing decision AND removes the dependency on the DDL-71 all-time UNION
# views (v_advisor_alltime / v_inbox_manager_alltime), whose ALL-TIME cut is the partial
# (7.7%) leaderboard the portal HOLDS/HIDES per the 100%-or-wipe rule. The all-time union
# views are read best-effort below for provenance only (null if absent — not displayed).
_ADV_NAME = "COALESCE(NULLIF(TRIM(m.advisor_name),''), NULLIF(TRIM(m.advisor),''))"
data["advisors"] = safe("advisors", lambda: {
    "window_label": WINDOW_LABEL,
    # WINDOWED leaderboard the portal SHOWS (Business ▸ Partners / IM ▸ Partners & Advisors).
    "since_window": q(f"""
        SELECT {_ADV_NAME} AS advisor, COUNT(*) AS bookings,
               max({PARTNER_NORM}) AS advisor_partner
        FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE} AND {_ADV_NAME} IS NOT NULL
        GROUP BY 1 ORDER BY bookings DESC"""),
    "since_window_total": one(f"""
        SELECT COUNT(*) FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE} AND {_ADV_NAME} IS NOT NULL"""),
    # HELD (partial) all-time union — provenance only; portal does not display it.
    "all_time_held": safe("advisors_alltime_held", lambda: q("""
        SELECT advisor_name AS advisor, bookings_all_time AS bookings
        FROM derived.v_advisor_alltime_summary ORDER BY bookings_all_time DESC""")),
})

data["inbox_managers"] = safe("inbox_managers", lambda: {
    "window_label": WINDOW_LABEL,
    # WINDOWED IM leaderboard the portal SHOWS (IM ▸ Inbox Managers).
    "since_window": q(f"""
        SELECT NULLIF(TRIM(m.inbox_manager),'') AS inbox_manager, COUNT(*) AS bookings
        FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE} AND NULLIF(TRIM(m.inbox_manager),'') IS NOT NULL
        GROUP BY 1 ORDER BY bookings DESC"""),
    "since_window_total": one(f"""
        SELECT COUNT(*) FROM core.meeting m
        WHERE {WINDOW_WHERE} AND {EMAIL_WHERE} AND NULLIF(TRIM(m.inbox_manager),'') IS NOT NULL"""),
    # HELD (partial) all-time union — provenance only; portal does not display it.
    "all_time_held": safe("im_alltime_held", lambda: q("""
        SELECT inbox_manager, bookings_all_time AS bookings
        FROM derived.v_inbox_manager_alltime_summary ORDER BY bookings_all_time DESC""")),
})

# ──────────────────────────────── Deals Funded + Bottom-line KPI — HELD/HIDDEN (Gap 1)
# Sam's "Gap 1": the strategic bottom-line KPI is deals_funded × commission ÷ all-in cost.
# The warehouse STRUCTURE for it is built (DDL 73: core.deal_funded + rollup views +
# core.v_kpi_bottom_line) but ships EMPTY — we have not started tracking funded deals yet.
# Per the 100%-or-WIPE rule (feedback_partial_data_100pct_or_wipe_20260614) this section is
# HELD/HIDDEN: it is emitted with held=True and EMPTY/null numbers so the portal renders
# NOTHING until real ~100%-complete funded-deal data exists. It must NEVER show a partial or
# zero-as-if-real number. The portal-side render gate keys on `deals_funded.held` (same hold
# convention as the partial all-time advisor/IM cuts above): when held is True, do not draw
# the tile. Flip held->False ONLY when core.deal_funded carries real, ~100%-complete data
# AND the all-in-cost denominator (core.v_kpi_bottom_line.all_in_cost) is wired (today NULL).
# Best-effort + table-guarded so it never errors before DDL 73 is applied (structure-first).
def _deals_funded_held():
    has_table = table_exists("core.deal_funded")
    n_rows = (one("SELECT COUNT(*) FROM core.deal_funded") or 0) if has_table else 0
    # Read whether the cost denominator is wired (all_in_cost non-null on any KPI row).
    cost_wired = False
    if has_table:
        cost_wired = bool(one(
            "SELECT COUNT(*) FROM core.v_kpi_bottom_line WHERE all_in_cost IS NOT NULL") or 0)
    # HARD HELD until BOTH conditions clear (real data AND cost denominator), per 100%-or-wipe.
    held = (not has_table) or (n_rows == 0) or (not cost_wired)
    reason = (
        "structure not yet applied (DDL 73 deals_funded)" if not has_table
        else "no funded-deal data yet — ships EMPTY in anticipation (Gap 1)" if n_rows == 0
        else "all-in-cost denominator not wired (core.v_kpi_bottom_line.all_in_cost is NULL)" if not cost_wired
        else "")
    return {
        "held": held,                 # portal render gate: True -> draw nothing
        "held_reason": reason,
        "row_count": n_rows,
        # EMPTY payloads while held — never a partial or zero-as-if-real number.
        "by_cm": [], "by_partner": [], "by_workspace": [], "by_month": [],
        "kpi_bottom_line": None,      # deals_funded × commission ÷ all-in cost (NULL until live)
        "definition": (
            "Strategic bottom-line KPI = deals_funded × commission ÷ all-in cost "
            "(feedback_bottom_line_kpi_only). Numerator = Σ commission over funded deals "
            "(core.v_deal_funded_resolved); denominator = monthly all-in cost from "
            "core.cost_ledger (NOT yet wired — stubbed join point in DDL 73). HELD until real "
            "~100%-complete funded-deal data AND the cost denominator both exist."),
    }


data["deals_funded"] = safe("deals_funded", _deals_funded_held)

# ───────────────────────────────────────── Instantly Lead Credits (the one true gap)
# No warehouse table. Pulled by scripts/portal_credits.py (Instantly billing
# plan-details API, read-only) and merged here if its JSON is present.
def _load_credits():
    if not os.path.exists(CREDITS_JSON):
        print(f"[portal_data] WARN credits: {CREDITS_JSON} not found — emitting null "
              f"(run scripts/portal_credits.py first)", file=sys.stderr)
        return None
    with open(CREDITS_JSON) as f:
        return json.load(f)


data["credits"] = safe("credits", _load_credits)

# Emit as a JS assignment so the portal can <script src> it with no fetch / no CORS,
# exactly like the existing im_bookings_data.js it replaces.
sys.stdout.write("// AUTO-GENERATED by renaissance-warehouse/scripts/portal_data.py — DO NOT EDIT.\n")
sys.stdout.write(f"// Source of truth: warehouse.duckdb (read-only). Generated {data['generated_at']}.\n")
sys.stdout.write("window.PORTAL_DATA = ")
sys.stdout.write(json.dumps(data, default=str, separators=(",", ":")))
sys.stdout.write(";\n")

conn.close()
