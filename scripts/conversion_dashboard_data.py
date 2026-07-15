"""Generate the Conversion (reply labels) dashboard feed — dashboards/conversion/data.json.

Feeds the portal tab "Conversion — Reply Labels" (dashboards/conversion/index.html).
Runs inside refresh_portal_feed.sh (conductor job portal-feed-refresh, daily@07:30 UTC)
via the standard lensgen convention: stdout > $REPO/dashboards/conversion/data.json.
READ-ONLY on the warehouse: reads the gated SERVING SNAPSHOT (CORE_DB_PATH), never the
live writer.

Two data layers:
  NATIVE (always available): weekly + campaign-grain sent / human replies / Instantly
    opportunities (raw_pipeline_campaign_daily_metrics.unique_*) + meetings
    (core.v_meeting_truth when applied — DDL 1106 — else core.meeting fallback).
  LABELS (self-activating): the 4-label scheme (opportunity/engagement/confused/
    not_interested + opt_out flag) from the labeling lane's views —
    core.v_label_weekly / core.v_reply_label_current / core.v_lead_label_cohort /
    core.v_opp_to_meeting_conversion / raw_reply_label_event. Until those views exist
    in the snapshot the feed emits status='pending_labels' and the tab renders its
    honest "backfill in progress" state. NO edit needed here when they land — every
    label read introspects the catalog first and adapts common column-name variants.

HONESTY CONTRACT (rendered by the tab, do not strip):
  label_meta = labeled N of M replying leads, coverage window (backfill lands
  recent-first), labeler_version. Rates are recomputed client-side from summed
  counts (portal convention: period ratio, never averaged ratios).

Never hard-fails on a missing/renamed label view: each label section degrades to
null + a note in label_meta.notes (stderr → /root/lens_feeds.err per the wrapper).
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
import duckdb

DB   = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")
DAYS = int(os.environ.get("CONV_DAYS", "190"))          # weekly window (~27 weeks)
CAMPAIGN_MIN_SENT = int(os.environ.get("CONV_CAMPAIGN_MIN_SENT", "1"))

conn = duckdb.connect(DB, read_only=True)

# Funding workspace allow-list — slugs, stable across Instantly renames. Same scope as
# the KPI + campaign-performance dashboards AND the labeling lane's v0 sample.
# (core.campaign_offer_scope refines this per-campaign once DDL 1103 is applied.)
FUNDING_WS = ('renaissance-2', 'renaissance-4', 'renaissance-5',
              'prospects-power', 'koi-and-destroy')
WS_IN = "('" + "','".join(FUNDING_WS) + "')"

def log(msg: str) -> None:
    print(f"[conversion-feed] {msg}", file=sys.stderr)

def exists(schema: str, name: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?", [schema, name]).fetchone()[0] > 0

def cols_of(schema: str, name: str) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, name]).fetchall()]

def pick(cols: list[str], *cands: str) -> str | None:
    low = {c.lower(): c for c in cols}
    for cand in cands:
        if cand in low:
            return low[cand]
    return None

def q(sql: str, params: list | None = None) -> list[dict]:
    cur = conn.execute(sql, params or [])
    names = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        out.append({n: (str(v) if hasattr(v, "isoformat") else v) for n, v in zip(names, row)})
    return out

notes: list[str] = []

# ── workspace display names (NEVER raw_pipeline_campaigns.workspace_name) ──────────
ws_names = {r["slug"]: r["name"] for r in q(
    f"SELECT slug, name FROM core.workspace WHERE slug IN {WS_IN}")}

# ── campaign scope: allow-listed workspaces, minus out-of-funding-scope campaigns
#    once core.campaign_offer_scope (DDL 1103) is applied ────────────────────────────
scope_join, scope_where = "", ""
if exists("core", "campaign_offer_scope"):
    scc = cols_of("core", "campaign_offer_scope")
    flag = pick(scc, "in_funding_scope")
    if flag:
        scope_join  = "LEFT JOIN core.campaign_offer_scope scp USING (campaign_id)"
        scope_where = f"AND COALESCE(scp.{flag}, TRUE)"   # unresolved => keep (stated basis)
        notes.append("funding scope: workspace allow-list + core.campaign_offer_scope")
if not scope_where:
    notes.append("funding scope: workspace allow-list only (campaign_offer_scope not applied yet)")

# ── NATIVE weekly: sent / human replies / auto replies / Instantly opps ─────────────
native_weekly = q(f"""
    SELECT CAST(CAST(date_trunc('week', cd.date) AS DATE) AS VARCHAR)      AS week,
           cd.workspace_id                                    AS ws,
           SUM(cd.sent)::BIGINT                               AS sent,
           SUM(cd.unique_replies)::BIGINT                     AS replies_human,
           SUM(cd.unique_replies_automatic)::BIGINT           AS replies_auto,
           SUM(cd.unique_opportunities)::BIGINT               AS native_opps
    FROM raw_pipeline_campaign_daily_metrics cd
    {scope_join}
    WHERE cd.workspace_id IN {WS_IN}
      AND cd.date >= current_date - {DAYS} AND cd.date < current_date + 1
      {scope_where}
    GROUP BY 1, 2 ORDER BY 1, 2
""")

# ── meetings: v_meeting_truth (DDL 1106) preferred, core.meeting fallback ───────────
meetings_weekly: list[dict]
meetings_by_campaign: dict[str, int] = {}
meeting_leads: str | None = None          # SQL snippet: distinct (lead_email) with a meeting
if exists("core", "v_meeting_truth"):
    meetings_src = "core.v_meeting_truth (email · ours · funding scope)"
    # EXACT 1109 meetings_v2 scope (email · ours · funding) — no workspace allow-list drop:
    # non-allow-list/deleted workspaces bucket to '(other/deleted ws)' and show under All only.
    meetings_weekly = q(f"""
        SELECT CAST(CAST(date_trunc('week', meeting_date) AS DATE) AS VARCHAR) AS week,
               CASE WHEN workspace_slug IN {WS_IN} THEN workspace_slug
                    WHEN NULLIF(workspace_slug, '') IS NULL THEN '(unattributed)'
                    ELSE '(other/deleted ws)' END AS ws,
               COUNT(*)::BIGINT AS meetings
        FROM core.v_meeting_truth
        WHERE channel_norm = 'Email' AND is_ours AND COALESCE(in_funding_scope, TRUE)
          AND meeting_date >= current_date - {DAYS}
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    meetings_by_campaign = {r["campaign_id"]: r["n"] for r in q(f"""
        SELECT campaign_id, COUNT(*)::BIGINT AS n FROM core.v_meeting_truth
        WHERE channel_norm = 'Email' AND is_ours AND campaign_id IS NOT NULL
        GROUP BY 1""")}
    meeting_leads = ("SELECT DISTINCT lower(lead_email) AS lead_email FROM core.v_meeting_truth "
                     "WHERE channel_norm = 'Email' AND is_ours AND lead_email IS NOT NULL")
else:
    meetings_src = "core.meeting fallback (email heuristic — v_meeting_truth not applied yet)"
    notes.append("meetings: " + meetings_src)
    meetings_weekly = q(f"""
        SELECT CAST(CAST(date_trunc('week', COALESCE(meeting_date, CAST(posted_at AS DATE))) AS DATE) AS VARCHAR) AS week,
               COALESCE(NULLIF(COALESCE(workspace_slug, workspace_canonical), ''), '(unattributed)') AS ws,
               COUNT(*)::BIGINT AS meetings
        FROM core.meeting m
        WHERE COALESCE(meeting_date, CAST(posted_at AS DATE)) >= current_date - {DAYS}
          AND (COALESCE(workspace_slug, workspace_canonical) IN {WS_IN}
               OR NULLIF(COALESCE(workspace_slug, workspace_canonical), '') IS NULL)
          AND ((m.source = 'sheet' AND m.channel = 'Email')
               OR (m.source <> 'sheet'
                   AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                                          'sendivo|\\bsms\\b|whatsapp|iskra')))
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    meetings_by_campaign = {r["campaign_id"]: r["n"] for r in q("""
        SELECT campaign_id, COUNT(*)::BIGINT AS n FROM core.meeting m
        WHERE campaign_id IS NOT NULL
          AND ((m.source = 'sheet' AND m.channel = 'Email')
               OR (m.source <> 'sheet'
                   AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                                          'sendivo|\\bsms\\b|whatsapp|iskra')))
        GROUP BY 1""")}
    meeting_leads = ("SELECT DISTINCT lower(lead_email) AS lead_email FROM core.meeting "
                     "WHERE lead_email IS NOT NULL")

# ── LABEL layer (self-activating) ───────────────────────────────────────────────────
label_source = None
labels_weekly: list[dict] = []
labels_by_campaign: list[dict] = []
opp_met_by_camp: dict[str, tuple] = {}
label_meta: dict = {"labeler_version": None, "labeled_replies": 0,
                    "total_replying_leads": 0, "coverage_pct": None,
                    "coverage_from": None, "coverage_to": None, "notes": notes}
opp_to_meeting: dict | None = None

LABEL_CANON = {"opportunity": "opp", "engagement": "eng", "confused": "conf",
               "not_interested": "ni", "not interested": "ni"}

def find_label_relation() -> tuple[str, list[str]] | None:
    """Newest-first preference: current-state view, then raw event table."""
    for schema, name in (("core", "v_reply_label_current"), ("main", "v_reply_label_current"),
                         ("main", "raw_reply_label_event"), ("core", "raw_reply_label_event")):
        if exists(schema, name):
            return f"{schema}.{name}", cols_of(schema, name)
    return None

rel = find_label_relation()
if rel:
    relname, rc = rel
    c_ws    = pick(rc, "workspace", "workspace_slug", "workspace_id")
    c_email = pick(rc, "lead_email", "email")
    c_label = pick(rc, "label", "current_label", "label_current", "verdict")
    c_opt   = pick(rc, "opt_out", "current_opt_out", "is_opt_out", "optout")
    c_ver   = pick(rc, "labeler_version", "version", "prompt_version")
    c_date  = pick(rc, "reply_date", "reply_at", "message_ts", "current_label_message_ts", "message_at", "labeled_at", "event_at", "created_at")
    c_camp  = pick(rc, "campaign_id", "current_campaign_id")
    if c_ws and c_email and c_label and c_date:
        label_source = relname
        # newest event per (ws, lead) = current label, if the relation is the raw event log
        dedup = "" if "current" in relname else f"QUALIFY row_number() OVER (PARTITION BY {c_ws}, lower({c_email}) ORDER BY {c_date} DESC) = 1"
        base = f"""
            SELECT {c_ws} AS ws, lower({c_email}) AS lead_email,
                   lower(CAST({c_label} AS VARCHAR)) AS label,
                   {f'COALESCE({c_opt}, FALSE)' if c_opt else 'FALSE'} AS opt_out,
                   {f'CAST({c_ver} AS VARCHAR)' if c_ver else 'NULL'} AS labeler_version,
                   CAST({c_date} AS DATE) AS d
                   {f', {c_camp} AS campaign_id' if c_camp else ', NULL AS campaign_id'}
            FROM {relname}
            {dedup}
        """
        try:
            labels_weekly = q(f"""
                WITH b AS ({base})
                SELECT CAST(CAST(date_trunc('week', d) AS DATE) AS VARCHAR) AS week, ws, label,
                       COUNT(*)::BIGINT AS n, SUM(CASE WHEN opt_out THEN 1 ELSE 0 END)::BIGINT AS opt_outs
                FROM b WHERE ws IN {WS_IN} AND label IN ('opportunity','engagement','confused','not_interested','not interested')
                GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
            """)
            for r in labels_weekly:
                r["label"] = LABEL_CANON.get(r["label"], r["label"])
            if c_camp:
                labels_by_campaign = q(f"""
                    WITH b AS ({base})
                    SELECT campaign_id, label, COUNT(*)::BIGINT AS n,
                           SUM(CASE WHEN opt_out THEN 1 ELSE 0 END)::BIGINT AS opt_outs
                    FROM b WHERE campaign_id IS NOT NULL
                      AND label IN ('opportunity','engagement','confused','not_interested','not interested')
                    GROUP BY 1, 2""")
                for r in labels_by_campaign:
                    r["label"] = LABEL_CANON.get(r["label"], r["label"])
                # per-campaign TRUE opp -> meeting (lead_email join, feed-side so the
                # tab never has to approximate it)
                try:
                    opp_met_by_camp = {r["campaign_id"]: (r["opp_leads"], r["opp_met"]) for r in q(f"""
                        WITH b AS ({base}),
                        opps AS (SELECT DISTINCT campaign_id, lead_email FROM b
                                 WHERE campaign_id IS NOT NULL AND label = 'opportunity'),
                        ml AS ({meeting_leads})
                        SELECT campaign_id, COUNT(*)::BIGINT AS opp_leads,
                               SUM(CASE WHEN ml.lead_email IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS opp_met
                        FROM opps LEFT JOIN ml USING (lead_email) GROUP BY 1""")}
                except Exception as e:
                    opp_met_by_camp = {}
                    notes.append(f"per-campaign opp_met failed: {e}")
            meta = q(f"""
                WITH b AS ({base})
                SELECT COUNT(*)::BIGINT AS labeled, MIN(d) AS dmin, MAX(d) AS dmax,
                       MAX(labeler_version) AS ver
                FROM b WHERE ws IN {WS_IN}
                  AND label IN ('opportunity','engagement','confused','not_interested','not interested')
            """)[0]
            label_meta.update({"labeler_version": meta["ver"], "labeled_replies": meta["labeled"],
                               "coverage_from": meta["dmin"], "coverage_to": meta["dmax"]})
            # TRUE opp -> meeting conversion (label-opp leads with a meeting, lead_email join)
            try:
                conv = q(f"""
                    WITH b AS ({base}),
                    opps AS (SELECT DISTINCT ws, lead_email FROM b
                             WHERE ws IN {WS_IN} AND label = 'opportunity'),
                    ml AS ({meeting_leads})
                    SELECT COUNT(*)::BIGINT AS opp_leads,
                           SUM(CASE WHEN ml.lead_email IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS opp_leads_met
                    FROM opps LEFT JOIN ml USING (lead_email)
                """)[0]
                opp_to_meeting = {"opp_leads": conv["opp_leads"], "opp_leads_met": conv["opp_leads_met"],
                                  "basis": "label-opportunity leads with any email meeting (lead_email join)"}
            except Exception as e:
                notes.append(f"opp_to_meeting compute failed: {e}")
        except Exception as e:
            label_source = None
            notes.append(f"label relation {relname} query failed: {e}")
    else:
        notes.append(f"label relation {relname} found but columns unrecognized: {rc}")

# published conversion view wins over the computed join when it exists
if exists("core", "v_opp_to_meeting_conversion"):
    try:
        rows = q("SELECT * FROM core.v_opp_to_meeting_conversion")
        opp_to_meeting = {"rows": rows[:200], "basis": "core.v_opp_to_meeting_conversion (labeling lane view)"}
    except Exception as e:
        notes.append(f"v_opp_to_meeting_conversion read failed: {e}")

# ── coverage denominator: distinct replying leads (all inbound, incl. ~6% autos) ────
try:
    denom = q(f"""
        SELECT COUNT(DISTINCT (workspace_id, lower(lead_email)))::BIGINT AS n,
               CAST(MIN(CASE WHEN message_at >= DATE '2024-01-01' THEN message_at END) AS DATE) AS dmin
        FROM core.email_message
        WHERE direction = 'inbound' AND workspace_id IN {WS_IN}
    """)[0]
    label_meta["total_replying_leads"] = denom["n"]
    label_meta["reply_text_from"] = denom["dmin"]
    if label_meta["labeled_replies"] and denom["n"]:
        label_meta["coverage_pct"] = round(100.0 * label_meta["labeled_replies"] / denom["n"], 1)
except Exception as e:
    notes.append(f"coverage denominator failed: {e}")

# ── NATIVE campaign table (label columns merged in when available) ──────────────────
campaigns = q(f"""
    SELECT cd.campaign_id,
           COALESCE(NULLIF(c.name, ''), cd.campaign_id)      AS name,
           cd.workspace_id                                    AS ws,
           SUM(cd.sent)::BIGINT                               AS sent,
           SUM(cd.unique_replies)::BIGINT                     AS replies_human,
           SUM(cd.unique_opportunities)::BIGINT               AS native_opps,
           CAST(MIN(cd.date) AS VARCHAR)                      AS first_day,
           CAST(MAX(cd.date) AS VARCHAR)                      AS last_day
    FROM raw_pipeline_campaign_daily_metrics cd
    LEFT JOIN core.campaign c USING (campaign_id)
    {scope_join}
    WHERE cd.workspace_id IN {WS_IN}
      AND cd.date >= current_date - {DAYS} AND cd.date < current_date + 1
      {scope_where}
    GROUP BY 1, 2, 3
    HAVING SUM(cd.sent) >= {CAMPAIGN_MIN_SENT} OR SUM(cd.unique_replies) > 0
    ORDER BY sent DESC
    LIMIT 400
""")
lbl_by_camp: dict[str, dict] = {}
for r in labels_by_campaign:
    d = lbl_by_camp.setdefault(r["campaign_id"], {"opp": 0, "eng": 0, "conf": 0, "ni": 0, "opt_outs": 0})
    d[r["label"]] = r["n"]
    d["opt_outs"] += r["opt_outs"] or 0
for c in campaigns:
    c["meetings"] = meetings_by_campaign.get(c["campaign_id"], 0)
    lb = lbl_by_camp.get(c["campaign_id"])
    c.update(lb if lb else {"opp": None, "eng": None, "conf": None, "ni": None, "opt_outs": None})
    om = opp_met_by_camp.get(c["campaign_id"])
    c["opp_met"] = om[1] if om else None

# ── corrected KPI context (DDL 1109) ────────────────────────────────────────────────
corrected_weekly = None
if exists("derived", "v_kpi_weekly_corrected"):
    try:
        corrected_weekly = q(f"""
            SELECT CAST(week_start AS VARCHAR) AS week, active_days_in_week,
                   sends_corrected_stitched::BIGINT AS sends_corrected,
                   meetings_email_v2, epm_corrected, epm_old, is_partial_week
            FROM derived.v_kpi_weekly_corrected
            WHERE week_start >= current_date - {DAYS}
            ORDER BY 1""")
    except Exception as e:
        notes.append(f"v_kpi_weekly_corrected read failed: {e}")
else:
    notes.append("corrected KPI context: derived.v_kpi_weekly_corrected not applied yet")

status = "ok" if (label_source and labels_weekly) else "pending_labels"

print(json.dumps({
    "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "snapshot_id": os.path.basename(os.path.realpath(DB)),
    "status": status,
    "window_days": DAYS,
    "label_source": label_source,
    "meetings_source": meetings_src,
    "workspaces": [{"slug": s, "name": ws_names.get(s, s)} for s in FUNDING_WS],
    "label_meta": label_meta,
    "weekly_native": native_weekly,
    "weekly_meetings": meetings_weekly,
    "weekly_labels": labels_weekly,
    "opp_to_meeting": opp_to_meeting,
    "campaigns": campaigns,
    "corrected_weekly": corrected_weekly,
}, default=str))
