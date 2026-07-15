"""Push the booking-site Conversion tab feed — portal Supabase dashboard_feeds key='conversion'.

Feeds the "Conversion" tab on renaissance-booking.com (generalrenaissance/booking-form).
Runs inside refresh_portal_feed.sh (conductor job portal-feed-refresh, daily@07:30 UTC),
right after the portal-repo conversion feed. READ-ONLY on the warehouse (CORE_DB_PATH =
serving snapshot); the only write is a PostgREST UPSERT into the PORTAL Supabase
(pxrdmjjaxtqycuxhxmgi) table public.dashboard_feeds (service key from
/root/renaissance-worker/.env; RLS = authenticated read-only, service_role write).

SCOPE RULE (Sam, 2026-07-15 — hard): COMPLETED SENDING DAYS ONLY. A partial/in-flight
day must NEVER render on the booking-site tab. Right now that is exactly 2026-07-14
(the one labeled day, MVP). Enforcement is belt-and-suspenders:
  1. here: HARD_MAX_DAY caps every event/metric row (labels AND native metrics), and
     the cap is additionally clamped to yesterday-ET so even a bad override can never
     include today;
  2. in the tab: index.html clamps again to its own HARD_CAP constant.
To extend when Sam greenlights forward motion: set BOOKING_CONV_MAX_DAY=YYYY-MM-DD
(one completed day at a time), or BOOKING_CONV_ROLLING=1 for always-yesterday-ET.
Do NOT flip ROLLING without Sam's explicit greenlight.

Label source: same self-activation as conversion_dashboard_data.py — introspects for
core.v_reply_label_current / raw_reply_label_event; until they exist in the snapshot,
upserts status='pending_labels' (the tab renders its honest empty state).
Never hard-fails the conductor: any error prints WARN to stderr and exits 0.
"""
from __future__ import annotations
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import duckdb

DB = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")
WORKER_ENV = os.environ.get("WORKER_ENV_FILE", "/root/renaissance-worker/.env")
HARD_MAX_DAY = "2026-07-14"            # MVP: the one Sam-approved completed labeled day
HARD_MIN_DAY = "2026-07-14"            # floor: excludes the partial Jul-13 sliver (backfill entered Jul-13 at 23:21 only); a day renders only if FULLY labeled

# 7 workspaces (Sam 2026-07-15): the 5 funding slugs + warm-leads + renaissance-1.
# The KPIs tab's "Warm Leads excluded" convention does NOT apply to the Conversion view —
# Sam explicitly wants both included here. (NOT the-gatekeepers.)
FUNDING_WS = ('renaissance-2', 'renaissance-4', 'renaissance-5',
              'prospects-power', 'koi-and-destroy', 'warm-leads', 'renaissance-1')
WS_IN = "('" + "','".join(FUNDING_WS) + "')"

def log(msg: str) -> None:
    print(f"[conversion-booking-feed] {msg}", file=sys.stderr)

def env_from_worker(key: str) -> str | None:
    if os.environ.get(key):
        return os.environ[key]
    try:
        with open(WORKER_ENV) as fh:
            for line in fh:
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return None

def effective_max_day() -> str:
    yesterday_et = (datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)).isoformat()
    cap = HARD_MAX_DAY
    if os.environ.get("BOOKING_CONV_ROLLING") == "1":
        cap = yesterday_et
    elif os.environ.get("BOOKING_CONV_MAX_DAY"):
        cap = os.environ["BOOKING_CONV_MAX_DAY"]
    return min(cap, yesterday_et)      # NEVER today, whatever the config says

def main() -> None:
    cap = effective_max_day()
    conn = duckdb.connect(DB, read_only=True)

    def exists(schema: str, name: str) -> bool:
        return conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            [schema, name]).fetchone()[0] > 0

    def cols_of(schema: str, name: str) -> list[str]:
        return [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=? AND table_name=?",
            [schema, name]).fetchall()]

    def pick(cols: list[str], *cands: str) -> str | None:
        low = {c.lower(): c for c in cols}
        for cand in cands:
            if cand in low:
                return low[cand]
        return None

    def q(sql: str) -> list[dict]:
        cur = conn.execute(sql)
        names = [d[0] for d in cur.description]
        return [{n: (str(v) if hasattr(v, "isoformat") else v) for n, v in zip(names, row)}
                for row in cur.fetchall()]

    ws_names = {r["slug"]: r["name"] for r in q(f"SELECT slug, name FROM core.workspace WHERE slug IN {WS_IN}")}

    # ── label relation (self-activating, same contract as conversion_dashboard_data.py) ──
    rel = None
    for schema, name in (("core", "v_reply_label_current"), ("main", "v_reply_label_current"),
                         ("main", "raw_reply_label_event"), ("core", "raw_reply_label_event")):
        if exists(schema, name):
            rel = (f"{schema}.{name}", cols_of(schema, name))
            break

    payload: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_id": os.path.basename(os.path.realpath(DB)),
        "status": "pending_labels",
        "labeler_version": None,
        "scope": {"mode": "completed_days_only", "max_day": cap, "min_day": HARD_MIN_DAY, "days": []},
        "coverage": None, "totals": None, "rows": [],
    }

    if rel:
        relname, rc = rel
        c_ws    = pick(rc, "workspace", "workspace_slug", "workspace_id")
        c_email = pick(rc, "lead_email", "email")
        c_label = pick(rc, "label", "current_label", "label_current", "verdict")
        c_opt   = pick(rc, "opt_out", "current_opt_out", "is_opt_out", "optout")
        c_ver   = pick(rc, "labeler_version", "version", "prompt_version")
        c_date  = pick(rc, "reply_date", "reply_at", "message_ts", "current_label_message_ts", "message_at", "labeled_at", "event_at", "created_at")
        if c_ws and c_email and c_label and c_date:
            dedup = "" if "current" in relname else \
                f"QUALIFY row_number() OVER (PARTITION BY {c_ws}, lower({c_email}) ORDER BY {c_date} DESC) = 1"
            base = f"""
                SELECT {c_ws} AS ws, lower({c_email}) AS lead_email,
                       lower(CAST({c_label} AS VARCHAR)) AS label,
                       {f'COALESCE({c_opt}, FALSE)' if c_opt else 'FALSE'} AS opt_out,
                       {f'CAST({c_ver} AS VARCHAR)' if c_ver else 'NULL'} AS labeler_version,
                       CAST({c_date} AS DATE) AS d
                FROM {relname} {dedup}
            """
            # HARD CAP: no event after the last completed day, ever (rule 1 of 2).
            scoped = f"""
                WITH b AS ({base})
                SELECT * FROM b
                WHERE ws IN {WS_IN} AND d <= DATE '{cap}' AND d >= DATE '{HARD_MIN_DAY}'
                  AND label IN ('opportunity','engagement','confused','not_interested','not interested')
            """
            days = [r["d"] for r in q(f"SELECT DISTINCT d FROM ({scoped}) ORDER BY d")]
            if days:
                days_in = "('" + "','".join(days) + "')"
                # POST-REPLY conversion (Sam 2026-07-15): an opp lead counts as converted only
                # if a meeting is booked ON OR AFTER the labeled reply date — a prior booking is
                # not a conversion of this reply (the lifetime join inflated warm-leads most).
                meeting_leads = ("SELECT DISTINCT lower(lead_email) AS lead_email, meeting_date "
                                 "FROM core.v_meeting_truth "
                                 "WHERE channel_norm = 'Email' AND is_ours AND lead_email IS NOT NULL "
                                 "AND meeting_date IS NOT NULL"
                                 ) if exists("core", "v_meeting_truth") else (
                                 "SELECT DISTINCT lower(lead_email) AS lead_email, "
                                 "COALESCE(meeting_date, CAST(posted_at AS DATE)) AS meeting_date "
                                 "FROM core.meeting WHERE lead_email IS NOT NULL")
                rows = q(f"""
                    WITH s AS ({scoped}), ml AS ({meeting_leads}),
                    lab AS (
                      SELECT ws,
                             COUNT(*)                                                    AS labeled,
                             SUM(CASE WHEN label = 'opportunity' THEN 1 ELSE 0 END)      AS opp,
                             SUM(CASE WHEN label = 'engagement' THEN 1 ELSE 0 END)       AS eng,
                             SUM(CASE WHEN label = 'confused' THEN 1 ELSE 0 END)         AS conf,
                             SUM(CASE WHEN label IN ('not_interested','not interested') THEN 1 ELSE 0 END) AS ni,
                             SUM(CASE WHEN opt_out THEN 1 ELSE 0 END)                    AS opt_outs
                      FROM s GROUP BY 1),
                    om AS (
                      SELECT s.ws, COUNT(DISTINCT s.lead_email) AS opp_leads,
                             COUNT(DISTINCT CASE WHEN ml.lead_email IS NOT NULL THEN s.lead_email END) AS opp_met
                      FROM s LEFT JOIN ml ON ml.lead_email = s.lead_email AND ml.meeting_date >= s.d
                      WHERE s.label = 'opportunity' GROUP BY 1),
                    nat AS (
                      SELECT workspace_id AS ws,
                             SUM(sent)::BIGINT                          AS sent,
                             SUM(unique_replies)::BIGINT                AS replies_native,
                             SUM(unique_replies_automatic)::BIGINT      AS replies_auto,
                             SUM(unique_opportunities)::BIGINT          AS native_opps
                      FROM raw_pipeline_campaign_daily_metrics
                      WHERE workspace_id IN {WS_IN} AND CAST(date AS VARCHAR) IN {days_in}
                      GROUP BY 1)
                    SELECT COALESCE(lab.ws, nat.ws) AS ws,
                           nat.sent, nat.replies_native, nat.replies_auto, nat.native_opps,
                           lab.labeled, lab.opp, lab.eng, lab.conf, lab.ni, lab.opt_outs,
                           om.opp_leads, om.opp_met
                    FROM lab FULL JOIN nat ON nat.ws = lab.ws
                    LEFT JOIN om ON om.ws = COALESCE(lab.ws, nat.ws)
                    ORDER BY 1
                """)
                # Coverage — like-for-like at LEAD grain (the labeling lane's measure):
                # denominator = the day's canonical replying leads from core.reply (BOTH
                # workspace_id encodings — UUID and slug — mapped via core.workspace_alias;
                # this is derived.v_reply_canonical's own base, queried directly because the
                # view is a slow full-scan and its is_auto_reply flag is a documented-BROKEN
                # heuristic that returns zero rows). numerator = those leads with ANY label
                # event in-window (ALL classes count as read — auto/bot/unreadable gates
                # included, those replies WERE read). Same universe on both sides, so 100%
                # is reachable exactly when the completion pass has covered the day.
                try:
                    cov = q(f"""
                        WITH wmap AS (
                          SELECT instantly_uuid AS wid, warehouse_slug AS slug
                          FROM core.workspace_alias
                          WHERE warehouse_slug IN {WS_IN} AND instantly_uuid IS NOT NULL
                          UNION ALL
                          SELECT warehouse_slug, warehouse_slug FROM core.workspace_alias
                          WHERE warehouse_slug IN {WS_IN}),
                        canon AS (
                          SELECT DISTINCT m.slug AS ws, lower(r.lead_email) AS lead_email
                          FROM core.reply r JOIN wmap m ON m.wid = r.workspace_id
                          WHERE CAST(CAST(r.reply_timestamp AS DATE) AS VARCHAR) IN {days_in}),
                        ev AS (
                          SELECT DISTINCT workspace_slug AS ws, lower(lead_email) AS lead_email
                          FROM main.raw_reply_label_event
                          WHERE workspace_slug IN {WS_IN}
                            AND CAST(CAST(message_ts AS DATE) AS VARCHAR) IN {days_in})
                        SELECT (SELECT COUNT(*) FROM canon)::BIGINT AS replying_leads,
                               (SELECT COUNT(*) FROM canon c JOIN ev e USING (ws, lead_email))::BIGINT AS read_leads
                    """)[0]
                    read_n, denom = cov["read_leads"], cov["replying_leads"]
                except Exception as e:
                    notes_unused = str(e)
                    log(f"coverage compute failed ({e}) — falling back to legacy grain")
                    denom = q(f"""
                        SELECT COUNT(DISTINCT (workspace_id, lower(lead_email)))::BIGINT AS n
                        FROM core.email_message
                        WHERE direction = 'inbound' AND workspace_id IN {WS_IN}
                          AND CAST(CAST(message_at AS DATE) AS VARCHAR) IN {days_in}
                    """)[0]["n"]
                    read_n = None
                unreadable_n = q(f"""
                    SELECT COUNT(*)::BIGINT AS n FROM main.raw_reply_label_event
                    WHERE workspace_slug IN {WS_IN}
                      AND CAST(CAST(message_ts AS DATE) AS VARCHAR) IN {days_in}
                      AND (label = 'unreadable' OR deterministic_gate = 'unreadable_no_text')
                """)[0]["n"]
                meta = q(f"SELECT MAX(labeler_version) AS ver, COUNT(*) AS n FROM ({scoped})")[0]
                for r in rows:
                    r["name"] = ws_names.get(r["ws"], r["ws"])
                tot = {k: sum(int(r[k] or 0) for r in rows) for k in
                       ("sent", "replies_native", "replies_auto", "native_opps", "labeled",
                        "opp", "eng", "conf", "ni", "opt_outs", "opp_leads", "opp_met")}
                payload.update({
                    "status": "ok",
                    "labeler_version": meta["ver"],
                    "scope": {"mode": "completed_days_only", "max_day": cap, "min_day": HARD_MIN_DAY, "days": days},
                    "coverage": {"labeled": (read_n if read_n is not None else meta["n"]),
                                 "replying_leads_days": denom,
                                 "pct": round(100.0 * (read_n if read_n is not None else meta["n"]) / denom, 1) if denom else None,
                                 "unreadable": unreadable_n},
                    "totals": tot, "rows": rows,
                })
            else:
                log(f"label relation {relname} present but zero rows within cap {cap}")
        else:
            log(f"label relation {relname} columns unrecognized: {rc}")
    else:
        log("label views not in snapshot yet — upserting pending_labels state")

    if os.environ.get("BOOKING_CONV_DRY") == "1":   # test mode: print, don't push
        print(json.dumps(payload, default=str))
        return

    # ── UPSERT into the portal Supabase (service key; RLS: authenticated read-only) ──
    purl = env_from_worker("PORTAL_SUPABASE_URL")
    pkey = env_from_worker("PORTAL_SUPABASE_SERVICE_ROLE_KEY")
    if not purl or not pkey:
        log("WARN missing PORTAL_SUPABASE creds — nothing pushed")
        return
    body = json.dumps([{"key": "conversion", "data": payload,
                        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}],
                      default=str).encode()
    req = urllib.request.Request(
        purl.rstrip("/") + "/rest/v1/dashboard_feeds", data=body, method="POST",
        headers={"apikey": pkey, "Authorization": "Bearer " + pkey,
                 "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates,return=minimal"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        print(f"  ok conversion-booking-feed upserted (status={payload['status']}, "
              f"days={payload['scope']['days']}, HTTP {resp.status})")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:                       # non-fatal by contract
        log(f"WARN failed (feed keeps last-known-good): {e}")
        sys.exit(0)
