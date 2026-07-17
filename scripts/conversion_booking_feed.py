"""Push the booking-site Conversion tab feed — portal Supabase dashboard_feeds key='conversion'.

v2 (Sam R30, 2026-07-17): DAY x WORKSPACE grain, rolling completed days (Jul 14/15/16 -> daily).
Feeds the "Conversion" tab on renaissance-booking.com (generalrenaissance/booking-form).
Runs inside refresh_portal_feed.sh (conductor job portal-feed-refresh, daily@07:30 UTC).
READ-ONLY on the warehouse (CORE_DB_PATH = serving snapshot); the only write is a PostgREST
UPSERT into the PORTAL Supabase (pxrdmjjaxtqycuxhxmgi) public.dashboard_feeds
(service key from /root/renaissance-worker/.env; RLS = authenticated read, service_role write).

SCOPE RULE: COMPLETED SENDING DAYS ONLY — the cap is yesterday-ET, always (the Jul-14-only
MVP pin is superseded by R30). A partial/in-flight day never ships. Override down with
BOOKING_CONV_MAX_DAY=YYYY-MM-DD if a day must be held back; the cap can never exceed
yesterday-ET regardless of config. Day rows appear automatically as label events land
(R18: Jul-15+ is labeled on the Instantly-positive slice only; Jul-14 was a full-read day —
per-day coverage 'mode' states which).

METRIC CONTRACT (R30): positives = opportunity + engaged (labels); denominators = native
Instantly daily facts (sent / unique_replies / unique_replies_automatic — complete facts);
Instantly's own opportunity auto-count carried only as a comparison field. opp->meeting =
meetings booked ON/AFTER the labeled reply date (grows for days).

Never hard-fails the conductor: any error prints WARN to stderr and exits 0.
"""
from __future__ import annotations
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import duckdb

DB = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")
WORKER_ENV = os.environ.get("WORKER_ENV_FILE", "/root/renaissance-worker/.env")
FULL_READ_THROUGH = "2026-07-14"   # R18 boundary: <= this day = full-read; after = positive-slice
MIN_DAY = "2026-07-14"             # R11 floor: the Jul-13 sliver (labels whose anchoring reply
                                   # trailed into Jul-13 during the Jul-14 batch) is NOT a
                                   # presentable day — first fully-covered day = 2026-07-14.

# 7 workspaces (Sam 2026-07-15): 5 funding slugs + warm-leads + renaissance-1 (NOT the-gatekeepers).
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
    cap = os.environ.get("BOOKING_CONV_MAX_DAY") or yesterday_et
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

    # ── label relation (self-activating; DDL 1110-1114 contract) ────────────────────
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
        "grain": "day_x_workspace",
        "labeler_version": None,
        "scope": {"mode": "completed_days_only", "max_day": cap, "days": []},
        "day_meta": [], "rows": [],
    }

    if rel:
        relname, rc = rel
        c_ws    = pick(rc, "workspace", "workspace_slug", "workspace_id")
        c_email = pick(rc, "lead_email", "email")
        c_label = pick(rc, "label", "current_label", "label_current", "verdict")
        c_opt   = pick(rc, "opt_out", "current_opt_out", "is_opt_out", "optout")
        c_ver   = pick(rc, "labeler_version", "version", "prompt_version")
        c_date  = pick(rc, "reply_date", "reply_at", "message_ts", "current_label_message_ts",
                       "message_at", "labeled_at", "event_at", "created_at")
        if c_ws and c_email and c_label and c_date:
            dedup = "" if "current" in relname else \
                f"QUALIFY row_number() OVER (PARTITION BY {c_ws}, lower({c_email}), CAST({c_date} AS DATE) ORDER BY {c_date} DESC) = 1"
            base = f"""
                SELECT {c_ws} AS ws, lower({c_email}) AS lead_email,
                       lower(CAST({c_label} AS VARCHAR)) AS label,
                       {f'COALESCE({c_opt}, FALSE)' if c_opt else 'FALSE'} AS opt_out,
                       {f'CAST({c_ver} AS VARCHAR)' if c_ver else 'NULL'} AS labeler_version,
                       CAST({c_date} AS DATE) AS d
                FROM {relname} {dedup}
            """
            scoped = f"""
                WITH b AS ({base})
                SELECT * FROM b
                WHERE ws IN {WS_IN} AND d BETWEEN DATE '{MIN_DAY}' AND DATE '{cap}'
                  AND label IN ('opportunity','engagement','confused','not_interested','not interested')
            """
            days = [r["d"] for r in q(f"SELECT DISTINCT CAST(d AS VARCHAR) AS d FROM ({scoped}) ORDER BY d")]
            if days:
                days_in = "('" + "','".join(days) + "')"
                meeting_leads = ("SELECT DISTINCT lower(lead_email) AS lead_email, meeting_date "
                                 "FROM core.v_meeting_truth "
                                 "WHERE channel_norm = 'Email' AND is_ours AND lead_email IS NOT NULL "
                                 "AND meeting_date IS NOT NULL"
                                 ) if exists("core", "v_meeting_truth") else (
                                 "SELECT DISTINCT lower(lead_email) AS lead_email, "
                                 "COALESCE(meeting_date, CAST(posted_at AS DATE)) AS meeting_date "
                                 "FROM core.meeting WHERE lead_email IS NOT NULL")
                # ── DAY x WORKSPACE rows: labels + POST-REPLY meetings + native daily facts ──
                rows = q(f"""
                    WITH s AS ({scoped}), ml AS ({meeting_leads}),
                    lab AS (
                      SELECT CAST(d AS VARCHAR) AS day, ws,
                             COUNT(*)                                               AS labeled,
                             SUM(CASE WHEN label = 'opportunity' THEN 1 ELSE 0 END) AS opp,
                             SUM(CASE WHEN label = 'engagement' THEN 1 ELSE 0 END)  AS eng,
                             SUM(CASE WHEN label = 'confused' THEN 1 ELSE 0 END)    AS conf,
                             SUM(CASE WHEN label IN ('not_interested','not interested') THEN 1 ELSE 0 END) AS ni,
                             SUM(CASE WHEN opt_out THEN 1 ELSE 0 END)               AS opt_outs
                      FROM s GROUP BY 1, 2),
                    om AS (
                      SELECT CAST(s.d AS VARCHAR) AS day, s.ws,
                             COUNT(DISTINCT s.lead_email) AS opp_leads,
                             COUNT(DISTINCT CASE WHEN ml.lead_email IS NOT NULL THEN s.lead_email END) AS opp_met
                      FROM s LEFT JOIN ml ON ml.lead_email = s.lead_email AND ml.meeting_date >= s.d
                      WHERE s.label = 'opportunity' GROUP BY 1, 2),
                    nat AS (
                      SELECT CAST(date AS VARCHAR) AS day, workspace_id AS ws,
                             SUM(sent)::BIGINT                     AS sent,
                             SUM(unique_replies)::BIGINT           AS replies_human,
                             SUM(unique_replies_automatic)::BIGINT AS replies_auto,
                             SUM(unique_opportunities)::BIGINT     AS native_opps
                      FROM raw_pipeline_campaign_daily_metrics
                      WHERE workspace_id IN {WS_IN} AND CAST(date AS VARCHAR) IN {days_in}
                      GROUP BY 1, 2)
                    SELECT COALESCE(lab.day, nat.day) AS day, COALESCE(lab.ws, nat.ws) AS ws,
                           nat.sent, nat.replies_human, nat.replies_auto, nat.native_opps,
                           lab.labeled, lab.opp, lab.eng, lab.conf, lab.ni, lab.opt_outs,
                           om.opp_leads, om.opp_met
                    FROM lab FULL JOIN nat ON nat.day = lab.day AND nat.ws = lab.ws
                    LEFT JOIN om ON om.day = COALESCE(lab.day, nat.day) AND om.ws = COALESCE(lab.ws, nat.ws)
                    ORDER BY 1, 2
                """)
                for r in rows:
                    r["name"] = ws_names.get(r["ws"], r["ws"])

                # ── per-day coverage: read leads / canonical reply-lead universe (core.reply,
                #    BOTH workspace_id encodings via core.workspace_alias — the fast base of
                #    derived.v_reply_canonical; its is_auto_reply flag is documented-broken) ──
                day_meta = {d: {"day": d, "read": 0, "replying": 0, "pct": None,
                                "mode": "full_read" if d <= FULL_READ_THROUGH else "positive_slice",
                                "unreadable": 0} for d in days}
                try:
                    for r in q(f"""
                        WITH wmap AS (
                          SELECT instantly_uuid AS wid, warehouse_slug AS slug
                          FROM core.workspace_alias
                          WHERE warehouse_slug IN {WS_IN} AND instantly_uuid IS NOT NULL
                          UNION ALL
                          SELECT warehouse_slug, warehouse_slug FROM core.workspace_alias
                          WHERE warehouse_slug IN {WS_IN}),
                        canon AS (
                          SELECT DISTINCT CAST(CAST(r.reply_timestamp AS DATE) AS VARCHAR) AS day,
                                 m.slug AS ws, lower(r.lead_email) AS lead_email
                          FROM core.reply r JOIN wmap m ON m.wid = r.workspace_id
                          WHERE CAST(CAST(r.reply_timestamp AS DATE) AS VARCHAR) IN {days_in}),
                        ev AS (
                          SELECT DISTINCT CAST(CAST(message_ts AS DATE) AS VARCHAR) AS day,
                                 workspace_slug AS ws, lower(lead_email) AS lead_email
                          FROM main.raw_reply_label_event
                          WHERE workspace_slug IN {WS_IN}
                            AND CAST(CAST(message_ts AS DATE) AS VARCHAR) IN {days_in})
                        SELECT c.day, COUNT(*)::BIGINT AS replying,
                               SUM(CASE WHEN e.lead_email IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS read
                        FROM canon c LEFT JOIN ev e USING (day, ws, lead_email)
                        GROUP BY 1"""):
                        if r["day"] in day_meta:
                            m = day_meta[r["day"]]
                            m["replying"], m["read"] = r["replying"], r["read"]
                            m["pct"] = round(100.0 * r["read"] / r["replying"], 1) if r["replying"] else None
                    for r in q(f"""
                        SELECT CAST(CAST(message_ts AS DATE) AS VARCHAR) AS day, COUNT(*)::BIGINT AS n
                        FROM main.raw_reply_label_event
                        WHERE workspace_slug IN {WS_IN}
                          AND CAST(CAST(message_ts AS DATE) AS VARCHAR) IN {days_in}
                          AND (label = 'unreadable' OR deterministic_gate = 'unreadable_no_text')
                        GROUP BY 1"""):
                        if r["day"] in day_meta:
                            day_meta[r["day"]]["unreadable"] = r["n"]
                except Exception as e:
                    log(f"coverage compute failed (non-fatal): {e}")

                # ── DAY-READINESS GATE [2026-07-17, Sam-caught incident]: the 07:30Z conductor
                # can run BEFORE the nightly loads yesterday's native facts — labels without
                # denominators rendered sent=0 / positive-RR>100% nonsense. A day renders only
                # when native facts are complete AND labels are ready; not-ready days are
                # OMITTED (they self-heal on a later refresh). Hard sanity assertions drop a
                # whole day loudly rather than ever rendering nonsense.
                ready_days = []
                for dday in days:
                    drows = [r for r in rows if r["day"] == dday]
                    tot_sent = sum(int(r["sent"] or 0) for r in drows)
                    tot_h    = sum(int(r["replies_human"] or 0) for r in drows)
                    tot_a    = sum(int(r["replies_auto"] or 0) for r in drows)
                    reasons = []
                    if tot_sent <= 0:
                        reasons.append("native sent=0 — nightly facts not loaded yet")
                    if tot_h <= 0:
                        reasons.append("native human replies=0 — nightly facts not loaded yet")
                    if tot_sent > 0 and (tot_h + tot_a) > 0.05 * tot_sent:
                        reasons.append(f"replies {tot_h+tot_a} > 5% of sent {tot_sent} — implausible")
                    for r in drows:
                        pos = int(r["opp"] or 0) + int(r["eng"] or 0)
                        if pos > int(r["replies_human"] or 0):
                            reasons.append(f"{r['ws']}: positives {pos} > human replies {r['replies_human']}")
                    if dday > FULL_READ_THROUGH:
                        # positive-slice day: the daily labeler's sweep must have run AFTER the
                        # day ended (max labeled_at past the day) or the evening tail is missing.
                        try:
                            wm = q(f"""SELECT CAST(MAX(labeled_at) AS VARCHAR) AS wm
                                       FROM main.raw_reply_label_event
                                       WHERE workspace_slug IN {WS_IN}
                                         AND CAST(CAST(message_ts AS DATE) AS VARCHAR) = '{dday}'""")[0]["wm"]
                            if not wm or wm[:10] <= dday:
                                reasons.append(f"daily-labeler watermark {wm} not past {dday} — sweep incomplete")
                        except Exception as e:
                            reasons.append(f"watermark check failed: {e}")
                    if reasons:
                        log(f"DAY GATE: DROPPING {dday}: " + " | ".join(reasons))
                    else:
                        ready_days.append(dday)
                rows = [r for r in rows if r["day"] in ready_days]
                days = ready_days
                ver = q(f"SELECT MAX(labeler_version) AS ver FROM ({scoped})")[0]["ver"]
                payload.update({
                    "status": "ok" if days else "pending_labels",
                    "labeler_version": ver,
                    "scope": {"mode": "completed_days_only", "max_day": cap, "days": days},
                    "day_meta": [day_meta[d] for d in days],
                    "rows": rows,
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
