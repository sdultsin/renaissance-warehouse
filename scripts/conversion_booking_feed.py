"""Push the booking-site conversion feed — portal Supabase dashboard_feeds key='conversion'.

v5 (MOF-19 period rollups + eng/conf conversion surface, 2026-07-18) — ADDITIVE ONLY,
existing consumers untouched:
  * day rows gain eng_cohort / conf_cohort (append-only event cohorts for engagement
    resp. confused — same convention as opp_cohort: DISTINCT leads with a label EVENT
    whose reply falls on the day). None outside labeled modes, like the other cohorts.
  * NEW top-level keys "weekly" (ISO week Mon-Sun) and "monthly" (calendar month):
    ORG-level rollups of the emitted day rows for the tab's trend surface. Label
    columns obey the 100%-or-wipe period gate — they ship ONLY when EVERY calendar
    day of the FULL period is covered by a label-sound day mode, else None (never a
    partially-summed label count). Native sums cover the period's covered days with
    days_covered/days_in_period for honesty. X_to_booked = meetings-in-period ÷
    period X-cohort (R38 simple-ratio lag-mismatch convention, Sam 2026-07-17;
    >100% possible by design). meetings here = the feed's v_meeting_truth meetings
    key (warehouse-consistent, DDL 1138), NOT the kpi-chain bookings. These are
    ANALYSIS surfaces, not targets — no secondary IM targets derive from eng/conf.
    Canonical warehouse home of the period semantics = core.v_kpi_weekly /
    core.v_kpi_monthly (DDL 1138); this feed stays escrow-direct for label freshness.

v4 (R36 all-sound-history backfill, 2026-07-17): the feed now emits ALL SOUND HISTORY
(day x workspace) — not just recent days — so the KPIs tab computes every range preset
by summing day rows and never mixes coverages (the MTD-bug class: 3 days of labeled opps
against a full month of meetings). Day paths:

  * day >= 2026-07-14 ("live path"): the v3 pipeline VERBATIM — native facts from
    raw_pipeline_campaign_daily_metrics, modes full_read / positive_slice /
    ledger_provisional, day-readiness gate, slice-coverage gate, per-ws assertions.
    Jul-14..16 rows are regression-frozen (byte-identical to the v3 output).
  * 2026-05-15 <= day < 2026-07-14 ("backfill_slice"): labels COMPLETE via the all-time
    backfill (522,328 events, 78,523/78,526 pool leads, COMPLETION-REPORT verified) —
    label columns from the escrow parquets with the exact v3 query shapes (cohorts +
    current-state), native denominators from core.v_sends_truth_daily (the 1105 MAX-stitch
    API restatement — the honest source for settled history; raw_pipeline undercounts the
    frozen region). Per-ws sanity (positives <= human) enforced; a violating day ships
    NATIVE-ONLY with mode='backfill_assert_failed' (labels wiped for the day, loud log).
  * 2024-01-15 <= day < 2026-05-15 ("pre_boundary"): native facts + meetings only; ALL
    label columns None — 100%-or-wipe. Boundary grounds (measured 2026-07-17): Instantly
    native positive marks are positive truth >= 2026-05-15 only (standing invariant), and
    event-basis mark recovery is ~0 before 2026-03-26 (webhook start), 0.64 in the Apr-13
    outage week, >=0.88 multi-source from May. Never partial label counts.

  SOUNDNESS CONTRACT for the tab: label columns are summable ONLY over day_meta modes
  {full_read, positive_slice, backfill_slice}; native columns over any shipped row.
  scope carries native_min_day / labels_sound_from / live_path_from so the tab can
  compute "N of M days" coverage per range.

  NEW row key: meetings = v_meeting_truth email-ours bookings by MEETING day x workspace,
  with the campaign-dim workspace backstop applied INLINE (DDL 1135's fix; inline so the
  feed is correct regardless of promote timing). Additive key — existing consumers safe.
  Canonical warehouse home of these semantics = DDL 1136 core.v_kpi_daily (promote
  cadence); the feed stays escrow-direct for label freshness.

v3 (Sam KPI-merge + cohort ruling, 2026-07-17): DAY x WORKSPACE grain, COHORT semantics.
This feed powers the merged KPIs tab on renaissance-booking.com (the separate
Conversion tab UI is retired; the feed lives on for the KPI columns + any consumer).

METRIC CONTRACT (Sam cohort ruling 2026-07-17 — the 19-vs-20 problem):
  - opp_cohort  = distinct leads with an OPPORTUNITY LABEL EVENT whose reply falls on the
                  day (label-event stream, append-only — a lead who later books
                  STILL counts; never a current-state count).
  - pos_cohort  = distinct leads with an opportunity OR engagement event that day.
  - opp_leads / opp_met = the day's opp cohort and, of it, leads with a meeting booked
                  ON/AFTER the reply (post-reply join; opp_met <= opp_leads by construction,
                  so opp->booked can never exceed 100%; it grows for days as opps book).
  - Current-state fields (opp/eng/conf/ni/labeled) remain for lineage/consumers.
  - Denominators stay native Instantly daily facts (sent / unique_replies / auto).

DAY MODES (day_meta.mode): full_read | positive_slice | ledger_provisional (v3, live path)
  + backfill_slice | pre_boundary | backfill_assert_failed (v4, history)
  + evening_complete (v4.1, R37: the just-closed UTC day, labeled by the evening pass —
  marker-gated, natives from the live pipeline mirror; label-sound for the tab; tops up
  via morning re-sweeps). Live-path mode semantics are unchanged from v3 (see git
  history for the full v3 docstring); 'ledger_provisional' days render '—' in the
  labeled columns (Sam labeled-only ruling 2026-07-17 — no provisional display).

DAY-READINESS GATE [Sam-caught 2026-07-17 incident]: live-path days ship ONLY with
complete native facts (sent>0, human>0; replies<=5% of sent when sent>=100k). History
days are settled — they keep the ratio sanity check but NOT the sent>0/human>0 checks
(89 real zero-send weekend days exist in the 2024 era with genuine replies).

SCOPE: COMPLETED SENDING DAYS ONLY — cap = yesterday-ET, always. Override down with
BOOKING_CONV_MAX_DAY=YYYY-MM-DD; the cap can never exceed yesterday-ET. History floor
BOOKING_CONV_MIN_DAY (default 2024-01-15 = the 1105 restatement floor).
Workspaces: 5 funding slugs + warm-leads + renaissance-1 + the-gatekeepers (Max's —
added 2026-07-17 for the KPI merge).

LABEL SOURCE = ESCROW PARQUETS, NOT THE SNAPSHOT (Sam flaw-fix 2026-07-17): label-event
data is read directly from the labeler's escrow parquets — the live daily stream UNION the
alltime backfill, deduped on the table's uniqueness grain (message_ref_table,
message_ref_id, labeler_version) — with the same current-state/cohort derivations the
warehouse views encode (DDL 1111/1112 semantics, replicated in this session's queries).
Sourcing labels from the local snapshot made every label update wait on a 107GB promote;
escrow-direct decouples label freshness from promotes entirely (a day's finalization =
sweep + parquet regen + feed refresh ≈ 3 min). Native denominators (sent / replies /
meetings / ledger marks) STAY on the snapshot — they only change nightly, promote timing
is fine for them. If no escrow parquet is readable the script falls back to the snapshot's
main.raw_reply_label_event with a loud WARN (payload.label_source records which path ran).

Runs inside refresh_portal_feed.sh (conductor job portal-feed-refresh, daily@07:30 UTC,
plus later same-day reruns — the 07:30 run predates the nightly's facts for yesterday).
READ-ONLY on the warehouse (CORE_DB_PATH = serving snapshot); the only write is a PostgREST
UPSERT into the PORTAL Supabase (pxrdmjjaxtqycuxhxmgi) public.dashboard_feeds.
Never hard-fails the conductor: any error prints WARN to stderr and exits 0.
"""
from __future__ import annotations
import json, os, re, sys, urllib.request
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import duckdb

DB = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")
WORKER_ENV = os.environ.get("WORKER_ENV_FILE", "/root/renaissance-worker/.env")
# Escrow parquets = the label-event truth at labeler freshness (env names match the
# reply_label_event entity's override convention; ⚠ REHOME with /root/mof ~2026-07-25).
ESCROW_DAILY = os.environ.get("REPLY_LABEL_ESCROW_PARQUET",
                              "/root/mof/labeling/backfill/escrow/events.parquet")
ESCROW_ALLTIME = os.environ.get("REPLY_LABEL_ESCROW_ALLTIME_PARQUET",
                                "/root/mof/labeling/backfill_alltime/escrow/events.parquet")
FULL_READ_THROUGH = "2026-07-14"   # R18 boundary: <= this day = full-read; after = positive-slice
LIVE_PATH_FROM = "2026-07-14"      # >= this day: the v3 live pipeline verbatim (regression-frozen)
LABEL_SOUND_FROM = "2026-05-15"    # R36 measured boundary: labels sound from here (see docstring)
NATIVE_MIN_DAY_DEFAULT = "2024-01-15"  # 1105 API-restatement floor (earliest stitched day)
SLICE_COMPLETE = 0.90              # labeled share of the day's ledger positive cohort => labels complete
# R37 (Sam 2026-07-17): same-day evening completeness. The evening pass (daily_v2/
# evening_pass.py, 00:05Z) labels the JUST-CLOSED UTC day and writes a marker with its
# live coverage + live per-workspace natives (pipeline campaign_daily_metrics — the same
# lineage as the raw_pipeline mirror the live path reads). A marker day with
# complete=true ships as mode='evening_complete' (the tab treats it as label-sound and
# includes it in range coverage). Later marks/replies top up via the morning pass +
# re-sweeps (append-only — counts only grow). Marker natives also BACKSTOP the live-path
# day gate overnight (the snapshot's raw_pipeline mirror lags the nightly; without the
# backstop the day would flap out of the feed between midnight ET and the nightly).
EVENING_MARKER_DIR = os.environ.get("BOOKING_CONV_EVENING_DIR",
                                    "/root/mof/labeling/daily_v2/evening")

# 8 workspaces (Sam 2026-07-17: +the-gatekeepers for the KPI merge)
FUNDING_WS = ('renaissance-2', 'renaissance-4', 'renaissance-5',
              'prospects-power', 'koi-and-destroy', 'warm-leads', 'renaissance-1',
              'the-gatekeepers')
WS_IN = "('" + "','".join(FUNDING_WS) + "')"

# EOD-PATCH [2026-07-20 kpi-tab-booking-fix]: im_bookings.workspace label -> warehouse slug.
# Verified 2026-07-20 against the warehouse's own Jul-18 v_meeting_truth per-slug attribution
# (exact 1:1 match, all 8 workspaces). Used ONLY by _portal_meetings_overlay() below to source
# meetings straight from the portal booking source-of-truth on days the warehouse snapshot has
# not yet mirrored (e.g. a failed nightly). Any unmapped label is skipped (conservative).
IMB_LABEL_TO_SLUG = {
    "warm leads": "warm-leads",
    "renaissance 1": "renaissance-1",
    "funding 5 (eyver)": "renaissance-2",
    "funding 1 (samuel)": "renaissance-4",
    "funding 2 (ido)": "renaissance-5",
    "funding 3 (leo)": "prospects-power",
    "funding 4 (sam)": "koi-and-destroy",
    "max ws": "the-gatekeepers",
    "max's workspace": "the-gatekeepers",
}

LABELS_IN = "('opportunity','engagement','confused','not_interested','not interested')"


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


def ledger_ws_key(name: str) -> str:
    """core.workspace.name -> raw_comms ledger 'workspace' slug (e.g. Max's workspace -> max-s-workspace)."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _portal_meetings_overlay(lo_day: str, hi_day: str) -> dict:
    """EOD-PATCH [2026-07-20 kpi-tab-booking-fix]. Live per-(submit-day, slug) EMAIL meetings
    straight from the portal im_bookings table — THE booking source of truth — for days the
    warehouse serving snapshot has not yet mirrored (its v_meeting_truth lags live portal by up
    to a nightly; a failed nightly makes a real booking day render 0 on the tab). Returns
    {(day, slug): count}: channel='Email', deleted_at IS NULL, deduped by (submit-day, email|
    phone), workspace-label mapped to slug and restricted to FUNDING_WS. Read-only on the
    warehouse; the feed's only write remains the portal dashboard_feeds upsert."""
    purl = env_from_worker("PORTAL_SUPABASE_URL")
    pkey = env_from_worker("PORTAL_SUPABASE_SERVICE_ROLE_KEY")
    if not purl or not pkey:
        raise RuntimeError("no portal creds (PORTAL_SUPABASE_URL / _SERVICE_ROLE_KEY)")
    url = (purl.rstrip("/") + "/rest/v1/im_bookings"
           "?select=date,email,phone,workspace"
           "&channel=eq.Email&deleted_at=is.null"
           f"&date=gte.{lo_day}&date=lte.{hi_day}&limit=100000")
    req = urllib.request.Request(url, headers={"apikey": pkey, "Authorization": "Bearer " + pkey})
    with urllib.request.urlopen(req, timeout=60) as r:
        booked = json.load(r)
    seen: set = set()      # (submit-day, identity) — dedup email OR phone within a submit-day
    counts: dict = {}
    for b in booked:
        day = b.get("date")
        slug = IMB_LABEL_TO_SLUG.get((b.get("workspace") or "").strip().lower())
        if not day or slug not in FUNDING_WS:
            continue
        em = (b.get("email") or "").strip().lower()
        ph = "".join(ch for ch in (b.get("phone") or "") if ch.isdigit())
        ident = em or ("ph:" + ph if ph else None)
        dk = (day, ident) if ident else (day, "row:" + str(len(seen)))
        if dk in seen:
            continue
        seen.add(dk)
        counts[(day, slug)] = counts.get((day, slug), 0) + 1
    return counts


def main() -> None:
    cap = effective_max_day()
    min_day = min(os.environ.get("BOOKING_CONV_MIN_DAY") or NATIVE_MIN_DAY_DEFAULT, cap)
    conn = duckdb.connect(DB, read_only=True)

    def exists(schema: str, name: str) -> bool:
        return conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            [schema, name]).fetchone()[0] > 0

    def table_known(name: str) -> bool:
        return conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name=?", [name]).fetchone()[0] > 0

    def q(sql: str) -> list[dict]:
        cur = conn.execute(sql)
        names = [d[0] for d in cur.description]
        return [{n: (str(v) if hasattr(v, "isoformat") else v) for n, v in zip(names, row)}
                for row in cur.fetchall()]

    ws_names = {r["slug"]: r["name"] for r in q(f"SELECT slug, name FROM core.workspace WHERE slug IN {WS_IN}")}
    led_to_slug = {ledger_ws_key(name): slug for slug, name in ws_names.items()}

    # ── label-event source: escrow parquets (labeler-fresh), snapshot table as fallback ──
    escrow = [p for p in (ESCROW_DAILY, ESCROW_ALLTIME) if os.path.isfile(p)]
    if escrow:
        if len(escrow) < 2:
            log(f"WARN only {len(escrow)}/2 escrow parquets readable ({escrow}) — proceeding with what exists")
        files = ", ".join("'" + p + "'" for p in escrow)
        # dedup on the event uniqueness grain (DDL 1110); rows are append-only and identical
        # across escrows when both carry a key — labeled_at DESC is a deterministic tiebreak
        ev_table = (f"(SELECT * FROM read_parquet([{files}], union_by_name=true) "
                    "QUALIFY row_number() OVER (PARTITION BY message_ref_table, message_ref_id, "
                    "labeler_version ORDER BY labeled_at DESC) = 1)")
        label_source = "escrow_parquet"
        have_events = True
    elif exists("main", "raw_reply_label_event"):
        log("WARN no escrow parquet readable — FALLING BACK to snapshot main.raw_reply_label_event "
            "(label freshness re-coupled to promotes until escrow returns)")
        ev_table = "main.raw_reply_label_event"
        label_source = "warehouse_snapshot_fallback"
        have_events = True
    else:
        ev_table = None
        label_source = None
        have_events = False

    payload: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_id": os.path.basename(os.path.realpath(DB)),
        "status": "pending_labels",
        "grain": "day_x_workspace",
        "cohort_basis": "label_events",          # v3: append-only event cohorts (Sam ruling 07-17)
        "label_source": label_source,            # escrow_parquet | warehouse_snapshot_fallback
        "labeler_version": None,
        "scope": {"mode": "completed_days_only", "max_day": cap, "days": [],
                  "native_min_day": min_day,               # v4: earliest sound native day
                  "labels_sound_from": LABEL_SOUND_FROM,   # v4: R36 measured label boundary
                  "live_path_from": LIVE_PATH_FROM},
        "day_meta": [], "rows": [],
    }

    have_ledger = table_known("raw_comms_instantly_lead_state_event")
    if not have_events:
        log("no escrow parquets AND raw_reply_label_event missing from snapshot — upserting pending_labels state")

    rows: list[dict] = []
    day_meta_out: list[dict] = []
    days_out: list[str] = []

    # ── R37 evening markers: {day: marker} for complete markers within the recent window ──
    markers: dict = {}
    try:
        if os.path.isdir(EVENING_MARKER_DIR):
            for fn in os.listdir(EVENING_MARKER_DIR):
                if not fn.endswith(".json") or fn.startswith("."):
                    continue
                d = fn[:-5]
                lo = (datetime.fromisoformat(cap) - timedelta(days=3)).date().isoformat()
                hi_ok = (datetime.fromisoformat(cap) + timedelta(days=1)).date().isoformat()
                if not (lo <= d <= hi_ok):
                    continue
                with open(os.path.join(EVENING_MARKER_DIR, fn)) as fh:
                    m = json.load(fh)
                if m.get("complete") and m.get("day") == d and m.get("natives"):
                    markers[d] = m
    except Exception as e:
        log(f"WARN evening marker scan failed (ignored): {e}")
    ev_day = next((d for d in sorted(markers, reverse=True) if d > cap), None)
    hi = ev_day or cap

    if have_events:
        rng = f"BETWEEN DATE '{LIVE_PATH_FROM}' AND DATE '{hi}'"         # live path (v3, frozen) + evening day labels
        rng_lab = f"BETWEEN DATE '{LABEL_SOUND_FROM}' AND DATE '{hi}'"   # label-sound span (v4)
        # ── native Instantly daily facts (denominators) ─────────────────────────────
        # live path (>= LIVE_PATH_FROM): raw_pipeline verbatim (v3 regression-frozen);
        # history (< LIVE_PATH_FROM): core.v_sends_truth_daily MAX-stitch restatement.
        nat = {(r["day"], r["ws"]): r for r in q(f"""
            SELECT CAST(date AS VARCHAR) AS day, workspace_id AS ws,
                   SUM(sent)::BIGINT AS sent, SUM(unique_replies)::BIGINT AS h,
                   SUM(unique_replies_automatic)::BIGINT AS a,
                   SUM(unique_opportunities)::BIGINT AS native_opps
            FROM raw_pipeline_campaign_daily_metrics
            WHERE workspace_id IN {WS_IN} AND CAST(date AS DATE) {rng}
            GROUP BY 1, 2""")}
        nat_hist = {(r["day"], r["ws"]): r for r in q(f"""
            SELECT CAST(date AS VARCHAR) AS day, workspace_slug AS ws,
                   SUM(sent_stitched)::BIGINT AS sent, SUM(replies_human_stitched)::BIGINT AS h,
                   SUM(replies_auto_stitched)::BIGINT AS a, SUM(opps_stitched)::BIGINT AS native_opps
            FROM core.v_sends_truth_daily
            WHERE workspace_slug IN {WS_IN}
              AND date >= DATE '{min_day}' AND date < DATE '{LIVE_PATH_FROM}' AND date <= DATE '{cap}'
            GROUP BY 1, 2""")}
        # ── meetings by MEETING day x workspace (email, ours) — campaign-dim workspace
        # backstop INLINE (= DDL 1135's fix; inline so correctness never waits on a promote)
        mtg = {(r["day"], r["ws"]): int(r["meetings"] or 0) for r in q(f"""
            WITH campdim AS (
              SELECT campaign_id, MAX(workspace_slug) AS ws_slug
              FROM core.v_campaign_dim_unified
              WHERE campaign_id IS NOT NULL AND workspace_slug IS NOT NULL AND workspace_slug <> ''
              GROUP BY 1)
            SELECT CAST(mt.meeting_date AS VARCHAR) AS day,
                   COALESCE(NULLIF(mt.workspace_slug, ''), cd.ws_slug) AS ws,
                   COUNT(*) AS meetings
            FROM core.v_meeting_truth mt
            LEFT JOIN campdim cd ON cd.campaign_id = mt.campaign_id
            WHERE mt.channel_norm = 'Email' AND mt.is_ours AND mt.meeting_date IS NOT NULL
              AND mt.meeting_date BETWEEN DATE '{min_day}' AND DATE '{hi}'
              AND COALESCE(NULLIF(mt.workspace_slug, ''), cd.ws_slug) IN {WS_IN}
            GROUP BY 1, 2""")} if exists("core", "v_meeting_truth") else {}
        # ── EOD-PATCH [2026-07-20 kpi-tab-booking-fix]: portal-direct meetings overlay ──────
        # The snapshot's im_bookings mirror (v_meeting_truth) lags live portal by up to a
        # nightly; when a nightly fails (2026-07-20 PASS-B segfault) a real booking day renders
        # 0 meetings on the tab (Jul-19: warehouse 0 vs 66 real bookings). For days the
        # warehouse has NOT mirrored — day > its max meeting day, up to cap, bounded 10d — take
        # meetings LIVE from portal im_bookings (the booking source of truth). Self-disables per
        # day the moment the warehouse catches up. Read-only on the warehouse; portal write only.
        try:
            _wh_max = max((d for (d, _s) in mtg), default="")
            _floor = (datetime.strptime(cap, "%Y-%m-%d") - timedelta(days=10)).date().isoformat()
            _lo = max(_wh_max, _floor)
            if _lo < cap:
                _lo = (datetime.strptime(_lo, "%Y-%m-%d") + timedelta(days=1)).date().isoformat()
                _ov = _portal_meetings_overlay(_lo, cap)
                for _k, _v in _ov.items():
                    mtg[_k] = _v
                log(f"portal-direct meetings overlay {_lo}..{cap}: {sum(_ov.values())} meetings "
                    f"/ {len({_d for _d, _ in _ov})} day(s) [warehouse mtg max={_wh_max or 'none'}]")
        except Exception as _e:
            log(f"WARN portal-direct meetings overlay skipped ({_e}) — warehouse values kept")
        # ── label events (escrow-fresh): append-only cohorts + current-state lineage ────
        ev_base = f"""
            SELECT DISTINCT workspace_slug AS ws, lower(lead_email) AS le,
                   CAST(message_ts AS DATE) AS d, lower(CAST(label AS VARCHAR)) AS label
            FROM {ev_table} ev
            WHERE workspace_slug IN {WS_IN} AND CAST(message_ts AS DATE) {rng_lab}
              AND lower(CAST(label AS VARCHAR)) IN {LABELS_IN}
        """
        cur_state = f"""
            SELECT workspace_slug AS ws, lower(lead_email) AS le,
                   CAST(message_ts AS DATE) AS d,
                   lower(CAST(label AS VARCHAR)) AS label, labeler_version
            FROM {ev_table} ev
            WHERE workspace_slug IN {WS_IN} AND CAST(message_ts AS DATE) {rng_lab}
              AND lower(CAST(label AS VARCHAR)) IN {LABELS_IN}
            QUALIFY row_number() OVER (PARTITION BY workspace_slug, lower(lead_email),
                                       CAST(message_ts AS DATE)
                                       ORDER BY labeled_at DESC, message_ts DESC, message_ref_id DESC) = 1
        """
        # ^ tie-break made fully deterministic 2026-07-17 (v4): equal labeled_at ties
        #   (same-batch labels on multi-message lead-days) previously resolved by scan
        #   order — current-state opp/eng/conf/ni flapped ±handfuls run-to-run. Cohort
        #   fields (the tab's display) were always tie-free. Latest MESSAGE now wins ties.
        labc = {(r["day"], r["ws"]): r for r in q(f"""
            SELECT CAST(d AS VARCHAR) AS day, ws, COUNT(*) AS labeled,
                   SUM(CASE WHEN label = 'opportunity' THEN 1 ELSE 0 END) AS opp,
                   SUM(CASE WHEN label = 'engagement' THEN 1 ELSE 0 END)  AS eng,
                   SUM(CASE WHEN label = 'confused' THEN 1 ELSE 0 END)    AS conf,
                   SUM(CASE WHEN label IN ('not_interested','not interested') THEN 1 ELSE 0 END) AS ni
            FROM ({cur_state}) GROUP BY 1, 2""")}
        coh = {(r["day"], r["ws"]): r for r in q(f"""
            SELECT CAST(d AS VARCHAR) AS day, ws,
                   COUNT(DISTINCT CASE WHEN label = 'opportunity' THEN le END) AS opp_cohort,
                   COUNT(DISTINCT CASE WHEN label IN ('opportunity','engagement') THEN le END) AS pos_cohort,
                   COUNT(DISTINCT CASE WHEN label = 'engagement' THEN le END) AS eng_cohort,
                   COUNT(DISTINCT CASE WHEN label = 'confused' THEN le END) AS conf_cohort
            FROM ({ev_base}) GROUP BY 1, 2""")}
        meeting_leads = ("SELECT DISTINCT lower(lead_email) AS le, meeting_date "
                        "FROM core.v_meeting_truth "
                        "WHERE channel_norm = 'Email' AND is_ours AND lead_email IS NOT NULL "
                        "AND meeting_date IS NOT NULL"
                        ) if exists("core", "v_meeting_truth") else (
                        "SELECT DISTINCT lower(lead_email) AS le, "
                        "COALESCE(meeting_date, CAST(posted_at AS DATE)) AS meeting_date "
                        "FROM core.meeting WHERE lead_email IS NOT NULL")
        omv = {(r["day"], r["ws"]): r for r in q(f"""
            WITH oc AS (SELECT DISTINCT ws, le, d FROM ({ev_base}) WHERE label = 'opportunity'),
            ml AS ({meeting_leads})
            SELECT CAST(oc.d AS VARCHAR) AS day, oc.ws,
                   COUNT(DISTINCT oc.le) AS opp_leads,
                   COUNT(DISTINCT CASE WHEN ml.le IS NOT NULL THEN oc.le END) AS opp_met
            FROM oc LEFT JOIN ml ON ml.le = oc.le AND ml.meeting_date >= oc.d
            GROUP BY 1, 2""")}
        wm = {r["day"]: r["wm"] for r in q(f"""
            SELECT CAST(CAST(message_ts AS DATE) AS VARCHAR) AS day,
                   CAST(MAX(labeled_at) AS VARCHAR) AS wm
            FROM {ev_table} ev
            WHERE workspace_slug IN {WS_IN} AND CAST(message_ts AS DATE) {rng}
            GROUP BY 1""")}
        # ── ledger: never-decrementing Instantly positive marks (provisional cohorts) ──
        led: dict = {}
        led_cov: dict = {}
        if have_ledger:
            # Coverage denominator = LABELABLE marks only (definition fix 2026-07-17):
            # exclude same-day auto/bot-gated leads (structurally unlabelable by design —
            # gate classes never enter label stats) and mark-lag leads whose labeled reply
            # belongs to an EARLIER day (they ARE labeled, in their correct reply-day
            # cohorts; Instantly's positive mark just landed later than the reply).
            # Counting either against the day compares mismatched denominators (Jul-16 sat
            # at 87% forever while 0 leads were actually unlabeled). Threshold unchanged.
            # gate_day/first_real read {ev_table} unfiltered: gates aren't in LABELS_IN and
            # mark-lag labels can predate the sound floor / sit in another workspace.
            led_rows = q(f"""
                WITH lp AS (
                  SELECT DISTINCT CAST(status_changed_at AS DATE) AS d, workspace AS lw,
                         lower(lead_email) AS le
                  FROM raw_comms_instantly_lead_state_event
                  WHERE observed_status >= 1 AND CAST(status_changed_at AS DATE) {rng}),
                ml AS ({meeting_leads}),
                ev AS (SELECT DISTINCT le, d FROM ({ev_base})),
                gate_day AS (
                  SELECT DISTINCT lower(lead_email) AS le, CAST(message_ts AS DATE) AS d
                  FROM {ev_table} g
                  WHERE lower(CAST(label AS VARCHAR)) IN ('auto','bot','labeler_error')),
                first_real AS (
                  SELECT lower(lead_email) AS le, MIN(CAST(message_ts AS DATE)) AS first_d
                  FROM {ev_table} fr
                  WHERE lower(CAST(label AS VARCHAR)) IN {LABELS_IN}
                  GROUP BY 1)
                SELECT CAST(lp.d AS VARCHAR) AS day, lp.lw,
                       COUNT(DISTINCT lp.le) AS led_cohort,
                       COUNT(DISTINCT CASE WHEN ml.le IS NOT NULL THEN lp.le END) AS led_met,
                       COUNT(DISTINCT CASE WHEN ev.le IS NOT NULL THEN lp.le END) AS led_labeled,
                       COUNT(DISTINCT CASE WHEN ev.le IS NULL
                                            AND (gd.le IS NOT NULL OR fr.first_d < lp.d)
                                           THEN lp.le END) AS led_unlabelable
                FROM lp
                LEFT JOIN ml ON ml.le = lp.le AND ml.meeting_date >= lp.d
                LEFT JOIN ev ON ev.le = lp.le AND ev.d = lp.d
                LEFT JOIN gate_day gd ON gd.le = lp.le AND gd.d = lp.d
                LEFT JOIN first_real fr ON fr.le = lp.le
                GROUP BY 1, 2""")
            for r in led_rows:
                slug = led_to_slug.get(r["lw"])
                if slug:
                    led[(r["day"], slug)] = r
                c = led_cov.setdefault(r["day"], {"cohort": 0, "labeled": 0, "unlabelable": 0})
                c["cohort"] += int(r["led_cohort"] or 0)
                c["labeled"] += int(r["led_labeled"] or 0)
                c["unlabelable"] += int(r["led_unlabelable"] or 0)
        else:
            log("ledger table missing — label-incomplete days will be omitted, not provisional")

        # ── per-day gate + mode + row assembly ──────────────────────────────────────
        all_days = sorted(d for d in ({d for d, _ in nat} | {d for d, _ in nat_hist}
                          | {d for d, _ in coh} | {d for d, _ in led}
                          | {d for d, _ in mtg if d < LIVE_PATH_FROM}) if d <= cap)
        for dday in all_days:
            if dday >= LIVE_PATH_FROM:
                # ═══ LIVE PATH — v3 verbatim (regression-frozen) ═══
                tot_sent = sum(int(r["sent"] or 0) for (d, _), r in nat.items() if d == dday)
                tot_h    = sum(int(r["h"] or 0)    for (d, _), r in nat.items() if d == dday)
                tot_a    = sum(int(r["a"] or 0)    for (d, _), r in nat.items() if d == dday)
                tot_lab  = sum(int(r["labeled"] or 0) for (d, _), r in labc.items() if d == dday)
                reasons = []
                if tot_sent <= 0: reasons.append("native sent=0 — nightly facts not loaded yet")
                if tot_h <= 0: reasons.append("native human replies=0 — nightly facts not loaded yet")
                if tot_sent >= 100_000 and (tot_h + tot_a) > 0.05 * tot_sent:
                    reasons.append(f"replies {tot_h+tot_a} > 5% of sent {tot_sent} — implausible")
                if reasons and dday in markers:
                    # R37 marker backstop: the snapshot's raw_pipeline mirror lags the
                    # nightly — an evening-completed day must not flap out overnight.
                    for slug, nv in (markers[dday].get("natives") or {}).items():
                        nat[(dday, slug)] = {"day": dday, "ws": slug,
                                             "sent": nv.get("sent"), "h": nv.get("h"),
                                             "a": nv.get("a"),
                                             "native_opps": nv.get("native_opps")}
                    tot_sent = sum(int(r["sent"] or 0) for (d, _), r in nat.items() if d == dday)
                    tot_h    = sum(int(r["h"] or 0)    for (d, _), r in nat.items() if d == dday)
                    tot_a    = sum(int(r["a"] or 0)    for (d, _), r in nat.items() if d == dday)
                    reasons = []
                    if tot_sent <= 0: reasons.append("marker natives sent=0")
                    if tot_h <= 0: reasons.append("marker natives human=0")
                    if not reasons:
                        log(f"DAY {dday}: native facts from EVENING MARKER (snapshot mirror "
                            f"not loaded yet — R37 backstop)")
                if reasons:
                    log(f"DAY GATE: DROPPING {dday}: " + " | ".join(reasons))
                    continue
                w = wm.get(dday)
                wm_ok = bool(w) and w[:10] > dday
                cov = led_cov.get(dday)
                # denominator = labelable marks only; all-excluded (labelable=0) with marks
                # present = vacuously complete (nothing left that could carry a same-day label)
                labelable = (cov["cohort"] - cov.get("unlabelable", 0)) if cov else None
                slice_cov = ((cov["labeled"] / labelable) if labelable > 0 else
                             (1.0 if cov["cohort"] > 0 else None)) if cov else None
                labels_complete = tot_lab > 0 and wm_ok and (
                    dday <= FULL_READ_THROUGH or (slice_cov is not None and slice_cov >= SLICE_COMPLETE))

                if labels_complete:
                    a_reasons = []
                    for slug in FUNDING_WS:
                        lc, om_, nr = labc.get((dday, slug)), omv.get((dday, slug)), nat.get((dday, slug))
                        pos = int((lc or {}).get("opp") or 0) + int((lc or {}).get("eng") or 0)
                        if pos > int((nr or {}).get("h") or 0):
                            a_reasons.append(f"{slug}: positives {pos} > human {(nr or {}).get('h')}")
                        if int((om_ or {}).get("opp_met") or 0) > int((om_ or {}).get("opp_leads") or 0):
                            a_reasons.append(f"{slug}: opp_met > opp_leads")
                    if a_reasons:
                        log(f"DAY GATE: DROPPING {dday} (labeled-mode assertion): " + " | ".join(a_reasons))
                        continue
                    mode = "full_read" if dday <= FULL_READ_THROUGH else "positive_slice"
                    for slug in FUNDING_WS:
                        lc = labc.get((dday, slug)); cc = coh.get((dday, slug))
                        om_ = omv.get((dday, slug)); nr = nat.get((dday, slug))
                        if not (lc or cc or nr):
                            continue
                        rows.append({
                            "day": dday, "ws": slug, "name": ws_names.get(slug, slug),
                            "sent": int((nr or {}).get("sent") or 0),
                            "replies_human": int((nr or {}).get("h") or 0),
                            "replies_auto": int((nr or {}).get("a") or 0),
                            "native_opps": int((nr or {}).get("native_opps") or 0),
                            "labeled": int((lc or {}).get("labeled") or 0),
                            "opp": int((lc or {}).get("opp") or 0),
                            "eng": int((lc or {}).get("eng") or 0),
                            "conf": int((lc or {}).get("conf") or 0),
                            "ni": int((lc or {}).get("ni") or 0),
                            "opp_cohort": int((cc or {}).get("opp_cohort") or 0),
                            "pos_cohort": int((cc or {}).get("pos_cohort") or 0),
                            "eng_cohort": int((cc or {}).get("eng_cohort") or 0),
                            "conf_cohort": int((cc or {}).get("conf_cohort") or 0),
                            "opp_leads": int((om_ or {}).get("opp_leads") or 0),
                            "opp_met": int((om_ or {}).get("opp_met") or 0),
                            "meetings": mtg.get((dday, slug), 0),
                        })
                elif any((dday, slug) in led for slug in FUNDING_WS):
                    mode = "ledger_provisional"
                    log(f"DAY {dday}: labels incomplete (slice_cov="
                        f"{round(slice_cov, 3) if slice_cov is not None else None} = "
                        f"{(cov or {}).get('labeled')}/{labelable} labelable "
                        f"[marks={(cov or {}).get('cohort')}, unlabelable={(cov or {}).get('unlabelable')}], "
                        f"wm_ok={wm_ok}) — shipping LEDGER-provisional cohorts")
                    for slug in FUNDING_WS:
                        lr = led.get((dday, slug)); nr = nat.get((dday, slug))
                        if not (lr or nr):
                            continue
                        lcoh = int((lr or {}).get("led_cohort") or 0)
                        rows.append({
                            "day": dday, "ws": slug, "name": ws_names.get(slug, slug),
                            "sent": int((nr or {}).get("sent") or 0),
                            "replies_human": int((nr or {}).get("h") or 0),
                            "replies_auto": int((nr or {}).get("a") or 0),
                            "native_opps": int((nr or {}).get("native_opps") or 0),
                            "labeled": None, "opp": None, "eng": None, "conf": None, "ni": None,
                            "opp_cohort": lcoh, "pos_cohort": None,
                            "eng_cohort": None, "conf_cohort": None,
                            "opp_leads": lcoh,
                            "opp_met": min(int((lr or {}).get("led_met") or 0), lcoh),
                            "meetings": mtg.get((dday, slug), 0),
                        })
                else:
                    log(f"DAY GATE: DROPPING {dday}: labels incomplete and no ledger rows")
                    continue
            else:
                # ═══ HISTORY PATH (v4): settled days — stitched natives, escrow labels ═══
                tot_sent = sum(int(r["sent"] or 0) for (d, _), r in nat_hist.items() if d == dday)
                tot_h    = sum(int(r["h"] or 0)    for (d, _), r in nat_hist.items() if d == dday)
                tot_a    = sum(int(r["a"] or 0)    for (d, _), r in nat_hist.items() if d == dday)
                if tot_sent >= 100_000 and (tot_h + tot_a) > 0.05 * tot_sent:
                    log(f"DAY GATE: DROPPING {dday}: replies {tot_h+tot_a} > 5% of sent {tot_sent} — implausible")
                    continue
                slice_cov = None
                if dday < LABEL_SOUND_FROM:
                    mode = "pre_boundary"        # labels 100%-or-wipe: pre-boundary => None
                else:
                    mode = "backfill_slice"      # all-time backfill => labels complete
                    a_reasons = []
                    for slug in FUNDING_WS:
                        lc, nr = labc.get((dday, slug)), nat_hist.get((dday, slug))
                        pos = int((lc or {}).get("opp") or 0) + int((lc or {}).get("eng") or 0)
                        if pos > int((nr or {}).get("h") or 0):
                            a_reasons.append(f"{slug}: positives {pos} > human {(nr or {}).get('h')}")
                    if a_reasons:
                        mode = "backfill_assert_failed"   # labels wiped for the day, natives kept
                        log(f"HISTORY ASSERT: {dday} labels WIPED (native-only row ships): "
                            + " | ".join(a_reasons))
                labeled_mode = (mode == "backfill_slice")
                for slug in FUNDING_WS:
                    nr = nat_hist.get((dday, slug))
                    lc = labc.get((dday, slug)) if labeled_mode else None
                    cc = coh.get((dday, slug)) if labeled_mode else None
                    om_ = omv.get((dday, slug)) if labeled_mode else None
                    mg = mtg.get((dday, slug), 0)
                    if not (nr or lc or cc or mg):
                        continue
                    rows.append({
                        "day": dday, "ws": slug, "name": ws_names.get(slug, slug),
                        "sent": int((nr or {}).get("sent") or 0),
                        "replies_human": int((nr or {}).get("h") or 0),
                        "replies_auto": int((nr or {}).get("a") or 0),
                        "native_opps": int((nr or {}).get("native_opps") or 0),
                        "labeled": int((lc or {}).get("labeled") or 0) if labeled_mode else None,
                        "opp": int((lc or {}).get("opp") or 0) if labeled_mode else None,
                        "eng": int((lc or {}).get("eng") or 0) if labeled_mode else None,
                        "conf": int((lc or {}).get("conf") or 0) if labeled_mode else None,
                        "ni": int((lc or {}).get("ni") or 0) if labeled_mode else None,
                        "opp_cohort": int((cc or {}).get("opp_cohort") or 0) if labeled_mode else None,
                        "pos_cohort": int((cc or {}).get("pos_cohort") or 0) if labeled_mode else None,
                        "eng_cohort": int((cc or {}).get("eng_cohort") or 0) if labeled_mode else None,
                        "conf_cohort": int((cc or {}).get("conf_cohort") or 0) if labeled_mode else None,
                        "opp_leads": int((om_ or {}).get("opp_leads") or 0) if labeled_mode else None,
                        "opp_met": int((om_ or {}).get("opp_met") or 0) if labeled_mode else None,
                        "meetings": mg,
                    })
            days_out.append(dday)
            day_meta_out.append({"day": dday, "mode": mode,
                                 "slice_cov": round(slice_cov, 3) if slice_cov is not None else None})

        # ═══ R37 EVENING DAY (> cap): marker natives + evening-pass labels ═══
        if ev_day:
            m = markers[ev_day]
            mnat = m.get("natives") or {}
            a_reasons = []
            for slug in FUNDING_WS:
                lc, nv = labc.get((ev_day, slug)), mnat.get(slug)
                pos = int((lc or {}).get("opp") or 0) + int((lc or {}).get("eng") or 0)
                if pos > int((nv or {}).get("h") or 0):
                    a_reasons.append(f"{slug}: positives {pos} > human {(nv or {}).get('h')}")
            ev_lab = sum(int((labc.get((ev_day, s)) or {}).get("labeled") or 0) for s in FUNDING_WS)
            if a_reasons:
                log(f"EVENING GATE: DROPPING {ev_day}: " + " | ".join(a_reasons))
            elif ev_lab <= 0:
                log(f"EVENING GATE: DROPPING {ev_day}: marker complete but no label events "
                    f"visible in escrow (regen race?)")
            else:
                for slug in FUNDING_WS:
                    nv = mnat.get(slug)
                    lc = labc.get((ev_day, slug)); cc = coh.get((ev_day, slug))
                    om_ = omv.get((ev_day, slug))
                    if not (nv or lc or cc):
                        continue
                    rows.append({
                        "day": ev_day, "ws": slug, "name": ws_names.get(slug, slug),
                        "sent": int((nv or {}).get("sent") or 0),
                        "replies_human": int((nv or {}).get("h") or 0),
                        "replies_auto": int((nv or {}).get("a") or 0),
                        "native_opps": int((nv or {}).get("native_opps") or 0),
                        "labeled": int((lc or {}).get("labeled") or 0),
                        "opp": int((lc or {}).get("opp") or 0),
                        "eng": int((lc or {}).get("eng") or 0),
                        "conf": int((lc or {}).get("conf") or 0),
                        "ni": int((lc or {}).get("ni") or 0),
                        "opp_cohort": int((cc or {}).get("opp_cohort") or 0),
                        "pos_cohort": int((cc or {}).get("pos_cohort") or 0),
                        "eng_cohort": int((cc or {}).get("eng_cohort") or 0),
                        "conf_cohort": int((cc or {}).get("conf_cohort") or 0),
                        "opp_leads": int((om_ or {}).get("opp_leads") or 0),
                        "opp_met": int((om_ or {}).get("opp_met") or 0),
                        "meetings": mtg.get((ev_day, slug), 0),
                    })
                days_out.append(ev_day)
                cov_pct = ((m.get("coverage") or {}).get("pct"))
                day_meta_out.append({"day": ev_day, "mode": "evening_complete",
                                     "slice_cov": cov_pct})
                payload["scope"]["max_day"] = ev_day
                payload["scope"]["evening_day"] = ev_day
                log(f"EVENING day {ev_day} shipped (coverage={cov_pct}, labeled={ev_lab})")

        # ── v5: ORG-level ISO-week / calendar-month rollups of the emitted rows ──
        # (additive "weekly"/"monthly" payload keys; label columns 100%-or-wipe at
        # period grain — see the v5 docstring block for the full contract)
        def period_rollups(kind: str) -> list[dict]:
            sound_modes = {"full_read", "positive_slice", "backfill_slice", "evening_complete"}
            mode_by_day = {m["day"]: m["mode"] for m in day_meta_out}
            max_day = payload["scope"].get("max_day") or cap

            def p_start(ds: str) -> str:
                if kind == "monthly":
                    return ds[:8] + "01"
                d0 = datetime.fromisoformat(ds).date()
                return (d0 - timedelta(days=d0.weekday())).isoformat()  # ISO Monday

            def p_end(ps: str) -> str:
                d0 = datetime.fromisoformat(ps).date()
                if kind == "weekly":
                    return (d0 + timedelta(days=6)).isoformat()
                nxt = (d0.replace(day=28) + timedelta(days=4)).replace(day=1)
                return (nxt - timedelta(days=1)).isoformat()

            per: dict = {}
            for r in rows:
                p = per.setdefault(p_start(r["day"]), {
                    "sent": 0, "replies_human": 0, "replies_auto": 0, "meetings": 0,
                    "opp_cohort": 0, "pos_cohort": 0, "eng_cohort": 0, "conf_cohort": 0})
                p["sent"] += int(r.get("sent") or 0)
                p["replies_human"] += int(r.get("replies_human") or 0)
                p["replies_auto"] += int(r.get("replies_auto") or 0)
                p["meetings"] += int(r.get("meetings") or 0)
                if mode_by_day.get(r["day"]) in sound_modes:
                    for kk in ("opp_cohort", "pos_cohort", "eng_cohort", "conf_cohort"):
                        p[kk] += int(r.get(kk) or 0)
            out: list[dict] = []
            for ps in sorted(per):
                pe = p_end(ps)
                d0 = datetime.fromisoformat(ps).date()
                d1 = datetime.fromisoformat(pe).date()
                cal_days = [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]
                n_cov = sum(1 for x in cal_days if x in mode_by_day)
                n_sound = sum(1 for x in cal_days if mode_by_day.get(x) in sound_modes)
                labels_complete = n_sound == len(cal_days)   # EVERY calendar day label-sound
                p = per[ps]
                e: dict = {"start": ps, "end": pe,
                           "days_in_period": len(cal_days), "days_covered": n_cov,
                           "days_label_sound": n_sound,
                           "complete": pe <= max_day, "labels_complete": labels_complete,
                           "sent": p["sent"], "replies_human": p["replies_human"],
                           "replies_auto": p["replies_auto"], "meetings": p["meetings"],
                           "kpi": round(p["sent"] / p["meetings"]) if p["meetings"] > 0 and p["sent"] > 0 else None}
                for kk in ("opp_cohort", "pos_cohort", "eng_cohort", "conf_cohort"):
                    e[kk] = p[kk] if labels_complete else None
                for num, den in (("opp_to_booked", "opp_cohort"), ("eng_to_booked", "eng_cohort"),
                                 ("conf_to_booked", "conf_cohort")):
                    e[num] = (round(p["meetings"] / p[den], 4)
                              if labels_complete and p[den] > 0 else None)
                out.append(e)
            return out

        if days_out:
            try:
                ver = q(f"SELECT MAX(labeler_version) AS ver FROM ({cur_state})")[0]["ver"]
            except Exception:
                ver = None
            payload.update({
                "status": "ok",
                "labeler_version": ver,
                # scope.max_day already = cap, or the evening day when one shipped (R37)
                "scope": {**payload["scope"], "days": days_out},
                "day_meta": day_meta_out,
                "rows": rows,
                "weekly": period_rollups("weekly"),     # v5 additive keys
                "monthly": period_rollups("monthly"),
            })
        else:
            log(f"no presentable days within cap {cap}")

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
    log(f"payload {len(body)/1048576:.2f} MiB · {len(rows)} rows · {len(days_out)} days "
        f"({days_out[0] if days_out else '-'} .. {days_out[-1] if days_out else '-'})")
    req = urllib.request.Request(
        purl.rstrip("/") + "/rest/v1/dashboard_feeds", data=body, method="POST",
        headers={"apikey": pkey, "Authorization": "Bearer " + pkey,
                 "Content-Type": "application/json",
                 "Prefer": "resolution=merge-duplicates,return=minimal"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        modes: dict = {}
        for m in day_meta_out:
            modes[m["mode"]] = modes.get(m["mode"], 0) + 1
        print(f"  ok conversion-booking-feed upserted (status={payload['status']}, "
              f"days={len(days_out)}, mode_counts={modes}, HTTP {resp.status})")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:                       # non-fatal by contract
        log(f"WARN failed (feed keeps last-known-good): {e}")
        sys.exit(0)
