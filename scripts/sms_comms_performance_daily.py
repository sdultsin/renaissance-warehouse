#!/usr/bin/env python3
"""SMS (+ WhatsApp) Campaign-Performance DAILY feed — the per-day, date-filterable
analogue of the email Campaign-Performance feed (daily_performance_warehouse.py).

It powers the portal `lens-sms-performance` dashboard, which mirrors the email
"Campaign Performance" UI (calendar date-range picker + presets, trend chart, KPI
tiles, leaderboard) but for the Sendivo SMS funnel, sliced by PRODUCT OFFER
(Business Funding / Pre-IPO / (unmapped)). WhatsApp rides along as a separate
channel series for its own tab.

WHY a new generator (not the existing sms_campaign_dashboard_data.py): the existing
one emits a flat lifetime-aggregate `campaigns` array + an org-total `daily` array —
neither crosses campaign x day, so a calendar picker can't slice it. The email CP
client does 100% client-side aggregation over a `per_day` array, so this generator
emits that exact shape.

DATA TRUTHS baked in (verified on snapshot warehouse_20260625, do not re-derive):
  * SEND side carries a real Sendivo 10DLC campaign_id + sub_account; offer is joined
    via raw_comms_brand.sendivo_campaign_id -> offer_type. ~18% of sends are on
    campaigns not yet in the brand map -> surfaced honestly as the '(unmapped)' offer,
    never hidden.
  * OPPORTUNITIES (core.opportunity source='sendivo', state<>'duplicate') carry NO
    campaign_id (always NULL) but DO carry the brand slug in workspace_id, which joins
    to core.v_channel_offer(channel='sms') -> offer. So opps are offer-attributable but
    NOT campaign-attributable -> shown at offer/channel level only.
  * MEETINGS (core.meeting channel='SMS' source='sheet') are 100% labelled
    'Business Funding' and carry NO campaign_id. Pre-IPO SMS meetings are sourced
    OUTSIDE the Funding-Form sheet and are NOT yet ingested -> for offer='Pre-IPO' the
    meetings value is emitted as null with meetings_status='not_ingested' (NEVER a
    false 0).
  * POSITIVE replies = qwen strict-sentiment classifier (derived.sms_reply_is_positive_qwen);
    it has no campaign/brand key -> channel total only. Classifier lags ~1-2 days, so
    the tail of the positive series is provisional.

DUAL BACKEND: on the droplet it reads the gated serving snapshot directly via DuckDB
(CORE_DB_PATH=/opt/duckdb/warehouse_current.duckdb, like its siblings). Run anywhere
else (e.g. a laptop) it falls back to the read-only HTTP query API
(WAREHOUSE_API_URL / WAREHOUSE_API_TOKEN) so the exact same SQL seeds the feed for dev.

  Box (nightly, in refresh_portal_feed.sh):
    CORE_DB_PATH=$SNAP python scripts/sms_comms_performance_daily.py \
        --out /root/portal/dashboards/lens-sms-performance/data/latest.json
  Local seed (uses the HTTP API):
    python scripts/sms_comms_performance_daily.py --api --out <portal>/dashboards/lens-sms-performance/data/latest.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# ----- offer label normalisation (single source of truth) ---------------------
OFFERS = ["Business Funding", "Pre-IPO", "(unmapped)"]
_OFFER_FROM_TYPE = {"funding": "Business Funding", "pre_ipo": "Pre-IPO"}


def _offer(offer_type: str | None) -> str:
    return _OFFER_FROM_TYPE.get((offer_type or "").strip().lower(), "(unmapped)")


import re
_COPY_RE = re.compile(r"(\s*\(copy\))+\s*$", re.IGNORECASE)


def _norm_blast(name: str) -> str:
    """Blast/script name EXACTLY as it appears in the Funding Form — word-for-word, no
    normalization beyond a trim (the SQL already TRIMs). [2026-06-26 Sam] Keep the Sendivo
    '(Copy)' suffix AND the original internal spacing so this table is identical to Grace's
    sheet row-for-row. (We used to strip '(Copy)' to group re-sends + collapse whitespace —
    both removed; ~50% of SMS bookings carry a '(Copy)' suffix so grouping mis-merged rows.)"""
    return (name or "").strip() or "(no blast)"


# ----- SQL (verified against snapshot warehouse_20260625) ---------------------
# NB: never alias a column `cost` or `days` — both are DuckDB reserved words.
SQL_SENDS = """
WITH brand AS (
  SELECT CAST(sendivo_campaign_id AS VARCHAR) AS scid, offer_type,
         row_number() OVER (PARTITION BY sendivo_campaign_id ORDER BY _loaded_at DESC) rn
  FROM raw_comms_brand WHERE sendivo_campaign_id IS NOT NULL
)
SELECT CAST(v.metric_date AS VARCHAR) AS date,
       COALESCE(CAST(v.campaign_id AS VARCHAR), '__unmapped__') AS campaign_id,
       COALESCE(v.campaign_name, '(unmapped numbers)') AS campaign_name,
       COALESCE(v.sub_account_name, '(none)') AS sub_account,
       b.offer_type AS offer_type,
       COALESCE(v.sent, 0)        AS sent,
       COALESCE(v.delivered, 0)   AS delivered,
       COALESCE(v.failed, 0)      AS failed,
       ROUND(COALESCE(v.cost_usd, 0), 4) AS cost_usd,
       COALESCE(v.replies, 0)     AS replies,
       COALESCE(v.opt_outs, 0)    AS opt_outs
FROM v_sms_campaign_performance v
LEFT JOIN (SELECT * FROM brand WHERE rn = 1) b
  ON CAST(v.campaign_id AS VARCHAR) = b.scid
WHERE v.metric_date IS NOT NULL
ORDER BY v.metric_date, v.sent DESC
"""

# OFFER is in the BLAST COPY, not the brand (Sam, 2026-06-25): "unsecured line of credit /
# funded next-day / 1% monthly" = Business Funding; "pre-IPO / accredited investors / SpaceX &
# Anthropic" = Pre-IPO. The brand map (raw_comms_brand) is frozen + ~17% unmapped, so we classify
# each Sendivo campaign by the DOMINANT offer of the copy it actually sent — joining our outbound
# copy to the replying recipient (raw_sendivo_inbound) -> our_number -> number->campaign map. This
# resolves the previously-"(unmapped)" campaigns (e.g. the MONEY-* education brands = Pre-IPO).
SQL_CAMPAIGN_OFFER = """
WITH ob AS (
  SELECT right(regexp_replace(phone10,'[^0-9]','','g'),10) p,
    CASE WHEN lower(message) LIKE '%pre-ipo%' OR lower(message) LIKE '%pre ipo%'
              OR lower(message) LIKE '%accredited%' OR lower(message) LIKE '%spacex%'
              OR lower(message) LIKE '%anthropic%' OR lower(message) LIKE '%invest%' THEN 'Pre-IPO'
         WHEN lower(message) LIKE '%credit%' OR lower(message) LIKE '%funded%'
              OR lower(message) LIKE '%unsecured%' OR lower(message) LIKE '%capital%'
              OR lower(message) LIKE '%funding%' THEN 'Business Funding'
         ELSE 'other' END oc
  FROM v_comms_sendivo_outbound WHERE message IS NOT NULL),
ib AS (SELECT DISTINCT right(regexp_replace(prospect_number,'[^0-9]','','g'),10) p,
                       right(regexp_replace(our_number,'[^0-9]','','g'),10) onum FROM raw_sendivo_inbound),
nc AS (SELECT right(regexp_replace(our_number,'[^0-9]','','g'),10) onum, CAST(campaign_id AS VARCHAR) cid
       FROM v_sendivo_number_campaign GROUP BY 1, 2)
SELECT nc.cid AS campaign_id, ob.oc AS offer, COUNT(*) AS n
FROM ob JOIN ib ON ib.p = ob.p JOIN nc ON nc.onum = ib.onum
WHERE ob.oc <> 'other' GROUP BY 1, 2
"""

# OPPORTUNITIES = qwen strict-positive REPLIES (Sam's definition: a positive reply = an
# opportunity). The old warm-call surface (core.opportunity) is worker-dependent and stopped
# being marked ~2026-06-17 (SMS AIM pause), so it under-reports recent days badly — NOT used.
# Channel total = SQL_POSITIVE; the per-offer split uses SQL_POSITIVE_BY_CAMPAIGN -> campaign offer.
SQL_POSITIVE_BY_CAMPAIGN = """
WITH nc AS (SELECT right(regexp_replace(our_number,'[^0-9]','','g'),10) onum, CAST(campaign_id AS VARCHAR) cid
            FROM v_sendivo_number_campaign GROUP BY 1, 2),
ib AS (  -- dedup raw_sendivo_inbound (re-ingested ~12x per inbound_message_id) to ONE row/message
  SELECT inbound_message_id, any_value(right(regexp_replace(our_number,'[^0-9]','','g'),10)) onum
  FROM raw_sendivo_inbound GROUP BY 1)
SELECT CAST(CAST(q.received_at AS DATE) AS VARCHAR) AS date, nc.cid AS campaign_id, COUNT(*) AS pos
FROM derived.sms_reply_is_positive_qwen q
JOIN ib ON ib.inbound_message_id = q.reply_id
JOIN nc ON nc.onum = ib.onum
WHERE q.is_positive AND q.received_at IS NOT NULL
GROUP BY 1, 2
"""

# Meetings carry the Funding-Form BLAST/SCRIPT name in campaign_name_raw (100% populated
# for SMS; campaign_id is NULL because the blast name doesn't resolve to a Sendivo 10DLC
# campaign — match_method='unmatched'). So meetings ARE per-blast attributable even though
# they are NOT offer-attributable (blast names don't map to brand/offer; the sheet's Offer
# column is empty for SMS). Grain: date x blast.
SQL_MEETINGS = """
SELECT CAST(meeting_date AS VARCHAR) AS date,
       COALESCE(NULLIF(TRIM(campaign_name_raw), ''), '(no blast)') AS blast,
       CASE WHEN offer = 'Pre-IPO' THEN 'Pre-IPO' ELSE 'Business Funding' END AS offer,
       COUNT(*) AS meetings
FROM core.meeting
WHERE channel = 'SMS' AND is_duplicate_of IS NULL
  AND meeting_date IS NOT NULL
GROUP BY 1, 2, 3
"""

SQL_POSITIVE = """
SELECT CAST(CAST(received_at AS DATE) AS VARCHAR) AS date,
       COUNT(*) FILTER (WHERE is_positive) AS positive_qwen
FROM derived.sms_reply_is_positive_qwen
WHERE received_at IS NOT NULL
GROUP BY 1
"""

SQL_WHATSAPP = """
SELECT CAST(metric_date AS VARCHAR) AS date,
       COALESCE(sent, 0)            AS sent,
       COALESCE(delivered, 0)       AS delivered,
       COALESCE(failed, 0)          AS failed,
       COALESCE(replies_total, 0)   AS replies,
       COALESCE(positive_replies, 0) AS positive,
       COALESCE(meetings_booked, 0) AS meetings,
       ROUND(COALESCE(cost_usd, 0), 4) AS cost_usd,
       delivery_rate
FROM v_omni_whatsapp_performance
WHERE metric_date IS NOT NULL
ORDER BY metric_date
"""


# ----- backends ---------------------------------------------------------------
class DuckBackend:
    def __init__(self, db_path: str):
        import duckdb  # local import so the API path needs no duckdb
        self.con = duckdb.connect(db_path, read_only=True)
        self.snapshot_id = os.path.basename(db_path)

    def rows(self, sql: str) -> list[dict]:
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


class ApiBackend:
    def __init__(self):
        self.url = (os.environ.get("WAREHOUSE_API_URL")
                    or "https://renaissance-droplet.tailae5c80.ts.net").rstrip("/")
        self.token = os.environ.get("WAREHOUSE_API_TOKEN")
        if not self.token:
            env = Path(__file__).resolve().parents[1] / ".env"
            alt = Path("/Users/sam/Documents/Claude Code/Renaissance/.env")
            for p in (env, alt):
                if p.exists():
                    for ln in p.read_text().splitlines():
                        if ln.startswith("WAREHOUSE_API_TOKEN="):
                            self.token = ln.split("=", 1)[1].strip()
                            break
                if self.token:
                    break
        if not self.token:
            raise SystemExit("WAREHOUSE_API_TOKEN not found (env or .env)")
        self.snapshot_id = None

    def rows(self, sql: str) -> list[dict]:
        body = json.dumps({"sql": sql}).encode()
        req = urllib.request.Request(
            self.url + "/query", data=body, method="POST",
            headers={"Authorization": "Bearer " + self.token,
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read())
        if payload.get("truncated"):
            print(f"[warn] query truncated: {sql[:60]}...", file=sys.stderr)
        self.snapshot_id = payload.get("snapshot_id") or self.snapshot_id
        cols = payload["columns"]
        return [dict(zip(cols, r)) for r in payload["rows"]]


# ----- build ------------------------------------------------------------------
def _blank_offer_bucket() -> dict:
    return {"sent": 0, "delivered": 0, "failed": 0, "cost_usd": 0.0,
            "replies": 0, "opt_outs": 0, "opportunities": 0, "meetings": 0}


def build(backend) -> dict:
    sends = backend.rows(SQL_SENDS)
    meetings = backend.rows(SQL_MEETINGS)
    positives = backend.rows(SQL_POSITIVE)
    pos_by_camp = backend.rows(SQL_POSITIVE_BY_CAMPAIGN)
    whatsapp = backend.rows(SQL_WHATSAPP)

    # campaign_id -> dominant offer, classified by the actual SMS COPY (see SQL_CAMPAIGN_OFFER).
    camp_off_rows = backend.rows(SQL_CAMPAIGN_OFFER)
    _co: dict[str, dict[str, int]] = {}
    for r in camp_off_rows:
        _co.setdefault(str(r["campaign_id"]), {})[r["offer"]] = int(r["n"])
    campaign_offer = {cid: max(o, key=o.get) for cid, o in _co.items()}

    days: dict[str, dict] = {}

    def day(d: str) -> dict:
        if d not in days:
            days[d] = {
                "date": d,
                "org": {"sent": 0, "delivered": 0, "failed": 0, "cost_usd": 0.0,
                        "replies": 0, "opt_outs": 0, "opportunities": 0,
                        "meetings": 0, "positive_qwen": 0},
                "by_offer": {o: _blank_offer_bucket() for o in OFFERS},
                "by_campaign": {},   # SEND dimension: Sendivo 10DLC campaign
                "by_blast": {},      # OUTCOME dimension: Funding-Form blast/script (meetings)
            }
        return days[d]

    # --- sends (campaign x day, + offer) ---
    unmapped_sent = 0
    total_sent = 0
    for r in sends:
        d = r["date"]
        rec = day(d)
        cid = r["campaign_id"]
        # COPY-based offer first (the blast determines the offer); brand map as fallback.
        offer = campaign_offer.get(str(cid)) or _offer(r.get("offer_type"))
        s = int(r["sent"]); dl = int(r["delivered"]); fl = int(r["failed"])
        c = float(r["cost_usd"]); rp = int(r["replies"]); oo = int(r["opt_outs"])
        total_sent += s
        if offer == "(unmapped)":
            unmapped_sent += s
        ob = rec["by_offer"][offer]
        ob["sent"] += s; ob["delivered"] += dl; ob["failed"] += fl
        ob["cost_usd"] += c; ob["replies"] += rp; ob["opt_outs"] += oo
        o = rec["org"]
        o["sent"] += s; o["delivered"] += dl; o["failed"] += fl
        o["cost_usd"] += c; o["replies"] += rp; o["opt_outs"] += oo
        cid = r["campaign_id"]
        crec = rec["by_campaign"].setdefault(cid, {
            "campaign_name": r["campaign_name"], "sub_account": r["sub_account"],
            "offer": offer, "sent": 0, "delivered": 0, "failed": 0,
            "cost_usd": 0.0, "replies": 0, "opt_outs": 0})
        crec["sent"] += s; crec["delivered"] += dl; crec["failed"] += fl
        crec["cost_usd"] += c; crec["replies"] += rp; crec["opt_outs"] += oo

    # --- OPPORTUNITIES = qwen positive replies (channel total) ---
    for r in positives:
        d = r["date"]
        rec = day(d)
        n = int(r["positive_qwen"])
        rec["org"]["opportunities"] += n
        rec["org"]["positive_qwen"] += n
    # per-offer opportunities split via the copy-based campaign_offer map (partial coverage;
    # positives whose number doesn't map to a campaign fall to '(unmapped)').
    for r in pos_by_camp:
        d = r["date"]
        rec = day(d)
        offer = campaign_offer.get(str(r["campaign_id"]), "(unmapped)")
        if offer not in rec["by_offer"]:
            offer = "(unmapped)"
        rec["by_offer"][offer]["opportunities"] += int(r["pos"])

    # --- meetings (day x blast x offer); Pre-IPO partner-sheet meetings now ingested ---
    for r in meetings:
        d = r["date"]
        rec = day(d)
        n = int(r["meetings"])
        offer = r.get("offer") or "Business Funding"
        if offer not in rec["by_offer"]:
            offer = "Business Funding"
        rec["org"]["meetings"] += n
        rec["by_offer"][offer]["meetings"] += n
        blast = _norm_blast(r.get("blast"))
        rec["by_blast"][blast] = rec["by_blast"].get(blast, 0) + n

    # --- assemble ordered per_day with derived flags ---
    all_dates = sorted(days.keys())
    today = max((d for d in all_dates if days[d]["org"]["sent"] > 0), default=(all_dates[-1] if all_dates else None))
    per_day = []
    for d in all_dates:
        rec = days[d]
        dt = datetime.strptime(d, "%Y-%m-%d").date()
        rec["dow"] = dt.strftime("%a")
        o = rec["org"]
        # data_gap: inbound present but send feed missing for the day (NULL sent days)
        rec["data_gap"] = bool(o["sent"] == 0 and (o["replies"] > 0 or o["opt_outs"] > 0))
        rec["intraday"] = bool(today is not None and d == today and d == date.today().isoformat())
        rec["org"]["delivery_rate"] = round(100 * o["delivered"] / o["sent"], 1) if o["sent"] else None
        per_day.append(rec)

    # --- WhatsApp channel series (its own tab) ---
    wa_per_day = []
    for r in whatsapp:
        s = int(r["sent"])
        wa_per_day.append({
            "date": r["date"],
            "dow": datetime.strptime(r["date"], "%Y-%m-%d").strftime("%a"),
            "sent": s, "delivered": int(r["delivered"]), "failed": int(r["failed"]),
            "replies": int(r["replies"]), "positive": int(r["positive"]),
            "meetings": int(r["meetings"]), "cost_usd": round(float(r["cost_usd"]), 2),
            "delivery_rate": (round(100 * int(r["delivered"]) / s, 1) if s else None),
        })

    mapped = total_sent - unmapped_sent
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "today": today,
        "date_range": {"min": all_dates[0] if all_dates else None,
                       "max": all_dates[-1] if all_dates else None},
        "snapshot_id": getattr(backend, "snapshot_id", None),
        "offers": OFFERS,
        "source": "v_sms_campaign_performance + core.opportunity + core.meeting + sms_reply_is_positive_qwen",
        "caveats": {
            "offer_send_coverage_pct": round(100 * mapped / total_sent, 1) if total_sent else None,
            "unmapped_send_share_pct": round(100 * unmapped_sent / total_sent, 1) if total_sent else None,
            "preipo_meetings": "not_offer_split",  # blast names don't map to offer; sheet Offer col empty for SMS
            "opportunities_def": "core.opportunity source=sendivo, state<>duplicate (Close warm-call opps); offer-attributable via workspace_id->brand, never campaign-attributable",
            "positive_def": "qwen strict-sentiment; channel total only; classifier lags ~1-2 days (tail provisional)",
            "meetings_def": "core.meeting channel=SMS source=sheet; 1480 rows, 100% blast-attributed via campaign_name_raw (Funding Form blast/script); NOT offer-attributable; NULL Sendivo campaign_id",
            "deals_funded": "NOT on this dashboard — core.deal_funded has channel/blast/meeting all NULL (180 deals, all-channel) so SMS deals are not attributable in the warehouse today; Sendivo's per-blast Deals-Won count is ALSO unreliable (shows 0 vs Funding-Form truth). Funding Form is SoT; deal->SMS/blast attribution needs a warehouse fix (re-derive from Funding Form). See handoff.",
        },
        "per_day": per_day,
        "whatsapp": {
            "per_day": wa_per_day,
            "note": "WhatsApp (Iskra). Channel-level only — per-template/campaign attribution pending vendor (Arseny); offer = 100% Business Funding for what this key can see (Pre-IPO WhatsApp not visible). Meetings via Funding-Form sheet.",
        },
    }
    return payload


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--db", default=os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb"))
    ap.add_argument("--api", action="store_true", help="force the HTTP query-API backend (local dev)")
    args = ap.parse_args()

    if args.api or not Path(args.db).exists():
        backend = ApiBackend()
        print(f"[info] backend=api ({backend.url})", file=sys.stderr)
    else:
        backend = DuckBackend(args.db)
        print(f"[info] backend=duckdb ({args.db})", file=sys.stderr)

    payload = build(backend)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, separators=(",", ":")))
    pd = payload["per_day"]
    print(f"[ok] wrote {args.out} — {len(pd)} days, range {payload['date_range']['min']}..{payload['date_range']['max']}, "
          f"offer send coverage {payload['caveats']['offer_send_coverage_pct']}% "
          f"({payload['caveats']['unmapped_send_share_pct']}% unmapped), snapshot {payload['snapshot_id']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
