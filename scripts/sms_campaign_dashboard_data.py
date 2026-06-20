"""Feed for the SMS per-campaign performance dashboard (F1).

Reads v_sms_campaign_performance (Sendivo per-campaign funnel — DDL 34) from the warehouse and
writes data/latest.json for /sms-campaign-performance/. The SMS analogue of
scripts/daily_performance_warehouse.py. Run after the warehouse `derived` phase / on the nightly.

Usage: python -m scripts.sms_campaign_dashboard_data [--db PATH] [--out PATH]
Default out: /root/lens/sms-campaign-performance/data/latest.json
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os

import duckdb

DEFAULT_DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
DEFAULT_OUT = "/root/lens/sms-campaign-performance/data/latest.json"


def _rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# G3: the view's inbound CTE counts raw webhook rows (count_star), and raw_sendivo_inbound
# carries ~8.9x duplicate webhooks (3,921,284 rows vs 439,384 DISTINCT inbound_message_id),
# so v_sms_campaign_performance.replies/opt_outs/positive_replies are inflated ~8.9x. The
# durable fix is a DDL change to the view (staged separately, auto-applies next nightly). Until
# the served snapshot carries it, the generator computes the DEDUPED inbound counts itself off
# raw_sendivo_inbound — mirroring the view's inbound CTE EXACTLY (same join to
# v_sendivo_number_campaign on '+'||ltrim(our_number,'+'), same metric_date = CAST(received_at
# AS DATE) grain, same opt-out/positive FILTER predicates) but with count(DISTINCT
# inbound_message_id) in place of count_star(). It is LEFT JOINed onto the view on
# (campaign_id, metric_date) and those three measures are sourced from it instead of from the
# view, so dedup is the ONLY behavioral change; delivered/sent/cost/failed stay from the view.
# 30d totals drop 3,921,284/3,250,705/670,579 -> 439,384/366,002/73,382 (replies further clamped
# by the existing D1 LEAST per row). The D1 clamps (LEAST(replies,delivered) etc.) sit on top.
INBOUND_DEDUP_CTE = """
            inbound_dedup AS (
                SELECT CAST(i.received_at AS DATE) AS metric_date,
                       nc.campaign_id,
                       count(DISTINCT i.inbound_message_id) AS replies,
                       count(DISTINCT i.inbound_message_id) FILTER (WHERE i.is_opt_out)       AS opt_outs,
                       count(DISTINCT i.inbound_message_id) FILTER (WHERE (NOT i.is_opt_out)) AS positive_replies
                FROM raw_sendivo_inbound AS i
                LEFT JOIN v_sendivo_number_campaign AS nc
                  ON (('+' || ltrim(i.our_number, '+')) = nc.our_number)
                GROUP BY 1, 2
            )"""


def build(db_path: str, out_path: str) -> dict:
    con = duckdb.connect(db_path, read_only=True)
    try:
        # Per-row containment guards (D1-guard / D6 clamp): the view (DDL 34) can
        # emit replies>delivered and delivered>sent (mismatched inbound/outbound date
        # keys + duplicate webhooks — root-cause fixed in the view separately). Clamp
        # LEAST(...) per source row BEFORE aggregating so totals stay sane until the
        # view is fixed. cost_usd kept RAW (D8) so totals compute from unrounded values.
        # G3: replies/opt_outs/positive_replies come from the deduped inbound CTE (LEFT
        # JOINed on campaign_id+metric_date), NOT from the view's inflated columns; sent/
        # delivered/failed/cost stay from the view.
        daily = _rows(con, """
            WITH""" + INBOUND_DEDUP_CTE + """
            SELECT p.metric_date::VARCHAR AS date,
                   COALESCE(sum(p.sent),0)::BIGINT       AS sent,
                   COALESCE(sum(LEAST(p.delivered,p.sent)),0)::BIGINT  AS delivered,
                   COALESCE(sum(p.failed),0)::BIGINT     AS failed,
                   COALESCE(sum(p.cost_usd),0)           AS cost_usd,
                   COALESCE(sum(LEAST(COALESCE(d.replies,0),p.delivered)),0)::BIGINT    AS replies,
                   COALESCE(sum(COALESCE(d.opt_outs,0)),0)::BIGINT   AS opt_outs,
                   COALESCE(sum(LEAST(COALESCE(d.positive_replies,0),p.delivered)),0)::BIGINT AS positive_replies,
                   CASE WHEN sum(p.sent)>0 THEN round(100.0*sum(LEAST(p.delivered,p.sent))/sum(p.sent),2) END AS delivery_rate
            FROM v_sms_campaign_performance p
            LEFT JOIN inbound_dedup d ON d.campaign_id = p.campaign_id AND d.metric_date = p.metric_date
            WHERE p.metric_date IS NOT NULL
            GROUP BY p.metric_date ORDER BY p.metric_date
        """)
        # per-campaign aggregated across the full available range.
        # D4: drop junk rows (NULL campaign_id / NULL campaign_name) → 50 real campaigns.
        # D5: GROUP BY campaign_id, sub_account_name so a campaign spanning two sub-accounts
        #     (2477) splits into real per-workspace rows; keep any_value(campaign_name)
        #     (invariant per campaign), drop the nondeterministic any_value(sub_account_name).
        #     sub_account_name IS NOT NULL drops the orphan inbound-only split row.
        # D6: LEFT JOIN v_kpi_sms (aggregated to campaign grain) for meetings + cost_per_meeting.
        #     v_kpi_sms attributes meetings to campaign_id only (NULL on the meeting rows
        #     today), so the per-campaign meetings column is honest 0 where unattributed;
        #     the 498 fleet total is carried in totals['meetings'] below.
        # D1-guard / D6 clamp: LEAST(replies,delivered) / LEAST(delivered,sent) per row, and
        #     guard reply_rate <= 100% as defence-in-depth until the view is fixed.
        # G3: replies/opt_outs/positive_replies sourced from the deduped inbound CTE
        # (LEFT JOIN on campaign_id+metric_date), not the view's inflated columns. This
        # makes per-campaign Positive% (positive_replies/delivered) realistic (sub-1%),
        # down from the ~6-25% the un-deduped counts produced.
        campaigns = _rows(con, """
            WITH kpi AS (
                SELECT campaign_id,
                       COALESCE(sum(meetings),0)::BIGINT AS meetings
                FROM v_kpi_sms
                WHERE campaign_id IS NOT NULL
                GROUP BY campaign_id
            ),""" + INBOUND_DEDUP_CTE + """
            SELECT p.campaign_id,
                   any_value(p.campaign_name) AS campaign_name,
                   p.sub_account_name AS sub_account,
                   COALESCE(sum(p.sent),0)::BIGINT       AS sent,
                   COALESCE(sum(LEAST(p.delivered,p.sent)),0)::BIGINT  AS delivered,
                   COALESCE(sum(p.failed),0)::BIGINT     AS failed,
                   round(COALESCE(sum(p.cost_usd),0),2)  AS cost_usd,
                   COALESCE(sum(LEAST(COALESCE(d.replies,0),p.delivered)),0)::BIGINT    AS replies,
                   COALESCE(sum(COALESCE(d.opt_outs,0)),0)::BIGINT   AS opt_outs,
                   COALESCE(sum(LEAST(COALESCE(d.positive_replies,0),p.delivered)),0)::BIGINT AS positive_replies,
                   COALESCE(any_value(k.meetings),0)::BIGINT AS meetings,
                   CASE WHEN sum(p.sent)>0 THEN round(100.0*sum(LEAST(p.delivered,p.sent))/sum(p.sent),2) END AS delivery_rate,
                   CASE WHEN sum(LEAST(p.delivered,p.sent))>0
                        THEN LEAST(round(100.0*sum(LEAST(COALESCE(d.replies,0),p.delivered))/sum(LEAST(p.delivered,p.sent)),2),100.0) END AS reply_rate,
                   CASE WHEN sum(LEAST(COALESCE(d.positive_replies,0),p.delivered))>0 THEN round(sum(p.cost_usd)/sum(LEAST(COALESCE(d.positive_replies,0),p.delivered)),2) END AS cost_per_positive,
                   CASE WHEN any_value(k.meetings)>0 THEN round(sum(p.cost_usd)/any_value(k.meetings),2) END AS cost_per_meeting
            FROM v_sms_campaign_performance p
            LEFT JOIN kpi k ON k.campaign_id = p.campaign_id
            LEFT JOIN inbound_dedup d ON d.campaign_id = p.campaign_id AND d.metric_date = p.metric_date
            WHERE p.campaign_id IS NOT NULL
              AND p.campaign_name IS NOT NULL
              AND p.sub_account_name IS NOT NULL
            GROUP BY p.campaign_id, p.sub_account_name
            HAVING sum(p.sent) > 0 OR sum(COALESCE(d.replies,0)) > 0
            ORDER BY sent DESC
        """)
        # D6: 498 SMS meetings live in v_kpi_sms but carry campaign_id=NULL (not
        # per-campaign attributable), so sum them unconditionally for the fleet total.
        kpi_totals = _rows(con, """
            SELECT COALESCE(SUM(meetings),0)::BIGINT      AS meetings,
                   COALESCE(SUM(opportunities),0)::BIGINT AS opportunities
            FROM v_kpi_sms
        """)[0]
        totals = {
            "sent": sum(d["sent"] for d in daily),
            "delivered": sum(d["delivered"] for d in daily),
            "cost_usd": round(sum(d["cost_usd"] for d in daily), 2),
            "replies": sum(d["replies"] for d in daily),
            "opt_outs": sum(d["opt_outs"] for d in daily),
            "positive_replies": sum(d["positive_replies"] for d in daily),
            "meetings": int(kpi_totals["meetings"]),
            # distinct real campaigns (D4 = 50); the rows array is 51 because 2477 splits per workspace (D5)
            "campaigns": len({d["campaign_id"] for d in campaigns}),
            "days": len(daily),
        }
        totals["delivery_rate"] = round(100.0 * totals["delivered"] / totals["sent"], 2) if totals["sent"] else None
        totals["cost_per_meeting"] = round(totals["cost_usd"] / totals["meetings"], 2) if totals["meetings"] else None
        # D8: totals were summed from RAW per-day cost above ($54,053.12); round the daily
        # JSON values for display only, AFTER the total is computed, so no rounding drift.
        for d in daily:
            d["cost_usd"] = round(d["cost_usd"], 2)
    finally:
        con.close()
    return {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "v_sms_campaign_performance",
        "date_range": {"min": daily[0]["date"] if daily else None, "max": daily[-1]["date"] if daily else None},
        "totals": totals,
        "daily": daily,
        "campaigns": campaigns,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()
    data = build(args.db, args.out)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(data, fh, indent=2, default=str)
    t = data["totals"]
    print(f"wrote {args.out}: {t['days']} days, {t['campaigns']} campaigns, "
          f"{t['sent']:,} sent, {t['replies']:,} replies, ${t['cost_usd']:,.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
