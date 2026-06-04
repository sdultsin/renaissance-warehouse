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


def build(db_path: str, out_path: str) -> dict:
    con = duckdb.connect(db_path, read_only=True)
    try:
        daily = _rows(con, """
            SELECT metric_date::VARCHAR AS date,
                   COALESCE(sum(sent),0)::BIGINT       AS sent,
                   COALESCE(sum(delivered),0)::BIGINT  AS delivered,
                   COALESCE(sum(failed),0)::BIGINT     AS failed,
                   round(COALESCE(sum(cost_usd),0),2)  AS cost_usd,
                   COALESCE(sum(replies),0)::BIGINT    AS replies,
                   COALESCE(sum(opt_outs),0)::BIGINT   AS opt_outs,
                   COALESCE(sum(positive_replies),0)::BIGINT AS positive_replies,
                   CASE WHEN sum(sent)>0 THEN round(100.0*sum(delivered)/sum(sent),2) END AS delivery_rate
            FROM v_sms_campaign_performance
            WHERE metric_date IS NOT NULL
            GROUP BY metric_date ORDER BY metric_date
        """)
        # per-campaign aggregated across the full available range
        campaigns = _rows(con, """
            SELECT campaign_id,
                   any_value(campaign_name) AS campaign_name,
                   any_value(sub_account_name) AS sub_account,
                   COALESCE(sum(sent),0)::BIGINT       AS sent,
                   COALESCE(sum(delivered),0)::BIGINT  AS delivered,
                   COALESCE(sum(failed),0)::BIGINT     AS failed,
                   round(COALESCE(sum(cost_usd),0),2)  AS cost_usd,
                   COALESCE(sum(replies),0)::BIGINT    AS replies,
                   COALESCE(sum(opt_outs),0)::BIGINT   AS opt_outs,
                   COALESCE(sum(positive_replies),0)::BIGINT AS positive_replies,
                   CASE WHEN sum(sent)>0 THEN round(100.0*sum(delivered)/sum(sent),2) END AS delivery_rate,
                   CASE WHEN sum(delivered)>0 THEN round(100.0*sum(replies)/sum(delivered),2) END AS reply_rate,
                   CASE WHEN sum(positive_replies)>0 THEN round(sum(cost_usd)/sum(positive_replies),2) END AS cost_per_positive
            FROM v_sms_campaign_performance
            GROUP BY campaign_id
            HAVING sum(sent) > 0 OR sum(replies) > 0
            ORDER BY sent DESC
        """)
        totals = {
            "sent": sum(d["sent"] for d in daily),
            "delivered": sum(d["delivered"] for d in daily),
            "cost_usd": round(sum(d["cost_usd"] for d in daily), 2),
            "replies": sum(d["replies"] for d in daily),
            "opt_outs": sum(d["opt_outs"] for d in daily),
            "positive_replies": sum(d["positive_replies"] for d in daily),
            "campaigns": len(campaigns),
            "days": len(daily),
        }
        totals["delivery_rate"] = round(100.0 * totals["delivered"] / totals["sent"], 2) if totals["sent"] else None
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
