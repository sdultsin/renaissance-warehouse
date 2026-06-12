"""Generate the KPI dashboard data feed (read-only) — /root/lens/kpi/data.json.

Feeds the KPI page (Overview tile "KPI — Emails per Meeting"). Reads the DDL-62
views only:
  * v_kpi_email — date x infra x cm x is_mca (sent / opps / replies h+a / meetings)
  * v_kpi_sms   — date x Sendivo campaign sends + date-level SMS meetings

Emits RAW daily-grain rows (last N days); the page aggregates client-side for
daily / weekly / monthly so every ratio is recomputed from summed measures
(period ratio), never averaged from daily ratios.

Cron (droplet, after the 07:00 UTC meetings refresh):
    20 7 * * * cd /root/renaissance-warehouse && .venv/bin/python scripts/kpi_dashboard_data.py > /root/lens/kpi/data.json.tmp && mv -f /root/lens/kpi/data.json.tmp /root/lens/kpi/data.json
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import duckdb

DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
DAYS = int(os.environ.get("KPI_DAYS", "92"))  # ~3 months -> monthly view has 3 full points

conn = duckdb.connect(DB, read_only=True)


def q(sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


data: dict = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "days": DAYS,
    # Raw daily email rows; ratios are recomputed by the page from sums.
    "email": q(f"""
        SELECT CAST(date AS VARCHAR) AS date, infra, cm, is_mca,
               sent, opportunities, replies_human, replies_auto, meetings
        FROM v_kpi_email
        WHERE date >= current_date - {DAYS} AND date < current_date + 1
        ORDER BY date
    """),
    # SMS daily totals (sends are per-campaign; meetings ride date-level rows).
    "sms_daily": q(f"""
        SELECT CAST(date AS VARCHAR) AS date,
               SUM(sent) AS sent, SUM(delivered) AS delivered,
               SUM(replies) AS replies, SUM(opportunities) AS opportunities,
               SUM(meetings) AS meetings, ROUND(SUM(cost_usd), 2) AS cost_usd
        FROM v_kpi_sms
        WHERE date >= current_date - {DAYS} AND date < current_date + 1
        GROUP BY 1 ORDER BY 1
    """),
    # Per-Sendivo-campaign cut, last 30d (the only sub-date grain SMS supports).
    "sms_by_campaign_30d": q("""
        SELECT campaign_name, SUM(sent) AS sent, SUM(delivered) AS delivered,
               SUM(replies) AS replies, SUM(opportunities) AS opportunities
        FROM v_kpi_sms
        WHERE date >= current_date - 30 AND campaign_id IS NOT NULL
        GROUP BY 1 HAVING SUM(sent) > 0 ORDER BY sent DESC LIMIT 40
    """),
}

json.dump(data, sys.stdout, default=str)
