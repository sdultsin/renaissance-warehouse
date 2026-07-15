"""Generate the KPI dashboard data feed — /root/lens/kpi/data.json.

Feeds the KPI page (Overview tile "KPI Dashboard"). Reads warehouse directly
with filtered CTEs:
  - Only 5 active CMs: IDO, LEO, SAM, EYVER, SAMUEL
  - Workspace allow-list: Funding 1-5 only (renaissance-4/5, prospects-power,
    koi-and-destroy, renaissance-2) — same scope as the campaign-performance dashboard
  - Sends sourced from raw_pipeline_campaign_daily_metrics (includes deleted campaigns
    with historical data; core.campaign_daily only has currently-listable campaigns)
  - Meetings from core.meeting (Slack-sourced; campaign_id + unmatched CM branch)

Emits RAW daily-grain rows; the page aggregates client-side for daily / weekly /
monthly so ratios are always recomputed from sums (period ratio, not averaged).

Cron (droplet, after 07:00 UTC meetings refresh):
    20 7 * * * cd /root/renaissance-warehouse && .venv/bin/python scripts/kpi_dashboard_data.py > /root/lens/kpi/data.json.tmp && mv -f /root/lens/kpi/data.json.tmp /root/lens/kpi/data.json
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
import duckdb

DB   = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
DAYS = int(os.environ.get("KPI_DAYS", "92"))

conn = duckdb.connect(DB, read_only=True)

ACTIVE_CMS = "('IDO','LEO','SAM','EYVER','SAMUEL')"

# Workspace allow-list: Funding 1-5 only (same as campaign-performance dashboard).
# Using workspace_id (slug) which is stable across Instantly renames.
FUNDING_WORKSPACES = "('renaissance-4','renaissance-5','prospects-power','koi-and-destroy','renaissance-2')"

BASE_CTE = f"""
WITH filtered_rp AS (
  SELECT DISTINCT ON (campaign_id)
    campaign_id,
    COALESCE(NULLIF(infra_type,''), 'unknown') AS infra,
    cm_name
  FROM raw_pipeline_campaigns
  WHERE workspace_id IN {FUNDING_WORKSPACES}
    AND (cm_name IN {ACTIVE_CMS} OR cm_name IS NULL)
  ORDER BY campaign_id, _loaded_at DESC
),
dims AS (
  SELECT rp.campaign_id, rp.infra,
    COALESCE(NULLIF(rp.cm_name,''), 'IDO') AS cm,
    COALESCE(c.is_mca, false) AS is_mca
  FROM filtered_rp AS rp
  LEFT JOIN core.campaign AS c USING (campaign_id)
),
sends AS (
  -- raw_pipeline_campaign_daily_metrics includes deleted/paused campaigns that
  -- sent in the window; core.campaign_daily only has currently-listable campaigns.
  SELECT cd.date, d.infra, d.cm, d.is_mca,
    SUM(cd.sent)                       AS sent,
    -- TODO(label-swap, cold-email-BI §9.3, armed 2026-07-15): swap this Instantly-native
    -- opportunity count (measured ~69% true-positive precision) for the label-derived TRUE
    -- opp count once reply-label coverage spans this feed's FULL window (KPI_DAYS=92d;
    -- the page displays 30d cuts of it).
    -- Deploy condition (check against the serving snapshot before flipping):
    --   SELECT min(reply_date) <= current_date - 92
    --      AND count(*) >= 0.95 * (SELECT count(DISTINCT (workspace_id, lower(lead_email)))
    --                              FROM core.email_message
    --                              WHERE direction='inbound' AND message_at >= current_date - 92)
    --   FROM core.v_reply_label_current WHERE label IN
    --     ('opportunity','engagement','confused','not_interested');
    -- The swap: replace the SUM below with a per-(date,campaign) count of label='opportunity'
    -- events (v_reply_label_current at lead grain, dated by reply_date, campaign_id join to
    -- dims) AND change the lens-kpi page tiles/columns to read "Opps (labeled)"
    -- (Renaissance-Portal dashboards/lens-kpi/index.html hero sub + Opps column headers).
    -- Until then this stays Instantly-native — do NOT mix sources silently.
    SUM(cd.unique_opportunities)       AS opportunities,
    SUM(cd.unique_replies)             AS replies_human,
    SUM(cd.unique_replies_automatic)   AS replies_auto,
    0::BIGINT                          AS bounces
  FROM raw_pipeline_campaign_daily_metrics AS cd
  INNER JOIN dims AS d USING (campaign_id)
  GROUP BY 1,2,3,4
),
email_meetings AS (
  SELECT date, infra, cm, is_mca, SUM(meetings) AS meetings FROM (
    -- Matched: campaign resolved to a campaign in our filtered scope
    SELECT CAST(m.posted_at AS DATE) AS date, d.infra,
      COALESCE(NULLIF(d.cm,''), 'IDO') AS cm, d.is_mca, COUNT(*) AS meetings
    FROM core.meeting AS m
    INNER JOIN dims AS d ON m.campaign_id = d.campaign_id
    WHERE m.source = 'slack'
      AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
          'sendivo|\\bsms\\b|whatsapp|iskra')
    GROUP BY 1,2,3,4
    UNION ALL
    -- Unmatched-but-attributed: campaign_id IS NULL (matcher refused ambiguous duplicates,
    -- e.g. EYVER), CM extracted from Slack text, not date-named junk
    SELECT CAST(m.posted_at AS DATE) AS date, 'google' AS infra,
      m.cm, false AS is_mca, COUNT(*) AS meetings
    FROM core.meeting AS m
    WHERE m.source = 'slack'
      AND m.campaign_id IS NULL
      AND m.cm IN {ACTIVE_CMS}
      AND NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
          'sendivo|\\bsms\\b|whatsapp|iskra')
      AND NOT regexp_matches(COALESCE(m.campaign_name_raw,''), '^\\d{{1,2}}/\\d{{1,2}}/\\d{{4}}')
    GROUP BY 1,2,3,4
  )
  GROUP BY 1,2,3,4
),
combined AS (
  SELECT
    COALESCE(s.date,  mt.date)  AS date,
    COALESCE(s.infra, mt.infra) AS infra,
    COALESCE(s.cm,    mt.cm)    AS cm,
    COALESCE(s.is_mca, mt.is_mca) AS is_mca,
    COALESCE(s.sent, 0)          AS sent,
    COALESCE(s.opportunities, 0) AS opportunities,
    COALESCE(s.replies_human, 0) AS replies_human,
    COALESCE(s.replies_auto, 0)  AS replies_auto,
    COALESCE(s.bounces, 0)       AS bounces,
    COALESCE(mt.meetings, 0)     AS meetings
  FROM sends AS s
  FULL JOIN email_meetings AS mt
    ON s.date=mt.date AND s.infra=mt.infra AND s.cm=mt.cm
    AND s.is_mca IS NOT DISTINCT FROM mt.is_mca
)
"""

def q(sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    out = []
    for row in rows:
        d = {}
        for col, val in zip(cols, row):
            d[col] = str(val) if hasattr(val, 'isoformat') else val
        out.append(d)
    return out

data: dict = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "days": DAYS,
    "email": q(BASE_CTE + f"""
        SELECT CAST(date AS VARCHAR) AS date, infra, cm, is_mca,
               sent::bigint AS sent, opportunities::bigint AS opportunities,
               replies_human::bigint AS replies_human, replies_auto::bigint AS replies_auto,
               meetings::bigint AS meetings
        FROM combined
        WHERE date >= current_date - {DAYS} AND date < current_date + 1
        ORDER BY date
    """),
    "sms_daily": q(f"""
        SELECT CAST(date AS VARCHAR) AS date,
               SUM(sent)::bigint AS sent, SUM(delivered)::bigint AS delivered,
               SUM(replies)::bigint AS replies, SUM(opportunities)::bigint AS opportunities,
               SUM(meetings)::bigint AS meetings, ROUND(SUM(cost_usd), 2) AS cost_usd
        FROM v_kpi_sms
        WHERE date >= current_date - {DAYS} AND date < current_date + 1
        GROUP BY 1 ORDER BY 1
    """),
    "sms_by_campaign_30d": q("""
        SELECT campaign_name, SUM(sent)::bigint AS sent, SUM(delivered)::bigint AS delivered,
               SUM(replies)::bigint AS replies, SUM(opportunities)::bigint AS opportunities
        FROM v_kpi_sms
        WHERE date >= current_date - 30 AND campaign_id IS NOT NULL
        GROUP BY 1 HAVING SUM(sent) > 0 ORDER BY sent DESC LIMIT 40
    """),
    # [2026-06-13 audit] OTD/Google/Outlook split — the ONLY clean ESP source is account grain
    # (campaign infra_type never says otd). Footnotes: ~19% NULL esp; Tariffs absent; feed lags up to 1d.
    "sends_by_esp": q(f"""
        SELECT CAST(date AS VARCHAR) AS date, COALESCE(esp,'unknown') AS esp,
               SUM(actual_sends)::bigint AS sends, COUNT(DISTINCT account_id) AS active_accounts
        FROM core.sending_account_daily
        WHERE date >= current_date - {DAYS} AND date < current_date + 1
        GROUP BY 1,2 ORDER BY 1, sends DESC
    """),
    "sends_by_esp_30d": q("""
        SELECT COALESCE(esp,'unknown') AS esp, SUM(actual_sends)::bigint AS sends,
               ROUND(100.0*SUM(actual_sends)/SUM(SUM(actual_sends)) OVER (),1) AS pct
        FROM core.sending_account_daily
        WHERE date >= current_date - 30 GROUP BY 1 ORDER BY sends DESC
    """),
}

json.dump(data, sys.stdout, default=str)
