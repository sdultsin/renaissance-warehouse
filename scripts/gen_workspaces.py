import duckdb, json, os, re, datetime

# Warehouse-native (2026-06-24): sent/opps/replies now come from the consolidation warehouse
# (raw_pipeline_campaign_daily_metrics) attributed via the COMPLETE campaign dimension —
# raw_pipeline_campaigns FULL-OUTER core.campaign (UUID->slug via core.workspace). The old path
# read Pipeline-Supabase campaign_daily_metrics LEFT JOIN campaigns, whose `campaigns` table goes
# stale and dropped active campaigns (EYVER's GENERAL MATRIX TEST set) into '(unmapped)' —
# under-counting Funding 5 ~4x (warehouse-flags#9). core.campaign recovers them. Meetings already
# come from the same warehouse snapshot. (Also drops the retired pipeline-Supabase dependency.)

START = "2026-05-14"
SNAP = "/opt/duckdb/warehouse_current.duckdb"
DISPLAY = {
  "renaissance-4":"Funding 1","renaissance-5":"Funding 2","prospects-power":"Funding 3",
  "koi-and-destroy":"Funding 4","renaissance-2":"Funding 5","renaissance-1":"Renaissance 1",
  "warm-leads":"Warm Leads","section-125-1":"Section 125 (1)","section-125-2":"Section 125 (2)",
  "erc-1":"Tariffs + Funding","the-gatekeepers":"The Gatekeepers","the-eagles":"The Eagles",
  "the-dyad":"The Dyad","automated-applications":"Automated Applications","equinox":"Equinox",
  "outlook-1":"Outlook 1","outlook-2":"Outlook 2","outlook-3":"Outlook 3",
  "renaissance-3":"Renaissance 3","renaissance-6":"Renaissance 6","renaissance-7":"Renaissance 7",
  "(unmapped)":"(new / unmapped)",
}
def disp(slug):
    if slug in DISPLAY: return DISPLAY[slug]
    return slug.replace("-"," ").title()

per_day = {}   # date -> display -> {sent,opps,replies,meetings}
def bucket(d, slug):
    name = disp(slug)
    return per_day.setdefault(d, {}).setdefault(name, {"sent":0,"opps":0,"replies":0,"meetings":0})

con = duckdb.connect(SNAP, read_only=True)

# 1) sent/opps/replies from the WAREHOUSE, attributed via the COMPLETE dimension
#    (raw_pipeline_campaigns FULL-OUTER core.campaign; UUID->slug via core.workspace).
srows = con.execute("""
  WITH rpc AS (
    SELECT campaign_id, workspace_id
    FROM (SELECT *, row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) rn
            FROM raw_pipeline_campaigns) WHERE rn = 1),
  cc AS (
    SELECT c.campaign_id, w.slug AS workspace_id
    FROM core.campaign c LEFT JOIN core.workspace w ON w.workspace_id = c.workspace_id),
  dim AS (
    SELECT COALESCE(rpc.campaign_id, cc.campaign_id)   AS campaign_id,
           COALESCE(rpc.workspace_id, cc.workspace_id) AS workspace_id
    FROM rpc FULL OUTER JOIN cc USING (campaign_id))
  SELECT CAST(m.date AS DATE)::text AS d, COALESCE(dim.workspace_id, '(unmapped)') AS ws,
         SUM(m.sent), SUM(m.unique_opportunities), SUM(m.unique_replies)
  FROM raw_pipeline_campaign_daily_metrics m
  LEFT JOIN dim USING (campaign_id)
  WHERE m.date >= CAST(? AS DATE)
  GROUP BY 1, 2
""", [START]).fetchall()
for d, slug, sent, opps, rep in srows:
    b = bucket(d, slug); b["sent"]+=int(sent or 0); b["opps"]+=int(opps or 0); b["replies"]+=int(rep or 0)

# 2) email meetings from snapshot core.meeting — attribute by the meeting's OWN clean
#    workspace_slug (QA FIX 2026-06-21: core.meeting.workspace_slug is the ingest-clean key that
#    matches the Funding Form per-workspace bookings).
mrows = con.execute("""
  SELECT CAST(m.posted_at AS DATE)::text, COALESCE(m.workspace_slug,'(unmapped)'), COUNT(*)
  FROM core.meeting m
  WHERE CAST(m.posted_at AS DATE) >= DATE '2026-05-14' AND m.is_duplicate_of IS NULL AND m.channel='Email'
  GROUP BY 1,2
""").fetchall()
for d, slug, n in mrows:
    bucket(d, slug)["meetings"] += int(n)
con.close()

days = sorted(per_day.keys())
# workspace display order by total sent desc
tot = {}
for d in per_day:
    for name,v in per_day[d].items():
        tot[name] = tot.get(name,0)+v["sent"]
order = sorted(tot, key=lambda n:-tot[n])
out = {
  "generated_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
  "source": "Warehouse raw_pipeline_campaign_daily_metrics x complete campaign dim (raw_pipeline_campaigns + core.campaign) + core.meeting (email channel)",
  "note": "Full per-workspace totals (no active-CM filter). Opps = daily unique_opportunities. Meetings = email channel, well-populated from 2026-06-01. Attribution recovers campaigns missing from the stale raw_pipeline_campaigns dim via core.campaign (warehouse-flags#9).",
  "start": START, "days": days, "workspaces": order, "per_day": per_day,
}
open("/root/portal/dashboards/lens-campaign-performance/data/workspaces.json","w").write(json.dumps(out, separators=(",",":")))
print("wrote workspaces.json: days", len(days), "workspaces", len(order))
# sanity
j=per_day.get("2026-06-22",{})
print("June22 Funding 5:", j.get("Funding 5"))
print("June22 Funding 3:", j.get("Funding 3"))
