import duckdb, psycopg2, json, os, re, datetime

PG_URL = None
for line in open("/root/data-pipeline-v2/.env"):
    if line.startswith("SUPABASE_DB_URL=") and "nmkaydqcnkjsehyqokgg" in line:
        PG_URL = line.split("=",1)[1].strip().strip('"')
if not PG_URL:
    # second line form: the var spans; fallback grep
    import subprocess
    PG_URL = subprocess.check_output("grep -oE 'postgresql://postgres.nmkaydqcnkjsehyqokgg:[^ \"]+' /root/data-pipeline-v2/.env | head -1", shell=True).decode().strip()

START = "2026-05-14"
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

# 1) sent/opps/replies from Pipeline (= Instantly)
pg = psycopg2.connect(PG_URL); cur = pg.cursor()
cur.execute("""
  SELECT m.date::text, COALESCE(c.workspace_id,'(unmapped)'),
         SUM(m.sent), SUM(m.opportunities), SUM(m.unique_replies)
  FROM campaign_daily_metrics m
  LEFT JOIN (SELECT DISTINCT ON (campaign_id) campaign_id, workspace_id FROM campaigns ORDER BY campaign_id) c
    ON c.campaign_id = m.campaign_id
  WHERE m.date >= %s GROUP BY 1,2
""", (START,))
for d, slug, sent, opps, rep in cur.fetchall():
    b = bucket(d, slug); b["sent"]+=int(sent or 0); b["opps"]+=int(opps or 0); b["replies"]+=int(rep or 0)
pg.close()

# 2) email meetings from snapshot core.meeting (campaign->workspace via raw_pipeline_campaigns)
con = duckdb.connect("/opt/duckdb/warehouse_current.duckdb", read_only=True)
mrows = con.execute("""
  WITH camp AS (SELECT campaign_id, any_value(workspace_id) AS ws FROM raw_pipeline_campaigns GROUP BY campaign_id)
  SELECT CAST(m.posted_at AS DATE)::text, COALESCE(c.ws,'(unmapped)'), COUNT(*)
  FROM core.meeting m LEFT JOIN camp c ON c.campaign_id=m.campaign_id
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
  "source": "Pipeline campaign_daily_metrics x campaigns (mirrors Instantly) + core.meeting (email channel)",
  "note": "Full per-workspace totals (no active-CM filter). Opps = daily opportunities. Meetings = email channel, well-populated from 2026-06-01.",
  "start": START, "days": days, "workspaces": order, "per_day": per_day,
}
open("/root/portal/dashboards/lens-campaign-performance/data/workspaces.json","w").write(json.dumps(out, separators=(",",":")))
print("wrote workspaces.json: days", len(days), "workspaces", len(order))
# sanity: June 17 Funding 4
j=per_day.get("2026-06-17",{})
print("June17 Funding 4:", j.get("Funding 4"))
print("June17 Warm Leads:", j.get("Warm Leads"))
