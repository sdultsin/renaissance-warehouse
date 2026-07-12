#!/usr/bin/env python3
"""Verify md-mode routing: analytics -> MD (fast), federation -> LOCAL (fast) via force_local."""
import os, sys, time
os.environ["WAREHOUSE_BACKEND"] = "md"
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")
sys.path.insert(0, "/opt/duckdb/bin_md_staging")
import common
TGT = "/opt/duckdb/warehouse_current.duckdb"

# 1) analytics path (force_local=False) -> MD
con = common.connect_ro(TGT, threads=2, memory_limit="2GB", force_local=False)
who = con.execute("SELECT current_database()").fetchone()[0]
t0=time.time(); r=con.execute("SELECT count(*) FROM core.reply").fetchone()[0]
print(f"analytics  -> db={who}  core.reply={r:,}  {(time.time()-t0)*1000:.0f}ms  (expect md:warehouse_a, fast)")
con.close()

# 2) federation path (force_local=True) -> LOCAL, then ATTACH mirror
con = common.connect_ro(TGT, force_local=True)
who = con.execute("SELECT current_database()").fetchone()[0]
lm = "/mnt/volume_nyc1_1781398428838/lead-mirror/lead_mirror_serving.duckdb"
con.execute(f"ATTACH '{lm}' AS leadmirror (READ_ONLY)")
t0=time.time(); n=con.execute("SELECT count(*) FROM leadmirror.mirror.leads_current_with_email_esp").fetchone()[0]
print(f"federation -> db={who}  inventory={n:,}  {(time.time()-t0)*1000:.0f}ms  (expect LOCAL snapshot, sub-second)")
con.close()
