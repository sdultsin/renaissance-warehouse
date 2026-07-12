#!/usr/bin/env python3
"""Smoke-test the STAGED read shim in md-mode against warehouse_a (prod files untouched).
Exercises the exact code path mcp_server uses: common.connect_ro + snapshot_id_of + federation
+ the read-only assertion. Proves the pointer resolves, real queries run, lead-mirror federation
works over an MD connection, and flags whether a read-scoped token is still needed."""
import os, sys, time
os.environ["WAREHOUSE_BACKEND"] = "md"
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")
sys.path.insert(0, "/opt/duckdb/bin_md_staging")   # the STAGED shim
import common

print("backend        =", common.warehouse_backend())
print("md_serving_db  =", common.md_serving_db(), "(from pointer file)")
print("snapshot_id_of =", common.snapshot_id_of("/opt/duckdb/warehouse_current.duckdb"))

con = common.connect_ro("/opt/duckdb/warehouse_current.duckdb", threads=2, memory_limit="2GB", statement_timeout_ms=60000)
# build marker (accurate provenance of the served color)
bm = con.execute("SELECT snapshot_id, built_at_utc, color FROM main._md_build_info").fetchone()
print("build marker   =", bm)
# a real monitor-style query
t0=time.time(); r=con.execute("SELECT count(*) FROM core.reply").fetchone()[0]; print(f"core.reply     = {r:,} ({(time.time()-t0)*1000:.0f}ms)")
t0=time.time(); r=con.execute("SELECT count(*) FROM main.v_account_health").fetchone()[0]; print(f"v_account_health (macro view) = {r} ({(time.time()-t0)*1000:.0f}ms)")
con.close()

# federation: ATTACH the LOCAL lead-mirror serving into the MD connection (the mcp _attach_federation path)
print("\n-- federation over MD (lead-mirror ATTACH into md:warehouse_a) --")
try:
    con = common.connect_ro("/opt/duckdb/warehouse_current.duckdb")
    lm = "/mnt/volume_nyc1_1781398428838/lead-mirror/lead_mirror_serving.duckdb"
    con.execute(f"ATTACH '{lm}' AS leadmirror (READ_ONLY)")
    t0=time.time(); n=con.execute("SELECT count(*) FROM leadmirror.mirror.leads_current_with_email_esp").fetchone()[0]
    print(f"federated lead inventory = {n:,} ({(time.time()-t0)*1000:.0f}ms) -- ATTACH local file into MD conn WORKS")
    con.close()
except Exception as e:
    print("federation FAILED:", str(e)[:160])

# read-only assertion (expected: with the WRITE-scoped token it will WARN we need a read-scoped one)
print("\n-- read-only assertion --")
try:
    con = common.connect_md_ro()
    con.execute("CREATE TABLE _ro_probe_xyz (x INTEGER)")
    con.execute("DROP TABLE IF EXISTS _ro_probe_xyz"); con.close()
    print("WRITE SUCCEEDED -> physical read-only NOT yet enforced; need MOTHERDUCK_TOKEN_RO before flip (SQL guard still blocks writes at API).")
except Exception:
    print("write rejected -> read-scoped token confirmed.")
