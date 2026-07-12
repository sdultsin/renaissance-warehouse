#!/usr/bin/env python3
"""Verify the F1-F7 corrected shim end-to-end against warehouse_a (prod untouched)."""
import os, sys, time
os.environ["WAREHOUSE_BACKEND"] = "md"
os.environ["SERVING_PROFILE"] = "prod"
os.environ["SERVING_CONFIG"] = "/opt/duckdb/bin_md_staging/config.yaml"
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")
sys.path.insert(0, "/opt/duckdb/bin_md_staging")
import common
TGT = "/opt/duckdb/warehouse_current.duckdb"

print("== routing ==")
c = common.connect_ro(TGT, threads=2, memory_limit="2GB", statement_timeout_ms=60000, md_db=common.md_serving_db())
print("  analytics  db=%-14s core.reply=%s" % (c.execute("SELECT current_database()").fetchone()[0], f"{c.execute('SELECT count(*) FROM core.reply').fetchone()[0]:,}")); c.close()
c = common.connect_ro(TGT, force_local=True)
print("  federation db=%-14s (force_local -> LOCAL)" % c.execute("SELECT current_database()").fetchone()[0]); c.close()
print("  snapshot_id_of =", common.snapshot_id_of(TGT))

print("== F7 pointer validation ==")
print("  md_serving_db() with valid pointer =", common.md_serving_db())

print("== F1/F2 read-only probe via the REAL assert_read_only ==")
try:
    import mcp_server
    ok, detail = mcp_server.assert_read_only()
    print(f"  assert_read_only -> read_only={ok} :: {detail}")
except Exception as e:
    print("  (import mcp_server failed, replicating logic):", str(e)[:100])
    con = common.connect_md_ro()
    probe="_ro_probe_selftest"
    try: con.execute(f"DROP DATABASE IF EXISTS {probe}")
    except Exception: pass
    try:
        con.execute(f"CREATE DATABASE {probe}")
        try: con.execute(f"DROP DATABASE IF EXISTS {probe}")
        except Exception: pass
        print("  replicated -> read_only=False (write token; scratch db used, verdict from CREATE)")
    except Exception:
        print("  replicated -> read_only=True")
    con.close()

print("== F2 verify serving color UNTOUCHED (no _ro_selftest residue on warehouse_a) ==")
c = common.connect_md_ro("warehouse_a")
resid = c.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse_a' AND table_name ILIKE '%ro_selftest%'").fetchone()[0]
dbs = [r[0] for r in c.execute("SHOW DATABASES").fetchall()]
print(f"  residue tables on warehouse_a = {resid} (want 0); _ro_probe_selftest present in DBs = {'_ro_probe_selftest' in dbs} (want False)")
c.close()
