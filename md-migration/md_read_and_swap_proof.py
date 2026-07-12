#!/usr/bin/env python3
"""De-risk the migration read side + swap mechanism, zero-touch on prod.
(1) Execute ALL 185 views on md:warehouse (count(*)) -> proves MD runs the whole read
    layer (macro/function/dialect parity) + captures latency.
(2) Probe MD's atomic DB-swap primitives (rename / replace) -> the staging->serving mechanism."""
import duckdb, os, time, statistics, subprocess

def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")

con = duckdb.connect("md:warehouse")

print("==== (1) READ-LAYER PROOF: execute all 185 views on MD ====")
views = con.execute("SELECT schema_name, view_name FROM duckdb_views() "
                    "WHERE database_name='warehouse' AND internal=false ORDER BY 1,2").fetchall()
ok, fails, lat = 0, [], []
for s, v in views:
    t0 = time.time()
    try:
        con.execute(f'SELECT count(*) FROM {s}."{v}"').fetchone()
        ms = (time.time()-t0)*1000; lat.append(ms); ok += 1
    except Exception as e:
        fails.append((f"{s}.{v}", str(e)[:120]))
print(f"views executed OK: {ok}/{len(views)}")
if lat:
    lat.sort()
    p = lambda q: lat[min(len(lat)-1, int(len(lat)*q))]
    print(f"latency ms: p50={p(.5):.0f} p90={p(.9):.0f} p95={p(.95):.0f} max={max(lat):.0f} "
          f"(>1s: {sum(1 for x in lat if x>1000)} views, >5s: {sum(1 for x in lat if x>5000)})")
if fails:
    print(f"FAILED views ({len(fails)}):")
    for name, err in fails: print(f"  {name}: {err}")
else:
    print("ALL views execute on MD — no function/dialect breaks.")

print("\n==== (2) SWAP PROOF: can we atomically swap a staging DB to serving on MD? ====")
def try_sql(label, sql):
    try:
        con.execute(sql); print(f"  OK   {label}"); return True
    except Exception as e:
        print(f"  FAIL {label}: {str(e)[:110]}"); return False
# clean slate
for db in ("_swap_new","_swap_live","_swap_old"):
    try: con.execute(f"DROP DATABASE IF EXISTS {db}")
    except Exception: pass
try_sql("CREATE DATABASE _swap_new", "CREATE DATABASE _swap_new")
con.execute("CREATE TABLE _swap_new.main.t AS SELECT 1 AS x") if try_sql("create schema main", "CREATE SCHEMA IF NOT EXISTS _swap_new.main") else None
# primitive A: ALTER DATABASE ... RENAME
rename_ok = try_sql("ALTER DATABASE _swap_new RENAME TO _swap_live", "ALTER DATABASE _swap_new RENAME TO _swap_live")
# primitive B: can we DROP then rename another into place (the swap)
if rename_ok:
    con.execute("CREATE DATABASE _swap_new2");
    try_sql("DROP DATABASE _swap_live (old)", "DROP DATABASE _swap_live")
    try_sql("ALTER DATABASE _swap_new2 RENAME TO _swap_live (swap-in)", "ALTER DATABASE _swap_new2 RENAME TO _swap_live")
# cleanup
for db in ("_swap_new","_swap_new2","_swap_live","_swap_old"):
    try: con.execute(f"DROP DATABASE IF EXISTS {db}")
    except Exception: pass
print("swap primitives probed (see OK/FAIL above).")
