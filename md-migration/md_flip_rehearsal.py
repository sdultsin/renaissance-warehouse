#!/usr/bin/env python3
"""Full flip rehearsal: drive the REAL mcp_server query path in md-mode (prod files, via venv)
without touching the live service. Exercises sql_is_read guard + connect_ro routing + federation
+ snapshot_id + get_schema + assert_read_only, on a battery of representative consumer queries.
Also identifies the heavy-tail views by name so we decide keep-warm vs materialize."""
import os, sys, time
os.environ["WAREHOUSE_BACKEND"] = "md"
os.environ["SERVING_PROFILE"] = "prod"
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")
os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")
sys.path.insert(0, "/opt/duckdb/bin")   # the PROMOTED prod shim
import mcp_server, common

print("== backend/pointer ==", common.warehouse_backend(), common.md_serving_db())

BATTERY = [
    ("core reply count",        "SELECT count(*) FROM core.reply"),
    ("workspace names",         "SELECT name FROM core.workspace LIMIT 5"),
    ("derived positive",        "SELECT count(*) FROM derived.reply_is_positive_strict"),
    ("macro view acct health",  "SELECT * FROM main.v_account_health LIMIT 3"),
    ("dash sample",             "SELECT * FROM main.v_accounts_per_domain LIMIT 3"),
    ("agg sends by day",        "SELECT count(*) FROM core.sending_account_daily"),
    ("guard: write blocked",    "CREATE TABLE x(y int)"),        # must be REJECTED by sql_is_read
    ("federation (->local)",    "SELECT count(*) FROM v_fundable_leads"),
]
for label, sql in BATTERY:
    t0 = time.time()
    try:
        r = mcp_server._run_query(sql)
        print(f"  OK   {label:24s} rows={r['row_count']:>3} snap={r['snapshot_id']:16s} {r['execution_ms']:.0f}ms")
    except Exception as e:
        tag = "OK(blocked)" if "read queries" in str(e) else "FAIL"
        print(f"  {tag:11s} {label:24s} {str(e)[:70]}")

print("\n== get_schema ==")
try:
    s = mcp_server.get_schema()
    print(f"  snapshot_id={s['snapshot_id']} table_count={s['table_count']}")
except Exception as e:
    print("  get_schema FAIL:", str(e)[:80])

print("\n== assert_read_only (expected read_only=False until MOTHERDUCK_TOKEN_RO) ==")
ok, detail = mcp_server.assert_read_only()
print(f"  read_only={ok} :: {detail}")

print("\n== heavy-tail views (slowest 10 by name, via a direct md conn) ==")
import duckdb
c = duckdb.connect("md:warehouse_a")
lat = []
for schema, v in c.execute("SELECT schema_name, view_name FROM duckdb_views() WHERE database_name='warehouse_a' AND internal=false").fetchall():
    t0 = time.time()
    try:
        c.execute(f'SELECT count(*) FROM {schema}."{v}"').fetchone(); lat.append((( time.time()-t0)*1000, f"{schema}.{v}"))
    except Exception:
        pass
lat.sort(reverse=True)
for ms, name in lat[:10]:
    print(f"  {ms:8.0f}ms  {name}")
c.close()
