#!/usr/bin/env python3
"""Port the 5 macros (loader skipped them) + the 3 views that depend on them into md:warehouse."""
import duckdb, subprocess
SNAP = subprocess.check_output(["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
con = duckdb.connect("md:")
con.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
con.execute("USE warehouse")

macs = con.execute("""SELECT schema_name, function_name, parameters, macro_definition
                      FROM duckdb_functions() WHERE database_name='snap'
                      AND function_type ILIKE '%macro%' AND internal=false""").fetchall()
ok = 0
for s, fn, params, body in macs:
    ps = ", ".join(params)
    is_table = body.lstrip().upper().startswith("SELECT")
    if is_table:
        stmt = f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS TABLE ({body})'
    else:
        stmt = f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS {body}'
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        con.execute(stmt); ok += 1
        print(f"MACRO OK: {s}.{fn} ({'table' if is_table else 'scalar'})")
    except Exception as e:
        print(f"MACRO FAIL {s}.{fn}: {str(e)[:160]}")
print(f"macros: {ok}/{len(macs)}")

for v in ["v_accounts_per_domain", "v_account_health", "v_infra_capacity_daily"]:
    r = con.execute(f"SELECT schema_name, sql FROM duckdb_views() WHERE database_name='snap' AND view_name='{v}'").fetchone()
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {r[0]}")
        con.execute(r[1])
        print(f"VIEW OK: {v}")
    except Exception as e:
        print(f"VIEW FAIL {v}: {str(e)[:160]}")

t = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse'").fetchone()[0]
vw = con.execute("SELECT count(*) FROM duckdb_views() WHERE database_name='warehouse'").fetchone()[0]
mc = con.execute("SELECT count(*) FROM duckdb_functions() WHERE database_name='warehouse' AND function_type ILIKE '%macro%' AND internal=false").fetchone()[0]
# structural parity vs snapshot
st = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name='snap'").fetchone()[0]
sv = con.execute("SELECT count(*) FROM duckdb_views() WHERE database_name='snap' AND internal=false").fetchone()[0]
print(f"MD warehouse NOW: {t} tables (snap {st}), {vw} views (snap {sv}), {mc} macros (snap {len(macs)})")
