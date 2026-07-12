#!/usr/bin/env python3
"""DDL REPLAY — recreate all macros + views in a MotherDuck database from a source catalog.

Why this exists: scripts/setup_db.py is VERSION-GATED (core.schema_version copies across with the
data, so on any copied/cloned database every DDL file no-ops). On MotherDuck, views/macros must be
(re)built by replaying the catalog definitions — the same way md_load_tables_v3 / md_publish port
them. This is the standalone repair/refresh tool (staged 2026-07-12, Lane B).

Usage (droplet):
  export motherduck_token=$(grep '^MOTHERDUCK_TOKEN=' /root/renaissance-warehouse/.env | cut -d= -f2- | tr -d '"')
  python3 ddl_replay.py --target warehouse                 # source = local served snapshot (default)
  python3 ddl_replay.py --target warehouse_b --from-md warehouse_a   # source = another md db
  python3 ddl_replay.py --target warehouse --materialize-heavy      # publish behavior for heavy views
"""
import duckdb, os, sys, re, subprocess, time

HEAVY_VIEWS = {"derived.v_reply_canonical", "core.reply_attribution"}

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def arg(flag, default=None):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default

target = arg("--target")
if not target:
    print(__doc__); sys.exit(1)
from_md = arg("--from-md")
mat_heavy = "--materialize-heavy" in sys.argv

con = duckdb.connect("md:")
if from_md:
    src = from_md
else:
    snap = subprocess.check_output(["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
    con.execute(f"ATTACH '{snap}' AS snap (READ_ONLY)")
    src = "snap"
    log(f"source = local snapshot {os.path.basename(snap)}")

con.execute(f"USE {target}")

# macros first (views depend on them — the 07-09 load's 3 view failures were exactly this)
macros = con.execute(
    "SELECT schema_name, function_name, parameters, macro_definition FROM duckdb_functions() "
    "WHERE database_name=? AND function_type ILIKE '%macro%' AND internal=false", [src]).fetchall()
for s, fn, params, body in macros:
    ps = ", ".join(params); tbl = body.lstrip().upper().startswith("SELECT")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    con.execute(f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS TABLE ({body})' if tbl
                else f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS {body}')
log(f"macros replayed: {len(macros)}")

vw = con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() "
                 "WHERE database_name=? AND internal=false", [src]).fetchall()
pending = list(vw); last_err = {}
for _p in range(1, 5):
    still = []
    for s, n, sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
            if mat_heavy and f"{s}.{n}" in HEAVY_VIEWS:
                con.execute(re.sub(r"^\s*CREATE\s+VIEW\s", "CREATE TABLE ", sql, count=1, flags=re.I))
            else:
                con.execute(re.sub(r"^\s*CREATE\s+VIEW\s", "CREATE OR REPLACE VIEW ", sql, count=1, flags=re.I))
        except Exception as e:
            still.append((s, n, sql)); last_err[f"{s}.{n}"] = str(e)[:120]
    log(f"pass {_p}: created={len(pending)-len(still)} remaining={len(still)}")
    if not still or len(still) == len(pending):
        pending = still; break
    pending = still

if pending:
    log(f"FAILED views ({len(pending)}):")
    for s, n, _ in pending:
        log(f"  {s}.{n}: {last_err.get(f'{s}.{n}', '?')}")
    sys.exit(2)
log(f"ALL views replayed: {len(vw)}/{len(vw)}")
