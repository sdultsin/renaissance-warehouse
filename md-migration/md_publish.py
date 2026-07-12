#!/usr/bin/env python3
"""MD serving publish — blue/green + pointer-file swap (rename is unsupported on MD).
Builds the current validated snapshot into the INACTIVE MD color (warehouse_a|warehouse_b),
validates row parity + view execution, writes a build marker, then atomically flips the
pointer file the read shim resolves. Reversible: readers on WAREHOUSE_BACKEND=local are
unaffected; a bad publish never touches the active color. Idempotent per snapshot.

Usage: md_publish.py            # auto-pick inactive color, publish, validate, flip
       md_publish.py --no-flip  # build+validate only (dark), do not flip the pointer"""
import duckdb, os, sys, time, json, subprocess, re

# Views that are too heavy to serve as views on MD (>60s API timeout when cold) and are NOT on any
# automated consumer path — materialize them as TABLES on MD (serving is a daily snapshot anyway, so
# a materialized table is consistent) so ad-hoc queries stay fast. Keep this list tight.
HEAVY_VIEWS = {"derived.v_reply_canonical", "core.reply_attribution"}

POINTER = "/opt/duckdb/md_serving_db"          # holds the active color name; shim reads this
TMP = "/mnt/volume_nyc1_1781398428838/tmp_mdload"
CHUNK, BIG = 4_000_000, 3_000_000
os.makedirs(TMP, exist_ok=True)

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("'")

os.environ["motherduck_token"] = env("MOTHERDUCK_TOKEN")
SNAP = subprocess.check_output(["readlink","-f","/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
active = open(POINTER).read().strip() if os.path.exists(POINTER) else None
target = "warehouse_b" if active == "warehouse_a" else "warehouse_a"
log(f"snapshot={os.path.basename(SNAP)} active={active} -> building INACTIVE target={target}")

def connect():
    c = duckdb.connect("md:")
    c.execute("SET preserve_insertion_order=false"); c.execute("SET memory_limit='9GB'")
    c.execute(f"SET temp_directory='{TMP}'")
    c.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
    return c

con = connect()
con.execute(f"DROP DATABASE IF EXISTS {target}")          # fresh inactive color
con.execute(f"CREATE DATABASE {target}")
tschemas=[r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name='snap'").fetchall()]
vschemas=[r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_views() WHERE database_name='snap'").fetchall()]
for s in set(tschemas)|set(vschemas): con.execute(f"CREATE SCHEMA IF NOT EXISTS {target}.{s}")

# ---- tables (small first; big ones rowid-chunked) ----
tbls=con.execute("SELECT schema_name,table_name,estimated_size FROM duckdb_tables() WHERE database_name='snap' ORDER BY estimated_size").fetchall()
t0=time.time(); bad=[]
for i,(s,n,est) in enumerate(tbls,1):
    fq=f'{target}.{s}."{n}"'; sq=f'snap.{s}."{n}"'
    ct=con.execute(f'SELECT count(*) FROM {sq}').fetchone()[0]
    if ct>BIG:
        con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq} LIMIT 0'); k=0
        while k<ct:
            con.execute(f'INSERT INTO {fq} SELECT * FROM {sq} WHERE rowid>={k} AND rowid<{k+CHUNK}'); k+=CHUNK
    else:
        con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq}')
    got=con.execute(f'SELECT count(*) FROM {fq}').fetchone()[0]
    if got!=ct: bad.append((f"{s}.{n}",ct,got))
log(f"tables built {len(tbls)} in {time.time()-t0:.0f}s; parity_mismatch={bad[:5]}")

# ---- macros then views (multi-pass) ----
con.execute(f"USE {target}")
for s,fn,params,body in con.execute("SELECT schema_name,function_name,parameters,macro_definition FROM duckdb_functions() WHERE database_name='snap' AND function_type ILIKE '%macro%' AND internal=false").fetchall():
    ps=", ".join(params); tbl=body.lstrip().upper().startswith("SELECT")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    con.execute(f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS TABLE ({body})' if tbl else f'CREATE OR REPLACE MACRO {s}."{fn}"({ps}) AS {body}')
vw=con.execute("SELECT schema_name,view_name,sql FROM duckdb_views() WHERE database_name='snap' AND internal=false").fetchall()
pending=list(vw)
for _p in range(4):
    still=[]
    for s,n,sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
            if f"{s}.{n}" in HEAVY_VIEWS:
                con.execute(re.sub(r"^\s*CREATE\s+VIEW\s", "CREATE TABLE ", sql, count=1, flags=re.I))
            else:
                con.execute(sql)
        except Exception: still.append((s,n,sql))
    if not still or len(still)==len(pending): pending=still; break
    pending=still
views_ok=len(vw)-len(pending)

# ---- validate before flip ----
vfail=[]
for s,v in con.execute("SELECT schema_name,view_name FROM duckdb_views() WHERE database_name=? AND internal=false", [target]).fetchall():
    try: con.execute(f'SELECT count(*) FROM {s}."{v}"').fetchone()
    except Exception as e: vfail.append((f"{s}.{v}",str(e)[:80]))
ok = (not bad) and (views_ok==len(vw)) and (not vfail)
# build marker for snapshot_id in md-mode
con.execute("CREATE SCHEMA IF NOT EXISTS main")
con.execute("CREATE OR REPLACE TABLE main._md_build_info AS SELECT ? AS snapshot_id, ? AS built_at_utc, ? AS color",
            [os.path.basename(SNAP), time.strftime('%Y-%m-%dT%H:%M:%SZ'), target])
log(f"tables_parity_ok={not bad} views_ok={views_ok}/{len(vw)} view_exec_fail={len(vfail)} -> VALIDATION {'PASS' if ok else 'FAIL'}")

if "--no-flip" in sys.argv:
    log(f"--no-flip: built+validated {target}; pointer UNCHANGED (still {active}).")
elif ok:
    tmp=POINTER+".tmp"; open(tmp,"w").write(target); os.replace(tmp, POINTER)  # atomic flip
    log(f"POINTER FLIPPED: {active} -> {target}. Readers in md-mode now serve {target}.")
else:
    log(f"VALIDATION FAILED — pointer NOT flipped (still serving {active}). Inspect {target}.")
    sys.exit(1)
log("DONE")
