#!/usr/bin/env python3
"""MD migration P1 loader v3 — memory-bounded, resumable, robust connection.
v2's memory fix worked (RSS held ~10GB); v2's bug was churning a fresh connection
per big table and cascading 'Connection already closed'. v3 keeps ONE stable
connection (memory bounded via preserve_insertion_order=false + memory_limit +
disk spill), chunks big tables with per-chunk retry, and only re-establishes the
connection inside error recovery (new-before-close, with retry). Read-only on snap."""
import duckdb, time, sys, json, os, subprocess

SNAP = subprocess.check_output(["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
OUT = "/root/md-migration"; TMP = "/mnt/volume_nyc1_1781398428838/tmp_mdload"
os.makedirs(OUT, exist_ok=True); os.makedirs(TMP, exist_ok=True)
CHUNK = 4_000_000; BIG = 3_000_000

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def connect():
    for attempt in range(6):
        try:
            c = duckdb.connect("md:")
            c.execute("SET preserve_insertion_order=false")
            c.execute("SET memory_limit='9GB'")
            c.execute(f"SET temp_directory='{TMP}'")
            c.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
            return c
        except Exception as e:
            log(f"  connect attempt {attempt+1} failed: {str(e)[:100]}"); time.sleep(6)
    raise RuntimeError("could not connect to MD after retries")

log(f"snapshot = {SNAP}")
con = connect()
con.execute("CREATE DATABASE IF NOT EXISTS warehouse")
tschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name='snap'").fetchall()]
vschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_views() WHERE database_name='snap'").fetchall()]
for s in set(tschemas) | set(vschemas): con.execute(f"CREATE SCHEMA IF NOT EXISTS warehouse.{s}")

tbls = con.execute("SELECT schema_name, table_name, estimated_size FROM duckdb_tables() WHERE database_name='snap' ORDER BY estimated_size").fetchall()
md_tables = set((r[0], r[1]) for r in con.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name='warehouse'").fetchall())
log(f"{len(tbls)} snap tables; {len(md_tables)} already in md:warehouse")

manifest = {"snapshot": SNAP, "started": time.time(), "tables": []}
t_start = time.time()
for i, (s, n, est) in enumerate(tbls, 1):
    t0 = time.time(); fq = f'warehouse.{s}."{n}"'; sq = f'snap.{s}."{n}"'
    try:
        snap_ct = con.execute(f'SELECT count(*) FROM {sq}').fetchone()[0]
        if (s, n) in md_tables:
            md_ct = con.execute(f'SELECT count(*) FROM {fq}').fetchone()[0]
            if md_ct == snap_ct:
                manifest["tables"].append({"t": f"{s}.{n}", "snap": snap_ct, "md": md_ct, "ok": True, "skip": True})
                log(f"[{i}/{len(tbls)}] {s}.{n} rows={snap_ct:,} SKIP"); continue
        if snap_ct > BIG:
            con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq} LIMIT 0')
            k = 0
            while k < snap_ct:
                for attempt in range(4):
                    try:
                        con.execute(f'INSERT INTO {fq} SELECT * FROM {sq} WHERE rowid >= {k} AND rowid < {k+CHUNK}')
                        break
                    except Exception as ce:
                        log(f"    chunk {k} retry {attempt+1}: {str(ce)[:90]}")
                        try: con.close()
                        except Exception: pass
                        con = connect(); time.sleep(3)
                        if attempt == 3: raise
                k += CHUNK
                log(f"    {s}.{n} -> {min(k,snap_ct):,}/{snap_ct:,}")
        else:
            con.execute(f'CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {sq}')
        md_ct = con.execute(f'SELECT count(*) FROM {fq}').fetchone()[0]
        ok = (md_ct == snap_ct); dt = time.time() - t0
        manifest["tables"].append({"t": f"{s}.{n}", "snap": snap_ct, "md": md_ct, "ok": ok, "s": round(dt, 1)})
        log(f"[{i}/{len(tbls)}] {s}.{n} rows={md_ct:,} {'OK' if ok else 'MISMATCH snap='+str(snap_ct)} {dt:.1f}s")
    except Exception as e:
        manifest["tables"].append({"t": f"{s}.{n}", "err": str(e)[:200]})
        log(f"[{i}/{len(tbls)}] {s}.{n} ERROR {str(e)[:160]}")
        try: con.close()
        except Exception: pass
        con = connect()  # recover a live handle for the next table

ok_ct = sum(1 for x in manifest['tables'] if x.get('ok'))
log(f"TABLES done in {time.time()-t_start:.0f}s; ok={ok_ct}/{len(tbls)}; "
    f"failed={[x['t'] for x in manifest['tables'] if not x.get('ok')][:20]}")

# ---- VIEWS (multi-pass) ----
try: con.execute("USE warehouse")
except Exception: con = connect(); con.execute("USE warehouse")
vw = con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() WHERE database_name='snap' AND internal=false").fetchall()
manifest["views"] = []; pending = list(vw)
for _pass in range(1, 5):
    still = []
    for s, n, sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}"); con.execute(sql)
            manifest["views"].append({"v": f"{s}.{n}", "ok": True, "pass": _pass})
        except Exception as e:
            still.append((s, n, sql)); manifest["_lasterr"] = str(e)[:200]
    log(f"views pass {_pass}: created={len(pending)-len(still)} remaining={len(still)}")
    if not still or len(still) == len(pending): pending = still; break
    pending = still
for s, n, sql in pending: manifest["views"].append({"v": f"{s}.{n}", "ok": False, "err": manifest.get("_lasterr","")})
log(f"VIEWS created={sum(1 for x in manifest['views'] if x.get('ok'))}/{len(vw)}; "
    f"failed={[x['v'] for x in manifest['views'] if not x.get('ok')][:30]}")

manifest["finished"] = time.time(); manifest["dur_s"] = round(manifest["finished"]-manifest["started"], 1)
with open(f"{OUT}/load_manifest.json", "w") as f: json.dump(manifest, f, indent=2)
log(f"MANIFEST -> {OUT}/load_manifest.json total={manifest['dur_s']}s"); log("DONE")
