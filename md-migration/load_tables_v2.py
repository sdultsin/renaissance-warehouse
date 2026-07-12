#!/usr/bin/env python3
"""MD migration P1 loader v2 — memory-bounded + resumable.
Fixes v1's unbounded RSS on big-table CTAS: preserve_insertion_order=false + disk
spill + rowid-chunked INSERT for large tables, with a fresh MD connection per big
table so buffered memory is fully released between tables. Resume-skips tables
already present in MD at the correct row count. Read-only on the snapshot."""
import duckdb, time, sys, json, os, subprocess

SNAP = subprocess.check_output(
    ["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
OUT = "/root/md-migration"
TMP = "/mnt/volume_nyc1_1781398428838/tmp_mdload"
os.makedirs(OUT, exist_ok=True); os.makedirs(TMP, exist_ok=True)
CHUNK = 5_000_000
BIG = 3_000_000

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

def connect():
    c = duckdb.connect("md:")
    c.execute("SET preserve_insertion_order=false")
    c.execute("SET memory_limit='10GB'")
    c.execute(f"SET temp_directory='{TMP}'")
    c.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
    return c

log(f"snapshot = {SNAP}")
con = connect()
con.execute("CREATE DATABASE IF NOT EXISTS warehouse")
tschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name='snap'").fetchall()]
vschemas = [r[0] for r in con.execute("SELECT DISTINCT schema_name FROM duckdb_views() WHERE database_name='snap'").fetchall()]
for s in set(tschemas) | set(vschemas):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS warehouse.{s}")

tbls = con.execute("SELECT schema_name, table_name, estimated_size FROM duckdb_tables() "
                   "WHERE database_name='snap' ORDER BY estimated_size").fetchall()
# existing MD tables (for resume)
md_tables = set((r[0], r[1]) for r in con.execute(
    "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name='warehouse'").fetchall())
log(f"{len(tbls)} snap tables; {len(md_tables)} already in md:warehouse")

manifest = {"snapshot": SNAP, "started": time.time(), "tables": []}
t_start = time.time()
for i, (s, n, est) in enumerate(tbls, 1):
    t0 = time.time()
    try:
        snap_ct = con.execute(f'SELECT count(*) FROM snap.{s}."{n}"').fetchone()[0]
        if (s, n) in md_tables:
            md_ct = con.execute(f'SELECT count(*) FROM warehouse.{s}."{n}"').fetchone()[0]
            if md_ct == snap_ct:
                manifest["tables"].append({"t": f"{s}.{n}", "snap": snap_ct, "md": md_ct, "ok": True, "skip": True})
                log(f"[{i}/{len(tbls)}] {s}.{n} rows={snap_ct:,} SKIP (already correct)")
                continue
        if snap_ct > BIG:
            # fresh connection to release memory; chunked by contiguous rowid
            con.close(); con = connect()
            con.execute(f'CREATE OR REPLACE TABLE warehouse.{s}."{n}" AS SELECT * FROM snap.{s}."{n}" LIMIT 0')
            k = 0
            while k < snap_ct:
                con.execute(f'INSERT INTO warehouse.{s}."{n}" SELECT * FROM snap.{s}."{n}" WHERE rowid >= {k} AND rowid < {k+CHUNK}')
                k += CHUNK
                log(f"    {s}.{n} chunk -> {min(k,snap_ct):,}/{snap_ct:,}")
        else:
            con.execute(f'CREATE OR REPLACE TABLE warehouse.{s}."{n}" AS SELECT * FROM snap.{s}."{n}"')
        md_ct = con.execute(f'SELECT count(*) FROM warehouse.{s}."{n}"').fetchone()[0]
        ok = (md_ct == snap_ct)
        if not ok:  # chunk fallback: single CTAS with spill
            con.execute(f'CREATE OR REPLACE TABLE warehouse.{s}."{n}" AS SELECT * FROM snap.{s}."{n}"')
            md_ct = con.execute(f'SELECT count(*) FROM warehouse.{s}."{n}"').fetchone()[0]; ok = (md_ct == snap_ct)
        dt = time.time() - t0
        manifest["tables"].append({"t": f"{s}.{n}", "snap": snap_ct, "md": md_ct, "ok": ok, "s": round(dt, 1)})
        log(f"[{i}/{len(tbls)}] {s}.{n} rows={md_ct:,} {'OK' if ok else 'MISMATCH snap='+str(snap_ct)} {dt:.1f}s")
    except Exception as e:
        manifest["tables"].append({"t": f"{s}.{n}", "err": str(e)[:200]})
        log(f"[{i}/{len(tbls)}] {s}.{n} ERROR {str(e)[:160]}")

ok_ct = sum(1 for x in manifest['tables'] if x.get('ok'))
log(f"TABLES done in {time.time()-t_start:.0f}s; ok={ok_ct}/{len(tbls)}")

# ---- VIEWS (multi-pass) ----
vw = con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() WHERE database_name='snap' AND internal=false").fetchall()
con.execute("USE warehouse")
manifest["views"] = []
pending = list(vw)
for _pass in range(1, 5):
    still = []
    for s, n, sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}"); con.execute(sql)
            manifest["views"].append({"v": f"{s}.{n}", "ok": True, "pass": _pass})
        except Exception as e:
            still.append((s, n, sql)); manifest["_lasterr"] = str(e)[:160]
    log(f"views pass {_pass}: created={len(pending)-len(still)} remaining={len(still)}")
    if not still or len(still) == len(pending): pending = still; break
    pending = still
for s, n, sql in pending:
    manifest["views"].append({"v": f"{s}.{n}", "ok": False})
log(f"VIEWS created={sum(1 for x in manifest['views'] if x.get('ok'))}/{len(vw)}; "
    f"failed={[x['v'] for x in manifest['views'] if not x.get('ok')][:25]}")

manifest["finished"] = time.time(); manifest["dur_s"] = round(manifest["finished"]-manifest["started"], 1)
with open(f"{OUT}/load_manifest.json", "w") as f: json.dump(manifest, f, indent=2)
log(f"MANIFEST -> {OUT}/load_manifest.json  total={manifest['dur_s']}s")
log("DONE")
