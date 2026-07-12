#!/usr/bin/env python3
"""MD migration P1 loader: copy all base tables + views from the validated
snapshot into md:warehouse. Idempotent (CREATE OR REPLACE). Read-only on the
snapshot; resolves the pointer ONCE and holds an open fd for the whole run so a
janitor rotation can't pull data out from under it. Logs per-table progress + a
JSON manifest for the parity report."""
import duckdb, time, sys, json, os, subprocess

SNAP = subprocess.check_output(
    ["readlink", "-f", "/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
OUT = "/root/md-migration"
os.makedirs(OUT, exist_ok=True)
manifest = {"snapshot": SNAP, "started": time.time(), "tables": [], "views": []}

def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)

log(f"snapshot = {SNAP}")
con = duckdb.connect("md:")
con.execute("CREATE DATABASE IF NOT EXISTS warehouse")
con.execute(f"ATTACH '{SNAP}' AS snap (READ_ONLY)")
schemas = [r[0] for r in con.execute(
    "SELECT DISTINCT schema_name FROM duckdb_tables() WHERE database_name='snap'").fetchall()]
# include view schemas too
vschemas = [r[0] for r in con.execute(
    "SELECT DISTINCT schema_name FROM duckdb_views() WHERE database_name='snap'").fetchall()]
for s in set(schemas) | set(vschemas):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS warehouse.{s}")
log(f"schemas: tables={schemas} views={vschemas}")

# ---- TABLES (small first: early progress + fail-fast) ----
tbls = con.execute(
    "SELECT schema_name, table_name, estimated_size FROM duckdb_tables() "
    "WHERE database_name='snap' ORDER BY estimated_size").fetchall()
log(f"{len(tbls)} base tables, ~{sum((t[2] or 0) for t in tbls):,} est rows")
t_start = time.time()
for i, (s, n, est) in enumerate(tbls, 1):
    t0 = time.time()
    try:
        con.execute(f'CREATE OR REPLACE TABLE warehouse.{s}."{n}" AS SELECT * FROM snap.{s}."{n}"')
        a = con.execute(f'SELECT count(*) FROM warehouse.{s}."{n}"').fetchone()[0]
        b = con.execute(f'SELECT count(*) FROM snap.{s}."{n}"').fetchone()[0]
        ok = (a == b)
        dt = time.time() - t0
        manifest["tables"].append({"t": f"{s}.{n}", "snap": b, "md": a, "ok": ok, "s": round(dt, 1)})
        log(f"[{i}/{len(tbls)}] {s}.{n} rows={a:,} {'OK' if ok else 'MISMATCH snap='+str(b)} {dt:.1f}s")
    except Exception as e:
        manifest["tables"].append({"t": f"{s}.{n}", "err": str(e)[:200]})
        log(f"[{i}/{len(tbls)}] {s}.{n} ERROR {str(e)[:160]}")
log(f"TABLES done in {time.time()-t_start:.0f}s; "
    f"ok={sum(1 for x in manifest['tables'] if x.get('ok'))}/{len(tbls)}")

# ---- VIEWS (multi-pass for inter-view deps) ----
vw = con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() "
                 "WHERE database_name='snap' AND internal=false").fetchall()
log(f"{len(vw)} views to port")
pending = list(vw)
con.execute("USE warehouse")
for _pass in range(1, 5):
    still = []
    for s, n, sql in pending:
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
            con.execute(sql)  # stored CREATE VIEW; unqualified refs resolve in warehouse
            manifest["views"].append({"v": f"{s}.{n}", "ok": True, "pass": _pass})
        except Exception as e:
            still.append((s, n, sql))
            last_err = str(e)[:160]
    log(f"views pass {_pass}: created={len(pending)-len(still)} remaining={len(still)}")
    if not still or len(still) == len(pending):
        pending = still
        break
    pending = still
for s, n, sql in pending:
    manifest["views"].append({"v": f"{s}.{n}", "ok": False, "err": "unresolved (likely local-path or missing dep)"})
log(f"VIEWS done; created={sum(1 for x in manifest['views'] if x.get('ok'))}/{len(vw)}; "
    f"failed={[x['v'] for x in manifest['views'] if not x.get('ok')][:20]}")

manifest["finished"] = time.time()
manifest["dur_s"] = round(manifest["finished"] - manifest["started"], 1)
with open(f"{OUT}/load_manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
log(f"MANIFEST -> {OUT}/load_manifest.json  total={manifest['dur_s']}s")
log("DONE")
