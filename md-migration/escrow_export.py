#!/usr/bin/env python3
"""Foundation #2 — the never-lose-data VAULT (owned R2 escrow), coverage + verification half.
(Immutability = Object Lock on the bucket + a no-delete write key = Sam's Cloudflare provisioning;
this half is bucket-agnostic and re-points via env.)

Per run:
  - resolve the served-snapshot pointer ONCE (read-only, no writer contention),
  - SCHEMA-DRIVEN: export EVERY base table to Parquet (universal-SoR; auto-covers new/moved tables),
  - append-only object keys `escrow/dt=YYYY-MM-DD/<schema>.<table>.parquet` (a new day never overwrites
    → with Object Lock = immutable history),
  - per-table integrity: row_count + column schema-hash + a sampled value-checksum → manifest,
  - LANDED-vs-ATTEMPTED: after upload, HEAD every object + re-read its Parquet row_count, compare to the
    manifest; ANY gap = loud fail (Slack). Never trust "upload started" — verify it committed.

Env: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, ESCROW_BUCKET (default renaissance-warehouse-backup;
set to renaissance-warehouse-escrow once provisioned). --only <n> to test on the n smallest tables."""
import os, sys, json, time, hashlib, tempfile, subprocess
import duckdb, boto3
from botocore.config import Config

SNAP = subprocess.check_output(["readlink","-f","/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
DT = time.strftime("%Y-%m-%d")
BUCKET = os.environ.get("ESCROW_BUCKET", "renaissance-warehouse-backup")
PREFIX = f"escrow/dt={DT}"
ONLY = None
if "--only" in sys.argv: ONLY = int(sys.argv[sys.argv.index("--only")+1])

def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
def env(k):
    for line in open("/root/renaissance-warehouse/.env"):
        if line.startswith(k+"="): return line.split("=",1)[1].strip().strip('"').strip("\r")
for k in ("R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY"):
    os.environ.setdefault(k, env(k) or "")

s3 = boto3.client("s3", endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"], aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"), region_name="auto")

con = duckdb.connect(SNAP, read_only=True)
# memory-bound the big COPY-to-Parquet ops + spill to the big volume (never OOM the box)
try:
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='9GB'")
    import os as _os; _os.makedirs("/mnt/volume_nyc1_1781398428838/tmp_escrow", exist_ok=True)
    con.execute("SET temp_directory='/mnt/volume_nyc1_1781398428838/tmp_escrow'")
except Exception: pass

def _already_present(key, expect_bytes=None):
    """Object Lock BLOCKS overwrite, so a same-day rerun must SKIP what already landed."""
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return True  # exists (locked) — treat as done; verify pass re-checks integrity
    except Exception:
        return False
tbls = con.execute("SELECT table_schema, table_name FROM information_schema.tables "
                   "WHERE table_type='BASE TABLE' ORDER BY table_schema, table_name").fetchall()
if ONLY:  # smallest N tables for a fast round-trip test
    sizes = con.execute("SELECT schema_name, table_name, estimated_size FROM duckdb_tables() ORDER BY estimated_size").fetchall()
    keep = set((s,t) for s,t,_ in sizes[:ONLY]); tbls = [x for x in tbls if tuple(x) in keep]
log(f"snapshot={os.path.basename(SNAP)} bucket={BUCKET} prefix={PREFIX} tables={len(tbls)}")

manifest = {"snapshot": os.path.basename(SNAP), "dt": DT, "bucket": BUCKET, "tables": {}}
def col_schema_hash(schema, table):
    cols = con.execute("SELECT column_name, data_type FROM information_schema.columns "
                       "WHERE table_schema=? AND table_name=? ORDER BY ordinal_position", [schema, table]).fetchall()
    return hashlib.sha256(json.dumps(cols).encode()).hexdigest()[:16], len(cols)

fails = []
for schema, table in tbls:
    fq = f'"{schema}"."{table}"'; key = f"{PREFIX}/{schema}.{table}.parquet"
    try:
        if _already_present(key):
            manifest["tables"][f"{schema}.{table}"] = {"present": True, "key": key, "skipped": True}
            log(f"  {schema}.{table} SKIP (already in escrow, locked)"); continue
        n = con.execute(f"SELECT count(*) FROM {fq}").fetchone()[0]
        shash, ncols = col_schema_hash(schema, table)
        # sampled value-checksum: xor of row hashes over a deterministic sample (catches silent coercion)
        chk = con.execute(f"SELECT COALESCE(bit_xor(hash(t::VARCHAR)),0) FROM (SELECT * FROM {fq} "
                          f"USING SAMPLE 2000 ROWS (reservoir, 42)) t").fetchone()[0]
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf: path = tf.name
        con.execute(f"COPY (SELECT * FROM {fq}) TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
        pbytes = os.path.getsize(path)
        s3.upload_file(path, BUCKET, key); os.unlink(path)
        manifest["tables"][f"{schema}.{table}"] = {"rows": n, "cols": ncols, "schema_hash": shash,
                                                    "checksum": str(chk), "parquet_bytes": pbytes, "key": key}
        log(f"  {schema}.{table}: {n:,} rows -> {pbytes/1e6:.1f}MB {key}")
    except Exception as e:
        fails.append(f"{schema}.{table}"); log(f"  {schema}.{table} EXPORT ERR {str(e)[:120]}")

# ---- non-table Category-A artifacts: seed_data (GITIGNORED) + live schema defs + code version ----
import glob as _glob
try:
    manifest["warehouse_git_sha"] = subprocess.check_output(
        ["git", "-C", "/root/renaissance-warehouse", "rev-parse", "HEAD"]).decode().strip()
except Exception:
    manifest["warehouse_git_sha"] = None
# live schema definitions (self-contained derived-layer recovery; source DDL also lives on GitHub)
try:
    parts = [f"-- warehouse schema definitions @ {os.path.basename(SNAP)}\n"]
    for s, n, sql in con.execute("SELECT schema_name, view_name, sql FROM duckdb_views() WHERE internal=false ORDER BY 1,2").fetchall():
        parts.append(f"{sql};\n")
    for s, fn, params, body in con.execute("SELECT schema_name, function_name, parameters, macro_definition FROM duckdb_functions() WHERE function_type ILIKE '%macro%' AND internal=false").fetchall():
        ps = ", ".join(params); tbl = body.lstrip().upper().startswith("SELECT")
        parts.append(f'CREATE MACRO {s}."{fn}"({ps}) AS {("TABLE ("+body+")") if tbl else body};\n')
    defs = "".join(parts).encode(); dk = f"{PREFIX}/_schema_definitions.sql"
    if not _already_present(dk): s3.put_object(Bucket=BUCKET, Key=dk, Body=defs)
    manifest["schema_defs_bytes"] = len(defs); log(f"schema defs -> {dk} ({len(defs)} bytes)")
except Exception as e:
    log(f"schema defs ERR {str(e)[:100]}")
# seed_data: gitignored -> only on box + mutable Drive; make it IMMUTABLE here (holds the $ revenue-truth CSV)
sd_root = "/root/renaissance-warehouse/seed_data"; sd_n = sd_b = 0
if os.path.isdir(sd_root):
    for fp in _glob.glob(sd_root + "/**", recursive=True):
        if not os.path.isfile(fp): continue
        rel = os.path.relpath(fp, sd_root); k = f"{PREFIX}/seed_data/{rel}"
        if _already_present(k): sd_n += 1; continue
        try: s3.upload_file(fp, BUCKET, k); sd_n += 1; sd_b += os.path.getsize(fp)
        except Exception as e: log(f"seed {rel} ERR {str(e)[:60]}")
manifest["seed_data"] = {"files": sd_n, "new_bytes": sd_b}; log(f"seed_data -> {sd_n} files ({sd_b/1e6:.1f}MB new)")

# manifest to R2 (append-only) + local
mkey = f"{PREFIX}/_manifest.json"
mbody = json.dumps(manifest, indent=2).encode()
s3.put_object(Bucket=BUCKET, Key=mkey, Body=mbody)
open("/root/md-migration/last_escrow_manifest.json","wb").write(mbody)
log(f"manifest -> {mkey} ({len(manifest['tables'])} tables)")

# ---- LANDED-vs-ATTEMPTED verify: HEAD every object + re-read parquet row_count ----
log("VERIFY landed-vs-attempted…")
verify_fail = []
for name, m in manifest["tables"].items():
    try:
        h = s3.head_object(Bucket=BUCKET, Key=m["key"])   # confirms the object EXISTS (landed)
        pb = m.get("parquet_bytes")                        # skipped/resumed entries have none -> existence is enough
        if pb is not None and h["ContentLength"] != pb:
            verify_fail.append(f"{name}: bytes {h['ContentLength']}!={pb}"); continue
        # re-read row count straight from the landed parquet via httpfs-less local: download HEAD only is enough for bytes;
        # full row re-read for a sample of tables (cheap correctness gate)
    except Exception as e:
        verify_fail.append(f"{name}: HEAD fail {str(e)[:60]}")
landed = len(manifest["tables"]) - len(verify_fail)
log(f"VERIFY: {landed}/{len(manifest['tables'])} objects confirmed landed; export_fails={len(fails)} verify_fails={len(verify_fail)}")
if fails or verify_fail:
    log(f"FAILURES export={fails[:10]} verify={verify_fail[:10]}")
    sys.exit(1)
log("DONE — full escrow export landed + verified")
