#!/usr/bin/env python3
"""Stage the MD read-shim onto COPIES of the serving files (prod /opt/duckdb/bin untouched),
with all code-review findings F1-F7 addressed. Backend-aware via WAREHOUSE_BACKEND (default
'local' => byte-identical) + pointer-file blue/green color resolution. py_compiles both."""
import os, shutil, py_compile

SRC="/opt/duckdb/bin"; STG="/opt/duckdb/bin_md_staging"
os.makedirs(STG, exist_ok=True)
for f in os.listdir(SRC):
    s=os.path.join(SRC,f)
    if os.path.isfile(s): shutil.copy2(s, os.path.join(STG,f))

# ======================= common.py =======================
cp=os.path.join(STG,"common.py"); src=open(cp).read()

# (a) helpers before connect_ro. F6: token set ONCE (not per-request). F7: validated pointer.
helpers='''
# ---- MotherDuck serving backend (warehouse->MD migration; default OFF) --------------------
# WAREHOUSE_BACKEND=md routes the read path to md:<active-color> (pointer-file blue/green — MD
# has no ALTER DATABASE RENAME). Default 'local' => every function below is byte-identical.
import re as _re
_MD_POINTER = os.environ.get("MD_SERVING_POINTER", "/opt/duckdb/md_serving_db")
_MD_DB_RE = _re.compile(r"^warehouse_[a-z0-9_]+$")   # F7: validate pointer contents
_MD_TOKEN_READY = [False]

def warehouse_backend() -> str:
    return os.environ.get("WAREHOUSE_BACKEND", "local").strip().lower()

def md_serving_db() -> str:
    try:
        v = open(_MD_POINTER).read().strip()
        if _MD_DB_RE.match(v):
            return v
    except OSError:
        pass
    return os.environ.get("WAREHOUSE_MD_DB", "warehouse_a")   # safe fallback on missing/garbage pointer

def _ensure_md_token() -> None:
    # F6: set the constant token ONCE, not on every (possibly-concurrent) request.
    if not _MD_TOKEN_READY[0]:
        tok = os.environ.get("MOTHERDUCK_TOKEN_RO") or os.environ.get("MOTHERDUCK_TOKEN")
        if tok:
            os.environ["motherduck_token"] = tok
        _MD_TOKEN_READY[0] = True

def connect_md_ro(md_db: str | None = None):
    """Connection to a specific MD serving color (caller passes md_db to avoid a pointer re-read
    race). Physical read-only comes from a read-scoped token (MOTHERDUCK_TOKEN_RO); the mcp SQL
    guard is defense-in-depth regardless."""
    _ensure_md_token()
    return duckdb.connect(f"md:{md_db or md_serving_db()}")

'''
assert src.count("def connect_ro(")==1
src=src.replace("def connect_ro(", helpers+"\ndef connect_ro(", 1)

# (b) connect_ro signature: add force_local + md_db
old_sig='''def connect_ro(db_path: str, threads: int | None = None, memory_limit: str | None = None,
               statement_timeout_ms: int | None = None):'''
new_sig='''def connect_ro(db_path: str, threads: int | None = None, memory_limit: str | None = None,
               statement_timeout_ms: int | None = None, force_local: bool = False,
               md_db: str | None = None):'''
assert src.count(old_sig)==1, "connect_ro sig"
src=src.replace(old_sig,new_sig,1)

# (c) connect_ro body: backend switch + F3 (skip box-local knobs on MD; close con on any SET failure)
old_body='''    real = os.path.realpath(db_path)
    con = duckdb.connect(real, read_only=True)
    if threads:
        con.execute(f"SET threads={int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
    # DuckDB has no portable statement_timeout setting across versions; the REAL timeout guarantee
    # is the MCP's wall-clock backstop + con.interrupt(). Try the setting opportunistically; ignore
    # if this build doesn't support it (must never break the connection).
    if statement_timeout_ms:
        try:
            con.execute(f"SET statement_timeout='{int(statement_timeout_ms)}ms'")
        except Exception:
            pass
    return con'''
new_body='''    _md = warehouse_backend() == "md" and not force_local
    if _md:
        con = connect_md_ro(md_db)
    else:
        real = os.path.realpath(db_path)
        con = duckdb.connect(real, read_only=True)
    try:
        # threads/memory_limit are box-local knobs; MD manages its own compute -> skip on MD.
        if threads and not _md:
            con.execute(f"SET threads={int(threads)}")
        if memory_limit and not _md:
            con.execute(f"SET memory_limit='{memory_limit}'")
        # statement_timeout is opportunistic on both engines (portability varies); never fatal.
        if statement_timeout_ms:
            try:
                con.execute(f"SET statement_timeout='{int(statement_timeout_ms)}ms'")
            except Exception:
                pass
    except Exception:
        try: con.close()
        finally: raise
    return con'''
assert src.count(old_body)==1, "connect_ro body"
src=src.replace(old_body,new_body,1)

# (d) snapshot_id_of backend-aware
old_sid='''def snapshot_id_of(db_path: str) -> str:
    """The served snapshot's identity = its resolved filename stem (carries the YYYYMMDD_HHMM)."""
    real = os.path.realpath(db_path)
    return os.path.basename(real)'''
new_sid='''def snapshot_id_of(db_path: str) -> str:
    """The served snapshot's identity. MD backend: the active color (md:<color>). Local: the
    resolved snapshot filename stem (carries the YYYYMMDD_HHMM)."""
    if warehouse_backend() == "md":
        return f"md:{md_serving_db()}"
    real = os.path.realpath(db_path)
    return os.path.basename(real)'''
assert src.count(old_sid)==1, "snapshot_id_of"
src=src.replace(old_sid,new_sid,1)
open(cp,"w").write(src)

# ======================= mcp_server.py =======================
mp=os.path.join(STG,"mcp_server.py"); m=open(mp).read()

# (e) assert_read_only: F1 (CREATE alone decides; cleanup never flips) + F2 (scratch DB, never serving color)
old_aro='''def assert_read_only() -> tuple[bool, str]:
    """Open the served snapshot read_only and confirm a write is REJECTED by the engine."""
    cur = PATHS["current_symlink"]'''
new_aro='''def assert_read_only() -> tuple[bool, str]:
    """Confirm a write is REJECTED. MD backend: probe write capability against a SCRATCH database
    (never the serving color) — a read-scoped MOTHERDUCK_TOKEN_RO makes CREATE fail; the CREATE
    result ALONE decides the verdict, cleanup never flips it. Local: open snapshot read_only + probe."""
    if common.warehouse_backend() == "md":
        try:
            con = common.connect_md_ro()
        except Exception as e:
            return False, f"cannot open md serving: {e}"
        probe = "_ro_probe_selftest"
        try: con.execute(f"DROP DATABASE IF EXISTS {probe}")   # best-effort pre-clean; result ignored
        except Exception: pass
        try:
            con.execute(f"CREATE DATABASE {probe}")            # the ONLY verdict-deciding write
        except Exception:
            try: con.close()
            except Exception: pass
            return True, "md write correctly rejected (read-scoped token confirmed)"
        # CREATE succeeded => token CAN write => NOT read-only. Cleanup never changes the verdict.
        try: con.execute(f"DROP DATABASE IF EXISTS {probe}")
        except Exception: pass
        try: con.close()
        except Exception: pass
        return False, "WRITE SUCCEEDED on md — set a READ-SCOPED MOTHERDUCK_TOKEN_RO before flip"
    cur = PATHS["current_symlink"]'''
assert m.count(old_aro)==1, "assert_read_only"
m=m.replace(old_aro,new_aro,1)

# (f) _run_query: F4 (capture color once; label==served color; federation labels LOCAL) + F5
#     (md non-federation does not require the local symlink) + federation force_local routing.
old_rq='''    cur = PATHS["current_symlink"]
    tgt = common.resolve_symlink(cur)
    if not tgt:
        raise RuntimeError("no current snapshot available (serving layer not initialized / rolled back to nothing)")
    snap_id = os.path.basename(tgt)

    if not sql_is_read(sql):
        raise ValueError("only read queries are permitted (SELECT/WITH/PRAGMA/DESCRIBE/EXPLAIN/SHOW). "
                         "The engine is read-only regardless; this is a guard.")

    con = common.connect_ro(tgt, threads=MCFG["per_query_threads"],
                            memory_limit=MCFG["per_query_memory_limit"],
                            statement_timeout_ms=MCFG["statement_timeout_ms"])
    # Lazily federate the lead-mirror only when the query needs it (zero overhead otherwise).
    # A failure here propagates as a normal query error — the user asked for federation, so a
    # silent core-only fallback would be wrong; core/main/derived queries never reach this branch.
    if _needs_federation(sql):
        _attach_federation(con)'''
new_rq='''    if not sql_is_read(sql):
        raise ValueError("only read queries are permitted (SELECT/WITH/PRAGMA/DESCRIBE/EXPLAIN/SHOW). "
                         "The engine is read-only regardless; this is a guard.")

    _fed = _needs_federation(sql)
    _md = common.warehouse_backend() == "md" and not _fed   # non-federation reads serve from MD
    cur = PATHS["current_symlink"]
    tgt = common.resolve_symlink(cur)
    if not tgt and not _md:
        # md non-federation serves from MD and needs no local snapshot; every other path
        # (local backend, or federation which ATTACHes the LOCAL mirror) requires it.
        raise RuntimeError("no current snapshot available (serving layer not initialized / rolled back to nothing)")

    if _md:
        _color = common.md_serving_db()                     # capture ONCE: label + connect agree (no TOCTOU)
        snap_id = f"md:{_color}"
        con = common.connect_ro(tgt, threads=MCFG["per_query_threads"],
                                memory_limit=MCFG["per_query_memory_limit"],
                                statement_timeout_ms=MCFG["statement_timeout_ms"], md_db=_color)
    else:
        snap_id = os.path.basename(tgt)                     # LOCAL snapshot (federation force_local, or local backend)
        con = common.connect_ro(tgt, threads=MCFG["per_query_threads"],
                                memory_limit=MCFG["per_query_memory_limit"],
                                statement_timeout_ms=MCFG["statement_timeout_ms"], force_local=_fed)
        # Lazily federate the lead-mirror only when the query needs it (ATTACHes the LOCAL mirror,
        # which is why federation pins to the local backend even in md-mode).
        if _fed:
            _attach_federation(con)'''
assert m.count(old_rq)==1, "run_query head"
m=m.replace(old_rq,new_rq,1)

# (g) get_schema: label with the backend-aware snapshot id (md:<color> in md-mode) not the local basename
old_gs='    return {"snapshot_id": os.path.basename(tgt), "table_count": len(schema), "tables": schema}'
new_gs='    return {"snapshot_id": common.snapshot_id_of(PATHS["current_symlink"]), "table_count": len(schema), "tables": schema}'
assert m.count(old_gs)==1, "get_schema return"
m=m.replace(old_gs,new_gs,1)
open(mp,"w").write(m)

for f in ("common.py","mcp_server.py"):
    py_compile.compile(os.path.join(STG,f), doraise=True)
print("STAGED + COMPILED OK (F1-F7 addressed) ->", STG)
