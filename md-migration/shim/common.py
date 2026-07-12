"""common.py — shared helpers for the serving-mcp layer.

Config loading (profile-aware), structured JSONL audit logging, atomic symlink resolution,
the validated-snapshot registry, disk-free checks, and a read-only DuckDB connect helper.
Imported by gate.py, publisher.py, mcp_server.py, surveillance.py, selfheal.py, watcher_checker.py.
"""
from __future__ import annotations
import json, os, sys, time, shutil, hashlib, fcntl, subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml
import duckdb

# ---- config -----------------------------------------------------------------
_CFG_CACHE = None

def _cfg_path() -> str:
    return os.environ.get("SERVING_CONFIG", "/opt/duckdb/bin/config.yaml")

def profile_name() -> str:
    return os.environ.get("SERVING_PROFILE", "prod")

def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def load_config() -> dict:
    """Load config.yaml; deep-merge the active profile's `overrides` over the prod defaults,
    and splice that profile's gate spec under cfg['gate']."""
    global _CFG_CACHE
    if _CFG_CACHE is not None:
        return _CFG_CACHE
    with open(_cfg_path()) as f:
        raw = yaml.safe_load(f)
    prof = profile_name()
    if prof not in raw.get("profiles", {}):
        raise SystemExit(f"unknown SERVING_PROFILE={prof}; have {list(raw.get('profiles',{}))}")
    pblock = raw["profiles"][prof]
    # operational config = top-level defaults, deep-merged with this profile's overrides
    base = {k: v for k, v in raw.items() if k != "profiles"}
    cfg = _deep_merge(base, pblock.get("overrides", {}))
    # gate spec = the profile's remaining keys (everything except `overrides`)
    cfg["gate"] = {k: v for k, v in pblock.items() if k != "overrides"}
    cfg["profile"] = prof
    _CFG_CACHE = cfg
    return cfg

# ---- time -------------------------------------------------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def utcstamp() -> str:
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def snap_stamp() -> str:
    # millisecond precision so two promotes in the same second never collide / clobber a served file
    return utcnow().strftime("%Y%m%d_%H%M%S_") + f"{utcnow().microsecond // 1000:03d}"

# ---- structured JSONL logging (internal audit only — NEVER notifies Sam) ----
def log_event(path: str, event: str, **fields) -> dict:
    rec = {"ts": utcstamp(), "event": event, **fields}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(rec, default=str)
    # append-only, line-atomic
    with open(path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(line + "\n")
        f.flush(); os.fsync(f.fileno())
        fcntl.flock(f, fcntl.LOCK_UN)
    return rec

# ---- loud operator alert (#cc-sam) — the ONE primitive that DOES notify Sam --
# The JSONL log above is silent audit. THIS is the deliberate exception: a loud Slack ping to
# #cc-sam, tagging Sam, for things the operator must see (a publish that promoted WITH warnings, or a
# real mechanical failure). Sam's steer for this single-contributor warehouse is "publish + loud
# alert" over "silently block + retry in the dark" — this is the alert half. Reuses the canonical
# warehouse helper (renaissance-warehouse/scripts/alert_slack.py: SLACK_TOKEN + SLACK_ALERT_CHANNEL
# from that repo's .env). Best-effort: NEVER raises (a Slack hiccup must not block/crash a publish)
# and stays SILENT in the `test` profile so fixture runs never ping Sam.
SAM_SLACK_ID = "U0AM2CQHW9E"
CC_SAM_CHANNEL = "C0AR0EA21C1"
_ALERT_HELPER = "/root/renaissance-warehouse/scripts/alert_slack.py"

def alert_sam(text: str, cfg: dict | None = None, tag: bool = True) -> bool:
    """Post a loud alert to #cc-sam (tagging Sam). Returns True iff Slack accepted it. Never raises;
    no-ops (prints) in the test profile."""
    msg = (f"<@{SAM_SLACK_ID}> " if tag else "") + text
    prof = (cfg or {}).get("profile") if cfg else os.environ.get("SERVING_PROFILE", "prod")
    if prof == "test":
        print(f"[alert_sam:test-noop] {msg}", flush=True)
        return False
    try:
        if not os.path.exists(_ALERT_HELPER):
            print(f"[alert_sam] helper missing at {_ALERT_HELPER}; not sent: {msg}", flush=True)
            return False
        env = dict(os.environ)
        env.setdefault("SLACK_ALERT_CHANNEL", CC_SAM_CHANNEL)
        r = subprocess.run([sys.executable, _ALERT_HELPER, msg], timeout=25, env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if r.returncode != 0:
            print(f"[alert_sam] post failed rc={r.returncode}: "
                  f"{r.stdout.decode('utf-8', 'replace')[:300]}", flush=True)
            return False
        return True
    except Exception as e:  # noqa: BLE001 — alerting must never break the caller
        print(f"[alert_sam] exception: {type(e).__name__}: {e}", flush=True)
        return False


# ---- symlink / filesystem ---------------------------------------------------
def resolve_symlink(link: str) -> str | None:
    """Resolve a symlink to its absolute target, or None if missing/dangling."""
    try:
        if not os.path.islink(link):
            # tolerate a plain file standing in for the link (defensive)
            return os.path.realpath(link) if os.path.exists(link) else None
        tgt = os.path.realpath(link)
        return tgt if os.path.exists(tgt) else None
    except OSError:
        return None

def atomic_symlink(link: str, target: str) -> None:
    """Atomically (re)point `link` -> `target` via a temp symlink + os.replace rename."""
    link = os.path.abspath(link); target = os.path.abspath(target)
    tmp = f"{link}.tmp.{os.getpid()}.{int(time.time()*1000)}"
    if os.path.islink(tmp) or os.path.exists(tmp):
        os.remove(tmp)
    os.symlink(target, tmp)
    os.replace(tmp, link)   # atomic rename over the existing link

def free_gb(path: str) -> float:
    st = shutil.disk_usage(path)
    return st.free / (1024 ** 3)

def file_size_bytes(path: str) -> int:
    try:
        return os.path.getsize(os.path.realpath(path))
    except OSError:
        return -1

def file_mtime(path: str) -> float | None:
    try:
        return os.path.getmtime(os.path.realpath(path))
    except OSError:
        return None

# ---- validated-snapshot registry (append-only JSONL) ------------------------
def registry_append(cfg: dict, rec: dict) -> None:
    log_event(cfg["paths"]["registry"], "register", **rec)

def registry_entries(cfg: dict) -> list[dict]:
    p = cfg["paths"]["registry"]
    if not os.path.exists(p):
        return []
    out = []
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line:
                try: out.append(json.loads(line))
                except json.JSONDecodeError: pass
    return out

def is_validated(cfg: dict, snapshot_path: str) -> bool:
    """True if this snapshot path passed the gate at some point (in the registry, passed=True)."""
    real = os.path.realpath(snapshot_path)
    for e in registry_entries(cfg):
        if e.get("passed") and os.path.realpath(e.get("snapshot", "")) == real:
            return True
    return False

# ---- DuckDB read-only connect ----------------------------------------------

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


def connect_ro(db_path: str, threads: int | None = None, memory_limit: str | None = None,
               statement_timeout_ms: int | None = None, force_local: bool = False,
               md_db: str | None = None):
    """Open a DuckDB connection READ-ONLY (the physical read-only guarantee).
    Per-connection threads/memory/timeout so one query can't starve the box."""
    _md = warehouse_backend() == "md" and not force_local
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
    return con

def snapshot_id_of(db_path: str) -> str:
    """The served snapshot's identity. MD backend: the active color (md:<color>). Local: the
    resolved snapshot filename stem (carries the YYYYMMDD_HHMM)."""
    if warehouse_backend() == "md":
        return f"md:{md_serving_db()}"
    real = os.path.realpath(db_path)
    return os.path.basename(real)

def quick_integrity_ok(db_path: str) -> tuple[bool, str]:
    """Cheap structural check: file opens read-only and a trivial query runs (catches half/corrupt files)."""
    try:
        con = duckdb.connect(os.path.realpath(db_path), read_only=True)
        con.execute("SELECT 1").fetchone()
        con.close()
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
