"""moderator_common.py — shared config / auth / store helpers for the Schema Moderator Service.

The service (moderator_server.py) is the always-on droplet authority for warehouse schema
review (BUILD-SPEC-v2). This module holds everything that is NOT a route handler:

  * config resolution (env-first, with safe defaults)
  * the scoped bearer-token whitelist loader (token<TAB>email[<TAB>scope]; reloaded per request)
  * a short-lived Postgres connection to the `moderator` schema (pipeline project, Supavisor 6543)
  * a thin read-API client (warehouse catalog lives in DuckDB, read over the existing /query route)
  * jsonl logging

Deterministic core only. No LLM here — the deep-review layer lives in moderator_server.py so the
key is loaded in exactly one place. Importing this module has no side effects beyond reading env.
"""
from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager

# ── config (env-first; defaults match the droplet layout in BUILD-SPEC-v2 §3.1) ──────────────
HOST            = os.environ.get("MODERATOR_HOST", "127.0.0.1")          # funnel proxies to localhost
PORT            = int(os.environ.get("MODERATOR_PORT", "8901"))
ALLOWED_TOKENS  = os.environ.get("MODERATOR_ALLOWED_TOKENS", "/opt/duckdb/allowed_tokens.txt")
ENGINE_DIR      = os.environ.get("MODERATOR_ENGINE_DIR", "/opt/moderator/engine")
WAREHOUSE_ROOT  = os.environ.get("WAREHOUSE_REPO_ROOT", "/root/renaissance-warehouse")
PG_DSN          = os.environ.get("MODERATOR_PG_DSN") or os.environ.get("PIPELINE_SUPABASE_DB_URL", "")
READ_API_URL    = os.environ.get("WAREHOUSE_API_URL", "https://renaissance-droplet.tailae5c80.ts.net").rstrip("/")
READ_API_TOKEN  = os.environ.get("WAREHOUSE_API_TOKEN", "")
# Catalog/lineage lives in DuckDB. The service is CO-LOCATED on the droplet, so it reads the
# served snapshot read-only directly (robust + fast; no MCP transport). Falls back to the
# read-API only if this path is unavailable.
DUCKDB_CURRENT  = os.environ.get("MODERATOR_DUCKDB_CURRENT", "/opt/duckdb/warehouse_current.duckdb")
LLM_FAIL_CLOSED = os.environ.get("MODERATOR_LLM_FAIL_CLOSED", "0") not in ("0", "false", "False", "")
LLM_TIMEOUT_S   = float(os.environ.get("MODERATOR_LLM_TIMEOUT_S", "90"))
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_KEY", "")
SERVER_LLM_ON   = os.environ.get("SCHEMA_GATE_SERVER_LLM", "1") not in ("0", "false", "False", "")
LLM_MODEL       = os.environ.get("MODERATOR_LLM_MODEL", "claude-opus-4-8")  # strongest reasoning model
LOG_PATH        = os.environ.get("MODERATOR_LOG", "/opt/moderator/logs/moderator.jsonl")
FUNNEL_PREFIX   = os.environ.get("MODERATOR_FUNNEL_PREFIX", "/moderator")

SCOPES = ("reader", "editor", "admin")
# scope -> the scopes it satisfies (admin ⊇ editor ⊇ reader).
_SCOPE_RANK = {"reader": 0, "editor": 1, "admin": 2}


# ── engine import (vendored copy of phase-1 core/schema_gate_lib.py, scp'd by deploy.sh) ──────
# The service is self-contained: deploy.sh copies the CANONICAL core/schema_gate_lib.py from the
# repo into ENGINE_DIR each deploy, so there is no drift and no coupling to the droplet's nightly
# warehouse checkout / the main-merge timing. Import is guarded so the skeleton still boots if the
# engine file is briefly absent (healthz then reports gate_version="engine-not-loaded").
def load_engine():
    if ENGINE_DIR not in sys.path:
        sys.path.insert(0, ENGINE_DIR)
    if WAREHOUSE_ROOT and os.path.join(WAREHOUSE_ROOT, "core") not in sys.path:
        sys.path.insert(0, os.path.join(WAREHOUSE_ROOT, "core"))  # fallback source
    import schema_gate_lib  # noqa: F401
    return schema_gate_lib


try:
    _ENGINE = load_engine()
    GATE_VERSION = getattr(_ENGINE, "SCHEMA_GATE_VERSION", "unknown")
except Exception as _e:  # pragma: no cover - degraded skeleton boot
    _ENGINE = None
    GATE_VERSION = "engine-not-loaded"


def engine():
    """Return the schema_gate_lib module, importing on first use if it wasn't at boot."""
    global _ENGINE, GATE_VERSION
    if _ENGINE is None:
        _ENGINE = load_engine()
        GATE_VERSION = getattr(_ENGINE, "SCHEMA_GATE_VERSION", "unknown")
    return _ENGINE


# ── logging ───────────────────────────────────────────────────────────────────────────────────
def log_event(event: str, **fields) -> None:
    rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "event": event, **fields}
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass  # never let logging take down a request


# ── scoped bearer-token whitelist (token<TAB>email[<TAB>scope]; reloaded per request) ───────────
def load_tokens() -> dict[str, dict]:
    """{token: {"email": str, "scope": str}}. A line with no 3rd column defaults to scope='reader'
    (so the existing read-API tokens grant catalog/ledger READ on the moderator, nothing more)."""
    out: dict[str, dict] = {}
    if not os.path.exists(ALLOWED_TOKENS):
        return out
    with open(ALLOWED_TOKENS) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            tok = parts[0]
            email = parts[1] if len(parts) > 1 else "?"
            scope = parts[2].lower() if len(parts) > 2 else "reader"
            if scope not in SCOPES:
                scope = "reader"
            out[tok] = {"email": email, "scope": scope}
    return out


def identify(token: str) -> dict | None:
    return load_tokens().get(token) if token else None


def scope_satisfies(have: str, need: str) -> bool:
    return _SCOPE_RANK.get(have, -1) >= _SCOPE_RANK.get(need, 99)


# ── Postgres (moderator schema; pipeline project; Supavisor 6543) ───────────────────────────────
@contextmanager
def pg_conn():
    """Short-lived autocommit connection. psycopg3. Caller uses `with pg_conn() as c:`."""
    import psycopg
    if not PG_DSN:
        raise RuntimeError("MODERATOR_PG_DSN / PIPELINE_SUPABASE_DB_URL is not set")
    # prepare_threshold=None disables psycopg3 auto-prepared-statements, which break on the
    # Supavisor TRANSACTION pooler (6543) where successive statements may hit different backends.
    conn = psycopg.connect(PG_DSN, autocommit=True, connect_timeout=10, prepare_threshold=None)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def pg_one(sql: str, params: tuple = ()):  # convenience: first column of first row (or None)
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


# ── DuckDB serving-snapshot reader (catalog/lineage; read-only, co-located on the droplet) ──────
@contextmanager
def duckdb_ro():
    """Open the CURRENT served warehouse snapshot read-only. Raises if unavailable; callers that
    must degrade gracefully (catalog not yet built pre-P7) wrap in try/except."""
    import duckdb
    if not os.path.exists(DUCKDB_CURRENT):
        raise FileNotFoundError(f"serving snapshot not found at {DUCKDB_CURRENT}")
    real = os.path.realpath(DUCKDB_CURRENT)
    con = duckdb.connect(real, read_only=True)
    try:
        yield con
    finally:
        try:
            con.close()
        except Exception:
            pass


# ── read-API client (fallback transport if the local snapshot is unavailable) ───────────────────
def read_api_snapshot_id(timeout: float = 6.0) -> str | None:
    """The served snapshot id from the read-API's UNAUTHENTICATED /healthz. None on any failure."""
    import httpx
    r = httpx.get(f"{READ_API_URL}/healthz", timeout=timeout)
    if r.status_code == 200:
        return (r.json() or {}).get("snapshot_id")
    return None


def read_api_query(sql: str, timeout: float = 30.0) -> dict:
    """Run a read-only SQL query against the warehouse via the read-API MCP /query route.
    Requires a reader token (WAREHOUSE_API_TOKEN). Returns {columns, rows, ...}."""
    import httpx
    if not READ_API_TOKEN:
        raise RuntimeError("WAREHOUSE_API_TOKEN not set — cannot query the warehouse catalog")
    headers = {"Authorization": f"Bearer {READ_API_TOKEN}", "Content-Type": "application/json"}
    # The read-API is an MCP streamable-HTTP server; the query tool is invoked via the MCP
    # JSON-RPC envelope. moderator_server wires the exact call; this helper is the transport.
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": "query", "arguments": {"sql": sql}}}
    r = httpx.post(f"{READ_API_URL}/mcp", headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()
