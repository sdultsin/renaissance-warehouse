"""mcp_server.py — SP-2 read-only MCP server (the access layer; Claude Code is the client).

The ONLY way anyone reads the warehouse. Read-only enforced PHYSICALLY (duckdb read_only=True),
asserted at startup + periodically. Per-person bearer tokens (whitelist, instant revocation).
Every request opens the CURRENT snapshot fresh -> auto-reloads after a publish/rollback, and every
result carries snapshot_id so staleness is visible + results are traceable.

Tools:
  query(sql)          -> {columns, rows, row_count, execution_ms, snapshot_id, truncated, note}
  get_schema()        -> live tables/columns from the served snapshot (introspected, never hardcoded)
  get_query_guide()   -> the current warehouse-query-prompt.md (served, never distributed)

Transport: streamable-HTTP (Starlette/uvicorn) behind a bearer-auth middleware + (SP-6) TLS.
Run: python mcp_server.py
"""
from __future__ import annotations
import json, os, sys, time, threading
import concurrent.futures as cf

import duckdb
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route
import uvicorn
from mcp.server.fastmcp import FastMCP

import common
import contextvars as _ctxvars, re as _relog
_CURRENT_USER = _ctxvars.ContextVar("mcp_user", default=None)

CFG = common.load_config()
PATHS = CFG["paths"]
MCFG = CFG["mcp"]

_READ_KEYWORDS = ("SELECT", "WITH", "PRAGMA", "DESCRIBE", "DESC", "EXPLAIN", "SHOW", "TABLE", "VALUES", "FROM", "SUMMARIZE")

# ---- lead-mirror federation (warehouse-unification Piece 1) -------------------
# ATTACH the lead-mirror SERVING snapshot read-only into the per-request warehouse
# connection so a single SQL can walk domain -> ... -> reply -> the full 29.58M lead
# inventory. LAZY: only attached when the query actually references a federation name,
# so the 99% of queries that touch only core/main/derived pay ZERO overhead. Read-only
# both files; no warehouse DDL (a view over an attached DB can't be created in the writer,
# which is why this lives in the serving layer rather than the moderator pipeline).
LEAD_MIRROR_PATH = os.environ.get(
    "LEAD_MIRROR_PATH", "/mnt/volume_nyc1_1781398428838/lead-mirror/lead_mirror_serving.duckdb")
# Baseline enterprise-exclude domain list (Fortune/Global-2000; a hard baseline lead filter).
# Absent file => v_fundable_leads FAILS LOUD rather than silently under-filtering (a partial
# fundable list poisons every downstream pull) — see lead_filters.yaml + the no-catchall rule.
ENTERPRISE_EXCLUDE_PATH = os.environ.get(
    "ENTERPRISE_EXCLUDE_PATH", "/opt/duckdb/enterprise_exclude_domains.txt")
# Federation surface names; if a (comment-stripped) query mentions any, we attach the mirror.
_FED_TOKENS = ("leadmirror", "v_fundable_leads", "v_lead_inventory", "v_meeting_lead_federated")


def _needs_federation(sql: str) -> bool:
    s = _strip_leading_comments(sql).lower()
    return any(tok in s for tok in _FED_TOKENS)


def _attach_federation(con) -> None:
    """ATTACH the lead-mirror read-only + expose v_lead_inventory (full 29.58M inventory) and
    v_fundable_leads (the single baseline eligibility definition: US, size<=100/NULL, ESG-clean,
    valid-only [no catch-all], no-bounce, no-B2C, enterprise-domain-excluded). TEMP objects only;
    nothing is written to either file. Raises on failure so a federation query never returns a
    silently wrong/partial result."""
    p = LEAD_MIRROR_PATH.replace("'", "''")
    con.execute(f"ATTACH '{p}' AS leadmirror (READ_ONLY)")
    ex = ENTERPRISE_EXCLUDE_PATH.replace("'", "''")
    con.execute(
        "CREATE TEMP TABLE _entx AS "
        f"SELECT lower(trim(column0)) AS d FROM read_csv('{ex}', header=false, "
        "columns={'column0':'VARCHAR'})")
    con.execute(
        "CREATE TEMP VIEW v_lead_inventory AS "
        "SELECT * FROM leadmirror.mirror.leads_current_with_email_esp")
    con.execute(
        "CREATE TEMP VIEW v_fundable_leads AS "
        "SELECT m.* FROM leadmirror.mirror.leads_current_with_email_esp m "
        "WHERE m.country = 'United States' "
        "AND (m.company_size <= 100 OR m.company_size IS NULL) "
        "AND m.email_domain_esg_provider IS NULL "
        "AND m.verification_status = 'valid' "
        "AND m.bounce_suppressed = false "
        "AND COALESCE(m.is_b2c, false) = false "
        "AND lower(split_part(m.email, '@', 2)) NOT IN (SELECT d FROM _entx)")
    # Meeting -> sourced-lead cohort lens (warehouse-unification sub-win): exposes the lead's
    # source/industry/size/state/recipient-ESP on each EMAIL-ATTRIBUTABLE meeting (the funding-form
    # sheet-meetings that carry lead_email). The email-less slack-meetings fall out via the WHERE
    # and are deliberately NOT re-attributed here (that is meeting-attribution's harder problem).
    con.execute(
        "CREATE TEMP VIEW v_meeting_lead_federated AS "
        "SELECT m.meeting_id, m.posted_at, m.meeting_date, m.campaign_id, m.campaign_name_raw, "
        "m.cm, m.partner, m.partner_key, m.workspace_canonical, m.workspace_slug, m.offer, "
        "m.channel, m.match_method, m.match_confidence, m.lead_email, "
        "li.source AS lead_source, li.source_list_name AS lead_source_list, "
        "li.general_industry AS lead_general_industry, li.specific_industry AS lead_specific_industry, "
        "li.company_size AS lead_company_size, li.state AS lead_state, li.is_b2c AS lead_is_b2c, "
        "li.email_domain_esp AS lead_recipient_esp, li.company_name AS lead_company_name "
        "FROM core.meeting m "
        "JOIN leadmirror.mirror.leads_current_with_email_esp li "
        "ON lower(m.lead_email) = lower(li.email) "
        "WHERE m.lead_email IS NOT NULL AND m.lead_email <> ''")


# ---- auth whitelist (token<TAB>email per line; '#' comments). Reloaded per request. ----
def load_tokens() -> dict[str, str]:
    p = PATHS["allowed_tokens"]
    out: dict[str, str] = {}
    if not os.path.exists(p):
        return out
    with open(p) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            tok = parts[0]
            email = parts[1] if len(parts) > 1 else "?"
            out[tok] = email
    return out


def token_email(token: str) -> str | None:
    return load_tokens().get(token)


# ---- read-only self-assertion (the real guarantee, verified not assumed) ----
def assert_read_only() -> tuple[bool, str]:
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
    cur = PATHS["current_symlink"]
    tgt = common.resolve_symlink(cur)
    if not tgt:
        return False, "no current snapshot to assert against"
    try:
        con = duckdb.connect(tgt, read_only=True)
    except Exception as e:
        return False, f"cannot open current read_only: {e}"
    try:
        con.execute("CREATE TABLE _ro_selftest_should_fail (x INTEGER)")
        con.close()
        return False, "WRITE SUCCEEDED on a read_only connection — NOT read-only!"
    except Exception:
        con.close()
        return True, "write correctly rejected by engine (read_only confirmed)"


# ---- SQL guard (defense-in-depth; engine read_only is the real guarantee) ----
def _strip_leading_comments(sql: str) -> str:
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            s = "" if nl == -1 else s[nl + 1:].lstrip()
        elif s.startswith("/*"):
            end = s.find("*/")
            s = "" if end == -1 else s[end + 2:].lstrip()
        else:
            return s


def sql_is_read(sql: str) -> bool:
    s = _strip_leading_comments(sql)
    if not s:
        return False
    first = s.split(None, 1)[0].upper().rstrip("(")
    return first in _READ_KEYWORDS


# ---- the server ----
mcp = FastMCP(
    name="renaissance-warehouse",
    instructions="Read-only access to the Renaissance DuckDB warehouse. Call get_query_guide() "
                 "first for the canonical source/grain/query of every metric. Every result carries "
                 "snapshot_id (the served snapshot's timestamp) so staleness is visible.",
    stateless_http=True,
    json_response=True,
    host=MCFG["host"],
    port=MCFG["port"],
)


def _run_query(sql: str) -> dict:
    if not sql_is_read(sql):
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
            _attach_federation(con)
    t0 = time.time()
    timeout_s = MCFG["statement_timeout_ms"] / 1000.0 + 2  # wall-clock backstop (the real timeout guarantee)

    def _exec():
        cur2 = con.execute(sql)
        cols = [d[0] for d in cur2.description] if cur2.description else []
        cap = int(MCFG["max_rows"])
        rows = cur2.fetchmany(cap + 1)
        truncated = len(rows) > cap
        if truncated:
            rows = rows[:cap]
        return cols, rows, truncated

    try:
        with cf.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_exec)
            try:
                cols, rows, truncated = fut.result(timeout=timeout_s)
            except cf.TimeoutError:
                con.interrupt()
                raise TimeoutError(f"query exceeded {timeout_s:.0f}s wall-clock; aborted "
                                   f"(explicit error, not a silent partial result)")
    finally:
        try: con.close()
        except Exception: pass

    ms = round((time.time() - t0) * 1000, 1)
    note = ("RESULT TRUNCATED to max_rows=%d — this is NOT the full result; "
            "add LIMIT/aggregation." % MCFG["max_rows"]) if truncated else ""
    # JSON-safe rows
    safe = [[(v.isoformat() if hasattr(v, "isoformat") else v) for v in r] for r in rows]
    try:
        _tables = sorted(set(_relog.findall(r"\b(?:core|main|derived|dash)\.[A-Za-z_][A-Za-z0-9_]*", sql)))
        common.log_event(os.path.join(PATHS["logs_dir"], "mcp_access.jsonl"), "query",
                         user=_CURRENT_USER.get(), tables=_tables[:25], row_count=len(safe),
                         ms=ms, snapshot_id=snap_id, truncated=truncated, sql=sql[:500])
    except Exception:
        pass
    return {"columns": cols, "rows": safe, "row_count": len(safe),
            "execution_ms": ms, "snapshot_id": snap_id, "truncated": truncated, "note": note}


@mcp.tool()
def query(sql: str) -> dict:
    """Run a read-only SQL query against the current warehouse snapshot.
    Returns columns, rows, row_count, execution_ms, snapshot_id, and a truncated flag.
    Only SELECT/WITH/PRAGMA/DESCRIBE/EXPLAIN/SHOW are accepted; the engine is physically read-only."""
    return _run_query(sql)


@mcp.tool()
def get_schema() -> dict:
    """Live tables + columns of the currently served snapshot (introspected, never hardcoded)."""
    tgt = common.resolve_symlink(PATHS["current_symlink"])
    if not tgt:
        raise RuntimeError("no current snapshot available")
    con = common.connect_ro(tgt)
    try:
        rows = con.execute(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema IN ('main','core','derived') "
            "ORDER BY table_schema, table_name, ordinal_position").fetchall()
    finally:
        con.close()
    schema: dict = {}
    for ts, tn, cn, dt in rows:
        schema.setdefault(f"{ts}.{tn}", []).append({"column": cn, "type": dt})
    return {"snapshot_id": common.snapshot_id_of(PATHS["current_symlink"]), "table_count": len(schema), "tables": schema}


@mcp.tool()
def get_query_guide() -> str:
    """The current canonical warehouse query guide (warehouse-query-prompt.md) — always served, never synced."""
    p = PATHS["query_guide"]
    if not os.path.exists(p):
        return "(query guide not yet deployed at %s)" % p
    with open(p) as f:
        return f.read()


# ---- bearer-auth middleware + unauthenticated /healthz ----
class BearerAuth(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path == "/healthz":
            ok, detail = assert_read_only()
            return JSONResponse({"ok": ok, "read_only": ok, "detail": detail,
                                 "snapshot_id": common.snapshot_id_of(PATHS["current_symlink"])
                                 if common.resolve_symlink(PATHS["current_symlink"]) else None},
                                status_code=200 if ok else 503)
        auth = request.headers.get("authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        email = token_email(token) if token else None
        if not email:
            common.log_event(os.path.join(PATHS["logs_dir"], "mcp_access.jsonl"),
                             "auth_reject", route=request.url.path,
                             reason="missing" if not token else "unknown_or_revoked")
            return JSONResponse({"error": "unauthorized: valid bearer token required"}, status_code=401)
        request.state.user_email = email
        _CURRENT_USER.set(email or "unknown")
        return await call_next(request)


# ---- SP-6 thin REST surface (the primary, lowest-friction read path: any HTTP client) ----
# Same auth (BearerAuth middleware, below), same read_only guarantees, same snapshot_id as MCP.
# /query (POST {"sql":...}) -> JSON ; /schema (GET) -> JSON ; /guide (GET) -> text.
async def rest_query(request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "body must be JSON: {\"sql\": \"<read-only SQL>\"}"}, status_code=400)
    sql = (body or {}).get("sql")
    if not sql or not isinstance(sql, str):
        return JSONResponse({"error": "missing 'sql' (string) in JSON body"}, status_code=400)
    try:
        return JSONResponse(_run_query(sql))
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except TimeoutError as e:
        return JSONResponse({"error": str(e)}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=400)


async def rest_schema(request):
    try:
        return JSONResponse(get_schema())
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=500)


async def rest_guide(request):
    return PlainTextResponse(get_query_guide())


def build_app():
    app = mcp.streamable_http_app()
    app.add_middleware(BearerAuth)
    # additive REST routes (auth + read-only inherited; MCP at /mcp is unchanged)
    app.router.routes.append(Route("/query", rest_query, methods=["POST"]))
    app.router.routes.append(Route("/schema", rest_schema, methods=["GET"]))
    app.router.routes.append(Route("/guide", rest_guide, methods=["GET"]))
    return app


def main():
    ok, detail = assert_read_only()
    common.log_event(os.path.join(PATHS["logs_dir"], "mcp_server.jsonl"),
                     "startup", read_only_ok=ok, detail=detail,
                     profile=CFG["profile"], port=MCFG["port"])
    if not ok and common.resolve_symlink(PATHS["current_symlink"]):
        # fail-closed: a current snapshot exists but isn't read-only -> refuse to serve
        common.log_event(os.path.join(PATHS["logs_dir"], "mcp_server.jsonl"),
                         "fail_closed", detail=detail)
        print(f"FAIL-CLOSED: read-only assertion failed: {detail}", file=sys.stderr)
        sys.exit(3)
    uvicorn.run(build_app(), host=MCFG["host"], port=MCFG["port"], log_level="warning")


if __name__ == "__main__":
    main()
