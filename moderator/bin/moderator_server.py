"""moderator_server.py — the always-on Schema Moderator Service (BUILD-SPEC-v2 §3).

One droplet service (Starlette + uvicorn, :8901, behind the existing Tailscale Funnel at the
`/moderator/*` path prefix) that owns the deterministic schema-review engine and the append-only
approval ledger. It is the apply-time authority: a DDL only ever lands if a content-hash-bound
`moderator.approval_ledger` row says it passed.

Two-layer gate (both required, BUILD-SPEC-v2 §5):
  1. deterministic FLOOR (free, certain) — phase-1 schema_gate_lib, in-process, never depends on a model.
  2. server-side LLM DEEP-REVIEW (paid, fail-CLOSED) — a strong reasoning model judges semantic/intent/
     foreseeable-downstream breakage on every change that clears the floor; it CAN block.

P1 = this skeleton: BearerAuth (+scope), funnel path-normalisation, a real /healthz, and route
stubs for the P2 engine endpoints. P2 fills the handlers in (and adds the LLM layer in this process,
the one place the key is loaded).

Run: python moderator_server.py
"""
from __future__ import annotations

import json
import os
import sys
import uuid

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import moderator_common as mc  # noqa: E402
import moderator_engine as eng  # noqa: E402


# ── (1) outermost ASGI middleware: strip an optional /moderator funnel prefix ───────────────────
# Tailscale `serve --set-path /moderator` may or may not strip the mount path depending on version/
# config. We normalise here so the service is correct either way: internal routes are defined
# WITHOUT the prefix, and an inbound /moderator/healthz becomes /healthz before routing.
class PathPrefixStrip:
    def __init__(self, app, prefix: str = mc.FUNNEL_PREFIX):
        self.app = app
        self.prefix = prefix.rstrip("/")

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and self.prefix:
            path = scope.get("path", "")
            if path == self.prefix or path.startswith(self.prefix + "/"):
                new_path = path[len(self.prefix):] or "/"
                scope = dict(scope)
                scope["path"] = new_path
                if scope.get("raw_path"):
                    # raw_path excludes the query string; re-encode the stripped path.
                    scope["raw_path"] = new_path.encode("ascii", "ignore")
        await self.app(scope, receive, send)


# ── (2) bearer auth (+scope), copied from serving-mcp mcp_server.BearerAuth + a scope column ────
PUBLIC_PATHS = {"/healthz", "/"}


class BearerAuth(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)
        auth = request.headers.get("authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        ident = mc.identify(token)
        if not ident:
            mc.log_event("auth_reject", route=request.url.path,
                         reason="missing" if not token else "unknown_or_revoked")
            return JSONResponse({"error": "unauthorized: valid bearer token required"}, status_code=401)
        request.state.email = ident["email"]
        request.state.scope = ident["scope"]
        return await call_next(request)


def require_scope(request, need: str):
    """Return a 403 JSONResponse if the caller's scope is insufficient, else None."""
    have = getattr(request.state, "scope", "reader")
    if not mc.scope_satisfies(have, need):
        return JSONResponse(
            {"error": f"forbidden: '{need}' scope required (you have '{have}')"}, status_code=403)
    return None


# ── (3) the real /healthz (unauthenticated) ─────────────────────────────────────────────────────
async def healthz(request):
    out = {
        "ok": True,
        "service": "schema-moderator",
        "phase": "live",
        "gate_version": mc.GATE_VERSION,
        "server_llm": mc.SERVER_LLM_ON,
        "llm_model": mc.LLM_MODEL if mc.SERVER_LLM_ON else None,
        "rules_version": None,
        "ledger_count": None,
        "catalog_snapshot_id": None,
        "pg": "unknown",
        "detail": "",
    }
    # Postgres store (rules + ledger) — the load-bearing dependency; ok=False if it's down.
    try:
        with mc.pg_conn() as c, c.cursor() as cur:
            cur.execute("SELECT max(rules_version) FROM moderator.rules_version")
            out["rules_version"] = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM moderator.approval_ledger")
            out["ledger_count"] = cur.fetchone()[0]
        out["pg"] = "ok"
    except Exception as e:
        out["ok"] = False
        out["pg"] = "error"
        out["detail"] = f"pg: {type(e).__name__}: {e}"
    # warehouse serving snapshot id — best-effort, non-fatal (catalog read is wired in P2).
    try:
        out["catalog_snapshot_id"] = mc.read_api_snapshot_id()
    except Exception:
        out["catalog_snapshot_id"] = None
    return JSONResponse(out, status_code=200 if out["ok"] else 503)


async def _json_body(request) -> dict:
    try:
        return await request.json()
    except Exception:
        return {}


def _actor(request, body) -> str:
    return body.get("actor") or getattr(request.state, "email", "?")


# POST /review — pure function of (submitted content + current rules + current catalog). Writes
# findings to moderator.issue; does NOT record a pass.
async def review(request):
    deny = require_scope(request, "editor")
    if deny is not None:
        return deny
    body = await _json_body(request)
    ddl_files = body.get("ddl_files") or []
    py_files = body.get("py_files") or []
    if not ddl_files and not py_files:
        return JSONResponse({"error": "no ddl_files or py_files in request"}, status_code=400)
    request_id = str(uuid.uuid4())
    actor = _actor(request, body)
    try:
        result = eng.review(ddl_files, py_files)
        eng.write_issues(result["findings"], request_id, actor, body.get("branch"),
                         result["rules_version"])
        mc.log_event("review", request_id=request_id, actor=actor, verdict=result["verdict"],
                     floor=result["floor_verdict"], llm=result["llm_status"],
                     findings=len(result["findings"]))
        return JSONResponse({"request_id": request_id, **result})
    except Exception as e:
        mc.log_event("review_error", request_id=request_id, error=f"{type(e).__name__}: {e}")
        return JSONResponse({"error": f"{type(e).__name__}: {e}", "request_id": request_id},
                            status_code=500)


# POST /record-pass — re-gate server-side against LIVE rules+catalog (never trusts the client),
# write the content-hash-bound approval_ledger row IFF verdict != block.
async def record_pass(request):
    deny = require_scope(request, "editor")
    if deny is not None:
        return deny
    body = await _json_body(request)
    ddl_files = body.get("ddl_files") or []
    if not ddl_files:
        return JSONResponse({"error": "record-pass requires ddl_files:[{path,content}]"}, status_code=400)
    request_id = body.get("request_id") or str(uuid.uuid4())
    actor = _actor(request, body)
    try:
        out = eng.record_pass(ddl_files, actor, body.get("branch"), request_id)
        mc.log_event("record_pass", request_id=request_id, actor=actor, verdict=out["verdict"],
                     recorded=len(out["recorded"]), rejected=out["rejected"])
        return JSONResponse({"request_id": request_id, **out},
                            status_code=409 if out["rejected"] else 200)
    except Exception as e:
        mc.log_event("record_pass_error", request_id=request_id, error=f"{type(e).__name__}: {e}")
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=500)


# GET /rules?version= — the active (or given) rule set as data + tier map + aliases.
async def get_rules(request):
    deny = require_scope(request, "reader")
    if deny is not None:
        return deny
    v = request.query_params.get("version")
    rv, rules = eng.load_rules(int(v) if v else None)
    return JSONResponse({"rules_version": rv, "rules": rules, "aliases": eng.load_aliases(),
                         "gate_version": mc.GATE_VERSION})


# GET /catalog?table=&column= — served projection of the DuckDB catalog/consumers + alias authority.
async def get_catalog(request):
    deny = require_scope(request, "reader")
    if deny is not None:
        return deny
    table = request.query_params.get("table")
    column = request.query_params.get("column")
    out = {"snapshot_id": None, "catalog": [], "consumers": [], "aliases": eng.load_aliases(),
           "catalog_built": False}
    try:
        with mc.duckdb_ro() as con:
            out["snapshot_id"] = os.path.basename(os.path.realpath(mc.DUCKDB_CURRENT))
            try:
                q = ("SELECT table_schema, table_name, column_name, data_type, canonical_name, status "
                     "FROM core.schema_catalog WHERE 1=1")
                p = []
                if table:
                    q += " AND lower(table_name)=lower(?)"; p.append(table)
                if column:
                    q += " AND lower(column_name)=lower(?)"; p.append(column)
                q += " ORDER BY table_schema, table_name, ordinal_position LIMIT 2000"
                out["catalog"] = [dict(zip(["table_schema", "table_name", "column_name", "data_type",
                                            "canonical_name", "status"], r)) for r in con.execute(q, p).fetchall()]
                out["catalog_built"] = True
                if column:
                    out["consumers"] = eng.resolve_consumers(con, table, column)
            except Exception as e:
                out["detail"] = f"catalog not built yet: {e}"
    except Exception as e:
        out["detail"] = f"snapshot unavailable: {e}"
    return JSONResponse(out)


# GET /ledger?since=&actor=&version= — append-only approval ledger (audit).
async def get_ledger(request):
    deny = require_scope(request, "reader")
    if deny is not None:
        return deny
    q = ("SELECT ledger_id, ddl_version, sql_file, content_sha256, verdict, rules_version, "
         "gate_version, actor, branch, CAST(request_id AS text), CAST(recorded_at AS text) "
         "FROM moderator.approval_ledger WHERE 1=1")
    params = []
    if request.query_params.get("since"):
        q += " AND recorded_at >= %s"; params.append(request.query_params["since"])
    if request.query_params.get("actor"):
        q += " AND actor = %s"; params.append(request.query_params["actor"])
    if request.query_params.get("version"):
        q += " AND rules_version = %s"; params.append(int(request.query_params["version"]))
    q += " ORDER BY recorded_at DESC LIMIT 500"
    cols = ["ledger_id", "ddl_version", "sql_file", "content_sha256", "verdict", "rules_version",
            "gate_version", "actor", "branch", "request_id", "recorded_at"]
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute(q, params)
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return JSONResponse({"count": len(rows), "ledger": rows})


# GET /issues?status=open — the live issue ledger.
async def get_issues(request):
    deny = require_scope(request, "reader")
    if deny is not None:
        return deny
    status = request.query_params.get("status", "open")
    cols = ["issue_id", "rule", "severity", "classification", "table_name", "column_name",
            "ddl_file", "detail", "status", "rules_version", "actor", "created_at"]
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT issue_id, rule, severity, classification, table_name, column_name, ddl_file, "
            "detail, status, rules_version, actor, CAST(created_at AS text) FROM moderator.issue "
            "WHERE (%s='all' OR status=%s) ORDER BY issue_id DESC LIMIT 500", [status, status])
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return JSONResponse({"count": len(rows), "issues": rows})


# POST /rules (admin) — append a new versioned rule set (snapshot copy-forward + edits) atomically.
# body: {note, upsert_rules:[{code,kind,tier,spec,detail_template}], disable_codes:[...],
#        aliases_add:[{alias,canonical_name,scope?,reason?}], source?}
async def post_rules(request):
    deny = require_scope(request, "admin")
    if deny is not None:
        return deny
    body = await _json_body(request)
    actor = _actor(request, body)
    try:
        new_version = eng.publish_rules(
            upsert_rules=body.get("upsert_rules") or [],
            disable_codes=body.get("disable_codes") or [],
            aliases_add=body.get("aliases_add") or [],
            note=body.get("note", ""), published_by=actor,
            source=body.get("source", "human"))
        mc.log_event("rules_published", rules_version=new_version, actor=actor)
        rv, rules = eng.load_rules(new_version)
        return JSONResponse({"rules_version": new_version, "rules": rules})
    except Exception as e:
        return JSONResponse({"error": f"{type(e).__name__}: {e}"}, status_code=500)


# POST /judge-advisory — accept a CLIENT-computed judge verdict; append as WARN/Info issues only.
# The server runs no LLM here; this can ONLY add warnings, never flip a deterministic block to pass.
async def judge_advisory(request):
    deny = require_scope(request, "editor")
    if deny is not None:
        return deny
    body = await _json_body(request)
    request_id = body.get("request_id")
    jf = body.get("judge_findings") or []
    findings = [{"rule": "JUDGE", "severity": ("Warn" if x.get("severity") != "info" else "Info"),
                 "classification": "CLIENT-JUDGE", "table_name": x.get("table"),
                 "column_name": x.get("column"), "detail": x.get("detail", ""), "consumers": []}
                for x in jf]
    rv = eng.active_rules_version()
    n = eng.write_issues(findings, request_id, _actor(request, body), body.get("branch"), rv)
    return JSONResponse({"recorded_advisory_issues": n, "request_id": request_id,
                         "note": "advisory only — never flips a deterministic verdict"})


# ── routes ──────────────────────────────────────────────────────────────────────────────────────
ROUTES = [
    Route("/healthz", healthz, methods=["GET"]),
    Route("/review", review, methods=["POST"]),
    Route("/record-pass", record_pass, methods=["POST"]),
    Route("/rules", get_rules, methods=["GET"]),
    Route("/rules", post_rules, methods=["POST"]),
    Route("/catalog", get_catalog, methods=["GET"]),
    Route("/ledger", get_ledger, methods=["GET"]),
    Route("/issues", get_issues, methods=["GET"]),
    Route("/judge-advisory", judge_advisory, methods=["POST"]),
]


def build_app() -> Starlette:
    app = Starlette(routes=ROUTES, middleware=[Middleware(BearerAuth)])
    return PathPrefixStrip(app)  # outermost: normalise the funnel prefix before auth/routing


def main():
    mc.log_event("startup", port=mc.PORT, host=mc.HOST, gate_version=mc.GATE_VERSION,
                 server_llm=mc.SERVER_LLM_ON, phase="live")
    uvicorn.run(build_app(), host=mc.HOST, port=mc.PORT, log_level="warning")


if __name__ == "__main__":
    main()
