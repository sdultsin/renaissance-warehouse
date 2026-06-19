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

import os
import sys

import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import moderator_common as mc  # noqa: E402


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


def _stub(name: str, need: str):
    async def handler(request):
        deny = require_scope(request, need)
        if deny is not None:
            return deny
        return JSONResponse(
            {"error": "not_implemented",
             "endpoint": name,
             "detail": f"{name} is implemented in P2 (engine wiring). Skeleton is live (P1).",
             "required_scope": need},
            status_code=501)
    return handler


# ── (3) the real /healthz (unauthenticated) ─────────────────────────────────────────────────────
async def healthz(request):
    out = {
        "ok": True,
        "service": "schema-moderator",
        "phase": "P1-skeleton",
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


# ── routes ──────────────────────────────────────────────────────────────────────────────────────
ROUTES = [
    Route("/healthz", healthz, methods=["GET"]),
    Route("/review", _stub("POST /review", "editor"), methods=["POST"]),
    Route("/record-pass", _stub("POST /record-pass", "editor"), methods=["POST"]),
    Route("/rules", _stub("GET /rules", "reader"), methods=["GET"]),
    Route("/rules", _stub("POST /rules", "admin"), methods=["POST"]),
    Route("/catalog", _stub("GET /catalog", "reader"), methods=["GET"]),
    Route("/ledger", _stub("GET /ledger", "reader"), methods=["GET"]),
    Route("/issues", _stub("GET /issues", "reader"), methods=["GET"]),
    Route("/judge-advisory", _stub("POST /judge-advisory", "editor"), methods=["POST"]),
]


def build_app() -> Starlette:
    app = Starlette(routes=ROUTES, middleware=[Middleware(BearerAuth)])
    return PathPrefixStrip(app)  # outermost: normalise the funnel prefix before auth/routing


def main():
    mc.log_event("startup", port=mc.PORT, host=mc.HOST, gate_version=mc.GATE_VERSION,
                 server_llm=mc.SERVER_LLM_ON, phase="P1-skeleton")
    uvicorn.run(build_app(), host=mc.HOST, port=mc.PORT, log_level="warning")


if __name__ == "__main__":
    main()
