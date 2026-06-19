#!/usr/bin/env python3
"""moderator_client.py — thin client for the Schema Moderator Service (BUILD-SPEC-v2 §3.3).

The single authority is the droplet service; this client is just transport + the author-time
UX. Stdlib-only (urllib) so it runs on any editor's laptop without extra deps.

URL/token resolution mirrors the `data-warehouse` skill (no minting, no asking Sam):
  URL   : $MODERATOR_API_URL  -> Renaissance .env (MODERATOR_API_URL)  -> default funnel/moderator
  TOKEN : $MODERATOR_API_TOKEN -> Renaissance .env (MODERATOR_API_TOKEN) -> SSH self-serve an
          editor-scoped token from /opt/duckdb/allowed_tokens.txt (always works with droplet SSH)

Commands:
  review  [--staged | --files A.sql B.py ...]   POST /review  -> checklist (+ exit 1 on block)
  record  --files A.sql ...                      POST /record-pass -> content-hash-bound ledger row
  loop    [--staged | --files ...]               review; if clean -> record; if block -> print the
                                                  fixes for the editor's Claude (the §7.1 loop is
                                                  Claude-driven: fix -> re-review -> record, <=6x)
  rules | issues [--status open] | catalog [--table T --column C] | ledger
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import subprocess
import sys
import urllib.request

DEFAULT_URL = "https://renaissance-droplet.tailae5c80.ts.net/moderator"
RENAISSANCE_ENV = os.environ.get(
    "RENAISSANCE_ENV", "/Users/sam/Documents/Claude Code/Renaissance/.env")
WAREHOUSE_HOST = os.environ.get("WAREHOUSE_HOST", "renaissance-worker")
PY_CONSUMER_DIRS = ("entities", "sources", "scripts")
MAX_LOOP = 6


def _env_file_get(key: str) -> str | None:
    try:
        with open(RENAISSANCE_ENV) as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{key}="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        return None
    return None


def base_url() -> str:
    return (os.environ.get("MODERATOR_API_URL") or _env_file_get("MODERATOR_API_URL")
            or DEFAULT_URL).rstrip("/")


def token() -> str:
    tok = os.environ.get("MODERATOR_API_TOKEN") or _env_file_get("MODERATOR_API_TOKEN")
    if tok:
        return tok
    # self-serve an editor-scoped token over SSH (root-readable; never fails with droplet SSH).
    try:
        out = subprocess.run(
            ["ssh", WAREHOUSE_HOST,
             "awk -F'\\t' '$3==\"editor\"||$3==\"admin\"{print $1; exit}' /opt/duckdb/allowed_tokens.txt"],
            capture_output=True, text=True, timeout=20)
        t = out.stdout.strip()
        if t:
            return t
    except Exception:
        pass
    sys.exit("moderator_client: no MODERATOR_API_TOKEN (env/.env) and SSH self-serve failed. "
             "Set MODERATOR_API_TOKEN or ensure droplet SSH works.")


def _req(method: str, path: str, body: dict | None = None, params: dict | None = None) -> dict:
    url = base_url() + path
    if params:
        from urllib.parse import urlencode
        url += "?" + urlencode({k: v for k, v in params.items() if v is not None})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token()}")
    req.add_header("Content-Type", "application/json")
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=180, context=ctx) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"error": f"HTTP {e.code}", "verdict": "error"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "verdict": "error"}


# ── file selection ────────────────────────────────────────────────────────────────────────────
def _staged() -> list[str]:
    try:
        out = subprocess.run(["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
                             capture_output=True, text=True, check=True).stdout
        return [l.strip() for l in out.splitlines() if l.strip()]
    except Exception:
        return []


def _is_ddl(p: str) -> bool:
    return p.endswith(".sql") and "sql/ddl" in p.replace("\\", "/")


def _is_py(p: str) -> bool:
    parts = p.split("/")
    return p.endswith(".py") and any(d in parts for d in PY_CONSUMER_DIRS)


def _payload(files: list[str]) -> tuple[list[dict], list[dict]]:
    ddl, py = [], []
    for f in files:
        if not os.path.exists(f):
            continue
        content = open(f).read()
        if _is_ddl(f):
            ddl.append({"path": f, "content": content})
        elif _is_py(f):
            py.append({"path": f, "content": content})
    return ddl, py


def _select(args) -> list[str]:
    if args.files:
        return args.files
    return _staged()


# ── pretty print ────────────────────────────────────────────────────────────────────────────────
def _print_checklist(result: dict) -> None:
    print("=" * 74)
    print(f"  SCHEMA MODERATOR — verdict: {result.get('verdict','?').upper()}   "
          f"(floor={result.get('floor_verdict','?')}, llm={result.get('llm_status','?')}, "
          f"rules_v={result.get('rules_version','?')})")
    print("=" * 74)
    findings = result.get("findings", [])
    if not findings:
        print("  [OK] clean — no findings.")
    for f in findings:
        sev = f.get("severity", "?")
        tag = {"Error": "BLOCK", "Warn": "WARN ", "Info": "INFO "}.get(sev, sev)
        print(f"  [{tag}] {f.get('rule','?'):<5} {f.get('detail','')}")
        fix = f.get("fix") or {}
        if fix.get("kind") and fix["kind"] not in ("none",):
            steps = fix.get("steps") or []
            print(f"         fix[{fix['kind']}]: " + (" | ".join(s for s in steps if s) or "see detail"))
    if result.get("llm_reasoning"):
        print(f"  LLM: {result['llm_reasoning'][:400]}")
    print("-" * 74)


# ── commands ──────────────────────────────────────────────────────────────────────────────────
def cmd_review(args) -> int:
    files = _select(args)
    ddl, py = _payload(files)
    if not ddl and not py:
        print("moderator_client: no DDL / entity / sync files staged — nothing to review.")
        return 0
    res = _req("POST", "/review", {"ddl_files": ddl, "py_files": py,
                                   "actor": os.environ.get("USER", "?"), "branch": _branch()})
    if res.get("error"):
        print(f"moderator_client: service error: {res['error']}")
        return 0 if not args.block else 2  # never block the commit on a transport error in Phase 1
    _print_checklist(res)
    return 1 if (res.get("verdict") == "block" and args.block) else 0


def cmd_record(args) -> int:
    ddl, _ = _payload(_select(args))
    if not ddl:
        print("moderator_client: no DDL files to record.")
        return 0
    res = _req("POST", "/record-pass",
               {"ddl_files": ddl, "actor": os.environ.get("USER", "?"), "branch": _branch()})
    if res.get("rejected"):
        print(f"  record-pass REJECTED (verdict={res.get('verdict')}). Fix findings and re-review:")
        _print_checklist(res)
        return 1
    for r in res.get("recorded", []):
        print(f"  recorded v{r['ddl_version']} {r['sql_file']} {r['verdict']} "
              f"sha={r['content_sha256'][:12]} ({'new' if r.get('new') else 'already-present'})")
    return 0


def cmd_loop(args) -> int:
    """The §7.1 auto-fix loop entry point. The client reviews; if BLOCK it prints the prescribed
    fixes and exits 2 — the editor's Claude applies them and re-runs (bounded to ~6 by CLAUDE.md),
    then `record`. If clean, it records straight away. File edits stay with Claude (judgement),
    not a brittle auto-editor."""
    files = _select(args)
    ddl, py = _payload(files)
    if not ddl and not py:
        print("moderator_client loop: nothing staged.")
        return 0
    res = _req("POST", "/review", {"ddl_files": ddl, "py_files": py,
                                   "actor": os.environ.get("USER", "?"), "branch": _branch()})
    if res.get("error"):
        print(f"moderator_client: service error: {res['error']}")
        return 0
    _print_checklist(res)
    if res.get("verdict") == "block":
        print("  -> BLOCK. Apply the fixes above, then re-run `moderator_client.py loop` "
              "(<=6 iterations); escalate to the orchestrator bus if still blocked.")
        return 2
    return cmd_record(args)


def cmd_ci(args) -> int:
    """GitHub Actions gate (BUILD-SPEC-v2 §7.2): review changed DDL/entity files + verify each
    changed DDL has a recorded approval-ledger pass for its head content hash. Emits GitHub
    annotations. FAILS the job (exit 1) only when MODERATOR_CI_ENFORCE=1 (the Sam-gated flip);
    advisory (exit 0) during the held WARN week."""
    enforce = os.environ.get("MODERATOR_CI_ENFORCE", "0") not in ("0", "false", "False", "")
    ddl, py = _payload(args.files or [])
    if not ddl and not py:
        print("moderator-gate: no DDL/entity/sync files changed — nothing to review.")
        return 0
    res = _req("POST", "/review", {"ddl_files": ddl, "py_files": py, "actor": "ci", "branch": _branch()})
    if res.get("error"):
        print(f"::warning::moderator service error: {res['error']} (CI advisory — not failing)")
        return 0
    _print_checklist(res)
    problems = 0
    if res.get("verdict") == "block":
        print("::error::moderator review verdict = BLOCK")
        problems += 1
    for f in ddl:  # ledger-presence: author must have run record-pass for the head content.
        sha = hashlib.sha256(f["content"].encode()).hexdigest()
        led = _req("GET", "/ledger", params={"sha": sha})
        if led.get("ledger"):
            print(f"  ledger OK: {f['path']} sha {sha[:12]} recorded.")
        else:
            print(f"::warning::no approval-ledger pass for {f['path']} (sha {sha[:12]}). "
                  f"Run `python scripts/moderator_client.py loop --files {f['path']}` before merge.")
            problems += 1
    if problems and enforce:
        print(f"::error::moderator-gate FAILING ({problems} problem(s)) [MODERATOR_CI_ENFORCE=1]")
        return 1
    if problems:
        print(f"moderator-gate: {problems} advisory problem(s) — non-blocking during the held WARN "
              f"week (set repo var MODERATOR_CI_ENFORCE=1 to enforce at P8).")
    return 0


def cmd_simple(args, path) -> int:
    params = {}
    if path == "/issues":
        params["status"] = args.status
    if path == "/catalog":
        params["table"] = args.table
        params["column"] = args.column
    res = _req("GET", path, params=params)
    print(json.dumps(res, indent=2)[:8000])
    return 0


def _branch() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                              capture_output=True, text=True).stdout.strip() or "?"
    except Exception:
        return "?"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Schema Moderator Service client")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("review", "loop"):
        sp = sub.add_parser(name)
        sp.add_argument("--staged", action="store_true")
        sp.add_argument("--files", nargs="*")
        sp.add_argument("--block", action="store_true", help="exit 1 on a block verdict")
    rec = sub.add_parser("record")
    rec.add_argument("--files", nargs="*")
    rec.add_argument("--staged", action="store_true")
    ci = sub.add_parser("ci")
    ci.add_argument("--files", nargs="*")
    sub.add_parser("rules")
    iss = sub.add_parser("issues"); iss.add_argument("--status", default="open")
    cat = sub.add_parser("catalog"); cat.add_argument("--table"); cat.add_argument("--column")
    sub.add_parser("ledger")
    args = p.parse_args(argv)
    if args.cmd == "review":
        return cmd_review(args)
    if args.cmd == "loop":
        return cmd_loop(args)
    if args.cmd == "record":
        return cmd_record(args)
    if args.cmd == "ci":
        return cmd_ci(args)
    return cmd_simple(args, "/" + args.cmd)


if __name__ == "__main__":
    sys.exit(main())
