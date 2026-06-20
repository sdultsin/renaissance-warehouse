#!/usr/bin/env python3
"""moderator_client.py — thin client for the Schema Moderator Service (BUILD-SPEC-v2 §3.3).

The single authority is the droplet service; this client is just transport + the author-time
UX. Stdlib-only (urllib) so it runs on any editor's laptop without extra deps.

URL/token resolution (NO Tailscale, NO SSH, NO VPN — the funnel is PUBLIC HTTPS):
  URL   : $MODERATOR_API_URL  -> Renaissance .env (MODERATOR_API_URL)  -> default funnel/moderator
  TOKEN : $MODERATOR_API_TOKEN -> $RENAISSANCE_ENV file (MODERATOR_API_TOKEN)
          If neither is set the client FAILS CLEARLY and tells you to run `doctor`. It does NOT
          attempt SSH by default (that false trail made writers chase SSH/Tailscale/admin). The
          old SSH self-serve is now strictly opt-in: MODERATOR_ALLOW_SSH_SELFSERVE=1.

First time? Run:  python scripts/moderator_client.py doctor
  -> a deterministic ✅/❌ checklist of YOUR setup with the exact copy-paste fix for each failure.

Commands:
  doctor                                         self-diagnose setup (token/url/reachable/scope/cwd)
  review  [--staged | --files A.sql B.py ...]   POST /review  -> checklist (+ exit 1 on block)
  record  --files A.sql ...                      POST /record-pass -> content-hash-bound ledger row
  loop    [--staged | --files ...]               review; if clean -> record; if block -> print the
                                                  fixes for the editor's Claude (the §7.1 loop is
                                                  Claude-driven: fix -> re-review -> record, <=6x)
  apply-enqueue --files A.sql ...                 add a recorded DDL to the serialized apply FIFO
  apply-now [--reason R] [--no-promote]           APPLY the ledger-approved enqueued DDLs to the LIVE
                                                  warehouse now (writer-flock-safe) + re-promote the
                                                  serving snapshot -> visible to readers in minutes,
                                                  no nightly wait, no SSH. (Default path is still:
                                                  it applies on the nightly. apply-now = make it now.)
  rules | issues [--status open] | catalog [--table T --column C] | ledger | apply-queue
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


# SSH self-serve is OFF by default. It is ONLY for Sam's own machine (which has droplet root SSH);
# a writer does NOT have droplet SSH and does NOT need it — their token is a personal editor token
# Sam handed them, which they export or put in their $RENAISSANCE_ENV file. Leaving SSH on-by-default
# was the root cause of the "SSH connection error" false trail that sent writers chasing
# SSH/Tailscale/admin access they never needed. Opt in explicitly with MODERATOR_ALLOW_SSH_SELFSERVE=1.
_TOKEN_MISSING_MSG = (
    "moderator_client: MODERATOR_API_TOKEN is not set "
    "(not in your shell env and not in your $RENAISSANCE_ENV file).\n"
    "  -> Run:  python scripts/moderator_client.py doctor   (it prints the exact fix)\n"
    "  -> You do NOT need SSH, Tailscale, or a VPN. The service is PUBLIC HTTPS. Just export your\n"
    "     personal editor token:\n"
    "         export MODERATOR_API_TOKEN=<your-personal-editor-token>\n"
    "         export MODERATOR_API_URL=" + DEFAULT_URL + "\n"
    "     (add those to ~/.zshrc / ~/.bashrc to persist; never commit the token).")


def _ssh_selfserve_enabled() -> bool:
    return os.environ.get("MODERATOR_ALLOW_SSH_SELFSERVE", "0").strip().lower() in (
        "1", "true", "yes", "on")


def env_or_file_token() -> str | None:
    """Token from shell env OR the $RENAISSANCE_ENV file — NEVER SSH. The `doctor` uses this so its
    'never uses SSH' guarantee is structural, not just documented."""
    return os.environ.get("MODERATOR_API_TOKEN") or _env_file_get("MODERATOR_API_TOKEN")


def resolve_token() -> str | None:
    """Return the token from shell env or the $RENAISSANCE_ENV file, else None.
    Opt-in only: if MODERATOR_ALLOW_SSH_SELFSERVE=1, fall back to SSH self-serve (Sam's machine)."""
    tok = env_or_file_token()
    if tok:
        return tok
    if _ssh_selfserve_enabled():
        # editor-only self-serve over droplet SSH (Sam's machine only); never auto-escalate to admin.
        try:
            out = subprocess.run(
                ["ssh", WAREHOUSE_HOST,
                 "awk '$3==\"editor\"{print $1; exit}' /opt/duckdb/allowed_tokens.txt"],
                capture_output=True, text=True, timeout=20)
            t = out.stdout.strip()
            if t:
                return t
        except Exception:
            pass
    return None


def token() -> str:
    tok = resolve_token()
    if tok:
        return tok
    sys.exit(_TOKEN_MISSING_MSG)


def _req(method: str, path: str, body: dict | None = None, params: dict | None = None,
         timeout: int = 180) -> dict:
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
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
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
        # Read BYTES then strict-decode UTF-8 so content.encode('utf-8') on the server reproduces the
        # exact file bytes the apply tooth hashes via read_bytes() — same sha across CRLF/LF.
        content = open(f, "rb").read().decode("utf-8")
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
            if fix.get("ambiguity") == "options" and fix.get("options"):
                print("         CHOOSE one (YOU decide — the gate won't pick between these):")
                for i, opt in enumerate(fix["options"], 1):
                    print(f"            {i}. {opt}")
            else:
                steps = fix.get("steps") or []
                print(f"         fix[{fix['kind']}]: "
                      + (" | ".join(s for s in steps if s) or "see detail"))
    if result.get("llm_reasoning"):
        print(f"  LLM: {result['llm_reasoning'][:400]}")
    print("-" * 74)


# ── doctor: deterministic self-diagnosis (NEVER attempts SSH; diagnoses + instructs) ─────────────
def _safe_text(raw: str) -> str:
    """Cap an arbitrary server body so a verbose/echoing/huge error page can't flood the checklist
    (defense-in-depth: a doctor must stay terse and never dump unbounded server output)."""
    raw = raw.strip()
    return raw if len(raw) <= 300 else raw[:300] + "…"


def _http_probe(method: str, path: str, tok: str | None, body: dict | None = None,
                timeout: int = 8) -> tuple[int | None, dict | str | None]:
    """Bare HTTP probe used by `doctor` ONLY. Returns (status_code, parsed_or_text). status_code is
    None on a transport failure (DNS/cert/connect). Never raises; never touches SSH. Fast-fails
    (short timeout) — doctor is a quick verdict, not the 180s production _req."""
    url = base_url() + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            raw = r.read().decode()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, _safe_text(raw)
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode()
        except Exception:
            pass
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, _safe_text(raw)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _scope_from_403(body) -> str | None:
    """Parse the scope out of a moderator 403 body, e.g.
    {"error":"forbidden: 'editor' scope required (you have 'reader')"} -> 'reader'."""
    if isinstance(body, dict):
        msg = body.get("error", "")
        if "you have '" in msg:
            return msg.split("you have '", 1)[1].split("'", 1)[0]
    return None


def _detect_scope(tok: str) -> tuple[str | None, str]:
    """Determine the token's scope using ONLY read-only / no-mutation probes (this never writes).
      POST /review {ddl_files:[],py_files:[]}  needs editor:
        - 401          -> bad/unknown token.
        - 403          -> body says "you have '<scope>'" (a reader); parse it.
        - 400/200      -> passed the editor scope gate (empty payload = validated no-op, NO write),
                          so the token is editor OR admin == has full write power, which is all a
                          writer needs. We report 'editor' (the writer-relevant capability).
    We deliberately do NOT POST to the mutating /rules route just to distinguish editor vs admin —
    a 'doctor' must never risk a side effect, and the editor/admin split is irrelevant to a writer."""
    code, body = _http_probe("POST", "/review", tok, body={"ddl_files": [], "py_files": []})
    if code == 401:
        return None, "token rejected (401 unauthorized) — unknown or revoked"
    if code is None:
        return None, f"could not reach service to check scope ({body})"
    if code == 403:
        have = _scope_from_403(body)
        if have:
            return have, f"scope = {have}"
        return "reader", "scope below editor (review forbidden)"
    if code in (200, 400):
        # passed the editor scope gate with a no-op payload — full write power confirmed.
        return "editor", "scope >= editor (full write power)"
    return None, f"unexpected /review status {code}: {body}"


def _is_warehouse_clone(cwd: str) -> tuple[bool, str]:
    has_client = os.path.exists(os.path.join(cwd, "scripts", "moderator_client.py"))
    has_ddl = os.path.isdir(os.path.join(cwd, "sql", "ddl"))
    if has_client and has_ddl:
        return True, "cwd is a renaissance-warehouse clone (scripts/ + sql/ddl present)"
    missing = []
    if not has_client:
        missing.append("scripts/moderator_client.py")
    if not has_ddl:
        missing.append("sql/ddl/")
    return False, "missing: " + ", ".join(missing)


def cmd_doctor(args) -> int:
    OK, BAD = "✅", "❌"
    fails: list[str] = []
    print("=" * 74)
    print("  SCHEMA MODERATOR — SETUP DOCTOR  (read-only self-diagnosis; never uses SSH)")
    print("=" * 74)

    # (a) MODERATOR_API_TOKEN present?  doctor resolves env/file ONLY (env_or_file_token) so its
    # "never uses SSH" promise is structural — it will NOT shell out even if the opt-in flag is set.
    # NEVER print the token value.
    tok = env_or_file_token()
    src = ("shell env $MODERATOR_API_TOKEN" if os.environ.get("MODERATOR_API_TOKEN")
           else f"$RENAISSANCE_ENV file ({RENAISSANCE_ENV})" if tok else None)
    if tok:
        print(f"  {OK} (a) token found via {src}  (len={len(tok)}, value hidden)")
    else:
        print(f"  {BAD} (a) MODERATOR_API_TOKEN NOT set (not in shell env, not in "
              f"$RENAISSANCE_ENV={RENAISSANCE_ENV})")
        print("         FIX (copy-paste; use YOUR personal editor token Sam sent you):")
        print(f"           export MODERATOR_API_TOKEN=<your-personal-editor-token>")
        print(f"           export MODERATOR_API_URL={DEFAULT_URL}")
        print("           # add both lines to ~/.zshrc or ~/.bashrc to persist")
        print("         You do NOT need SSH / Tailscale / a VPN — the service is PUBLIC HTTPS.")
        fails.append("token")

    # (b) MODERATOR_API_URL set + correct?
    url = base_url()
    url_src = ("shell env" if os.environ.get("MODERATOR_API_URL")
               else "$RENAISSANCE_ENV file" if _env_file_get("MODERATOR_API_URL")
               else "built-in default")
    if url == DEFAULT_URL:
        print(f"  {OK} (b) MODERATOR_API_URL = {url}  (via {url_src})")
    else:
        print(f"  {BAD} (b) MODERATOR_API_URL = {url}  (via {url_src}) — does NOT match the canonical:")
        print(f"           {DEFAULT_URL}")
        print(f"         FIX:  export MODERATOR_API_URL={DEFAULT_URL}")
        fails.append("url")

    # (c) GET /healthz reachable over plain HTTPS (proves no Tailscale/SSH needed).
    code, body = _http_probe("GET", "/healthz", None)
    healthy = code == 200 and isinstance(body, dict) and body.get("ok")
    if healthy:
        print(f"  {OK} (c) GET {url}/healthz reachable over public HTTPS "
              f"(pg={body.get('pg')}, rules_v={body.get('rules_version')}) "
              f"— confirms NO Tailscale/SSH/VPN needed")
    elif code is None:
        print(f"  {BAD} (c) could NOT reach {url}/healthz over HTTPS: {body}")
        print("         FIX: check the URL (step b) and your internet. This is plain public HTTPS —")
        print("              do NOT install Tailscale or try SSH; that is NOT the problem.")
        fails.append("reachable")
    else:
        print(f"  {BAD} (c) {url}/healthz reachable but UNHEALTHY (HTTP {code}, ok!=true): {body}")
        print("         The service is degraded (your setup is fine) — escalate to the orchestrator "
              "bus, not Sam.")
        fails.append("reachable")

    # (d) token resolves + to what SCOPE? Only when we have a token AND the service is HEALTHY
    # (a degraded service would give an unreliable scope verdict — defer to fixing (c) first).
    if tok and healthy:
        scope, note = _detect_scope(tok)
        if scope in ("editor", "admin"):
            print(f"  {OK} (d) token resolves -> scope = {scope.upper()} = FULL write power over the "
                  f"warehouse (cols/views/tables/data/syncs). This is all a writer needs.")
        elif scope == "reader":
            print(f"  {BAD} (d) token resolves -> scope = READER (read-only). You CANNOT author schema "
                  f"changes with a reader token.")
            print("         FIX: this is the wrong token. Use the personal EDITOR token Sam sent you")
            print("              for the moderator (not the read-only cc-service-reader / warehouse token).")
            fails.append("scope")
        elif scope:  # some other non-write scope label the server reported
            print(f"  {BAD} (d) token scope = '{scope}' — not editor; you cannot write. "
                  f"Use your personal EDITOR token.")
            fails.append("scope")
        else:
            print(f"  {BAD} (d) could not confirm scope: {note}")
            if "401" in note or "rejected" in note:
                print("         FIX: the token is unknown/revoked. Re-paste your personal editor token")
                print("              exactly (no quotes/spaces); if it still fails, ask Sam to re-issue it.")
            fails.append("scope")
    elif not tok:
        print(f"  {BAD} (d) scope not checked — no token (fix (a) first).")
    else:
        print(f"  {BAD} (d) scope not checked — service not healthy (fix (c) first).")

    # (e) cwd is a renaissance-warehouse clone?
    cwd = os.getcwd()
    ok_clone, clone_note = _is_warehouse_clone(cwd)
    if ok_clone:
        print(f"  {OK} (e) {clone_note}")
    else:
        print(f"  {BAD} (e) cwd is NOT a renaissance-warehouse clone — {clone_note}")
        print("         FIX: clone the repo and run commands from inside it:")
        print("           git clone https://github.com/sdultsin/renaissance-warehouse.git")
        print("           cd renaissance-warehouse")
        fails.append("clone")

    print("-" * 74)
    if not fails:
        print(f"  {OK} ALL CHECKS PASS — you are set up to edit the warehouse. Author your")
        print("     sql/ddl/NN_*.sql (or entities|sources|scripts/*.py) then run:")
        print("       python scripts/moderator_client.py loop --files <your-files>")
        print("     A 'queued for nightly' / 'recorded' result is CORRECT — changes apply on the")
        print("     ~03:30 UTC nightly rebuild by design, not instantly.")
        return 0
    print(f"  {BAD} {len(fails)} check(s) failed: {', '.join(fails)} — apply the FIX lines above, then")
    print("     re-run:  python scripts/moderator_client.py doctor")
    return 1


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
    ddl, py = _payload(_select(args))
    if not ddl:
        print("moderator_client: no DDL files to record.")
        return 0
    res = _req("POST", "/record-pass",
               {"ddl_files": ddl, "py_files": py, "actor": os.environ.get("USER", "?"),
                "branch": _branch()})
    if res.get("rejected"):
        print(f"  record-pass REJECTED (verdict={res.get('verdict')}). Fix findings and re-review:")
        _print_checklist(res)
        return 1
    for r in res.get("recorded", []):
        print(f"  recorded v{r['ddl_version']} {r['sql_file']} {r['verdict']} "
              f"sha={r['content_sha256'][:12]} ({'new' if r.get('new') else 'already-present'})")
    return 0


def cmd_loop(args) -> int:
    """The §7.1 auto-fix loop entry point. ONE authoritative call: /record-pass re-gates (floor+LLM)
    AND records on pass — so the clean path costs a single LLM deep-review, not two. On BLOCK it
    prints the prescribed fixes and exits 2; the editor's Claude applies them and re-runs (bounded
    to ~6 by CLAUDE.md), then loop again. File edits stay with Claude, not a brittle auto-editor.
    A py-only change (no DDL to record) just reviews."""
    ddl, py = _payload(_select(args))
    if not ddl and not py:
        print("moderator_client loop: nothing staged.")
        return 0
    actor = os.environ.get("USER", "?")
    path = "/record-pass" if ddl else "/review"  # record-pass needs a DDL file; py-only -> review
    res = _req("POST", path, {"ddl_files": ddl, "py_files": py, "actor": actor, "branch": _branch()})
    if res.get("error"):
        print(f"moderator_client: service error: {res['error']}")
        return 0
    _print_checklist(res)
    if res.get("rejected") or res.get("verdict") == "block":
        print("  -> BLOCK. Apply the fixes above, then re-run `moderator_client.py loop` "
              "(<=6 iterations); escalate to the orchestrator bus if still blocked.")
        return 2
    for r in res.get("recorded", []):
        print(f"  recorded v{r['ddl_version']} {r['sql_file']} {r['verdict']} "
              f"sha={r['content_sha256'][:12]} ({'new' if r.get('new') else 'already-present'})")
    return 0


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


def cmd_proposals(args) -> int:
    """Weekly rule-evolution review (§8): list pending proposals + evidence + draft. The human says
    promote/edit/reject/snooze; Claude calls `proposals-decide`."""
    res = _req("GET", "/proposals", params={"status": getattr(args, "status", "pending")})
    props = res.get("proposals", [])
    if not props:
        print("no proposals.")
        return 0
    for p in props:
        ev = p.get("evidence") or {}
        print(f"\n#{p['proposal_id']} [{p['status']}] {p['pattern']}")
        print(f"   evidence: {ev.get('count')}x in {ev.get('window_days')}d, issues {ev.get('issue_ids')}")
        print(f"   draft: {json.dumps(p.get('draft_rule'))}")
    print("\nDecide: moderator_client.py proposals-decide --id N --decision promote|reject|snooze "
          "[--edit '<json publish_rules change>']")
    return 0


def cmd_proposals_decide(args) -> int:
    body = {"proposal_id": args.id, "decision": args.decision}
    if args.edit:
        body["edit"] = json.loads(args.edit)
    print(json.dumps(_req("POST", "/proposals/decide", body), indent=2))
    return 0


def cmd_proposals_detect(args) -> int:
    print(json.dumps(_req("POST", "/proposals/detect", {}), indent=2))
    return 0


def cmd_feedback(args) -> int:
    body = {"kind": args.kind, "detail": args.detail, "ddl_file": args.ddl_file}
    print(json.dumps(_req("POST", "/feedback", body), indent=2))
    return 0


def cmd_apply_enqueue(args) -> int:
    ddl, _ = _payload(_select(args))
    if not ddl:
        print("no DDL files to enqueue.")
        return 0
    body = {"ddl_files": ddl, "actor": os.environ.get("USER", "?"), "branch": _branch()}
    print(json.dumps(_req("POST", "/apply/enqueue", body), indent=2))
    return 0


def cmd_apply_queue(args) -> int:
    print(json.dumps(_req("GET", "/apply/queue", params={"status": args.status}), indent=2))
    return 0


def cmd_apply_process(args) -> int:
    print(json.dumps(_req("POST", "/apply/process", {}), indent=2))
    return 0


def cmd_apply_now(args) -> int:
    """ON-DEMAND real-time apply: physically apply your ledger-approved enqueued DDLs to the LIVE
    warehouse (under the writer flock, content-hash-bound to the ledger) AND re-promote the serving
    snapshot so READERS see the change in minutes — instead of waiting for the ~03:30 UTC nightly.

    Flock-safe: it queues behind the nightly / a running promote, never clobbers. The snapshot copy
    is ~50GB so the promote can take several minutes (~10) — that's the cost of 'make it live now'.
    Enqueue first (`apply-enqueue --files ...`) if you haven't; apply-now drains what's queued+passed.
    """
    body = {"actor": os.environ.get("USER", "?")}
    if getattr(args, "no_promote", False):
        body["promote"] = False
    if getattr(args, "promote_only", False):
        body["force_promote"] = True
    if getattr(args, "reason", None):
        body["reason"] = args.reason
    print("apply-now: applying ledger-approved DDLs + re-promoting the serving snapshot "
          "(the snapshot copy can take several minutes / ~10 — please wait, do not interrupt)…")
    # The promote copies the ~50GB warehouse (~10 min), far longer than the default 180s socket
    # timeout — give apply-now a generous window so the client waits for the real result instead of
    # timing out mid-promote. Overridable via MODERATOR_APPLY_NOW_TIMEOUT_S.
    an_timeout = int(os.environ.get("MODERATOR_APPLY_NOW_TIMEOUT_S", "1800"))
    res = _req("POST", "/apply-now", body, timeout=an_timeout)
    if res.get("error") and not res.get("applied"):
        print(f"  apply-now ERROR: {res['error']}")
        return 1
    applied = res.get("applied", [])
    if not applied:
        print(f"  {res.get('detail','nothing queued to apply')}")
    for a in applied:
        mark = {"committed": "[OK]   ", "blocked": "[BLOCK]", "failed": "[FAIL] "}.get(
            a.get("status", ""), "[?]    ")
        print(f"  {mark} v{a.get('ddl_version')} {a.get('sql_file','')}: {a.get('detail','')}")
    promote = res.get("promote") or {}
    if promote.get("promoted"):
        print(f"  PROMOTE: serving snapshot re-promoted -> {promote.get('snapshot_id','?')} "
              f"(copy {promote.get('copy_s','?')}s). Readers see your change now.")
    elif promote.get("promote_busy"):
        print("  PROMOTE: another promote is already running — your apply LANDED in the live DB and "
              "will be served by that in-flight promote (or re-run apply-now to confirm).")
    elif promote.get("promote_refused_window"):
        print("  PROMOTE: inside the 03:30-05:45 UTC nightly window — promote deferred. Your apply "
              "LANDED in the live DB; it'll be served by the nightly promote (or re-run after 05:45).")
    elif promote.get("detail"):
        print(f"  PROMOTE: {promote['detail']}")
    elif promote.get("error"):
        print(f"  PROMOTE ERROR: {promote['error']} — the apply LANDED; re-run apply-now to promote.")
    fresh = res.get("freshness") or {}
    print(f"  FRESHNESS: serving snapshot={fresh.get('snapshot_id','?')}, "
          f"live max DDL version={fresh.get('live_schema_version_max','?')} "
          f"(elapsed {res.get('elapsed_s','?')}s)")
    return 0 if res.get("ok") else 1


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
    sub.add_parser("doctor", help="self-diagnose your setup (run this FIRST)")
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
    prop = sub.add_parser("proposals"); prop.add_argument("--status", default="pending")
    sub.add_parser("proposals-detect")
    pd = sub.add_parser("proposals-decide")
    pd.add_argument("--id", type=int, required=True)
    pd.add_argument("--decision", required=True, choices=["promote", "reject", "snooze"])
    pd.add_argument("--edit", help="JSON publish_rules change to apply on promote")
    fb = sub.add_parser("feedback")
    fb.add_argument("--kind", required=True, choices=["escape", "false_positive"])
    fb.add_argument("--detail", required=True)
    fb.add_argument("--ddl-file", dest="ddl_file")
    ae = sub.add_parser("apply-enqueue"); ae.add_argument("--files", nargs="*"); ae.add_argument("--staged", action="store_true")
    aq = sub.add_parser("apply-queue"); aq.add_argument("--status", default="all")
    sub.add_parser("apply-process")
    an = sub.add_parser("apply-now", help="apply ledger-approved enqueued DDLs LIVE now + re-promote (no nightly wait)")
    an.add_argument("--no-promote", dest="no_promote", action="store_true",
                    help="apply to the live DB but skip the serving re-promote (advanced)")
    an.add_argument("--promote-only", dest="promote_only", action="store_true",
                    help="force a serving re-promote even when nothing is queued (surface an "
                         "already-applied change) — triggers the ~10-min snapshot copy")
    an.add_argument("--reason", help="reason string recorded in the publish/apply log")
    args = p.parse_args(argv)
    if args.cmd == "doctor":
        return cmd_doctor(args)
    if args.cmd == "review":
        return cmd_review(args)
    if args.cmd == "loop":
        return cmd_loop(args)
    if args.cmd == "record":
        return cmd_record(args)
    if args.cmd == "ci":
        return cmd_ci(args)
    if args.cmd == "proposals":
        return cmd_proposals(args)
    if args.cmd == "proposals-detect":
        return cmd_proposals_detect(args)
    if args.cmd == "proposals-decide":
        return cmd_proposals_decide(args)
    if args.cmd == "feedback":
        return cmd_feedback(args)
    if args.cmd == "apply-enqueue":
        return cmd_apply_enqueue(args)
    if args.cmd == "apply-queue":
        return cmd_apply_queue(args)
    if args.cmd == "apply-process":
        return cmd_apply_process(args)
    if args.cmd == "apply-now":
        return cmd_apply_now(args)
    return cmd_simple(args, "/" + args.cmd)


if __name__ == "__main__":
    sys.exit(main())
