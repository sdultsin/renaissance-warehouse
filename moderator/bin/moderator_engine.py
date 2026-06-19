"""moderator_engine.py — the server-side two-layer schema-review gate (BUILD-SPEC-v2 §5).

This is where /review and /record-pass actually think. It is the ONLY place the verdict is
authoritative: the apply tooth trusts a ledger row, never the client.

Layer 1 — DETERMINISTIC FLOOR (free, certain): reuse the phase-1 engine (schema_gate_lib) to
classify ops + resolve consumers/canonicals from the live catalog, then evaluate the VERSIONED
rule rows in moderator.rule (rules-as-data; the predicate vocabulary the rows were seeded with).
Produces findings + fixes + a floor verdict. Never depends on a model.

Layer 2 — LLM DEEP-REVIEW (paid, fail-closed): on every change that clears the floor, a strong
reasoning model sees the change + catalog/lineage context and judges semantic/intent/foreseeable-
downstream breakage determinism can't. It can BLOCK. Runs in-process (one key, server-side,
un-bypassable). If the model is unavailable: fail-CLOSED when MODERATOR_LLM_FAIL_CLOSED=1 (P8),
fail-open-with-warning during the held calibration window.

Store split: rules + alias authority + ledger live in Postgres (moderator schema); catalog +
consumers live in DuckDB (read-only serving snapshot). Both are read here.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict

import moderator_common as mc

_TIER_SEVERITY = {"block": "Error", "warn": "Warn", "info": "Info", "process": "Info"}


# ── rules + alias authority (Postgres) ──────────────────────────────────────────────────────────
def active_rules_version() -> int | None:
    return mc.pg_one("SELECT max(rules_version) FROM moderator.rules_version")


def load_rules(version: int | None = None) -> tuple[int, list[dict]]:
    """Return (rules_version, [rule dict]) for the active (or given) version, enabled + currently
    valid. Each dict: {code, kind, tier, spec, detail_template}."""
    rv = version if version is not None else active_rules_version()
    if rv is None:
        return 0, []
    rows = []
    with mc.pg_conn() as c, c.cursor() as cur:
        # Full-snapshot-per-version model: every row of a published version is valid for that
        # version, so a ledger row's rules_version reproduces the exact rule set it was judged against.
        cur.execute(
            "SELECT code, kind, tier, spec, detail_template FROM moderator.rule "
            "WHERE rules_version=%s AND enabled ORDER BY code", (rv,))
        for code, kind, tier, spec, detail in cur.fetchall():
            rows.append({"code": code, "kind": kind, "tier": tier,
                         "spec": spec if isinstance(spec, dict) else json.loads(spec),
                         "detail_template": detail or ""})
    return rv, rows


_RULES_LOCK_KEY = 911002  # arbitrary constant for pg_advisory_xact_lock (serialise rule edits)


def publish_rules(upsert_rules, disable_codes, aliases_add, note, published_by, source="human") -> int:
    """Append a new full-snapshot rule version atomically (the service is the sole serialised writer
    of rule changes). Copies the current active version forward, applies upserts (replace by code)
    and disables, bumps rules_version, adds any new aliases. Returns the new rules_version."""
    disable = {c.upper() for c in (disable_codes or [])}
    with mc.pg_conn() as c:
        with c.transaction():
            with c.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (_RULES_LOCK_KEY,))
                cur.execute("SELECT COALESCE(max(rules_version), 0) FROM moderator.rules_version")
                cur_v = cur.fetchone()[0]
                new_v = cur_v + 1
                # snapshot current rules by code
                cur.execute(
                    "SELECT code, kind, tier, spec, detail_template FROM moderator.rule "
                    "WHERE rules_version=%s AND enabled", (cur_v,))
                by_code = {}
                for code, kind, tier, spec, detail in cur.fetchall():
                    by_code[code] = {"code": code, "kind": kind, "tier": tier,
                                     "spec": spec if isinstance(spec, dict) else json.loads(spec),
                                     "detail_template": detail or ""}
                for u in (upsert_rules or []):
                    by_code[u["code"]] = {"code": u["code"], "kind": u["kind"], "tier": u["tier"],
                                          "spec": u["spec"], "detail_template": u.get("detail_template", "")}
                for code in disable:
                    by_code.pop(code, None)
                cur.execute(
                    "INSERT INTO moderator.rules_version (rules_version, published_by, note, source) "
                    "VALUES (%s,%s,%s,%s)", (new_v, published_by, note, source))
                for r in by_code.values():
                    cur.execute(
                        "INSERT INTO moderator.rule (rules_version, code, kind, tier, spec, "
                        "detail_template, added_by) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (new_v, r["code"], r["kind"], r["tier"], json.dumps(r["spec"]),
                         r["detail_template"], published_by))
                for a in (aliases_add or []):
                    cur.execute(
                        "INSERT INTO moderator.column_alias (alias, canonical_name, scope, reason, "
                        "added_by, rules_version) VALUES (%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT (alias, scope) DO NOTHING",
                        (a["alias"], a["canonical_name"], a.get("scope", "global"),
                         a.get("reason"), published_by, new_v))
    return new_v


def load_aliases() -> dict[str, str]:
    """{alias: canonical} from the moderator authority (global scope), lowercased."""
    out: dict[str, str] = {}
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute("SELECT alias, canonical_name FROM moderator.column_alias WHERE scope='global'")
        for a, canon in cur.fetchall():
            out[a.lower()] = canon.lower()
    return out


# ── catalog + consumers (DuckDB serving snapshot; degrade gracefully pre-P7) ─────────────────────
def load_catalog() -> tuple[set, set, str | None]:
    """Return (canonical_cols, all_cols, snapshot_id). Empty sets if the catalog isn't built yet."""
    canonical: set[str] = set()
    allcols: set[str] = set()
    snap = None
    try:
        with mc.duckdb_ro() as con:
            snap = os.path.basename(os.path.realpath(mc.DUCKDB_CURRENT))
            try:
                for (cn,) in con.execute(
                    "SELECT DISTINCT COALESCE(canonical_name, column_name) FROM core.schema_catalog"
                ).fetchall():
                    if cn:
                        canonical.add(cn.lower())
                for (c,) in con.execute(
                    "SELECT DISTINCT column_name FROM core.schema_catalog").fetchall():
                    if c:
                        allcols.add(c.lower())
            except Exception:
                pass  # catalog tables not present yet (pre-P7 nightly) — degrade
    except Exception:
        pass
    return canonical, allcols, snap


def resolve_consumers(con, table: str | None, column: str | None) -> list[str]:
    """['file:line (confidence)'] from core.schema_consumers; rename-resilient rows excluded.
    `con` is an open read-only DuckDB connection (or None to skip)."""
    if con is None or not column:
        return []
    try:
        if table:
            rows = con.execute(
                "SELECT consumer_file, consumer_line, confidence, rename_resilient "
                "FROM core.schema_consumers WHERE lower(column_name)=lower(?) "
                "AND (table_name IS NULL OR lower(table_name)=lower(?))", [column, table]).fetchall()
        else:
            rows = con.execute(
                "SELECT consumer_file, consumer_line, confidence, rename_resilient "
                "FROM core.schema_consumers WHERE lower(column_name)=lower(?)", [column]).fetchall()
    except Exception:
        return []
    out = []
    for f, ln, conf, resilient in rows:
        if resilient:
            continue
        out.append(f"{f}:{ln} ({conf})" if ln else f"{f} ({conf})")
    return sorted(set(out))


# ── per-op facts (the fixed predicate vocabulary the rule rows reference) ─────────────────────────
def _op_facts(lib, op, aliases, canonical, allcols, con) -> dict:
    schema, tbl = lib.split_table_ref(op.table)
    col = op.column
    naming = lib.naming_findings(op.table, col) if op.op == "add_column" else []
    extra = (op.extra or "").lower()
    consumers = []
    if op.op in ("drop_column", "drop_table", "rename_column", "rename_table"):
        consumers = resolve_consumers(con, tbl, col or tbl)
    dupe = lib.alias_dupe_finding(op.table, col, aliases, canonical) if op.op == "add_column" else None
    return {
        "op": op.op, "classification": op.classification,
        "schema": schema, "table": tbl, "column": col, "new_name": op.new_name,
        "line": op.line, "consumers": consumers,
        "facts": {
            "has_consumers": bool(consumers),
            "not_snake_case": any(f.rule == "R3" for f in naming),
            "off_list_abbrev": any(f.rule == "R5" for f in naming),
            "alias_of_existing": dupe is not None,
            "lexical_near_dupe": bool(lib.lexical_near_dupes(col, allcols)) if op.op == "add_column" else False,
            "type_narrowing": op.op == "alter_type",
            "set_not_null_without_default": op.op == "set_not_null",
            "add_not_null_without_default": op.op == "add_column" and "not null" in extra and "default" not in extra,
        },
        "_dupe_canonical": (dupe.column_name and aliases.get((col or "").lower())) if dupe else None,
    }


def _cond_holds(condset: dict, facts: dict) -> bool:
    """All keys in condset must be satisfied (value True => fact True)."""
    for k, want in condset.items():
        if bool(facts.get(k, False)) != bool(want):
            return False
    return True


def _when_fires(when: dict, facts: dict) -> bool:
    all_of = when.get("all_of") or {}
    any_of = when.get("any_of") or []
    if not _cond_holds(all_of, facts):
        return False
    if any_of:
        return any(_cond_holds(cs, facts) for cs in any_of)
    return True


def _render(template: str, ctx: dict) -> str:
    try:
        return template.format_map(defaultdict(str, ctx))
    except Exception:
        return template


# ── the deterministic floor ───────────────────────────────────────────────────────────────────
def floor_review(ddl_files: list[dict], py_files: list[dict]) -> dict:
    """Run the deterministic floor over the submitted files. Returns
    {rules_version, catalog_snapshot_id, findings:[...], floor_verdict}."""
    lib = mc.engine()
    rv, rules = load_rules()
    aliases = load_aliases()
    canonical, allcols, snap = load_catalog()
    op_rules = [r for r in rules if (r["spec"] or {}).get("applies") == "ddl-op"]
    file_rules = [r for r in rules if (r["spec"] or {}).get("applies") == "ddl-file"]

    findings: list[dict] = []
    con = None
    try:
        con = mc.duckdb_ro().__enter__()
    except Exception:
        con = None
    try:
        for f in ddl_files:
            path, sql = f["path"], f["content"]
            ops = lib.classify_ddl(sql)
            intent = lib.parse_intent(sql)
            depends = lib.parse_depends(sql)
            # file-scoped rules (R4 intent/deps) — fire once per file if it touches columns.
            if ops:
                file_facts = {"missing_intent_marker": not intent, "missing_depends": not depends}
                for r in file_rules:
                    spec = r["spec"] or {}
                    match_ops = set((spec.get("match") or {}).get("op") or [])
                    if match_ops and not any(o.op in match_ops for o in ops):
                        continue
                    if _when_fires(spec.get("when") or {}, file_facts):
                        missing = ", ".join(m for m, want in
                                            (("an `-- @gate:` intent marker", file_facts["missing_intent_marker"]),
                                             ("a `-- Depends on NN` line", file_facts["missing_depends"])) if want)
                        ctx = {"ddl_file": path, "missing": missing or "intent metadata"}
                        findings.append(_finding(r, ctx, ddl_file=path))
            # op-scoped rules (R1/R2/R3/R5).
            for op in ops:
                of = _op_facts(lib, op, aliases, canonical, allcols, con)
                for r in op_rules:
                    spec = r["spec"] or {}
                    m = spec.get("match") or {}
                    if m.get("op") and of["op"] not in m["op"]:
                        continue
                    if m.get("classification") and of["classification"] not in m["classification"]:
                        continue
                    if not _when_fires(spec.get("when") or {}, of["facts"]):
                        continue
                    reason = _reason_for(r["code"], of)
                    ctx = {"op": of["op"].replace("_", " ").upper(), "table": of["table"] or "?",
                           "column": of["column"] or "", "classification": of["classification"],
                           "consumer_count": len(of["consumers"]),
                           "consumers": ", ".join(of["consumers"][:8]) or "none statically known",
                           "reason": reason, "canonical": of["_dupe_canonical"] or "", "ddl_file": path}
                    findings.append(_finding(r, ctx, ddl_file=path, op=of))
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

    floor_verdict = _verdict_from(findings)
    return {"rules_version": rv, "catalog_snapshot_id": snap,
            "findings": findings, "floor_verdict": floor_verdict, "catalog_built": bool(allcols)}


def _reason_for(code: str, of: dict) -> str:
    f = of["facts"]
    if code == "R3":
        bits = []
        if f["not_snake_case"]:
            bits.append("not lower_snake_case")
        if f["alias_of_existing"]:
            bits.append(f"synonym of canonical `{of['_dupe_canonical']}`")
        return "; ".join(bits) or "naming"
    if code == "R5":
        bits = []
        if f["lexical_near_dupe"]:
            bits.append("lexically near an existing column")
        if f["type_narrowing"]:
            bits.append("ALTER TYPE may narrow/rewrite")
        if f["set_not_null_without_default"]:
            bits.append("SET NOT NULL fails on existing NULLs — backfill first")
        if f["off_list_abbrev"]:
            bits.append("contains a non-approved abbreviation")
        return "; ".join(bits) or "advisory"
    return of["classification"] or ""


def _finding(rule: dict, ctx: dict, ddl_file: str | None = None, op: dict | None = None) -> dict:
    return {
        "rule": rule["code"], "tier": rule["tier"],
        "severity": _TIER_SEVERITY.get(rule["tier"], "Warn"),
        "classification": (op or {}).get("classification"),
        "table_schema": (op or {}).get("schema"),
        "table_name": (op or {}).get("table"),
        "column_name": (op or {}).get("column"),
        "ddl_file": ddl_file,
        "detail": _render(rule["detail_template"], ctx),
        "consumers": (op or {}).get("consumers") or [],
        "fix": (rule["spec"] or {}).get("fix") or {"kind": "none"},
        "source": "floor",
    }


def _verdict_from(findings: list[dict]) -> str:
    if any(f.get("tier") == "block" for f in findings):
        return "block"
    if any(f.get("tier") in ("warn", "info") for f in findings):
        return "pass-with-warn"
    return "pass"


# ── LLM deep-review (layer 2) ────────────────────────────────────────────────────────────────────
_DEEP_TOOL = {
    "name": "report_deep_review",
    "description": "Report the schema deep-review verdict for the proposed change.",
    "input_schema": {
        "type": "object",
        "properties": {
            "verdict": {"type": "string", "enum": ["pass", "block"],
                        "description": "block ONLY for a real semantic/intent/downstream break the "
                                       "deterministic floor cannot catch; otherwise pass."},
            "findings": {"type": "array", "items": {"type": "object", "properties": {
                "severity": {"type": "string", "enum": ["block", "warn", "info"]},
                "detail": {"type": "string"},
                "table": {"type": "string"}, "column": {"type": "string"},
                "fix": {"type": "string"}}, "required": ["severity", "detail"]}},
            "reasoning": {"type": "string"},
        },
        "required": ["verdict", "findings", "reasoning"],
    },
}

_DEEP_SYSTEM = (
    "You are the deep-review layer of the Renaissance warehouse schema moderator — the LAST line "
    "of defence between five editors' DDL changes and a broken shared DuckDB warehouse (~100 syncs "
    "read it). A fast deterministic floor already checked mechanical breakage (bare rename/drop with "
    "known consumers, non-canonical names, missing intent). YOUR job is what determinism cannot see: "
    "semantic/intent breakage, a change that is structurally valid but WRONG, and foreseeable "
    "downstream effects across consumers. Be precise and conservative: BLOCK only for a real, "
    "explained break — never for style the floor already owns. Prefer pass-with-warnings to blocking "
    "when uncertain, but DO block a genuine silent-corruption or consumer-break risk. Always return "
    "concrete fixes. Respond ONLY via the report_deep_review tool."
)


def llm_deep_review(ddl_files, py_files, floor_result) -> dict:
    """Return {status: 'pass'|'block'|'unavailable', findings:[...], reasoning, model}.
    Never raises — a transport/API failure becomes status='unavailable' (caller fail-policies it)."""
    if not mc.SERVER_LLM_ON:
        return {"status": "disabled", "findings": [], "reasoning": "", "model": None}
    if not mc.ANTHROPIC_KEY:
        return {"status": "unavailable", "findings": [],
                "reasoning": "no ANTHROPIC_API_KEY configured", "model": mc.LLM_MODEL}
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=mc.ANTHROPIC_KEY, timeout=mc.LLM_TIMEOUT_S)
        prompt = _deep_prompt(ddl_files, py_files, floor_result)
        resp = client.messages.create(
            model=mc.LLM_MODEL, max_tokens=2500, system=_DEEP_SYSTEM,
            tools=[_DEEP_TOOL], tool_choice={"type": "tool", "name": "report_deep_review"},
            messages=[{"role": "user", "content": prompt}],
        )
        payload = None
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use" and block.name == "report_deep_review":
                payload = block.input
                break
        if payload is None:
            return {"status": "unavailable", "findings": [],
                    "reasoning": "model returned no structured verdict", "model": mc.LLM_MODEL}
        verdict = "block" if payload.get("verdict") == "block" else "pass"
        findings = [{
            "rule": "LLM", "tier": ("block" if x.get("severity") == "block" else x.get("severity", "warn")),
            "severity": {"block": "Error", "warn": "Warn", "info": "Info"}.get(x.get("severity"), "Warn"),
            "classification": "SEMANTIC", "table_name": x.get("table"), "column_name": x.get("column"),
            "detail": x.get("detail", ""), "fix": {"kind": "llm", "steps": [x.get("fix", "")]},
            "consumers": [], "source": "llm",
        } for x in (payload.get("findings") or [])]
        return {"status": verdict, "findings": findings,
                "reasoning": payload.get("reasoning", ""), "model": mc.LLM_MODEL}
    except Exception as e:  # network / API / SDK — fail-closed is decided by the caller
        mc.log_event("llm_deep_review_error", error=f"{type(e).__name__}: {e}")
        return {"status": "unavailable", "findings": [],
                "reasoning": f"{type(e).__name__}: {e}", "model": mc.LLM_MODEL}


def _deep_prompt(ddl_files, py_files, floor_result) -> str:
    lib = mc.engine()
    parts = ["# Proposed warehouse schema change\n"]
    for f in ddl_files:
        parts.append(f"## DDL file: {f['path']}\n```sql\n{f['content']}\n```\n")
    for f in (py_files or []):
        parts.append(f"## Python consumer: {f['path']}\n```python\n{f['content'][:4000]}\n```\n")
    # catalog/lineage context for the touched objects
    canonical, allcols, _ = load_catalog()
    ctx_lines = []
    con = None
    try:
        con = mc.duckdb_ro().__enter__()
    except Exception:
        con = None
    try:
        seen = set()
        for f in ddl_files:
            for op in lib.classify_ddl(f["content"]):
                schema, tbl = lib.split_table_ref(op.table)
                key = (tbl, op.column)
                if key in seen:
                    continue
                seen.add(key)
                cons = resolve_consumers(con, tbl, op.column or tbl)
                ctx_lines.append(f"- {op.op} on {schema}.{tbl}.{op.column or ''} "
                                 f"-> {len(cons)} known consumer(s): {', '.join(cons[:10]) or 'none statically known'}")
                if con is not None and tbl:
                    try:
                        cols = con.execute(
                            "SELECT column_name FROM core.schema_catalog WHERE lower(table_name)=lower(?) "
                            "ORDER BY ordinal_position", [tbl]).fetchall()
                        if cols:
                            ctx_lines.append(f"    existing columns of {tbl}: "
                                             + ", ".join(c[0] for c in cols[:60]))
                    except Exception:
                        pass
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    parts.append("# Catalog / lineage context\n" + ("\n".join(ctx_lines) or "(catalog not yet built)") + "\n")
    fl = floor_result.get("findings", [])
    parts.append("# Deterministic floor findings (already covered — do NOT just repeat these)\n"
                 + ("\n".join(f"- [{x['rule']}/{x['severity']}] {x['detail']}" for x in fl) or "(floor clean)")
                 + "\n")
    parts.append("\nJudge what the floor cannot: semantic/intent breakage, structurally-valid-but-wrong "
                 "changes, and foreseeable downstream effects. Return your verdict via the tool.")
    return "\n".join(parts)


# ── orchestration: full review (floor -> LLM, fail-policy) ───────────────────────────────────────
def review(ddl_files, py_files) -> dict:
    floor = floor_review(ddl_files, py_files)
    findings = list(floor["findings"])
    floor_verdict = floor["floor_verdict"]
    llm = {"status": "skipped", "findings": [], "reasoning": "", "model": None}

    if floor_verdict == "block":
        verdict = "block"  # the floor is sufficient; LLM not needed to confirm a hard mechanical break
    else:
        llm = llm_deep_review(ddl_files, py_files, floor)
        findings.extend(llm["findings"])
        if llm["status"] == "block":
            verdict = "block"
        elif llm["status"] == "unavailable":
            if mc.LLM_FAIL_CLOSED:
                verdict = "block"
                findings.append({"rule": "LLM", "tier": "block", "severity": "Error",
                                 "classification": "DEEP-REVIEW-UNAVAILABLE",
                                 "detail": f"Deep-review unavailable ({llm['reasoning']}); failing CLOSED "
                                           f"— holding until the model can judge this change.",
                                 "consumers": [], "fix": {"kind": "retry"}, "source": "llm"})
            else:
                findings.append({"rule": "LLM", "tier": "warn", "severity": "Warn",
                                 "classification": "DEEP-REVIEW-UNAVAILABLE",
                                 "detail": f"Deep-review unavailable ({llm['reasoning']}); failing OPEN during "
                                           f"the held calibration window (set MODERATOR_LLM_FAIL_CLOSED=1 at P8).",
                                 "consumers": [], "fix": {"kind": "retry"}, "source": "llm"})
                verdict = _verdict_from(findings)
        else:  # pass / disabled
            verdict = _verdict_from(findings)

    return {"verdict": verdict, "findings": findings, "rules_version": floor["rules_version"],
            "catalog_snapshot_id": floor["catalog_snapshot_id"], "catalog_built": floor["catalog_built"],
            "floor_verdict": floor_verdict, "llm_status": llm["status"], "llm_reasoning": llm["reasoning"]}


# ── issue ledger + approval ledger writers (Postgres) ────────────────────────────────────────────
def write_issues(findings, request_id, actor, branch, rules_version) -> int:
    if not findings:
        return 0
    n = 0
    with mc.pg_conn() as c, c.cursor() as cur:
        for f in findings:
            try:
                cur.execute(
                    "INSERT INTO moderator.issue (request_id, rule, severity, classification, "
                    "table_schema, table_name, column_name, ddl_file, detail, consumers, "
                    "rules_version, actor) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    [request_id, f.get("rule"), f.get("severity"), f.get("classification"),
                     f.get("table_schema"), f.get("table_name"), f.get("column_name"),
                     f.get("ddl_file"), f.get("detail"), json.dumps(f.get("consumers") or []),
                     rules_version, actor])
                n += 1
            except Exception as e:
                mc.log_event("issue_write_error", error=f"{type(e).__name__}: {e}")
    return n


def _ddl_version_of(path: str) -> int | None:
    base = os.path.basename(path)
    stem = base.split("_", 1)[0]
    try:
        return int(stem)
    except ValueError:
        return None


def record_pass(ddl_files, actor, branch, request_id) -> dict:
    """Re-gate server-side against LIVE rules+catalog, then write ONE content-hash-bound ledger row
    per ddl file IFF verdict != block. The ONLY way a pass enters moderator.approval_ledger."""
    result = review(ddl_files, py_files=[])
    rv = result["rules_version"]
    if result["verdict"] == "block":
        return {"recorded": [], "rejected": True, "verdict": "block",
                "rules_version": rv, "findings": result["findings"],
                "detail": "server re-gate returned BLOCK — pass NOT recorded; fix and resubmit."}
    gate_version = mc.GATE_VERSION
    recorded = []
    with mc.pg_conn() as c, c.cursor() as cur:
        for f in ddl_files:
            ver = _ddl_version_of(f["path"])
            if ver is None:
                continue
            sha = hashlib.sha256(f["content"].encode("utf-8")).hexdigest()
            try:
                cur.execute(
                    "INSERT INTO moderator.approval_ledger (ddl_version, sql_file, content_sha256, "
                    "verdict, rules_version, gate_version, findings, actor, branch, request_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (ddl_version, content_sha256) DO NOTHING",
                    [ver, os.path.basename(f["path"]), sha, result["verdict"], rv, gate_version,
                     json.dumps(result["findings"]), actor, branch, request_id])
                recorded.append({"ddl_version": ver, "sql_file": os.path.basename(f["path"]),
                                 "content_sha256": sha, "verdict": result["verdict"],
                                 "new": cur.rowcount == 1})  # False = already in ledger (idempotent)
            except Exception as e:
                mc.log_event("ledger_write_error", error=f"{type(e).__name__}: {e}", file=f["path"])
    return {"recorded": recorded, "rejected": False, "verdict": result["verdict"],
            "rules_version": rv, "findings": result["findings"]}
