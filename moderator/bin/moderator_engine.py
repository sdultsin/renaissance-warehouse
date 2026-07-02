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
import subprocess
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


def resolve_table_consumers(con, table: str | None) -> list[str]:
    """Consumers of ANY column of `table` — used for DROP TABLE / RENAME TABLE (where there is no
    single column). Without this, a table-level op resolves zero consumers and the breaking-change
    rule never fires."""
    if con is None or not table:
        return []
    try:
        rows = con.execute(
            "SELECT consumer_file, consumer_line, confidence FROM core.schema_consumers "
            "WHERE lower(table_name)=lower(?) AND NOT rename_resilient", [table]).fetchall()
    except Exception:
        return []
    out = [f"{f}:{ln} ({conf})" if ln else f"{f} ({conf})" for f, ln, conf in rows]
    return sorted(set(out))


# ── per-op facts (the fixed predicate vocabulary the rule rows reference) ─────────────────────────
def _op_facts(lib, op, aliases, canonical, allcols, con) -> dict:
    schema, tbl = lib.split_table_ref(op.table)
    col = op.column
    naming = lib.naming_findings(op.table, col) if op.op == "add_column" else []
    extra = (op.extra or "").lower()
    consumers = []
    if op.op in ("drop_column", "rename_column"):
        consumers = resolve_consumers(con, tbl, col)
    elif op.op in ("drop_table", "rename_table"):
        consumers = resolve_table_consumers(con, tbl)  # any column of the table, not the table-as-column
    # alias dupe: look up with a lowercased column so a mixed-case synonym (Email_Address) is caught.
    dupe = (lib.alias_dupe_finding(op.table, (col or "").lower(), aliases, canonical)
            if op.op == "add_column" else None)
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
            "creates_unique_index": op.op == "create_unique_index",
            "creates_index": op.op in ("create_index", "create_unique_index"),
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


# ── W2 floor index rule (expand/contract for CREATE [UNIQUE] INDEX) ───────────────────────────────
# The 2026-06-26 landmine: a bare `CREATE UNIQUE INDEX IF NOT EXISTS <new> ON <tbl>(<col>)` shipped
# as a "rename" without DROPping the old index leaves TWO unique indexes on the same column, FATALing
# the nightly upsert. This mirrors R1 (column rename/drop must be expand/contract): a new index on a
# column that ALREADY has a same-uniqueness index under a DIFFERENT name is REFUSED (tier=block)
# unless the SAME diff also DROPs the old index. A 2nd UNIQUE index on a column always WARNs.
# Normalized index-expression match for `_existing_indexes_on_column` — case-insensitive AND
# quote-stripped, mirroring W1's _KEY_INDEX_EXPR_NORM so a `_KEY`-cased existing index (Bypass 2) is
# matched as an existing enforcement on the column.
_EXPR_NORM = "replace(lower(trim(expressions, '[]')), '\"', '')"


def _existing_indexes_on_column(con, table: str | None, column: str | None) -> tuple[list[dict], bool]:
    """Return ([{index_name, is_unique, kind}], catalog_readable) for the unique/non-unique ENFORCEMENTS
    whose SOLE expression/column is `column` on `table`, from the live serving snapshot.

    Fuses two catalogs (mirroring W1): duckdb_indexes() (kind='index') + duckdb_constraints()
    (kind='constraint' for a single-column UNIQUE / PRIMARY KEY on `column`; constraints are always
    treated as is_unique=True). Column casing is normalized so a `_KEY`-cased index (Bypass 2) and a
    constraint-backed enforcement (Bypass 1) both register as an EXISTING enforcement.

    `catalog_readable` is False when the serving snapshot could not be positively read (con is None, or
    the index probe raised) — the floor must then FAIL-CLOSED (WARN/BLOCK) on a NEW unique enforcement
    rather than silently pass, because it cannot prove the column has no pre-existing enforcement
    (snapshot lags the authoritative warehouse in the promote-lag window). A genuinely-empty result
    from a readable catalog returns ([], True)."""
    if con is None or not table or not column:
        return [], False
    out: list[dict] = []
    # (1) indexes (normalized casing). A probe failure => catalog NOT readable => fail-closed.
    try:
        rows = con.execute(
            "SELECT index_name, is_unique FROM duckdb_indexes() "
            f"WHERE lower(table_name)=lower(?) AND {_EXPR_NORM}=lower(?)",
            [table, column]).fetchall()
    except Exception:
        return [], False
    for r in rows:
        out.append({"index_name": r[0], "is_unique": bool(r[1]), "kind": "index"})
    # (2) UNIQUE / PRIMARY KEY constraints whose sole column is `column` (Bypass-1 enforcement). Best-
    # effort: if duckdb_constraints() is unavailable we keep the index result (still a readable catalog).
    try:
        crows = con.execute(
            "SELECT constraint_type FROM duckdb_constraints() "
            "WHERE lower(table_name)=lower(?) AND constraint_type IN ('UNIQUE','PRIMARY KEY') "
            "  AND list_transform(constraint_column_names, x -> lower(x)) = [lower(?)]",
            [table, column]).fetchall()
        for (ctype,) in crows:
            out.append({"index_name": f"{ctype} constraint", "is_unique": True, "kind": "constraint"})
    except Exception:
        pass
    return out, True


def _is_constraint_op(op) -> bool:
    """True if this synthetic create_unique_index op came from an inline UNIQUE / PRIMARY KEY
    constraint (Bypass 1) rather than a CREATE UNIQUE INDEX — marked by classify_ddl with a
    `constraint:` prefix in new_name. A constraint has no index name and thus no DROP-INDEX to pair."""
    return (op.new_name or "").lower().startswith("constraint:")


def _index_findings(lib, ddl_files, op_rules, con) -> list[dict]:
    """Deterministic dup-unique-enforcement findings across the submitted diff (the W2 floor for the
    2026-06-26 dup-unique-_key landmine). Covers BOTH a CREATE [UNIQUE] INDEX and a Bypass-1 inline
    UNIQUE / PRIMARY KEY constraint (CREATE TABLE column/table constraint or ALTER TABLE ADD CONSTRAINT
    — surfaced by classify_ddl as synthetic create_unique_index ops with new_name='constraint:<kind>').

    A new unique enforcement on a column is REFUSED (block) when the column ALREADY carries a
    same-uniqueness enforcement (index OR constraint, casing-normalized) under a different name, unless
    the SAME diff DROPs the old INDEX (expand/contract — constraints have no DROP-INDEX pairing).
    Additionally:
      * BYPASS 1 same-diff: when THIS diff itself introduces >1 unique enforcement on the SAME
        (table, column) — e.g. an inline `_key ... UNIQUE` constraint AND a `CREATE UNIQUE INDEX`
        on _key in the same submission — that is the dup landmine being CREATED in one shot; BLOCK
        even if the catalog shows nothing.
      * FAIL-CLOSED: when the serving-snapshot catalog cannot be positively read (con is None / probe
        raised — the promote-lag window where the snapshot lags the authoritative warehouse), a NEW
        unique enforcement WARNs (cannot prove the column has no existing same-column enforcement).

    Built in-floor (like the CONTRACT check) so it works without a published rule row; a matching rule
    row (match.op create_unique_index) supplies tier/code if the operator seeded one."""
    findings: list[dict] = []
    # the whole submitted diff's DROP INDEX set (expand/contract is satisfied if the old index is
    # dropped ANYWHERE in the same change, not necessarily the same file).
    dropped: set = set()
    for f in ddl_files:
        try:
            dropped |= lib.parse_dropped_indexes(f["content"])
        except Exception:
            pass
    # a pre-published rule row to honour (tier/detail), if the operator seeded one.
    idx_rule = next((r for r in op_rules
                     if "create_unique_index" in ((r["spec"] or {}).get("match") or {}).get("op", [])
                     or "create_index" in ((r["spec"] or {}).get("match") or {}).get("op", [])), None)
    block_tier = (idx_rule or {}).get("tier", "block")
    rule_code = (idx_rule or {}).get("code", "W2-INDEX")

    # PASS 1: count how many NEW unique enforcements THIS diff introduces per (table, column), so a
    # same-diff index+constraint pair on the same column (the Bypass-1 shape) is caught even against an
    # empty/lagging catalog. Key on (schema.table.lower, column.lower).
    intra: dict[tuple, int] = {}
    for f in ddl_files:
        try:
            ops = lib.classify_ddl(f["content"])
        except Exception:
            ops = []
        for op in ops:
            if op.op != "create_unique_index" or not op.column:
                continue
            _, t = lib.split_table_ref(op.table)
            intra[((t or "").lower(), op.column.lower())] = \
                intra.get(((t or "").lower(), op.column.lower()), 0) + 1

    seen_intra_block: set = set()  # emit the same-diff dup block once per (table,col)
    for f in ddl_files:
        path = f["path"]
        for op in lib.classify_ddl(f["content"]):
            if op.op not in ("create_unique_index", "create_index"):
                continue
            schema, tbl = lib.split_table_ref(op.table)
            is_constraint = _is_constraint_op(op)
            new_name = (op.new_name or "").split(".")[-1].lower()
            col = op.column  # single indexed/constrained column (composite -> None, skipped below)
            is_unique = op.op == "create_unique_index"
            disp = ("inline UNIQUE/PRIMARY KEY constraint" if is_constraint
                    else f"{'CREATE UNIQUE INDEX' if is_unique else 'CREATE INDEX'} {op.new_name}")
            cols_disp = op.extra or (col or "?")
            key = ((tbl or "").lower(), (col or "").lower())

            # BYPASS-1 SAME-DIFF: this diff itself adds >1 unique enforcement on the same column.
            if is_unique and col and intra.get(key, 0) > 1 and key not in seen_intra_block:
                seen_intra_block.add(key)
                detail = (f"This change introduces {intra[key]} unique enforcements on "
                          f"{schema}.{tbl}(`{col}`) in the SAME diff (e.g. an inline UNIQUE/PRIMARY "
                          f"KEY constraint AND a CREATE UNIQUE INDEX on the same column). Two unique "
                          f"enforcements on one column FATAL the nightly upsert ('Failed to delete all "
                          f"rows from index'). Keep exactly ONE unique enforcement on `{col}`.")
                findings.append({
                    "rule": rule_code, "tier": block_tier,
                    "severity": _TIER_SEVERITY.get(block_tier, "Error"),
                    "classification": "UNIQUE-ENFORCEMENT-DUP-SAMEDIFF",
                    "table_schema": schema, "table_name": tbl, "column_name": col,
                    "ddl_file": path, "detail": detail, "consumers": [],
                    "fix": {"kind": "single_enforcement"}, "source": "floor"})
                # fall through: still evaluate against the catalog below (additive, but de-duped by key)

            if not col:
                continue  # composite enforcement: not the single-col _key dup-FATAL class

            existing, catalog_readable = _existing_indexes_on_column(con, tbl, col)
            same_uniqueness = [e for e in existing
                               if e["is_unique"] == is_unique
                               and e["index_name"].lower() != new_name]

            if not same_uniqueness:
                # No pre-existing same-uniqueness enforcement VISIBLE on this column under another name.
                # FAIL-CLOSED: if the catalog could not be positively read (promote-lag / snapshot
                # unreadable), a NEW unique enforcement WARNs — we cannot prove there is no existing
                # same-column enforcement, and W1's catalog-delta over the AUTHORITATIVE warehouse is
                # the hard backstop. (A non-unique index does not FATAL the upsert -> no fail-closed.)
                if is_unique and not catalog_readable and key not in seen_intra_block:
                    detail = (f"{disp} adds a NEW unique enforcement on {schema}.{tbl}(`{col}`), but "
                              f"the serving-snapshot catalog could not be positively read (con "
                              f"unavailable or lagging the authoritative warehouse in the promote-lag "
                              f"window), so the floor CANNOT confirm `{col}` has no existing unique "
                              f"enforcement. Failing closed: confirm there is exactly ONE unique "
                              f"enforcement on `{col}` after apply (W1's post-apply catalog-delta gate "
                              f"is the authoritative backstop).")
                    findings.append({
                        "rule": rule_code, "tier": "warn", "severity": "Warn",
                        "classification": "UNIQUE-ENFORCEMENT-UNVERIFIED",
                        "table_schema": schema, "table_name": tbl, "column_name": col,
                        "ddl_file": path, "detail": detail, "consumers": [],
                        "fix": {"kind": "verify_single_enforcement"}, "source": "floor"})
                continue

            old_names = [e["index_name"] for e in same_uniqueness]
            # only an INDEX can be dropped to satisfy expand/contract; a pre-existing CONSTRAINT must be
            # dropped via ALTER ... DROP CONSTRAINT, which the floor does not auto-pair -> stays a block.
            droppable = [e for e in same_uniqueness if e["kind"] == "index"]
            old_dropped = bool(droppable) and all(
                e["index_name"].split(".")[-1].lower() in dropped for e in droppable) \
                and len(droppable) == len(same_uniqueness)

            if is_unique and not old_dropped:
                fix = {"kind": "expand_contract"}
                if droppable:
                    fix["drop"] = f"DROP INDEX IF EXISTS {droppable[0]['index_name']};"
                detail = (f"{disp} on {schema}.{tbl}({cols_disp}) adds a SECOND unique enforcement on "
                          f"`{col}`, which already has unique enforcement(s) [{', '.join(old_names)}]. "
                          f"Two unique enforcements on the same column FATAL the nightly upsert "
                          f"('Failed to delete all rows from index'). If this is a rename, make it "
                          f"expand/contract: DROP the old enforcement in the SAME diff"
                          + (f" (e.g. `DROP INDEX IF EXISTS {droppable[0]['index_name']};`)" if droppable else "")
                          + ". (IF NOT EXISTS does NOT make this safe — it keys off the new name and "
                          f"leaves the old enforcement in place.)")
                findings.append({
                    "rule": rule_code, "tier": block_tier,
                    "severity": _TIER_SEVERITY.get(block_tier, "Error"),
                    "classification": "UNIQUE-INDEX-DUP", "table_schema": schema, "table_name": tbl,
                    "column_name": col, "ddl_file": path, "detail": detail,
                    "consumers": [], "fix": fix, "source": "floor"})
            elif is_unique and old_dropped:
                findings.append({
                    "rule": "W2-INDEX", "tier": "info", "severity": "Info",
                    "classification": "UNIQUE-INDEX-RENAME", "table_schema": schema, "table_name": tbl,
                    "column_name": col, "ddl_file": path,
                    "detail": (f"{disp} on `{col}` pairs a DROP of the prior unique index "
                               f"({', '.join(old_names)}) in the same diff — expand/contract OK."),
                    "consumers": [], "fix": {"kind": "none"}, "source": "floor"})
            else:
                findings.append({
                    "rule": "W2-INDEX", "tier": "warn", "severity": "Warn",
                    "classification": "INDEX-DUP", "table_schema": schema, "table_name": tbl,
                    "column_name": col, "ddl_file": path,
                    "detail": (f"{disp} on `{col}` adds a 2nd index on a column that already has "
                               f"index(es) [{', '.join(old_names)}] — likely redundant; drop the old "
                               f"one if this is a rename."),
                    "consumers": [], "fix": {"kind": "expand_contract"}, "source": "floor"})
    return findings


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
    had_ddl_ops = False
    py_has_sql = False
    con = None
    try:
        con = mc.duckdb_ro_open()
    except Exception:
        con = None
    try:
        for f in ddl_files:
            path, sql = f["path"], f["content"]
            ops = lib.classify_ddl(sql)
            if ops:
                had_ddl_ops = True
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
        # W2 index expand/contract rule (deterministic, in-floor): a new UNIQUE index on a column that
        # already has a same-uniqueness index under a different name is REFUSED unless the same diff
        # DROPs the old one (the 2026-06-26 dup-unique-_key landmine). Runs once over the whole diff.
        try:
            findings.extend(_index_findings(lib, ddl_files, op_rules, con))
            if any(o.op in ("create_index", "create_unique_index")
                   for fdl in ddl_files for o in lib.classify_ddl(fdl["content"])):
                had_ddl_ops = True  # an index-only diff is still schema-relevant (-> deep review)
        except Exception:
            pass  # the index rule must never break the floor
        # py contract check (fail-loud): an INSERT column list referencing a column not in the live
        # catalog is drift. Needs the catalog (degrades to py_has_sql detection pre-P7).
        kw = {"select", "insert", "update", "delete", "from", "where", "into", "values", "table",
              "join", "on", "group", "order", "by", "core", "derived", "raw", "main", "as", "and", "or"}
        for f in (py_files or []):
            literals, parse_ok = lib.extract_sql_from_python(f["content"])
            if not parse_ok:
                continue
            for lit in literals:
                low = lit.text.lower()
                if "insert into" in low or "select" in low or "update " in low:
                    py_has_sql = True
                if lit.dynamic or not allcols:
                    continue
                cols, clean = lib.columns_referenced_q(lit.text)
                if not clean:
                    continue
                if "insert into" in low and "(" in lit.text:
                    unknown = {c for c in cols if c.isidentifier() and len(c) > 2
                               and c not in allcols and not c.startswith("_") and c not in kw}
                    if unknown:
                        findings.append({
                            "rule": "CONTRACT", "tier": "warn", "severity": "Warn",
                            "classification": "CONTRACT", "table_schema": None, "table_name": None,
                            "column_name": None, "ddl_file": f["path"],
                            "detail": f"{f['path']}:{lit.line}: INSERT references column(s) not in the live "
                                      f"catalog: {', '.join(sorted(unknown)[:5])}. Ship the DDL first, else drift.",
                            "consumers": [], "fix": {"kind": "annotate_consumer"}, "source": "floor"})
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

    floor_verdict = _verdict_from(findings)
    # The LLM deep-review only runs on a SCHEMA-relevant change — real DDL ops, SQL-bearing py, or any
    # floor finding. A pure-Python (no-SQL) edit clears the floor as a cheap no-op (no paid LLM call).
    schema_relevant = had_ddl_ops or py_has_sql or bool(findings)
    return {"rules_version": rv, "catalog_snapshot_id": snap, "findings": findings,
            "floor_verdict": floor_verdict, "catalog_built": bool(allcols),
            "schema_relevant": schema_relevant}


def floor_review_sql(sql: str, path: str = "qa.sql", py_files=None) -> dict:
    """QA convenience: run the deterministic floor over a single synthetic DDL STRING. Point the
    catalog at a scratch snapshot with `MODERATOR_DUCKDB_CURRENT=/tmp/gate-qa/<x>.duckdb` so the
    index expand/contract rule can see (or not see) an existing unique index. Equivalent to
    floor_review([{path, content: sql}], py_files)."""
    return floor_review([{"path": path, "content": sql}], py_files or [])


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
                "fix": {"type": "string"},
                "ambiguity": {"type": "string", "enum": ["unambiguous", "options"],
                              "description": "unambiguous = ONE correct deterministic fix (auto-apply); "
                                             "options = materially-different valid fixes — the SUBMITTING "
                                             "HUMAN must choose; you must NOT pick. Default to 'options' "
                                             "whenever judgement/intent is involved (expected most of the time)."},
                "options": {"type": "array", "items": {"type": "string"},
                            "description": "when ambiguity='options', the 2+ materially-different fixes to "
                                           "present to the submitter to choose between."}},
                "required": ["severity", "detail", "ambiguity"]}},
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
    "concrete fixes. Respond ONLY via the report_deep_review tool.\n\n"
    "AUTONOMY OF FIXES: for each finding set `ambiguity`. Use 'unambiguous' ONLY when there is exactly "
    "ONE correct deterministic fix (e.g. a pure mechanical rename to the canonical name). Use 'options' "
    "— and fill `options` with the 2+ materially-different valid fixes — whenever judgement or intent is "
    "involved (which is MOST of the time: which canonical name, whether a change is a rename vs a new "
    "column, what the down-migration should preserve). You MUST NOT choose between materially-different "
    "options — the SUBMITTING human owns that judgement. Never defer to Sam; ambiguity goes to the person "
    "doing the work.\n\n"
    "INDEX INTEGRITY (a known landmine — see the live schema snapshot's dup_unique_key_flag and the "
    "reviewer learnings): a 2nd UNIQUE index on a column that already has one (classically `_key` on a "
    "raw_pipeline_* table) FATALs the nightly upsert ('Failed to delete all rows from index'). A "
    "`CREATE UNIQUE INDEX IF NOT EXISTS <new>` is NOT safe as a 'rename' — IF NOT EXISTS keys off the "
    "new name and leaves the old index in place. BLOCK an index change that would leave two unique "
    "indexes on the same column unless the same diff DROPs the old one (expand/contract)."
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
            "detail": x.get("detail", ""),
            "fix": {"kind": "llm", "ambiguity": x.get("ambiguity", "options"),
                    "steps": [x.get("fix", "")] if x.get("fix") else [],
                    "options": x.get("options") or []},
            "consumers": [], "source": "llm",
        } for x in (payload.get("findings") or [])]
        return {"status": verdict, "findings": findings,
                "reasoning": payload.get("reasoning", ""), "model": mc.LLM_MODEL}
    except Exception as e:  # network / API / SDK — fail-closed is decided by the caller
        mc.log_event("llm_deep_review_error", error=f"{type(e).__name__}: {e}")
        return {"status": "unavailable", "findings": [],
                "reasoning": f"{type(e).__name__}: {e}", "model": mc.LLM_MODEL}


# ── W3 self-updating context: live-schema snapshot (incl. dup-unique flag) + reviewer learnings ──
def _live_schema_snapshot(con, touched_tables) -> str:
    """Live-schema snapshot the LLM sees at review time: the dup-unique-_key flag per raw_pipeline_*
    table (the 2026-06-26 guard), the per-table column list, and sync_registry status for the touched
    objects. Read-only over the serving snapshot. Returns a markdown block (best-effort; '' on miss)."""
    if con is None:
        return ""
    tbls = sorted({t for t in (touched_tables or []) if t})
    in_clause = ", ".join("?" for _ in tbls) if tbls else None
    lines: list[str] = []
    # (a.1) DUPLICATE-UNIQUE-INDEX FLAG — the headline guard.
    try:
        rows = con.execute(
            "SELECT table_name, count(*) AS n, (count(*) > 1) AS dup_flag, "
            "       string_agg(index_name, ', ' ORDER BY index_name) AS idxs "
            "FROM duckdb_indexes() "
            "WHERE table_name LIKE 'raw_pipeline%' AND is_unique "
            "  AND trim(expressions, '[]') = '_key' "
            "GROUP BY table_name ORDER BY dup_flag DESC, table_name").fetchall()
        if rows:
            lines.append("## Unique-index-on-_key per raw_pipeline_* table "
                         "(dup_unique_key_flag=TRUE => a 2nd unique _key index would FATAL the upsert):")
            for tn, n, dup, idxs in rows:
                lines.append(f"- {tn}: {n} unique _key index(es) [{idxs}]"
                             + ("  <-- DUPLICATE (already at risk)" if dup else ""))
    except Exception:
        pass
    # (a.2) PER-TABLE COLUMN LIST for the touched tables (+ raw_pipeline family).
    try:
        if in_clause:
            q = ("SELECT table_schema, table_name, "
                 "string_agg(column_name, ', ' ORDER BY ordinal_position) AS cols "
                 "FROM information_schema.columns "
                 f"WHERE table_name IN ({in_clause}) OR table_name LIKE 'raw_pipeline%' "
                 "GROUP BY table_schema, table_name ORDER BY table_schema, table_name")
            rows = con.execute(q, tbls).fetchall()
        else:
            rows = con.execute(
                "SELECT table_schema, table_name, "
                "string_agg(column_name, ', ' ORDER BY ordinal_position) AS cols "
                "FROM information_schema.columns WHERE table_name LIKE 'raw_pipeline%' "
                "GROUP BY table_schema, table_name ORDER BY table_schema, table_name").fetchall()
        if rows:
            lines.append("## Columns of touched tables (ordinal order):")
            for sch, tn, cols in rows:
                lines.append(f"- {sch}.{tn}: {cols}")
    except Exception:
        pass
    # (a.3) SYNC_REGISTRY status for the touched objects (send-sensitivity / cadence / freshness).
    try:
        if in_clause:
            q = ("SELECT name, table_schema, status, expected_cadence, sla_hours, is_send_sensitive, "
                 "last_synced_at, last_biz_date, last_row_delta, row_count FROM core.sync_registry "
                 f"WHERE replace(name, table_schema || '.', '') IN ({in_clause}) "
                 f"OR name IN ({in_clause}) OR name LIKE 'raw_pipeline%' "
                 "ORDER BY is_send_sensitive DESC, table_schema, name")
            rows = con.execute(q, tbls + tbls).fetchall()
        else:
            rows = con.execute(
                "SELECT name, table_schema, status, expected_cadence, sla_hours, is_send_sensitive, "
                "last_synced_at, last_biz_date, last_row_delta, row_count FROM core.sync_registry "
                "WHERE name LIKE 'raw_pipeline%' ORDER BY is_send_sensitive DESC, table_schema, name").fetchall()
        if rows:
            lines.append("## sync_registry status for touched objects:")
            for r in rows:
                lines.append(f"- {r[0]} [{r[2]}] cadence={r[3]} sla_h={r[4]} send_sensitive={r[5]} "
                             f"last_synced={r[6]} last_biz={r[7]} last_delta={r[8]} rows={r[9]}")
    except Exception:
        pass
    return ("# Live schema snapshot (read at review time)\n" + "\n".join(lines) + "\n") if lines else ""


def _recent_learnings(con, limit: int = 25) -> str:
    """Recent active reviewer learnings (block-level first, then most recent) rendered for the prompt.
    Reads core.review_learnings on the same RO connection. '' if the table isn't present yet."""
    if con is None:
        return ""
    try:
        # cast created_at to VARCHAR: fetching a TIMESTAMPTZ via py-duckdb requires pytz (not always
        # installed in the moderator venv) and would raise; the prompt doesn't render the timestamp.
        rows = con.execute(
            "SELECT id, CAST(created_at AS VARCHAR), category, severity, outcome, rule_text, lesson, "
            "       example_ddl, source_incident FROM core.review_learnings "
            "WHERE NOT superseded ORDER BY (severity = 'block') DESC, created_at DESC LIMIT ?",
            [limit]).fetchall()
    except Exception:
        return ""  # table not created yet (W3 DDL staged, not applied) — degrade silently
    if not rows:
        return ""
    out = ["# Reviewer learnings (recent — apply these; highest-priority first)"]
    for r in rows:
        rid, created, cat, sev, outcome, rule_text, lesson, example, incident = r
        out.append(f"- [{sev}/{cat}/{outcome}] RULE: {rule_text}")
        if lesson:
            out.append(f"    why: {lesson}")
        if incident:
            out.append(f"    incident: {incident}")
    return "\n".join(out) + "\n"


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
        con = mc.duckdb_ro_open()
    except Exception:
        con = None
    touched_tables: set = set()
    live_snapshot = ""
    learnings_block = ""
    try:
        seen = set()
        for f in ddl_files:
            for op in lib.classify_ddl(f["content"]):
                schema, tbl = lib.split_table_ref(op.table)
                if tbl:
                    touched_tables.add(tbl)
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
        # W3: fetch the live-schema snapshot (with the dup-unique flag) + recent reviewer learnings on
        # the SAME read-only connection, at review time, so the LLM sees the live duplicate-index state
        # and the lessons the gate has learned.
        try:
            live_snapshot = _live_schema_snapshot(con, touched_tables)
        except Exception:
            live_snapshot = ""
        try:
            learnings_block = _recent_learnings(con)
        except Exception:
            learnings_block = ""
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    if learnings_block:
        parts.append(learnings_block)
    if live_snapshot:
        parts.append(live_snapshot)
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
    elif not floor.get("schema_relevant", True):
        llm = {"status": "skipped-not-schema", "findings": [], "reasoning": "", "model": None}
        verdict = floor_verdict  # pure non-schema change — cheap no-op, no paid LLM call
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


# ── DDL/schema_version ALLOCATOR (auto-assign the next number; collision-proof across writers) ────
# The v96 incident: David picked version 96 from a STALE local checkout while origin/main was already
# at 113 — a number collision the bus-local `ddl-number` wlock can't prevent (a laptop writer bypasses
# it). The moderator IS the chokepoint every writer hits, so the authoritative allocator lives here.
# next = 1 + max over EVERY source that could already own a number; reserved atomically so two
# concurrent callers never get the same one. Never reuses a GAP (a gap number may be applied-but-not-
# in-repo or vice versa — reuse would re-collide).
def _ensure_version_reservation(cur) -> None:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS moderator.version_reservation ("
        "  version integer PRIMARY KEY,"
        "  reserved_by text,"
        "  request_id text,"
        "  reserved_at timestamptz NOT NULL DEFAULT now())")


def _fetch_origin_main() -> None:
    """Best-effort `git fetch origin main` so origin/main is current before we read it (a number that
    just merged upstream but isn't yet on this box would otherwise be UNDERCOUNTED -> a re-collision,
    the exact class we prevent). Tolerates failure: a stale ref only undercounts, and the apply-time
    apply==commit check is the backstop. Disable with MODERATOR_APPLY_FETCH=0 (shared with apply)."""
    if os.environ.get("MODERATOR_APPLY_FETCH", "1") in ("0", "false", "False", ""):
        return
    try:
        subprocess.run(["git", "-C", mc.WAREHOUSE_ROOT, "fetch", "origin", "main"],
                       capture_output=True, timeout=30)
    except Exception:
        pass


def _max_repo_ddl_version() -> int:
    """Highest NN among committed sql/ddl/NN_*.sql at origin/main (the repo's view). 0 if unknown."""
    try:
        p = subprocess.run(
            ["git", "-C", mc.WAREHOUSE_ROOT, "ls-tree", "-r", "--name-only", "origin/main", "sql/ddl/"],
            capture_output=True, text=True, timeout=30)
        if p.returncode != 0:
            return 0
        best = 0
        for line in p.stdout.splitlines():
            stem = os.path.basename(line.strip()).split("_", 1)[0]
            if stem.isdigit():
                best = max(best, int(stem))
        return best
    except Exception:
        return 0


def _max_applied_version() -> int:
    """Highest version applied in the live DB (core.schema_version, read off the serving snapshot)."""
    try:
        with mc.duckdb_ro() as con:
            v = con.execute("SELECT max(version) FROM core.schema_version").fetchone()[0]
            return int(v) if v is not None else 0
    except Exception:
        return 0


def next_schema_version(actor: str | None = None, request_id: str | None = None) -> dict:
    """Allocate + RESERVE the next free DDL/schema_version number, authoritatively across ALL writers.
    next = 1 + max over {committed sql/ddl/NN at origin/main, applied core.schema_version, in-flight
    apply_queue+approval_ledger, prior reservations}. The reserve is an atomic PK insert with retry,
    so two concurrent callers get DISTINCT numbers. Returns {version, reserved, sources, suggested_file}."""
    _fetch_origin_main()  # current origin/main before reading it (don't undercount a just-merged number)
    repo_max = _max_repo_ddl_version()
    applied_max = _max_applied_version()
    sources = {"repo_max": repo_max, "applied_max": applied_max}
    with mc.pg_conn() as c, c.cursor() as cur:
        _ensure_version_reservation(cur)
        cur.execute("SELECT coalesce(max(ddl_version),0) FROM moderator.apply_queue "
                    "WHERE status IN ('queued','reviewing','applying')")
        queue_max = cur.fetchone()[0] or 0
        cur.execute("SELECT coalesce(max(ddl_version),0) FROM moderator.approval_ledger")
        ledger_max = cur.fetchone()[0] or 0
        sources.update(queue_max=queue_max, ledger_max=ledger_max)
        base = max(repo_max, applied_max, queue_max, ledger_max)
        # Reserve atomically: candidate = max(base, max(reservation))+1; INSERT; on PK clash bump+retry.
        # autocommit conn -> a failed INSERT is its own rolled-back txn, so the retry runs clean.
        for _ in range(50):
            cur.execute("SELECT coalesce(max(version),0) FROM moderator.version_reservation")
            res_max = cur.fetchone()[0] or 0
            candidate = max(base, res_max) + 1
            try:
                cur.execute(
                    "INSERT INTO moderator.version_reservation (version, reserved_by, request_id) "
                    "VALUES (%s,%s,%s)", (candidate, actor or "?", request_id))
                sources["reservation_max_before"] = res_max
                return {"version": candidate, "reserved": True, "sources": sources,
                        "suggested_file": f"sql/ddl/{candidate}_<name>.sql"}
            except Exception:
                continue  # another caller grabbed `candidate` between our read + insert — retry
    return {"version": None, "reserved": False, "sources": sources,
            "error": "could not reserve a version after 50 attempts (contention?) — retry"}


# ── §8 rule-evolution engine: deterministic weekly detection -> rule_proposal ────────────────────
def _draft_for_cluster(rule: str, classification: str | None, col: str | None, aliases: dict) -> dict:
    """A ready-to-edit draft for a recurring-finding cluster. The human confirms/edits before
    promote; nothing auto-changes a rule. Drafts use the publish_rules vocabulary."""
    if classification == "DUPE" and col in aliases:
        canon = aliases[col]
        return {"kind": "note",
                "summary": f"`{col}` is already a declared alias of `{canon}` but keeps getting "
                           f"flagged + waived. Tighten R3 to block this alias, or confirm the waivers."}
    if classification in ("DUPE", "NAMING"):
        return {"kind": "alias", "aliases_add": [{"alias": col, "canonical_name": "<SET_CANONICAL>",
                                                  "reason": f"recurring {classification} on {col}"}],
                "summary": f"`{col}` recurs as a {classification} finding. Add an alias to its canonical "
                           f"(fill <SET_CANONICAL>) and promote, or reject if intentional."}
    return {"kind": "note",
            "summary": f"{rule}/{classification} recurs on `{col}` — review whether a rule/tier change "
                       f"is warranted (edit the draft to an upsert_rules/aliases_add change to promote)."}


def detect_proposals(window_days: int = 7, min_count: int = 3) -> dict:
    """Deterministic, keyless clustering of recent issues into rule_proposal rows. Dedupes against
    pending/snoozed proposals with the same pattern. Returns counts."""
    created = 0
    aliases = load_aliases()
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT rule, classification, lower(column_name) AS col, count(*) AS n, "
            "array_agg(issue_id) AS ids FROM moderator.issue "
            "WHERE created_at > now() - make_interval(days => %s) AND column_name IS NOT NULL "
            "GROUP BY rule, classification, lower(column_name) HAVING count(*) >= %s",
            (window_days, min_count))
        clusters = cur.fetchall()
        for rule, classification, col, n, ids in clusters:
            pattern = f"{rule}/{classification} recurring on column `{col}` ({n}x/{window_days}d)"
            cur.execute("SELECT 1 FROM moderator.rule_proposal WHERE pattern=%s "
                        "AND status IN ('pending','snoozed') LIMIT 1", (pattern,))
            if cur.fetchone():
                continue
            draft = _draft_for_cluster(rule, classification, col, aliases)
            cur.execute(
                "INSERT INTO moderator.rule_proposal (pattern, evidence, draft_rule) VALUES (%s,%s,%s)",
                (pattern, json.dumps({"issue_ids": list(ids), "count": n, "window_days": window_days,
                                      "rule": rule, "classification": classification, "column": col}),
                 json.dumps(draft)))
            created += 1
    return {"clusters": len(clusters), "proposals_created": created,
            "window_days": window_days, "min_count": min_count}


def list_proposals(status: str = "pending") -> list[dict]:
    cols = ["proposal_id", "pattern", "evidence", "draft_rule", "status", "detected_at"]
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute(
            "SELECT proposal_id, pattern, evidence, draft_rule, status, CAST(detected_at AS text) "
            "FROM moderator.rule_proposal WHERE (%s='all' OR status=%s) "
            "ORDER BY proposal_id DESC LIMIT 200", (status, status))
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _set_proposal_status(pid: int, status: str, decided_by: str) -> None:
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute("UPDATE moderator.rule_proposal SET status=%s, decided_by=%s, decided_at=now() "
                    "WHERE proposal_id=%s", (status, decided_by, pid))


def decide_proposal(pid: int, decision: str, decided_by: str, edit: dict | None = None) -> dict:
    """promote (optionally with an edited draft) | reject | snooze. promote applies the draft's
    aliases_add/upsert_rules via publish_rules (a new rules_version). This is the ONLY human touch
    in rule evolution (weekly), never per schema-change."""
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute("SELECT draft_rule FROM moderator.rule_proposal WHERE proposal_id=%s", (pid,))
        row = cur.fetchone()
    if not row:
        raise ValueError(f"proposal {pid} not found")
    draft = row[0] if isinstance(row[0], dict) else json.loads(row[0])
    if decision == "promote":
        d = edit or draft
        new_v = None
        if d.get("aliases_add") or d.get("upsert_rules") or d.get("disable_codes"):
            new_v = publish_rules(
                upsert_rules=d.get("upsert_rules", []), disable_codes=d.get("disable_codes", []),
                aliases_add=d.get("aliases_add", []), note=f"promoted rule_proposal {pid}",
                published_by=decided_by, source="weekly-auto-proposal")
        _set_proposal_status(pid, "promoted", decided_by)
        return {"promoted": pid, "rules_version": new_v}
    if decision in ("reject", "snooze"):
        _set_proposal_status(pid, "rejected" if decision == "reject" else "snoozed", decided_by)
        return {decision: pid}
    raise ValueError("decision must be one of: promote | reject | snooze")


_CREATE_TABLE_RE = __import__("re").compile(r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)", __import__("re").IGNORECASE)
_CREATE_VIEW_RE = __import__("re").compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<v>[\w.\"]+)", __import__("re").IGNORECASE)


def generate_down_migration(sql: str) -> tuple[str, bool]:
    """Best-effort deterministic reverse of a forward DDL (§ Ledger -> rollback). Returns
    (down_sql, fully_reversible). Reversible ops emit exact inverses; lossy ops (DROP/ALTER TYPE)
    emit a commented marker pointing at the before-snapshot — never a silent wrong reverse. The
    down is ADVISORY: a human reviews it before an actual rollback."""
    lib = mc.engine()
    created_raw = [m.group("tbl") for m in _CREATE_TABLE_RE.finditer(sql)]
    created_view = [m.group("v") for m in _CREATE_VIEW_RE.finditer(sql)]
    # unqualified names of created tables — ops on them are covered by the DROP TABLE (skip them).
    created_unq = {lib.split_table_ref(t)[1] for t in created_raw}
    lines: list[str] = ["-- AUTO-GENERATED best-effort reverse; REVIEW before applying."]
    reversible = True
    for op in lib.classify_ddl(sql):
        t = op.table
        _, t_unq = lib.split_table_ref(t)
        if t_unq in created_unq:
            continue  # table is created in this same migration; DROP TABLE below reverses it whole
        if op.op == "add_column":
            lines.append(f"ALTER TABLE {t} DROP COLUMN IF EXISTS {op.column};")
        elif op.op == "rename_column":
            lines.append(f"ALTER TABLE {t} RENAME COLUMN {op.new_name} TO {op.column};")
        elif op.op == "rename_table":
            # RENAME TO target must be UNQUALIFIED (can't move schemas); the subject is the new name.
            lines.append(f"ALTER TABLE {op.new_name} RENAME TO {t_unq};")
        elif op.op == "set_not_null":
            lines.append(f"ALTER TABLE {t} ALTER COLUMN {op.column} DROP NOT NULL;")
        elif op.op in ("drop_column", "drop_table"):
            reversible = False
            lines.append(f"-- IRREVERSIBLE: {op.op} {t}.{op.column or ''} drops data; restore from "
                         f"before_ddl / the warehouse backup snapshot (catalog_version).")
        elif op.op == "alter_type":
            reversible = False
            lines.append(f"-- MANUAL: ALTER TYPE on {t}.{op.column} — reverse needs the prior type "
                         f"(see before_ddl).")
    for t in created_raw:
        lines.append(f"DROP TABLE IF EXISTS {t};")
    for v in created_view:
        lines.append(f"DROP VIEW IF EXISTS {v};")
    return ("\n".join(lines), reversible)


def _before_ddl_snapshot(ddl_files) -> str | None:
    """Best-effort 'before' snapshot of the objects the change touches: their current column lists
    from the live catalog. Degrades to None pre-substrate (catalog not built)."""
    lib = mc.engine()
    tables = set()
    for f in ddl_files:
        for op in lib.classify_ddl(f["content"]):
            schema, tbl = lib.split_table_ref(op.table)
            if tbl:
                tables.add((schema, tbl))  # keep schema so same-named tables don't merge
    if not tables:
        return None
    out = []
    try:
        with mc.duckdb_ro() as con:
            for schema, tbl in sorted(tables):
                try:
                    rows = con.execute(
                        "SELECT table_schema, column_name, data_type, is_nullable "
                        "FROM core.schema_catalog WHERE lower(table_name)=lower(?) "
                        "AND lower(table_schema)=lower(?) ORDER BY ordinal_position", [tbl, schema]).fetchall()
                    if rows:
                        cols = ", ".join(f"{r[1]} {r[2]}{'' if r[3] else ' NOT NULL'}" for r in rows)
                        out.append(f"-- {rows[0][0]}.{tbl} BEFORE: {cols}")
                except Exception:
                    continue
    except Exception:
        return None
    return "\n".join(out) or None


# ── CORE self-improvement loops (§ Self-improvement): escape->rule, false-positive->relax ─────────
def record_feedback(kind: str, detail: str, actor: str, ddl_file: str | None = None,
                    evidence: dict | None = None) -> dict:
    """Turn a gate ESCAPE (a break that got through) or a FALSE-POSITIVE (a human-overridden BLOCK)
    into a rule_proposal, so the weekly ~10s human-confirm evolves the rules. escape -> propose a NEW
    rule to catch the class; false_positive -> propose RELAXING the offending rule. Never auto-changes
    a rule (still gated on the weekly confirm)."""
    if kind not in ("escape", "false_positive"):
        raise ValueError("kind must be 'escape' or 'false_positive'")
    tag = "ESCAPE" if kind == "escape" else "FALSE-POSITIVE"
    pattern = f"{tag}: {detail[:160]}"
    if kind == "escape":
        draft = {"kind": "note", "summary": f"A change ESCAPED the gate and broke something: {detail}. "
                 "Propose a NEW rule/check to catch this class (edit the draft to an upsert_rules change to promote)."}
    else:
        draft = {"kind": "note", "summary": f"A human OVERRODE a BLOCK as a false positive: {detail}. "
                 "Propose RELAXING the offending rule — soften its tier or add an exemplar/alias (edit to promote)."}
    ev = evidence or {}
    ev.update({"detail": detail, "ddl_file": ddl_file, "actor": actor, "origin": kind})
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute("INSERT INTO moderator.rule_proposal (pattern, evidence, draft_rule) "
                    "VALUES (%s,%s,%s) RETURNING proposal_id", (pattern, json.dumps(ev), json.dumps(draft)))
        pid = cur.fetchone()[0]
    mc.log_event("feedback_recorded", kind=kind, proposal_id=pid, actor=actor)
    return {"proposal_id": pid, "kind": kind, "pattern": pattern}


# ── Concurrency: serialized + QA-gated APPLY queue (§ Concurrency) ────────────────────────────────
# REVIEW is parallel (stateless /review,/record-pass). APPLY is FIFO behind one advisory lock; each
# item is RE-REVIEWED against the now-current catalog on dequeue (a prior apply may have moved the
# schema), then [SUBSTRATE HOOK] applied to the warehouse + canary'd on a serving copy, green=commit/
# red=rollback. Pre-substrate the physical apply is the nightly under the writer flock; this provides
# the durable FIFO + re-review-on-dequeue. Items are claimed by a per-item atomic short transaction
# (pooler-safe), NOT a session advisory lock (unreliable over the Supavisor transaction pooler).


def enqueue_apply(ddl_files, actor, branch, request_id) -> dict:
    enq = []
    with mc.pg_conn() as c, c.cursor() as cur:
        for f in ddl_files:
            ver = _ddl_version_of(f["path"])
            sha = hashlib.sha256(f["content"].encode("utf-8")).hexdigest()
            try:
                # dedup against LIVE (non-terminal) statuses — a re-enqueue while already in flight is a no-op;
                # a committed/failed prior row does NOT block re-submitting the same content.
                cur.execute(
                    "INSERT INTO moderator.apply_queue (request_id, ddl_version, sql_file, "
                    "content_sha256, content, actor, branch) "
                    "SELECT %s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS ("
                    "  SELECT 1 FROM moderator.apply_queue WHERE content_sha256=%s "
                    "  AND status IN ('queued','reviewing','applying')) RETURNING queue_id",
                    (request_id, ver, os.path.basename(f["path"]), sha, f["content"], actor, branch, sha))
                row = cur.fetchone()
                if row:
                    enq.append({"queue_id": row[0], "sql_file": os.path.basename(f["path"]), "ddl_version": ver})
            except Exception as e:
                mc.log_event("enqueue_error", error=f"{type(e).__name__}: {e}", file=f["path"])
    return {"enqueued": enq}


def apply_queue_status(status: str = "all") -> list[dict]:
    cols = ["queue_id", "request_id", "ddl_version", "sql_file", "actor", "status", "enqueued_at"]
    with mc.pg_conn() as c, c.cursor() as cur:
        cur.execute("SELECT queue_id, CAST(request_id AS text), ddl_version, sql_file, actor, status, "
                    "CAST(enqueued_at AS text) FROM moderator.apply_queue "
                    "WHERE (%s='all' OR status=%s) ORDER BY enqueued_at LIMIT 200", (status, status))
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def process_apply_queue(max_items: int = 10) -> dict:
    """Drain the FIFO. Each item is CLAIMED atomically in a SHORT transaction (FOR UPDATE SKIP LOCKED
    + flip to 'reviewing' — pooler-safe; no two processors take the same row), then RE-REVIEWED
    against the CURRENT rules+catalog (never stale) OUTSIDE the claim txn (LLM latency is fine here),
    then marked committed/failed in a short txn. The PHYSICAL warehouse apply + canary-on-serving-copy
    is the SUBSTRATE HOOK; true single-applier serialization binds there on the warehouse writer flock.
    Pre-substrate the nightly performs the physical apply against the now-ledgered, re-reviewed DDL.

    Session-level advisory locks are NOT used: they don't serialize reliably over the Supavisor
    transaction pooler (the backend can change between autocommit statements). The per-item atomic
    claim is the correct pooler-safe primitive."""
    processed = []
    for _ in range(max_items):
        claimed = None
        with mc.pg_conn() as c:
            with c.transaction():
                with c.cursor() as cur:
                    cur.execute("SELECT queue_id, content, sql_file, ddl_version, actor, branch, "
                                "CAST(request_id AS text) FROM moderator.apply_queue "
                                "WHERE status='queued' ORDER BY enqueued_at "
                                "FOR UPDATE SKIP LOCKED LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        cur.execute("UPDATE moderator.apply_queue SET status='reviewing', "
                                    "started_at=now() WHERE queue_id=%s", (row[0],))
                        claimed = row
        if not claimed:
            break
        qid, content, sql_file, ver, actor, branch, req = claimed
        try:
            rp = record_pass([{"path": sql_file or f"{ver or 0}_x.sql", "content": content}], [],
                             actor, branch, req)
            final = "failed" if (rp.get("rejected") or rp.get("verdict") == "block") else "committed"
            result = {"verdict": rp.get("verdict"), "canary": "substrate-pending",
                      "recorded": rp.get("recorded")}
        except Exception as e:
            final, result = "failed", {"error": f"{type(e).__name__}: {e}"}
        with mc.pg_conn() as c, c.cursor() as cur:
            cur.execute("UPDATE moderator.apply_queue SET status=%s, finished_at=now(), result=%s "
                        "WHERE queue_id=%s", (final, json.dumps(result), qid))
        processed.append({"queue_id": qid, "result": final, "verdict": result.get("verdict")})
    return {"processed": processed}


def record_pass(ddl_files, py_files, actor, branch, request_id, reason=None) -> dict:
    """Re-gate server-side against LIVE rules+catalog (incl. py contract + LLM), then write ONE
    content-hash-bound ledger row per ddl file IFF verdict != block. The ONLY way a pass enters
    moderator.approval_ledger."""
    result = review(ddl_files, py_files or [])
    rv = result["rules_version"]
    common = {"verdict": result["verdict"], "rules_version": rv, "findings": result["findings"],
              "floor_verdict": result["floor_verdict"], "llm_status": result["llm_status"],
              "llm_reasoning": result["llm_reasoning"], "catalog_snapshot_id": result["catalog_snapshot_id"]}
    if result["verdict"] == "block":
        return {"recorded": [], "rejected": True,
                "detail": "server re-gate returned BLOCK — pass NOT recorded; fix and resubmit.",
                **common}
    gate_version = mc.GATE_VERSION
    catalog_version = result.get("catalog_snapshot_id")
    before_ddl = _before_ddl_snapshot(ddl_files)  # rollback restore reference (None pre-substrate)
    recorded = []
    with mc.pg_conn() as c, c.cursor() as cur:
        for f in ddl_files:
            ver = _ddl_version_of(f["path"])
            if ver is None:
                continue
            sha = hashlib.sha256(f["content"].encode("utf-8")).hexdigest()
            down_migration, _reversible = generate_down_migration(f["content"])
            try:
                cur.execute(
                    "INSERT INTO moderator.approval_ledger (ddl_version, sql_file, content_sha256, "
                    "verdict, rules_version, gate_version, findings, actor, branch, request_id, "
                    "before_ddl, down_migration, reason, catalog_version) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (ddl_version, content_sha256) DO NOTHING",
                    [ver, os.path.basename(f["path"]), sha, result["verdict"], rv, gate_version,
                     json.dumps(result["findings"]), actor, branch, request_id,
                     before_ddl, down_migration, reason, catalog_version])
                recorded.append({"ddl_version": ver, "sql_file": os.path.basename(f["path"]),
                                 "content_sha256": sha, "verdict": result["verdict"],
                                 "new": cur.rowcount == 1})  # False = already in ledger (idempotent)
            except Exception as e:
                mc.log_event("ledger_write_error", error=f"{type(e).__name__}: {e}", file=f["path"])
    return {"recorded": recorded, "rejected": False, **common}


# ── QA CLI: run the deterministic floor over a synthetic DDL string/file ───────────────────────────
# `python moderator_engine.py --floor-sql <file.sql>`  (or  --floor-inline "<ddl>")
# Point the catalog at a scratch snapshot:  MODERATOR_DUCKDB_CURRENT=/tmp/gate-qa/x.duckdb
# Exits nonzero when floor_verdict == 'block' so QA can assert the W2 index rule blocks.
if __name__ == "__main__":
    import argparse as _argparse
    _ap = _argparse.ArgumentParser(description="QA: run the deterministic floor over a DDL string.")
    _g = _ap.add_mutually_exclusive_group(required=True)
    _g.add_argument("--floor-sql", help="path to a .sql file to floor-review")
    _g.add_argument("--floor-inline", help="inline DDL string to floor-review")
    _ap.add_argument("--path", default="qa.sql", help="logical path/name for the DDL")
    _args = _ap.parse_args()
    _sql = open(_args.floor_sql).read() if _args.floor_sql else _args.floor_inline
    _res = floor_review_sql(_sql, path=_args.path)
    print(json.dumps({"floor_verdict": _res["floor_verdict"],
                      "rules_version": _res["rules_version"],
                      "catalog_built": _res["catalog_built"],
                      "catalog_snapshot_id": _res["catalog_snapshot_id"],
                      "findings": _res["findings"]}, indent=2, default=str))
    raise SystemExit(1 if _res["floor_verdict"] == "block" else 0)
