"""Schema-gate shared library — pure, importable helpers for the gate + manifest + QA.

No side effects on import. Holds:
  * the DDL classifier (Atlas migrate-lint taxonomy)
  * the canonical-naming / alias dupe checks
  * the sqlglot static-lineage extractor (SQL files + SQL-in-Python literals)
  * SCHEMA_GATE_VERSION + GATE_SCHEMAS

PHASE 1 IS WARN-ONLY. Nothing here ever blocks; callers decide. This module only
*classifies* and *finds consumers*; the executable wrappers (scripts/schema_gate.py,
entities/schema_manifest.py) write the rows and choose to warn vs block.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path

# Bumped when classification / lineage logic changes materially (recorded on each pass).
SCHEMA_GATE_VERSION = "phase1-1.0"

# Schemas this gate reasons about (warehouse-owned). information_schema, pg_*, temp
# scratch (_run_*), etc. are out of scope.
GATE_SCHEMAS = ("main", "core", "derived", "raw")

# Approved abbreviations (everything else in a new name should be spelled out).
APPROVED_ABBREVS = {
    "id", "ids", "url", "uri", "ip", "dns", "mx", "spf", "dkim", "dmarc", "ptr",
    "ts", "tz", "ms", "pct", "qty", "num", "avg", "min", "max", "sum", "cnt",
    "sms", "esp", "crm", "kpi", "eop", "rr", "cta", "uk", "us", "ca", "au",
    "api", "db", "sql", "csv", "json", "html", "sla", "im", "iam", "otd", "cc",
    "mca", "ns", "raw", "src", "uuid", "e164", "mv",
}

# Unit/type suffixes we want in names (advisory naming-quality signal).
UNIT_SUFFIXES = ("_count", "_pct", "_at", "_date", "_ts", "_amount", "_usd",
                 "_seconds", "_minutes", "_hours", "_days", "_rate", "_ratio",
                 "_id", "_email", "_phone", "_url", "_flag", "_is_active")

SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_INTENT_RE = re.compile(r"--\s*@gate\s*:\s*(?P<intent>[^\n]+)", re.IGNORECASE)
_DEPENDS_RE = re.compile(r"--\s*Depends on\s+(?P<dep>[0-9,\s]+)", re.IGNORECASE)


# ── Findings ──────────────────────────────────────────────────────────────────
@dataclass
class Finding:
    rule: str                       # R1..R6 / DRIFT / CONTRACT
    severity: str                   # Error | Warn | Info
    detail: str
    classification: str | None = None
    table_schema: str | None = None
    table_name: str | None = None
    column_name: str | None = None
    consumers: list[str] = field(default_factory=list)  # ["file:line", ...]


# ── DDL classification (Atlas migrate-lint taxonomy) ──────────────────────────
# Lightweight regex classifier. sqlglot's DuckDB ALTER parsing is uneven, and we only
# need to *recognize* the dangerous op classes + the touched (table, column) — not
# fully parse them. Conservative: anything ambiguous classifies toward the riskier tier.

_RE_DROP_COL = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+DROP\s+(?:COLUMN\s+)?(?:IF\s+EXISTS\s+)?(?P<col>[\w\"]+)",
    re.IGNORECASE,
)
_RE_DROP_TABLE = re.compile(
    r"\bDROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)",
    re.IGNORECASE,
)
_RE_RENAME_COL = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+RENAME\s+(?:COLUMN\s+)?(?P<old>[\w\"]+)\s+TO\s+(?P<new>[\w\"]+)",
    re.IGNORECASE,
)
_RE_RENAME_TABLE = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+RENAME\s+TO\s+(?P<new>[\w.\"]+)",
    re.IGNORECASE,
)
_RE_ALTER_TYPE = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+(?:ALTER|MODIFY)\s+(?:COLUMN\s+)?(?P<col>[\w\"]+)\s+(?:SET\s+DATA\s+)?TYPE\s+(?P<type>[\w()]+)",
    re.IGNORECASE,
)
_RE_SET_NOT_NULL = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+ALTER\s+(?:COLUMN\s+)?(?P<col>[\w\"]+)\s+SET\s+NOT\s+NULL",
    re.IGNORECASE,
)
# ADD COLUMN only. Negative lookahead excludes constraint adds (ADD PRIMARY KEY / ADD
# CONSTRAINT / ADD FOREIGN KEY / ADD UNIQUE / ADD CHECK) which are NOT new columns and
# would otherwise misclassify as add_column(column='PRIMARY'/'CONSTRAINT'/...) and emit a
# bogus naming finding. (Adversarial-review LOW fix.)
_RE_ADD_COL = re.compile(
    r"\bALTER\s+TABLE\s+(?P<tbl>[\w.\"]+)\s+ADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?!PRIMARY\b|CONSTRAINT\b|FOREIGN\b|UNIQUE\b|CHECK\b|KEY\b|INDEX\b)"
    r"(?P<col>[\w\"]+)\s+(?P<rest>[^,;)]*)",
    re.IGNORECASE,
)


@dataclass
class DDLOp:
    op: str                         # drop_column | drop_table | rename_column | rename_table
                                    # | alter_type | set_not_null | add_column
    classification: str             # DESTRUCTIVE | BREAKING-RENAME | DATA-DEPENDENT | LOCK-REWRITE | ADD
    table: str | None
    column: str | None = None
    new_name: str | None = None
    extra: str | None = None
    line: int | None = None


def _unquote(tok: str | None) -> str | None:
    if tok is None:
        return None
    return tok.strip().strip('"').strip()


def _line_of(sql: str, idx: int) -> int:
    return sql.count("\n", 0, idx) + 1


def classify_ddl(sql: str) -> list[DDLOp]:
    """Find every destructive/risky op in a DDL string. ADD COLUMN is also surfaced
    (so the dupe/naming engine can see new names). Idempotent CREATE ... IF NOT EXISTS
    is the safe path and is intentionally NOT flagged."""
    ops: list[DDLOp] = []

    for m in _RE_RENAME_COL.finditer(sql):
        ops.append(DDLOp("rename_column", "BREAKING-RENAME", _unquote(m.group("tbl")),
                         column=_unquote(m.group("old")), new_name=_unquote(m.group("new")),
                         line=_line_of(sql, m.start())))
    for m in _RE_RENAME_TABLE.finditer(sql):
        # skip if this match is actually a RENAME COLUMN (already captured)
        if re.search(r"RENAME\s+(?:COLUMN\s+)?[\w\"]+\s+TO", m.group(0), re.IGNORECASE) \
           and not re.search(r"RENAME\s+TO", m.group(0), re.IGNORECASE):
            continue
        ops.append(DDLOp("rename_table", "BREAKING-RENAME", _unquote(m.group("tbl")),
                         new_name=_unquote(m.group("new")), line=_line_of(sql, m.start())))
    for m in _RE_DROP_COL.finditer(sql):
        ops.append(DDLOp("drop_column", "DESTRUCTIVE", _unquote(m.group("tbl")),
                         column=_unquote(m.group("col")), line=_line_of(sql, m.start())))
    for m in _RE_DROP_TABLE.finditer(sql):
        ops.append(DDLOp("drop_table", "DESTRUCTIVE", _unquote(m.group("tbl")),
                         line=_line_of(sql, m.start())))
    for m in _RE_ALTER_TYPE.finditer(sql):
        ops.append(DDLOp("alter_type", "LOCK-REWRITE", _unquote(m.group("tbl")),
                         column=_unquote(m.group("col")), extra=m.group("type"),
                         line=_line_of(sql, m.start())))
    for m in _RE_SET_NOT_NULL.finditer(sql):
        ops.append(DDLOp("set_not_null", "DATA-DEPENDENT", _unquote(m.group("tbl")),
                         column=_unquote(m.group("col")), line=_line_of(sql, m.start())))
    for m in _RE_ADD_COL.finditer(sql):
        ops.append(DDLOp("add_column", "ADD", _unquote(m.group("tbl")),
                         column=_unquote(m.group("col")), extra=(m.group("rest") or "").strip(),
                         line=_line_of(sql, m.start())))
    return ops


def split_table_ref(ref: str | None) -> tuple[str | None, str | None]:
    """'core.opportunity' -> ('core','opportunity'); 'raw_x' -> ('main','raw_x')."""
    if not ref:
        return (None, None)
    ref = _unquote(ref) or ref
    if "." in ref:
        s, _, t = ref.partition(".")
        return (s, t)
    return ("main", ref)


# ── Intent / deps parsing (R4) ────────────────────────────────────────────────
def parse_intent(sql: str) -> str | None:
    m = _INTENT_RE.search(sql)
    return m.group("intent").strip() if m else None


def parse_depends(sql: str) -> list[int]:
    m = _DEPENDS_RE.search(sql)
    if not m:
        return []
    out = []
    for tok in re.split(r"[,\s]+", m.group("dep").strip()):
        if tok.isdigit():
            out.append(int(tok))
    return out


# ── Naming / alias dupe engine (R3) ───────────────────────────────────────────
def naming_findings(table: str | None, column: str | None) -> list[Finding]:
    """Deterministic name-quality checks for a NEW column. Never blocks in Phase 1."""
    out: list[Finding] = []
    if not column:
        return out
    schema, tbl = split_table_ref(table)
    if not SNAKE_CASE_RE.match(column):
        out.append(Finding(
            rule="R3", severity="Error", classification="NAMING",
            table_schema=schema, table_name=tbl, column_name=column,
            detail=f"Column `{column}` is not snake_case. Canonical naming requires "
                   f"lower_snake_case (e.g. `{_to_snake(column)}`).",
        ))
    # Off-list abbreviations — advisory WARN (R5). A 2-4 char vowel-less token not on the
    # approved list reads as a cryptic abbreviation we'd rather see spelled out.
    if SNAKE_CASE_RE.match(column):
        bad = [p for p in column.split("_")
               if p not in APPROVED_ABBREVS and _looks_abbrev(p)]
        if bad:
            out.append(Finding(
                rule="R5", severity="Warn", classification="NAMING",
                table_schema=schema, table_name=tbl, column_name=column,
                detail=f"Column `{column}` contains likely-abbreviation(s) {bad} not on the "
                       f"approved list. Spell them out unless they're a standard term.",
            ))
    return out


def _looks_abbrev(part: str) -> bool:
    # crude: 2-4 char token with no vowel is likely an abbreviation we want spelled out.
    return 2 <= len(part) <= 4 and not any(v in part for v in "aeiou")


def _to_snake(name: str) -> str:
    s = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return re.sub(r"[^a-z0-9_]+", "_", s).strip("_")


def alias_dupe_finding(table: str | None, column: str | None,
                       aliases: dict[str, str], canonical_cols: set[str]) -> Finding | None:
    """If `column` is a known synonym (per column_aliases) of an EXISTING canonical
    column, surface a dupe. `aliases` = {alias: canonical}; `canonical_cols` = the set
    of canonical names that already exist in the catalog."""
    if not column:
        return None
    canonical = aliases.get(column)
    if canonical and canonical != column and canonical in canonical_cols:
        schema, tbl = split_table_ref(table)
        return Finding(
            rule="R3", severity="Error", classification="DUPE",
            table_schema=schema, table_name=tbl, column_name=column,
            detail=f"Column `{column}` is a synonym of canonical `{canonical}` "
                   f"(per core.column_aliases). Use `{canonical}` or declare a new alias.",
        )
    return None


def lexical_near_dupes(column: str | None, existing: set[str],
                       jw_threshold: float = 0.92) -> list[Finding]:
    """Advisory-only (R5/WARN) lexical near-dupe via jaro_winkler. Pure-python jw so the
    gate has no DuckDB dependency at author time. Never blocks."""
    out: list[Finding] = []
    if not column:
        return out
    for ex in existing:
        if ex == column:
            continue
        if abs(len(ex) - len(column)) > 4:
            continue
        score = _jaro_winkler(column, ex)
        if score >= jw_threshold:
            out.append(Finding(
                rule="R5", severity="Warn", classification="DUPE",
                column_name=column,
                detail=f"Column `{column}` is lexically close to existing `{ex}` "
                       f"(jaro_winkler={score:.2f}). Possible dupe — confirm or waive.",
            ))
    return out


def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    match_dist = max(len(s1), len(s2)) // 2 - 1
    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    for i, c1 in enumerate(s1):
        lo = max(0, i - match_dist)
        hi = min(i + match_dist + 1, len(s2))
        for j in range(lo, hi):
            if s2_matches[j] or s2[j] != c1:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    t = 0
    k = 0
    for i in range(len(s1)):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            t += 1
        k += 1
    t /= 2
    return (matches / len(s1) + matches / len(s2) + (matches - t) / matches) / 3


def _jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    j = _jaro(s1, s2)
    prefix = 0
    for c1, c2 in zip(s1, s2):
        if c1 == c2:
            prefix += 1
        else:
            break
        if prefix == 4:
            break
    return j + prefix * p * (1 - j)


# ── Static lineage: SQL extraction from .sql + Python string literals ──────────
# This is the load-bearing risk (the 549 dynamic f-string sites). We parse what we can
# with sqlglot and FAIL-CLOSED on the rest: a Python file with SQL-looking f-strings we
# cannot statically resolve is recorded as an 'assumed' consumer of the touched column.

_SQL_KEYWORDS = ("select", "insert", "update", "delete", "from", "join", "where",
                 "group by", "order by", "create table", "create view", "with ")


def sqlglot_available() -> bool:
    """True iff sqlglot can be imported. The static-lineage engine needs it; without it
    the consumer map degrades to almost-all fail-closed 'assumed' rows. Callers should
    surface this ONCE (not silently) — see schema_manifest._append_drift_issues."""
    try:
        import sqlglot  # noqa: F401
        return True
    except Exception:
        return False


def _string_looks_sql(s: str) -> bool:
    low = s.lower()
    return any(k in low for k in _SQL_KEYWORDS)


def _has_fstring_placeholder(node: ast.AST) -> bool:
    """True if this is an f-string (JoinedStr) with at least one {expr} field —
    i.e. a DYNAMIC sql string we can't fully resolve statically."""
    return isinstance(node, ast.JoinedStr) and any(
        isinstance(v, ast.FormattedValue) for v in node.values
    )


@dataclass
class SQLLiteral:
    text: str                       # the (possibly partial) SQL text
    line: int
    dynamic: bool                   # True if it contained an f-string placeholder


def extract_sql_from_python(py_text: str) -> tuple[list[SQLLiteral], bool]:
    """Return (literals, parse_ok). literals = SQL-looking strings found in the AST,
    each flagged dynamic if it was an f-string with a placeholder. parse_ok=False if the
    file did not parse as Python at all (then the caller fail-closes the whole file)."""
    literals: list[SQLLiteral] = []
    try:
        tree = ast.parse(py_text)
    except SyntaxError:
        return ([], False)

    for node in ast.walk(tree):
        # Plain string constants.
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if _string_looks_sql(node.value):
                literals.append(SQLLiteral(node.value, getattr(node, "lineno", 0), False))
        # f-strings: reconstruct the static skeleton, mark dynamic.
        elif isinstance(node, ast.JoinedStr):
            parts = []
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    parts.append(v.value)
                else:
                    parts.append(" __DYN__ ")  # placeholder for {expr}
            joined = "".join(parts)
            if _string_looks_sql(joined):
                literals.append(SQLLiteral(joined, getattr(node, "lineno", 0),
                                           _has_fstring_placeholder(node)))
    return (literals, True)


def columns_referenced(sql_text: str) -> set[str]:
    """Best-effort set of column identifiers referenced by a SQL string. Wrapper that
    discards the parse-quality flag (back-compat for the consumer map, where a noisy
    regex skim is acceptable — it only ever OVER-counts consumers, which is fail-safe)."""
    cols, _clean = columns_referenced_q(sql_text)
    return cols


def columns_referenced_q(sql_text: str) -> tuple[set[str], bool]:
    """Return (columns, parsed_cleanly). parsed_cleanly=True only when sqlglot parsed the
    statement(s) without falling back to the regex skim. The contract check uses the flag
    to AVOID flagging on a regex-skim result (which grabs table names / keywords and would
    false-positive). Columns come from Column nodes + INSERT target column lists."""
    import sqlglot
    from sqlglot import exp

    cols: set[str] = set()
    cleaned = sql_text.replace("__DYN__", "_dyn_")
    try:
        stmts = sqlglot.parse(cleaned, read="duckdb")
        if not stmts or all(s is None for s in stmts):
            raise ValueError("empty parse")
        for stmt in stmts:
            if stmt is None:
                continue
            for col in stmt.find_all(exp.Column):
                if col.name:
                    cols.add(col.name.lower())
            # INSERT target column lists: INSERT INTO t (a, b, c)
            for ins in stmt.find_all(exp.Insert):
                schema = ins.this
                if isinstance(schema, exp.Schema):
                    for ident in schema.expressions:
                        if isinstance(ident, exp.Identifier):
                            cols.add(ident.name.lower())
                        elif isinstance(ident, exp.Column) and ident.name:
                            cols.add(ident.name.lower())
        return cols, True
    except Exception:
        # sqlglot choked (often a __DYN__-riddled / truncated fragment) — regex skim.
        # Marked parsed_cleanly=False so callers that need precision can ignore it.
        for m in re.finditer(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
            cols.add(m.group(0).lower())
        return cols, False
