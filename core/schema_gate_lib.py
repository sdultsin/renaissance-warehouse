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
# CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON [schema.]<tbl> [USING <method>] (<cols>). Captures
# uniqueness + IF-NOT-EXISTS + index name + table + the parenthesised column list, so the floor can
# apply the expand/contract rule to an index "rename" (a new UNIQUE index on a column that already
# has one).
#
# QA-D BYPASS HARDENING (2026-06-26): DuckDB 1.5.3 accepts an optional `USING <method>` clause AND
# SQL comments BETWEEN the table name and the "(cols)" column list — e.g.
#   `CREATE UNIQUE INDEX uxk ON raw_pipeline_scratch USING art (_key);`
#   `CREATE UNIQUE INDEX uxk ON raw_pipeline_scratch /*c*/ (_key);`
# The old regex required "(" to be adjacent to <tbl> (only \s* between), so BOTH forms slipped past
# the floor and created a 2nd unique index on _key undetected. We now (1) strip SQL comments before
# classifying index DDL (see _strip_sql_comments / classify_ddl), and (2) allow an optional
# `USING <method>` clause and arbitrary whitespace/newlines between <tbl> and "(". The table name is
# matched with the explicit-newline-safe whitespace class so a column list on a following line is
# still captured. (\s already spans newlines in Python re; no re.DOTALL is needed because the cols
# body uses [^)]* which also spans lines.)
_RE_CREATE_INDEX = re.compile(
    r"\bCREATE\s+(?P<unique>UNIQUE\s+)?INDEX\s+(?P<ine>IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[\w.\"]+)\s+ON\s+(?P<tbl>[\w.\"]+)"
    r"(?:\s+USING\s+(?P<method>[\w\"]+))?"   # optional access method (USING art / USING hnsw / ...)
    r"\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE,
)
# DROP INDEX [IF EXISTS] <name> — so the floor can see whether an index "rename" pairs its new
# CREATE with a DROP of the old index in the SAME diff (expand/contract) or leaves a duplicate.
_RE_DROP_INDEX = re.compile(
    r"\bDROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(?P<name>[\w.\"]+)",
    re.IGNORECASE,
)

# ── BYPASS-1 HARDENING (2026-06-26): inline / ALTER UNIQUE & PRIMARY KEY constraints ─────────────
# A unique enforcement on `_key` does NOT have to be a CREATE UNIQUE INDEX. DuckDB also enforces it
# via a UNIQUE / PRIMARY KEY *constraint* — and a constraint-backed enforcement is INVISIBLE to a
# duckdb_indexes()-only check (it lives in duckdb_constraints()). So:
#   CREATE TABLE raw_pipeline_x (_key VARCHAR NOT NULL UNIQUE, ...);
#   CREATE UNIQUE INDEX uxk_x ON raw_pipeline_x(_key);
# yields TWO enforcements on _key (Bypass 1). The floor must SEE the inline UNIQUE/PRIMARY KEY so it
# can apply the same dup-enforcement rule. We surface two shapes:
#   (1) an inline column constraint inside CREATE TABLE: `<col> <type> ... UNIQUE`/`PRIMARY KEY`
#   (2) a table-level constraint (inline in CREATE TABLE or via ALTER ... ADD [CONSTRAINT n] ...):
#         `UNIQUE (<col>)` / `PRIMARY KEY (<col>)`
# Each is emitted as a synthetic create_unique_index DDLOp (column = the single constrained column),
# carrying a "constraint:" marker in `new_name` so the floor knows there is no DROP-INDEX rename to
# pair and so W1 (catalog-delta) is the authoritative backstop. Composite constraints (>1 column) do
# NOT enforce single-column uniqueness on _key and are intentionally skipped.
_RE_CREATE_TABLE_HEAD = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)\s*\(",
    re.IGNORECASE,
)
# table-level `UNIQUE (cols)` / `PRIMARY KEY (cols)` (optionally `CONSTRAINT name` prefixed).
_RE_TABLE_CONSTRAINT = re.compile(
    r"(?:\bCONSTRAINT\s+[\w\"]+\s+)?\b(?P<kind>UNIQUE|PRIMARY\s+KEY)\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE,
)
# inline column-level constraint: `<col> <rest-up-to-comma> UNIQUE|PRIMARY KEY` (single column).
# We scan each top-level column definition; an inline UNIQUE / PRIMARY KEY keyword (NOT followed by
# "(", which would make it a table-level constraint) marks the column as uniquely enforced.
_RE_INLINE_COL_DEF = re.compile(r"^\s*(?P<col>[\w\"]+)\s+(?P<rest>.*)$", re.IGNORECASE | re.DOTALL)
_RE_INLINE_UNIQUE_KW = re.compile(r"\b(?:UNIQUE|PRIMARY\s+KEY)\b(?!\s*\()", re.IGNORECASE)
# ALTER TABLE <tbl> ADD [CONSTRAINT n] UNIQUE|PRIMARY KEY (cols)
_RE_ALTER_ADD_CONSTRAINT = re.compile(
    r"\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)\s+ADD\s+"
    r"(?:CONSTRAINT\s+[\w\"]+\s+)?(?P<kind>UNIQUE|PRIMARY\s+KEY)\s*\((?P<cols>[^)]*)\)",
    re.IGNORECASE,
)


def _split_top_level_commas(body: str) -> list[str]:
    """Split a CREATE TABLE body on top-level commas (ignoring commas inside parentheses), so each
    element is one column-definition or table-level constraint clause."""
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return parts


def _matched_paren_body(sql: str, open_idx: int) -> tuple[str, int]:
    """Given the index of an opening '(' in `sql`, return (body_without_outer_parens, index_after_close).
    Returns ('', open_idx) if unbalanced."""
    depth = 0
    for i in range(open_idx, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[open_idx + 1:i], i + 1
    return "", open_idx


_CONSTRAINT_KEYWORDS = ("primary", "foreign", "unique", "check", "constraint", "key", "references")


def _constraint_unique_ops(sql: str) -> list["DDLOp"]:
    """Extract synthetic create_unique_index DDLOps for single-column UNIQUE / PRIMARY KEY enforcement
    introduced by an inline CREATE TABLE constraint OR an ALTER TABLE ADD CONSTRAINT (Bypass 1). Each
    op's `new_name` is a `constraint:<kind>` marker (there is no index name / DROP-able old index to
    pair), `column` is the single constrained column, `extra` is the column name, classification is
    `UNIQUE-CONSTRAINT`. Comments are stripped length-preservingly so reported lines stay exact."""
    ops: list["DDLOp"] = []
    cleaned = _strip_sql_comments(sql)

    # (A) CREATE TABLE ( ... ) — inline column constraints + table-level constraints.
    for m in _RE_CREATE_TABLE_HEAD.finditer(cleaned):
        tbl = _unquote(m.group("tbl"))
        open_idx = m.end() - 1  # the '(' captured at the end of the head regex
        body, _ = _matched_paren_body(cleaned, open_idx)
        if not body:
            continue
        head_line = _line_of(cleaned, m.start())
        for clause in _split_top_level_commas(body):
            cl = clause.strip()
            if not cl:
                continue
            first_word = re.match(r"\s*([\w\"]+)", cl)
            lead = _unquote(first_word.group(1)).lower() if first_word else ""
            # table-level constraint clause (starts with UNIQUE / PRIMARY KEY / CONSTRAINT)
            if lead in ("unique", "primary", "constraint"):
                tcm = _RE_TABLE_CONSTRAINT.search(cl)
                if tcm:
                    cols = [c.strip().strip('"') for c in (tcm.group("cols") or "").split(",") if c.strip()]
                    if len(cols) == 1:
                        kind = re.sub(r"\s+", " ", tcm.group("kind").upper())
                        ops.append(DDLOp("create_unique_index", "UNIQUE-CONSTRAINT", tbl,
                                         column=cols[0], new_name=f"constraint:{kind.lower().replace(' ', '_')}",
                                         extra=cols[0], line=head_line))
                continue
            # inline column definition: `<col> <type...> [UNIQUE|PRIMARY KEY]` (no table-level "(")
            cdef = _RE_INLINE_COL_DEF.match(cl)
            if not cdef:
                continue
            col = _unquote(cdef.group("col"))
            if not col or col.lower() in _CONSTRAINT_KEYWORDS:
                continue
            rest = cdef.group("rest") or ""
            if _RE_INLINE_UNIQUE_KW.search(rest):
                kind = "primary_key" if re.search(r"\bPRIMARY\s+KEY\b", rest, re.IGNORECASE) else "unique"
                ops.append(DDLOp("create_unique_index", "UNIQUE-CONSTRAINT", tbl,
                                 column=col, new_name=f"constraint:{kind}", extra=col, line=head_line))

    # (B) ALTER TABLE ... ADD [CONSTRAINT n] UNIQUE|PRIMARY KEY (col)
    for m in _RE_ALTER_ADD_CONSTRAINT.finditer(cleaned):
        cols = [c.strip().strip('"') for c in (m.group("cols") or "").split(",") if c.strip()]
        if len(cols) != 1:
            continue
        kind = re.sub(r"\s+", " ", m.group("kind").upper()).lower().replace(" ", "_")
        ops.append(DDLOp("create_unique_index", "UNIQUE-CONSTRAINT", _unquote(m.group("tbl")),
                         column=cols[0], new_name=f"constraint:{kind}", extra=cols[0],
                         line=_line_of(cleaned, m.start())))
    return ops


def parse_dropped_indexes(sql: str) -> set[str]:
    """Bare index names dropped by `sql` (DROP INDEX ...), lowercased + unquoted, schema stripped.
    Used by the floor's index expand/contract rule to confirm a same-diff DROP of the old index."""
    out: set[str] = set()
    for m in _RE_DROP_INDEX.finditer(_strip_sql_comments(sql)):
        n = _unquote(m.group("name")) or ""
        out.add(n.split(".")[-1].lower())
    return out


@dataclass
class DDLOp:
    op: str                         # drop_column | drop_table | rename_column | rename_table
                                    # | alter_type | set_not_null | add_column
                                    # | create_index | create_unique_index
    classification: str             # DESTRUCTIVE | BREAKING-RENAME | DATA-DEPENDENT | LOCK-REWRITE
                                    # | ADD | INDEX[-IF-NOT-EXISTS] | UNIQUE-INDEX[-IF-NOT-EXISTS]
    table: str | None
    column: str | None = None       # for create_*index: the single indexed column (None if composite)
    new_name: str | None = None     # for create_*index: the index NAME
    extra: str | None = None        # for create_*index: the raw parenthesised column list
    line: int | None = None


def _unquote(tok: str | None) -> str | None:
    if tok is None:
        return None
    return tok.strip().strip('"').strip()


# Comment bodies are replaced with EQUAL-LENGTH blanks (newlines preserved) so that character offsets
# — and therefore _line_of() line numbers and the start indexes of every OTHER op's match — are left
# byte-for-byte identical. We only need this for the index pass (the QA-D bypass hid the column list
# behind a comment), but stripping length-preservingly means we can safely reuse the cleaned text for
# any regex without shifting reported lines. String/quoted-identifier contents are NOT comment-aware
# here (a `--`/`/*` inside a string literal would be blanked), which is acceptable for DDL index
# statements; the floor over-blanking can only ever make a comment-disguised bypass MORE visible, and
# the absolute apply-path guard (W1 catalog delta) is the regex-independent backstop regardless.
_RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_RE_LINE_COMMENT = re.compile(r"--[^\n]*")


def _blank_keep_newlines(m: "re.Match") -> str:
    """Replace a matched comment with the same number of chars, keeping any embedded newlines so line
    numbers and downstream match offsets are preserved exactly."""
    return "".join("\n" if ch == "\n" else " " for ch in m.group(0))


def _strip_sql_comments(sql: str) -> str:
    """Blank out /* ... */ (incl. multi-line) and -- to-end-of-line comments WITHOUT changing the
    length of the string (comment chars -> spaces, newlines kept). Block comments are removed first so
    a `--` inside a `/* */` is treated as part of the block comment, not a line comment."""
    if not sql or ("/*" not in sql and "--" not in sql):
        return sql
    sql = _RE_BLOCK_COMMENT.sub(_blank_keep_newlines, sql)
    sql = _RE_LINE_COMMENT.sub(_blank_keep_newlines, sql)
    return sql


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
    # CREATE [UNIQUE] INDEX — surfaced so the floor can apply the expand/contract rule to an index
    # "rename" (a 2nd UNIQUE index on a column that already has one is the dup-unique-_key landmine).
    # Unlike a CREATE TABLE, an `IF NOT EXISTS` here does NOT make it safe: IF NOT EXISTS keys off the
    # NEW index name, so it still adds a duplicate when the OLD index under a different name survives.
    # We encode the indexed columns in `extra`, the index name in `new_name`, the single-column target
    # (for the per-column dup probe) in `column`, and whether IF NOT EXISTS was used in classification.
    # Strip SQL comments (length-preservingly, so _line_of offsets stay exact) before matching CREATE
    # INDEX: the QA-D bypass hid the "(cols)" behind a `/*c*/` comment between <tbl> and "(", and an
    # optional `USING <method>` clause is now tolerated by the regex itself. Comment-stripping also
    # closes the `... ON t -- x\n (_key)` variant. The cleaned text is index-only; the other ops above
    # already matched against the raw `sql` (their patterns don't straddle a comment).
    sql_idx = _strip_sql_comments(sql)
    for m in _RE_CREATE_INDEX.finditer(sql_idx):
        is_unique = bool(m.group("unique"))
        cols_raw = (m.group("cols") or "").strip()
        cols = [c.strip().strip('"') for c in cols_raw.split(",") if c.strip()]
        single_col = cols[0] if len(cols) == 1 else None
        cls = ("UNIQUE-INDEX" if is_unique else "INDEX") + ("-IF-NOT-EXISTS" if m.group("ine") else "")
        ops.append(DDLOp("create_unique_index" if is_unique else "create_index", cls,
                         _unquote(m.group("tbl")), column=single_col,
                         new_name=_unquote(m.group("name")),
                         extra=cols_raw, line=_line_of(sql_idx, m.start())))
    # BYPASS 1: inline UNIQUE / PRIMARY KEY constraints in CREATE TABLE, and ALTER TABLE ADD
    # CONSTRAINT ... UNIQUE / PRIMARY KEY — surfaced as synthetic single-column create_unique_index
    # ops (classification UNIQUE-CONSTRAINT, new_name='constraint:<kind>') so the floor's dup-unique
    # rule sees a constraint-backed enforcement on a column (e.g. _key) that may ALSO carry a separate
    # unique index. (W1's catalog-delta over duckdb_constraints() is the regex-independent backstop.)
    ops.extend(_constraint_unique_ops(sql))
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
