#!/usr/bin/env python3
"""Schema-gate — the deterministic review agent for the DuckDB warehouse.

The "review agent / middleman" from the 2026-06-18 Data Analysis Sync. It runs two
engines over a proposed schema change:
  1. breaking-change / lineage engine — classify DROP/RENAME/type-narrow/NOT-NULL
     (Atlas migrate-lint taxonomy), resolve impact against core.schema_consumers,
     report the exact file:line consumers, prescribe expand/contract.
  2. taxonomy / semantic-dupe engine — new ADD COLUMN must be snake_case, canonical,
     and not a synonym of an existing canonical column (core.column_aliases). Lexical
     near-dupe is advisory (WARN).

############################################################################
# PHASE 1 IS WARN-ONLY. THE GATE NEVER BLOCKS ANYTHING.                     #
#   * `review` ALWAYS exits 0 (even on Error-severity findings) unless      #
#     SCHEMA_GATE_BLOCK=1 is explicitly set (the Phase-2 flip, Sam's call). #
#   * It writes findings to core.schema_issue (the ledger) but refuses no   #
#     commit, no apply, no nightly. The pre-commit hook is non-blocking.    #
#   * The apply tooth (core.db.apply_ddl_file) only WARNs on a missing pass.#
# This is mandatory: a Phase-1 that refused un-gated DDL would break the    #
# other 3 editors' nightly. See BUILD-SPEC §9 / the standing GO.            #
############################################################################

Modes:
    schema_gate.py review [--staged | --files A.sql B.py ...] [--db PATH] [--no-write]
        Author-time check of the worktree/staged diff. Prints the checklist. Writes
        findings to core.schema_issue (unless --no-write / DB read-only / DB absent).
        ALWAYS exits 0 in Phase 1.

    schema_gate.py record <ddl_file> [--db PATH]
        On a passed DDL file, write the checksum-bound core.schema_gate_pass row the
        apply tooth consults. (Author runs this after `review`; the pre-commit hook
        does it automatically for staged DDL.)

    schema_gate.py status [--db PATH]
        Print open issues + recent gate passes (the queryable contract, human view).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

# Allow running as `python scripts/schema_gate.py` (repo root not on path).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core import schema_gate_lib as lib  # noqa: E402
from core.config import DB_PATH, REPO_ROOT  # noqa: E402

log = logging.getLogger("scripts.schema_gate")

# The Phase-2 flip. Default OFF — Phase 1 is WARN-ONLY by construction.
BLOCK_MODE = os.environ.get("SCHEMA_GATE_BLOCK", "0") == "1"

PY_CONSUMER_DIRS = ("entities", "sources", "scripts")


# ── DB helpers (read-only by default; opening read-write only to write the ledger) ──
def _open_db(db_path: Path | None, *, read_only: bool):
    import duckdb
    path = db_path or DB_PATH
    if not path.exists():
        return None
    try:
        return duckdb.connect(str(path), read_only=read_only)
    except Exception as exc:  # locked by the nightly, etc. — degrade to no-write.
        log.warning("schema_gate: could not open DB (%s): %s", "ro" if read_only else "rw", exc)
        return None


def _ensure_tables(con) -> None:
    """Make the gate self-bootstrapping: apply DDL 84 if the tables aren't there yet
    (so `review` works on a fresh clone before the nightly applies it). Idempotent."""
    ddl = REPO_ROOT / "sql" / "ddl" / "84_schema_gate.sql"
    try:
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='core' AND table_name='schema_issue'"
        ).fetchone()
        if not exists and ddl.exists():
            con.execute(ddl.read_text())
    except Exception as exc:
        log.warning("schema_gate: ensure_tables skipped: %s", exc)


def _load_catalog(con):
    """Return (canonical_cols: set, all_cols: set, aliases: {alias->canonical})."""
    canonical: set[str] = set()
    allcols: set[str] = set()
    aliases: dict[str, str] = {}
    if con is None:
        return canonical, allcols, aliases
    try:
        for (cn,) in con.execute(
            "SELECT DISTINCT COALESCE(canonical_name, column_name) FROM core.schema_catalog"
        ).fetchall():
            if cn:
                canonical.add(cn.lower())
        for (c,) in con.execute(
            "SELECT DISTINCT column_name FROM core.schema_catalog"
        ).fetchall():
            if c:
                allcols.add(c.lower())
        for a, c in con.execute(
            "SELECT alias, canonical_name FROM core.column_aliases WHERE scope='global'"
        ).fetchall():
            aliases[a.lower()] = c.lower()
    except Exception as exc:
        log.warning("schema_gate: catalog not yet built (%s) — naming/dupe checks degrade", exc)
    return canonical, allcols, aliases


def _resolve_consumers(con, schema: str | None, table: str | None, column: str | None):
    """Return list of 'file:line (confidence)' consumers of the touched column from
    core.schema_consumers. Matches on column_name, narrowing to table when known."""
    if con is None or not column:
        return []
    try:
        if table:
            rows = con.execute(
                "SELECT consumer_file, consumer_line, confidence, rename_resilient "
                "FROM core.schema_consumers "
                "WHERE lower(column_name)=lower(?) "
                "  AND (table_name IS NULL OR lower(table_name)=lower(?))",
                [column, table],
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT consumer_file, consumer_line, confidence, rename_resilient "
                "FROM core.schema_consumers WHERE lower(column_name)=lower(?)",
                [column],
            ).fetchall()
    except Exception:
        return []
    out = []
    for f, ln, conf, resilient in rows:
        if resilient:
            continue  # column-name-agnostic by design — not impacted by a rename
        loc = f"{f}:{ln}" if ln else f
        out.append(f"{loc} ({conf})")
    return sorted(set(out))


# ── The two engines, over a single DDL file ───────────────────────────────────
def gate_ddl_file(sql_file: Path, con) -> list[lib.Finding]:
    sql = sql_file.read_text()
    rel = _rel(sql_file)
    findings: list[lib.Finding] = []

    # R4 — declared intent + deps (advisory if missing).
    if lib.classify_ddl(sql) and not lib.parse_intent(sql):
        findings.append(lib.Finding(
            rule="R4", severity="Warn", classification="INTENT",
            detail=f"{rel}: column-touching DDL has no `-- @gate:` intent marker. "
                   f"Add `-- @gate: add | rename A->B | drop | alter-type` (+ `-- Depends on NN`).",
        ))

    canonical, allcols, aliases = _load_catalog(con)
    ops = lib.classify_ddl(sql)

    for op in ops:
        schema, tbl = lib.split_table_ref(op.table)

        if op.op in ("drop_column", "drop_table"):
            consumers = _resolve_consumers(con, schema, tbl, op.column or tbl)
            findings.append(lib.Finding(
                rule="R1", severity="Error", classification=op.classification,
                table_schema=schema, table_name=tbl, column_name=op.column,
                consumers=consumers,
                detail=_drop_detail(rel, op, consumers),
            ))

        elif op.op in ("rename_column", "rename_table"):
            consumers = _resolve_consumers(con, schema, tbl, op.column or tbl)
            findings.append(lib.Finding(
                rule="R1", severity="Error", classification=op.classification,
                table_schema=schema, table_name=tbl, column_name=op.column,
                consumers=consumers,
                detail=_rename_detail(rel, op, consumers),
            ))

        elif op.op == "alter_type":
            findings.append(lib.Finding(
                rule="R5", severity="Warn", classification=op.classification,
                table_schema=schema, table_name=tbl, column_name=op.column,
                detail=f"{rel}:{op.line}: ALTER TYPE on `{tbl}.{op.column}` -> {op.extra}. "
                       f"Type changes can narrow/lose data + rewrite the table. Confirm widening-only.",
            ))

        elif op.op == "set_not_null":
            findings.append(lib.Finding(
                rule="R5", severity="Warn", classification=op.classification,
                table_schema=schema, table_name=tbl, column_name=op.column,
                detail=f"{rel}:{op.line}: SET NOT NULL on `{tbl}.{op.column}` without a default "
                       f"fails if any existing row is NULL (DATA-DEPENDENT). Backfill first.",
            ))

        elif op.op == "add_column":
            # R3 — naming + alias dupe (deterministic). R5 — lexical near-dupe (advisory).
            findings.extend(lib.naming_findings(op.table, op.column))
            dupe = lib.alias_dupe_finding(op.table, op.column, aliases, canonical)
            if dupe:
                findings.append(dupe)
            findings.extend(lib.lexical_near_dupes(op.column, allcols))

    return findings


def _drop_detail(rel, op, consumers):
    n = len(consumers)
    target = f"`{op.table}.{op.column}`" if op.column else f"table `{op.table}`"
    if n == 0:
        return (f"{rel}:{op.line}: DROP {target} ({op.classification}). No statically-known "
                f"consumers found — but verify against dynamic syncs before dropping.")
    head = ", ".join(consumers[:6]) + (f", +{n-6} more" if n > 6 else "")
    return (f"{rel}:{op.line}: DROP {target} ({op.classification}) blocks {n} consumer(s): "
            f"{head}. Use expand/contract: deprecate -> migrate each consumer -> drop when none remain.")


def _rename_detail(rel, op, consumers):
    n = len(consumers)
    what = (f"column `{op.table}.{op.column}` -> `{op.new_name}`" if op.op == "rename_column"
            else f"table `{op.table}` -> `{op.new_name}`")
    head = ", ".join(consumers[:6]) + (f", +{n-6} more" if n > 6 else "") if consumers else "none statically known"
    return (f"{rel}:{op.line}: RENAME {what} ({op.classification}). Consumers: {head}. "
            f"Bare rename forbidden — use expand/contract: ADD the new name, dual-populate in the "
            f"nightly, migrate consumers one DDL at a time, DROP the old only when no consumer remains.")


# ── Python entity/sync check (author-time, fail-loud not fail-closed) ─────────
def gate_python_file(py_file: Path, con) -> list[lib.Finding]:
    """Lightweight author-time check of an entity/sync .py: surface explicit INSERT
    column lists whose columns don't exist in the live catalog (the contract idea,
    checked early). fail-LOUD only — a .py can't be apply-time-gated."""
    findings: list[lib.Finding] = []
    rel = _rel(py_file)
    try:
        text = py_file.read_text()
    except Exception:
        return findings
    literals, parse_ok = lib.extract_sql_from_python(text)
    if not parse_ok:
        return findings  # not valid python — leave to the linter, not us
    _, allcols, _ = _load_catalog(con)
    if not allcols:
        return findings
    for litobj in literals:
        if litobj.dynamic:
            continue  # dynamic INSERTs can't be statically validated; nightly contract covers
        cols, clean = lib.columns_referenced_q(litobj.text)
        if not clean:
            continue  # regex-skim fallback over-counts — don't flag a contract miss on it
        unknown = {c for c in cols if c.isidentifier() and len(c) > 2
                   and c not in allcols and not c.startswith("_")
                   and c not in ("select", "insert", "update", "from", "where", "into",
                                 "values", "table", "core", "derived", "raw", "main")}
        # Only flag when the literal is clearly an INSERT with a column list (high signal).
        if "insert into" in litobj.text.lower() and "(" in litobj.text and unknown:
            sample = ", ".join(sorted(unknown)[:5])
            findings.append(lib.Finding(
                rule="CONTRACT", severity="Warn", classification="CONTRACT",
                detail=f"{rel}:{litobj.line}: INSERT references column(s) not in the live catalog: "
                       f"{sample}. If this is a NEW column, ship its DDL first; else it's a typo/rename drift.",
            ))
    return findings


# ── Writing the ledger ─────────────────────────────────────────────────────────
def write_issues(con, findings: list[lib.Finding], ddl_file: str | None) -> int:
    if con is None or not findings:
        return 0
    try:
        next_id = con.execute(
            "SELECT COALESCE(max(issue_id),0)+1 FROM core.schema_issue"
        ).fetchone()[0]
    except Exception:
        return 0
    written = 0
    for f in findings:
        try:
            con.execute(
                """
                INSERT INTO core.schema_issue
                  (issue_id, rule, severity, classification, table_schema, table_name,
                   column_name, ddl_file, detail, consumers, status, phase_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
                """,
                [next_id, f.rule, f.severity, f.classification, f.table_schema,
                 f.table_name, f.column_name, ddl_file, f.detail,
                 json.dumps(f.consumers) if f.consumers else None,
                 "block" if BLOCK_MODE else "warn"],
            )
            next_id += 1
            written += 1
        except Exception as exc:
            log.warning("schema_gate: issue write failed: %s", exc)
    return written


# ── File selection ─────────────────────────────────────────────────────────────
def _rel(p: Path) -> str:
    try:
        return str(p.resolve().relative_to(REPO_ROOT))
    except Exception:
        return str(p)


def _staged_files() -> list[Path]:
    try:
        out = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True,
        ).stdout
    except Exception:
        return []
    return [REPO_ROOT / line.strip() for line in out.splitlines() if line.strip()]


def _is_ddl(p: Path) -> bool:
    return p.suffix == ".sql" and "sql/ddl" in str(p).replace("\\", "/")


def _is_py_consumer(p: Path) -> bool:
    parts = p.parts
    return p.suffix == ".py" and any(d in parts for d in PY_CONSUMER_DIRS)


# ── Commands ────────────────────────────────────────────────────────────────────
def cmd_review(args) -> int:
    if args.files:
        files = [Path(f) for f in args.files]
    elif args.staged:
        files = _staged_files()
    else:
        files = _staged_files()  # default to staged

    ddl_files = [f for f in files if _is_ddl(f) and f.exists()]
    py_files = [f for f in files if _is_py_consumer(f) and f.exists()]

    if not ddl_files and not py_files:
        print("schema_gate: no DDL / entity / sync files in the change — nothing to review.")
        return 0

    if not lib.sqlglot_available():
        print("  [WARN ] sqlglot not importable — static lineage DISABLED; consumer/impact "
              "lists will be incomplete. `pip install sqlglot` to restore.")

    write = not args.no_write
    con = _open_db(Path(args.db) if args.db else None, read_only=False) if write else \
        _open_db(Path(args.db) if args.db else None, read_only=True)
    if con is not None and write:
        _ensure_tables(con)

    all_findings: list[tuple[str, list[lib.Finding]]] = []
    total_written = 0
    for f in ddl_files:
        fnd = gate_ddl_file(f, con)
        all_findings.append((_rel(f), fnd))
        if write:
            total_written += write_issues(con, fnd, _rel(f))
    for f in py_files:
        fnd = gate_python_file(f, con)
        all_findings.append((_rel(f), fnd))
        if write:
            total_written += write_issues(con, fnd, _rel(f))

    if con is not None:
        try:
            con.close()
        except Exception:
            pass

    # ── The checklist (the "review agent" UX) ──
    print("=" * 72)
    print("  SCHEMA GATE — review  (PHASE 1: WARN-ONLY — this NEVER blocks)")
    print("=" * 72)
    errors = warns = 0
    for rel, fnd in all_findings:
        if not fnd:
            print(f"  [OK]   {rel}  — clean")
            continue
        for x in fnd:
            tag = "ERROR" if x.severity == "Error" else ("WARN " if x.severity == "Warn" else "INFO ")
            if x.severity == "Error":
                errors += 1
            elif x.severity == "Warn":
                warns += 1
            print(f"  [{tag}] {x.rule:<8} {x.detail}")
    print("-" * 72)
    print(f"  {errors} error-severity finding(s), {warns} warning(s).")
    if total_written:
        print(f"  Logged {total_written} issue(s) to core.schema_issue (queryable ledger).")
    if errors and not BLOCK_MODE:
        print("  PHASE 1: these are LOGGED, NOT BLOCKING. The commit/apply proceeds normally.")
        print("  (Flip to blocking is a separate Sam decision — SCHEMA_GATE_BLOCK=1.)")

    if BLOCK_MODE and errors:
        print("  SCHEMA_GATE_BLOCK=1 — BLOCKING on error-severity findings (Phase 2 mode).")
        return 1
    return 0  # Phase 1: ALWAYS pass.


def cmd_record(args) -> int:
    ddl_file = Path(args.ddl_file)
    if not ddl_file.exists():
        print(f"schema_gate record: file not found: {ddl_file}", file=sys.stderr)
        return 2
    stem = ddl_file.stem
    try:
        version = int(stem.split("_", 1)[0])
    except ValueError:
        print(f"schema_gate record: {ddl_file.name} has no leading version int — skip.")
        return 0
    content = ddl_file.read_bytes()
    sha = hashlib.sha256(content).hexdigest()

    con = _open_db(Path(args.db) if args.db else None, read_only=False)
    if con is None:
        print("schema_gate record: DB unavailable — pass not recorded (Phase 1: apply still proceeds).")
        return 0
    _ensure_tables(con)
    # Re-gate to record the verdict + issue count alongside the pass.
    fnd = gate_ddl_file(ddl_file, con)
    errs = sum(1 for x in fnd if x.severity == "Error")
    verdict = "pass" if errs == 0 else "pass-with-warn"  # Phase 1: even errors record as a (warn) pass
    issue_n = write_issues(con, fnd, _rel(ddl_file))
    try:
        con.execute(
            """
            INSERT INTO core.schema_gate_pass
              (version, sql_file, content_sha256, verdict, gated_by, gate_version, issue_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (version, content_sha256) DO UPDATE SET
              verdict = excluded.verdict, passed_at = now(), issue_count = excluded.issue_count
            """,
            [version, ddl_file.name, sha, verdict,
             os.environ.get("USER", "unknown"), lib.SCHEMA_GATE_VERSION, issue_n],
        )
        print(f"schema_gate: recorded pass for {ddl_file.name} v{version} "
              f"sha256={sha[:12]}… verdict={verdict} issues={issue_n}")
    except Exception as exc:
        print(f"schema_gate record: write failed (Phase 1: non-fatal): {exc}")
    finally:
        con.close()
    return 0


def cmd_status(args) -> int:
    con = _open_db(Path(args.db) if args.db else None, read_only=True)
    if con is None:
        print("schema_gate status: DB unavailable.")
        return 0
    try:
        print("=== open schema issues ===")
        rows = con.execute(
            "SELECT issue_id, rule, severity, classification, "
            "COALESCE(table_name,'') , COALESCE(column_name,''), detail "
            "FROM core.schema_issue WHERE status='open' ORDER BY issue_id DESC LIMIT 40"
        ).fetchall()
        if not rows:
            print("  (none)")
        for r in rows:
            print(f"  #{r[0]} {r[1]}/{r[2]} {r[3] or ''} {r[4]}.{r[5]}: {r[6][:110]}")
        print("\n=== recent gate passes ===")
        for r in con.execute(
            "SELECT version, sql_file, verdict, issue_count, CAST(passed_at AS VARCHAR) "
            "FROM core.schema_gate_pass ORDER BY passed_at DESC LIMIT 15"
        ).fetchall():
            print(f"  v{r[0]} {r[1]} {r[2]} issues={r[3]} @ {r[4]}")
        catn = con.execute("SELECT count(*) FROM core.schema_catalog").fetchone()[0]
        consn = con.execute("SELECT count(*) FROM core.schema_consumers").fetchone()[0]
        aln = con.execute("SELECT count(*) FROM core.column_aliases").fetchone()[0]
        print(f"\ncatalog={catn} columns, consumers={consn}, aliases={aln}")
    except Exception as exc:
        print(f"schema_gate status: {exc}")
    finally:
        con.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Warehouse schema review gate (Phase 1: WARN-ONLY)")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("review", help="author-time check of staged/worktree diff")
    pr.add_argument("--staged", action="store_true", help="check git-staged files (default)")
    pr.add_argument("--files", nargs="*", help="explicit files to check instead of staged")
    pr.add_argument("--db", type=str, default=None)
    pr.add_argument("--no-write", action="store_true", help="don't write to schema_issue")
    pr.set_defaults(func=cmd_review)

    prec = sub.add_parser("record", help="record a checksum-bound gate pass for a DDL file")
    prec.add_argument("ddl_file")
    prec.add_argument("--db", type=str, default=None)
    prec.set_defaults(func=cmd_record)

    pst = sub.add_parser("status", help="print open issues + recent passes")
    pst.add_argument("--db", type=str, default=None)
    pst.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
