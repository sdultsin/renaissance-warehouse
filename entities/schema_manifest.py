"""Schema-gate graft A — the nightly rebuilder of the queryable schema contract.

Auto-discovered by core/orchestrator.py:discover_and_register (the entities/*.py glob),
so it wires into the nightly with ZERO extra plumbing. Registered under the 'derived'
phase (runs LAST, 05:40 — after every raw/canonical/derived table exists), so the
catalog reflects the fully-built warehouse.

Each run, idempotently:
  1. Rebuild core.schema_catalog from live information_schema (the rot-proof truth of
     every column). Preserves curated canonical_name/status; new columns default
     canonical_name = column_name, status = 'active'. Stale columns are marked
     status='absent' (NOT deleted — we keep the history + any open issues against them).
  2. Rebuild core.schema_consumers: static sqlglot parse over sql/ddl/*.sql + SQL string
     literals in entities|sources|scripts/*.py, UNION the declared registry
     (schema_consumers rows with confidence='declared' persist), UNION fail-closed
     'assumed' rows for any .py that did not parse / had dynamic SQL we couldn't resolve.

PHASE 1 IS WARN-ONLY: this only POPULATES the contract tables + appends DRIFT issues to
core.schema_issue. It NEVER fails the phase, NEVER blocks anything. A failure here is
caught and logged so the nightly is unaffected (returns a notes-only PhaseResult).

This is the *brain.md lesson made real: the runtime reads from these tables, never prose.
"""

from __future__ import annotations

import ast
import logging
import re
from pathlib import Path

from core import schema_gate_lib as lib
from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.schema_manifest")

PY_DIRS = ("entities", "sources", "scripts")
# Per-column declared-consumer marker: `# @consumes: schema.table.col:line, ...`
# and a rename-resilient marker: `# @gate-resilient: <col1> <col2> ...` (a file that
# picks a column from a priority list and is immune to a rename of any single one).
_DECLARE_RE = re.compile(r"#\s*@consumes\s*:\s*(?P<spec>[^\n]+)", re.IGNORECASE)
_RESILIENT_RE = re.compile(r"#\s*@gate-resilient\s*:\s*(?P<cols>[^\n]+)", re.IGNORECASE)


def register(registry: Registry) -> None:
    registry.add_phase("derived", "schema_manifest", run_schema_manifest)


def run_schema_manifest(ctx: RunContext) -> PhaseResult:
    """Never raises — fail-safe so the gate can never break the nightly (Phase 1)."""
    db = ctx.db
    try:
        _ensure_tables(db)
        cat_n = _rebuild_catalog(db)
        cons_n, assumed_n = _rebuild_consumers(db)
        drift_n = _append_drift_issues(db)
        notes = {
            "catalog_columns": cat_n,
            "consumers": cons_n,
            "assumed_fail_closed": assumed_n,
            "drift_issues_appended": drift_n,
            "phase_mode": "warn-only",
        }
        logger.info("schema_manifest: catalog=%d consumers=%d assumed=%d drift=%d",
                    cat_n, cons_n, assumed_n, drift_n)
        return PhaseResult(rows_in=cat_n, rows_out=cons_n, notes=notes)
    except Exception as exc:  # noqa: BLE001 — Phase 1 must never break the nightly.
        logger.warning("schema_manifest soft-failed (Phase 1 non-fatal): %s", exc)
        return PhaseResult(notes={"soft_error": str(exc)[:300], "phase_mode": "warn-only"})


# ── DDL bootstrap ──────────────────────────────────────────────────────────────
def _ensure_tables(db) -> None:
    exists = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='schema_catalog'"
    ).fetchone()
    if not exists:
        ddl = REPO_ROOT / "sql" / "ddl" / "84_schema_gate.sql"
        if ddl.exists():
            db.execute(ddl.read_text())


# ── 1. Catalog rebuild ─────────────────────────────────────────────────────────
def _rebuild_catalog(db) -> int:
    """UPSERT every live column; mark vanished ones status='absent'. Curated
    canonical_name/status survive (we only overwrite type/position/nullable/last_seen)."""
    live = db.execute(
        f"""
        SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
               c.ordinal_position, (c.is_nullable = 'YES') AS is_nullable,
               COALESCE(t.table_type, 'BASE TABLE') AS object_type
        FROM information_schema.columns c
        LEFT JOIN information_schema.tables t
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema IN {lib.GATE_SCHEMAS}
          -- Session TEMP tables (entity scratch: _ci_universe, _df_stage,
          -- sending_account_PRE, ...) surface in information_schema with
          -- table_schema='main' but table_catalog='temp'. The manifest runs
          -- mid-nightly in the same session as the entities that create them, so
          -- without this filter they get cataloged 'active', vanish when the
          -- session ends, and warehouse_qa reports phantom SCHEMA-DRIFT
          -- (85 ghost columns on 2026-07-07).
          AND c.table_catalog <> 'temp'
        """
    ).fetchall()

    db.execute("DROP TABLE IF EXISTS _live_cat")
    db.execute(
        """
        CREATE TEMP TABLE _live_cat
          (table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR,
           data_type VARCHAR, ordinal_position INTEGER, is_nullable BOOLEAN,
           object_type VARCHAR)
        """
    )
    if live:
        db.executemany(
            "INSERT INTO _live_cat VALUES (?, ?, ?, ?, ?, ?, ?)", live
        )

    # UPSERT live columns (preserve curated canonical_name/status/notes via COALESCE on conflict).
    db.execute(
        """
        INSERT INTO core.schema_catalog
          (table_schema, table_name, column_name, data_type, ordinal_position,
           is_nullable, canonical_name, status, object_type, first_seen_at, last_seen_at)
        SELECT table_schema, table_name, column_name, data_type, ordinal_position,
               is_nullable, column_name, 'active', object_type, now(), now()
        FROM _live_cat
        ON CONFLICT (table_schema, table_name, column_name) DO UPDATE SET
          data_type        = excluded.data_type,
          ordinal_position = excluded.ordinal_position,
          is_nullable      = excluded.is_nullable,
          object_type      = excluded.object_type,
          last_seen_at     = now(),
          -- if it had been marked absent and reappeared, reactivate; else keep curated status
          status = CASE WHEN core.schema_catalog.status = 'absent' THEN 'active'
                        ELSE core.schema_catalog.status END
        """
    )
    # Mark columns no longer live as 'absent' (history-preserving; never delete).
    db.execute(
        """
        UPDATE core.schema_catalog SET status = 'absent', last_seen_at = last_seen_at
        WHERE status <> 'absent'
          AND NOT EXISTS (
            SELECT 1 FROM _live_cat l
            WHERE l.table_schema = core.schema_catalog.table_schema
              AND l.table_name   = core.schema_catalog.table_name
              AND l.column_name  = core.schema_catalog.column_name)
        """
    )
    db.execute("DROP TABLE IF EXISTS _live_cat")
    return db.execute("SELECT count(*) FROM core.schema_catalog WHERE status='active'").fetchone()[0]


# ── 2. Consumer rebuild (static ∪ declared ∪ fail-closed) ──────────────────────
def _rebuild_consumers(db) -> tuple[int, int]:
    """Wipe static+assumed rows (declared rows persist), then re-derive."""
    # MEDIUM-fix: if sqlglot is unimportable the static-lineage engine is dead and the
    # consumer map becomes almost-all fail-closed 'assumed' wildcards. That is a SILENT
    # gutting of the gate's core value — surface it ONCE, explicitly, both to the log and
    # to the queryable ledger, instead of letting it pass unnoticed.
    if not lib.sqlglot_available():
        logger.warning(
            "schema_manifest: sqlglot is NOT importable — static SQL lineage is DISABLED; "
            "consumer map is degrading to fail-closed 'assumed' rows. `pip install sqlglot` "
            "on the warehouse box to restore real lineage."
        )
        _emit_sqlglot_missing_issue(db)

    db.execute("DELETE FROM core.schema_consumers WHERE confidence IN ('static','assumed')")

    rows: list[tuple] = []     # (schema, table, column, file, line, confidence, resilient, notes)
    assumed = 0

    # (a) Static parse over .sql DDL.
    ddl_dir = REPO_ROOT / "sql" / "ddl"
    for f in sorted(ddl_dir.glob("*.sql")):
        if f.name.endswith(".bak") or ".bak-" in f.name:
            continue
        rel = _rel(f)
        try:
            cols = lib.columns_referenced(f.read_text())
            for c in cols:
                rows.append((None, None, c, rel, None, "static", False, "ddl static parse"))
        except Exception:
            assumed += 1
            rows.append((None, None, "*", rel, None, "assumed", False, "ddl unparseable — fail-closed"))

    # (b) Static parse over SQL-in-Python literals (+ declared markers + resilient markers).
    for pd in PY_DIRS:
        d = REPO_ROOT / pd
        if not d.exists():
            continue
        for f in sorted(d.glob("*.py")):
            if f.name.startswith("_"):
                continue
            rel = _rel(f)
            try:
                text = f.read_text()
            except Exception:
                continue
            # Declared resilient columns: this file is column-name-agnostic for them.
            resilient_cols = set()
            for m in _RESILIENT_RE.finditer(text):
                for c in re.split(r"[,\s]+", m.group("cols").strip()):
                    if c:
                        resilient_cols.add(c.lower())
                        rows.append((None, None, c.lower(), rel, None, "declared", True,
                                     "declared rename-resilient (picks from a priority list)"))
            # Declared explicit consumers.
            for m in _DECLARE_RE.finditer(text):
                for spec in re.split(r"[,]+", m.group("spec").strip()):
                    spec = spec.strip()
                    if not spec:
                        continue
                    sch, tbl, col, ln = _parse_declare(spec)
                    if col:
                        rows.append((sch, tbl, col.lower(), rel, ln, "declared", False, "declared consumer"))

            literals, parse_ok = lib.extract_sql_from_python(text)
            if not parse_ok:
                # Whole file unparseable as python — fail-closed.
                assumed += 1
                rows.append((None, None, "*", rel, None, "assumed", False, "py unparseable — fail-closed"))
                continue
            file_dynamic = False
            for litobj in literals:
                if litobj.dynamic:
                    file_dynamic = True
                    continue  # can't statically resolve a {expr}-laden SQL string
                try:
                    for c in lib.columns_referenced(litobj.text):
                        if c.lower() in resilient_cols:
                            continue
                        rows.append((None, None, c.lower(), rel, litobj.line, "static",
                                     False, "py SQL literal"))
                except Exception:
                    pass
            if file_dynamic:
                # FAIL-CLOSED: file has dynamic SQL we couldn't fully resolve. Record a
                # wildcard 'assumed' marker so the gate treats it as a possible consumer of
                # ANY touched column (annotate once with @consumes / @gate-resilient to clear).
                assumed += 1
                rows.append((None, None, "*", rel, None, "assumed", False,
                             "py has dynamic f-string SQL — fail-closed (annotate @consumes once)"))

    # Bulk insert.
    if rows:
        db.executemany(
            """
            INSERT INTO core.schema_consumers
              (table_schema, table_name, column_name, consumer_file, consumer_line,
               confidence, rename_resilient, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    total = db.execute("SELECT count(*) FROM core.schema_consumers").fetchone()[0]
    return total, assumed


def _emit_sqlglot_missing_issue(db) -> None:
    """Write a single open Info issue that sqlglot is missing (deduped — don't pile up)."""
    try:
        already = db.execute(
            "SELECT 1 FROM core.schema_issue WHERE rule='DRIFT' AND status='open' "
            "AND detail LIKE '%sqlglot is not importable%'"
        ).fetchone()
        if already:
            return
        next_id = db.execute("SELECT COALESCE(max(issue_id),0)+1 FROM core.schema_issue").fetchone()[0]
        db.execute(
            """
            INSERT INTO core.schema_issue
              (issue_id, rule, severity, classification, detail, status, phase_mode)
            VALUES (?, 'DRIFT', 'Info', 'CONTRACT',
                    'Static SQL lineage is DISABLED because sqlglot is not importable on this box. '
                    'The consumer map is fail-closed (all-assumed); run `pip install sqlglot` to restore '
                    'real lineage. Until then DROP/RENAME impact lists are unreliable.', 'open', 'warn')
            """,
            [next_id],
        )
    except Exception as exc:
        logger.warning("schema_manifest: could not record sqlglot-missing issue: %s", exc)


def _parse_declare(spec: str):
    """'core.opportunity.lead_email:88' or 'core.opportunity.lead_email' -> (sch,tbl,col,line)."""
    loc = None
    if ":" in spec:
        spec, _, loc_s = spec.rpartition(":")
        loc = int(loc_s) if loc_s.strip().isdigit() else None
    parts = spec.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2], loc
    if len(parts) == 2:
        return None, parts[0], parts[1], loc
    if len(parts) == 1:
        return None, None, parts[0], loc
    return None, None, None, loc


# ── 3. Drift issues (catalog vs the curated alias/canonical assumptions) ────────
def _append_drift_issues(db) -> int:
    """Append low-severity DRIFT/Info issues the manifest can see deterministically:
    e.g. two live columns that are each other's alias but both 'active' (an un-migrated
    dupe pair). Phase 1: Info-only, never blocking. The heavy catalog-vs-live drift check
    lives in warehouse_qa.py (graft B) — this is just the structural-dupe surface."""
    n = 0
    try:
        # An alias pair where BOTH the alias and the canonical exist as live active
        # columns somewhere = an un-consolidated dupe worth surfacing once.
        dupes = db.execute(
            """
            WITH live AS (
              SELECT DISTINCT lower(column_name) AS col FROM core.schema_catalog WHERE status='active'
            )
            SELECT a.alias, a.canonical_name
            FROM core.column_aliases a
            JOIN live la ON la.col = lower(a.alias)
            JOIN live lc ON lc.col = lower(a.canonical_name)
            WHERE lower(a.alias) <> lower(a.canonical_name)
            """
        ).fetchall()
        next_id = db.execute("SELECT COALESCE(max(issue_id),0)+1 FROM core.schema_issue").fetchone()[0]
        for alias, canon in dupes:
            # Don't re-append if an open issue for this pair already exists.
            already = db.execute(
                "SELECT 1 FROM core.schema_issue WHERE rule='DRIFT' AND status='open' "
                "AND column_name=? AND detail LIKE ?",
                [alias, f"%canonical `{canon}`%"],
            ).fetchone()
            if already:
                continue
            db.execute(
                """
                INSERT INTO core.schema_issue
                  (issue_id, rule, severity, classification, column_name, detail, status, phase_mode)
                VALUES (?, 'DRIFT', 'Info', 'DUPE', ?, ?, 'open', 'warn')
                """,
                [next_id, alias,
                 f"Live columns `{alias}` and canonical `{canon}` both active — an "
                 f"un-consolidated synonym pair. Migrate `{alias}` -> `{canon}` (expand/contract)."],
            )
            next_id += 1
            n += 1
    except Exception as exc:
        logger.warning("schema_manifest drift surface skipped: %s", exc)
    return n


def _rel(p: Path) -> str:
    try:
        return str(p.resolve().relative_to(REPO_ROOT))
    except Exception:
        return str(p)
