"""moderator_integrity_gate.py — W1 POST-APPLY INTEGRITY GATE (the dup-unique-index landmine guard).

Background (2026-06-26 incident): an index "rename" shipped as a bare
`CREATE UNIQUE INDEX IF NOT EXISTS <new> ON raw_pipeline_*(_key)` WITHOUT dropping the old index.
Because IF NOT EXISTS keys off the NEW name, the old unique index on `_key` survived, leaving TWO
unique indexes on the same `(_key)` column. The nightly upsert (which DELETE-then-INSERTs by `_key`
via ON CONFLICT) then FATALed with "Failed to delete all rows from index. Only deleted 0 out of 1
rows" because it could not consistently maintain both unique indexes. 9 of 12 raw_pipeline_* tables
still carry exactly this duplicate `ux_*_key` + `uxk_*` pair.

This gate is the structural check of record. It is:
  * callable STANDALONE for QA — `python moderator_integrity_gate.py --db <path>` (nonzero on fail);
  * callable from the moderator apply path — `run_post_apply_integrity(conn, ...)` right after
    `apply_ddl_file` commits (conn still open, flock still held).

Two checks (per the build):
  (a) STRUCTURAL (the gate of record): at most ONE unique index per `_key` on every upsert-target
      table (= every table carrying a unique index on `_key`; that is the raw_pipeline_* family plus
      any other ON-CONFLICT(_key) upsert target). Read via duckdb_indexes().
  (b) SENTINEL UPSERT SMOKE-TEST (defense-in-depth): for each upsert table, in a BEGIN/.../ROLLBACK,
      INSERT one synthetic row ON CONFLICT (_key) DO UPDATE and assert it does not raise the
      "Failed to delete all rows from index" FATAL. NOTE: on DuckDB v1.5.2/1.5.3 this FATAL no longer
      self-reproduces (the ART unique-index delete path was hardened), so the smoke-test FALSE-PASSES
      on the current engine — it is kept only to catch a future engine regression. (a) is the gate.

Scope control (so a pre-existing dup elsewhere does not false-block an unrelated apply):
  * `tables=None` (standalone / audit) -> check EVERY upsert-target table in the DB.
  * `tables=[...]` (the apply path) -> check ONLY the tables the just-applied DDL touched, so the
    9 pre-existing duplicate-index tables don't block a change that didn't touch them. The apply path
    passes the set of tables named in the applied SQL (raw_pipeline_* family is expanded when the SQL
    touches any raw_pipeline_* table, since the upsert-FATAL blast radius is that whole family).

On failure the gate (when given a writer `conn` / `core_db`) records one row per offending table to
core.schema_issue and emits a Slack alert (best-effort, gated on CC_SLACK_BOT_TOKEN), and APPENDS a
core.review_learnings row so future deep-reviews cite the incident. It NEVER raises into the apply
path: it returns a structured dict the caller uses to set the per-row status to 'failed' (block).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

# Tables named by a DDL/SQL string (the apply-path SCOPE). We pull the target of every op that can
# create/affect an index or upsert target: CREATE [UNIQUE] INDEX ... ON <tbl>, ALTER TABLE <tbl>,
# CREATE TABLE <tbl>, INSERT INTO <tbl>. Bare table name only (schema-qualified -> last segment),
# unquoted. The whole raw_pipeline_* family is expanded in when ANY raw_pipeline_* table is named,
# since the upsert-FATAL blast radius is that family (a shared-index landmine can surface elsewhere).
_RE_INDEX_ON = re.compile(r"\bON\s+(?P<tbl>[\w.\"]+)\s*\(", re.IGNORECASE)
_RE_ALTER_TBL = re.compile(r"\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)", re.IGNORECASE)
_RE_CREATE_TBL = re.compile(r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<tbl>[\w.\"]+)", re.IGNORECASE)
_RE_INSERT_INTO = re.compile(r"\bINSERT\s+INTO\s+(?P<tbl>[\w.\"]+)", re.IGNORECASE)
_RE_DROP_INDEX = re.compile(r"\bDROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(?P<idx>[\w.\"]+)", re.IGNORECASE)


def _bare(tok: str) -> str:
    tok = (tok or "").strip().strip('"')
    return tok.split(".")[-1].strip('"') if "." in tok else tok


def tables_touched_by_sql(sql: str) -> list[str]:
    """The set of bare table names a SQL string targets, used to SCOPE the apply-path gate so a
    pre-existing dup on an UNTOUCHED table cannot false-block this apply. Expands the whole
    raw_pipeline_* family in when any raw_pipeline_* table is referenced (shared blast radius).
    Returns [] when nothing schema-relevant is named (caller then treats as "no scoping needed")."""
    if not sql:
        return []
    names: set[str] = set()
    for rx in (_RE_INDEX_ON, _RE_ALTER_TBL, _RE_CREATE_TBL, _RE_INSERT_INTO):
        for m in rx.finditer(sql):
            b = _bare(m.group("tbl"))
            if b:
                names.add(b)
    # a DROP INDEX names the index, not the table; we can't map it back to a table by name alone, but
    # any DROP INDEX in the same file is almost always paired with a CREATE INDEX ON <tbl> we already
    # captured — so no extra work needed. (Kept as a hook in case a future op needs it.)
    _ = _RE_DROP_INDEX
    if any(n.lower().startswith("raw_pipeline") for n in names):
        names.add("__raw_pipeline_family__")  # sentinel -> expanded by run_post_apply_integrity
    return sorted(names)

# ── Normalized `_key` unique-enforcement detector (Bypass 1 + Bypass 2 hardened) ──────────────────
# A table's unique-enforcement on `_key` is the SUM of two independent ART-backed enforcements DuckDB
# can carry, EACH of which makes the nightly upsert's DELETE-then-INSERT path FATAL when there is more
# than one:
#   (1) a UNIQUE INDEX whose sole indexed expression is `_key`  -> duckdb_indexes()
#   (2) a UNIQUE / PRIMARY KEY *constraint* whose sole column is `_key`  -> duckdb_constraints()
# These two catalogs are DISJOINT for this class (verified on duckdb 1.5.3): an inline `_key VARCHAR
# UNIQUE` / `PRIMARY KEY` constraint appears ONLY in duckdb_constraints() (NOT in duckdb_indexes());
# an explicit `CREATE UNIQUE INDEX ... ON t(_key)` appears ONLY in duckdb_indexes(). So summing the
# two counts never double-counts a single enforcement — it correctly catches BYPASS 1 (inline UNIQUE
# constraint + a separate unique index = 1 + 1 = 2 enforcements on _key that W1 previously missed
# because it counted only duckdb_indexes()).
#
# BYPASS 2 (column casing): duckdb_indexes().expressions renders the literal source casing, so
# `ON t(_KEY)` reads as '[_KEY]'. We NORMALIZE the index expression — strip the surrounding [], strip
# any embedded double-quotes, lowercase, trim — and compare == '_key', so '[_KEY]', '["_key"]', etc.
# all match. The constraint side compares the lowercased sole column name to '_key' the same way.
#
# `_KEY_INDEX_EXPR_NORM` is the normalized index-expression SQL fragment (reused everywhere a
# duckdb_indexes() row is matched against `_key`).
_KEY_INDEX_EXPR_NORM = "replace(lower(trim(expressions, '[]')), '\"', '')"

# Per-table count of unique INDEXES on `_key` (normalized, case-insensitive, quote-stripped).
_KEY_UNIQUE_INDEX_COUNTS_SQL = (
    "SELECT table_name, COUNT(*) AS n, "
    "       string_agg(index_name, ', ' ORDER BY index_name) AS names "
    "FROM duckdb_indexes() "
    f"WHERE is_unique = TRUE AND {_KEY_INDEX_EXPR_NORM} = '_key' "
    "{table_filter} "
    "GROUP BY table_name"
)

# Per-table count of UNIQUE / PRIMARY KEY *constraints* whose sole column is `_key`. duckdb_constraints()
# exposes constraint_column_names as a VARCHAR[]; the single-column case is list_lower(...) == ['_key'].
# (A composite PK/UNIQUE that merely *includes* _key does NOT enforce single-column uniqueness on _key
# and is correctly excluded.)
_KEY_UNIQUE_CONSTRAINT_COUNTS_SQL = (
    "SELECT table_name, COUNT(*) AS n, "
    "       string_agg(constraint_type, ', ' ORDER BY constraint_type) AS names "
    "FROM duckdb_constraints() "
    "WHERE constraint_type IN ('UNIQUE', 'PRIMARY KEY') "
    "  AND list_transform(constraint_column_names, x -> lower(x)) = ['_key'] "
    "{table_filter} "
    "GROUP BY table_name"
)


def _combined_key_enforcement_counts(conn, tables=None) -> dict:
    """{table_name: {"idx": n_unique_indexes_on__key, "cons": n_unique_or_pk_constraints_on__key,
                      "total": idx+cons, "index_names": [...], "constraint_kinds": [...]}}
    over every (in-scope) table that carries AT LEAST ONE such enforcement. This is the SINGLE source
    of truth for W1's "how many unique enforcements does _key have" question — it fuses the two
    disjoint catalogs (duckdb_indexes() + duckdb_constraints()) and normalizes column casing, closing
    Bypass 1 (constraint-backed enforcement) and Bypass 2 (casing). Read-only; degrades to {} on any
    catalog error (e.g. duckdb_constraints() unavailable on a future engine — handled by the caller)."""
    flt, params = _table_filter(tables)
    out: dict = {}
    # (1) unique indexes on _key
    try:
        for tname, n, names in conn.execute(
                _KEY_UNIQUE_INDEX_COUNTS_SQL.format(table_filter=flt), params).fetchall():
            e = out.setdefault(tname, {"idx": 0, "cons": 0, "index_names": [], "constraint_kinds": []})
            e["idx"] = int(n)
            e["index_names"] = [s.strip() for s in (names or "").split(",") if s.strip()]
    except Exception:
        raise  # an index-probe failure must propagate so the caller can fail-closed
    # (2) UNIQUE / PRIMARY KEY constraints on _key (Bypass 1). duckdb_constraints() exists on 1.5.3;
    # if a future/older engine lacks it the index-only count still holds and we degrade gracefully.
    try:
        for tname, n, names in conn.execute(
                _KEY_UNIQUE_CONSTRAINT_COUNTS_SQL.format(table_filter=flt), params).fetchall():
            e = out.setdefault(tname, {"idx": 0, "cons": 0, "index_names": [], "constraint_kinds": []})
            e["cons"] = int(n)
            e["constraint_kinds"] = [s.strip() for s in (names or "").split(",") if s.strip()]
    except Exception:
        pass  # constraint catalog unavailable -> index-only enforcement count (still a valid floor)
    for e in out.values():
        e["total"] = e["idx"] + e["cons"]
    return out


# Every table that carries a unique index OR a UNIQUE/PK constraint on `_key` is an ON-CONFLICT(_key)
# upsert target (the upsert DELETE-then-INSERTs by _key). Used to scope the smoke-test.
_UPSERT_TABLES_INDEX_SQL = (
    "SELECT DISTINCT table_name FROM duckdb_indexes() "
    f"WHERE is_unique = TRUE AND {_KEY_INDEX_EXPR_NORM} = '_key' "
    "{table_filter}"
)
_UPSERT_TABLES_CONSTRAINT_SQL = (
    "SELECT DISTINCT table_name FROM duckdb_constraints() "
    "WHERE constraint_type IN ('UNIQUE', 'PRIMARY KEY') "
    "  AND list_transform(constraint_column_names, x -> lower(x)) = ['_key'] "
    "{table_filter}"
)

# The catalog-delta snapshot deliberately watches the WHOLE upsert blast-radius (all current unique-on-
# _key tables) regardless of which tables the SQL text named, so a scope-regex miss (the exact QA-D
# bypass) cannot create a blind spot — the catalog defines what we watch, not the parsed SQL.


def snapshot_key_index_counts(conn) -> dict:
    """{table_name: total_unique_enforcements_on__key} across EVERY table that currently has at least
    one unique INDEX or UNIQUE/PK CONSTRAINT on `_key` (the upsert blast radius). Regex-independent: it
    reads the live catalog (both duckdb_indexes() AND duckdb_constraints(), normalized for casing), so
    it counts a constraint-backed enforcement (Bypass 1) and a `_KEY`-cased index (Bypass 2) toward the
    total. Used as the PRE-apply baseline so the post-apply check fails only on a count that INCREASED.
    A table with no _key unique enforcement simply does not appear (treated as 0 on the AFTER side)."""
    try:
        counts = _combined_key_enforcement_counts(conn, tables=None)
    except Exception:
        return {}
    return {t: e["total"] for t, e in counts.items()}

_FATAL_NEEDLE = "failed to delete all rows from index"


_RAW_FAMILY_SENTINEL = "__raw_pipeline_family__"


def _table_filter(tables) -> tuple[str, list]:
    """Build the scoping clause + params (empty when tables is None = check ALL upsert tables).
    The `__raw_pipeline_family__` sentinel (added by tables_touched_by_sql when a raw_pipeline_* table
    is referenced) expands to `OR table_name LIKE 'raw_pipeline%'` so the whole upsert blast radius is
    checked when any raw_pipeline_* table is touched."""
    if not tables:
        return "", []
    raw_family = _RAW_FAMILY_SENTINEL in tables
    names = sorted({t for t in tables if t and t != _RAW_FAMILY_SENTINEL})
    clauses = []
    params: list = []
    if names:
        placeholders = ", ".join("?" for _ in names)
        clauses.append(f"table_name IN ({placeholders})")
        params.extend(names)
    if raw_family:
        clauses.append("table_name LIKE 'raw_pipeline%'")
    if not clauses:
        return "", []
    return "AND (" + " OR ".join(clauses) + ")", params


def _enforcement_labels(entry: dict) -> list[str]:
    """Human-readable list of the unique enforcements on _key for a failure report: each index name,
    plus 'UNIQUE constraint' / 'PRIMARY KEY constraint' for each constraint."""
    labels = list(entry.get("index_names") or [])
    for kind in (entry.get("constraint_kinds") or []):
        labels.append(f"{kind} constraint")
    return labels


def find_duplicate_key_indexes(conn, tables=None) -> list[dict]:
    """ABSOLUTE check: return [{table, count, indexes:[...]}] for every (in-scope) table with >1 unique
    ENFORCEMENT on _key — counting unique INDEXES on _key (any column casing) PLUS UNIQUE/PRIMARY KEY
    CONSTRAINTS on _key together (idx + cons > 1). Empty list == PASS. `conn` is any open DuckDB
    connection (read-only is fine). This is the standalone/audit gate of record (the cron guard).

    This closes BYPASS 1 (an inline `_key ... UNIQUE` / `PRIMARY KEY` constraint plus a separate unique
    index = two enforcements on _key that the old index-only count missed) and BYPASS 2 (a `_KEY`-cased
    index expression that the old literal '[_key]' match missed). The `count`/`indexes` fields keep
    their names for back-compat with core.schema_issue / the CLI, but `indexes` now lists every
    enforcement (index names + constraint kinds). The apply path uses the catalog-DELTA variant
    (find_newly_introduced_dup_key_indexes) so a PRE-EXISTING dup can't false-block an apply that
    didn't create it."""
    counts = _combined_key_enforcement_counts(conn, tables=tables)
    out = []
    for table_name in sorted(counts):
        entry = counts[table_name]
        if entry["total"] > 1:
            out.append({"table": table_name, "count": int(entry["total"]),
                        "indexes": _enforcement_labels(entry),
                        "unique_indexes": int(entry["idx"]), "unique_constraints": int(entry["cons"])})
    return out


def _current_dup_index_names(conn, table_name) -> list[str]:
    """The enforcement labels (index names + constraint kinds) on _key currently on `table_name`,
    for the failure report. Normalized + constraint-aware (matches the detector)."""
    try:
        counts = _combined_key_enforcement_counts(conn, tables=[table_name])
    except Exception:
        return []
    entry = counts.get(table_name)
    return _enforcement_labels(entry) if entry else []


def find_newly_introduced_dup_key_indexes(conn, pre_snapshot) -> list[dict]:
    """CATALOG-DELTA check (the apply-path gate of record). Compare the AFTER per-table unique-_key
    ENFORCEMENT totals (indexes + UNIQUE/PK constraints, normalized) against `pre_snapshot` (a
    {table: total} captured BEFORE the apply via snapshot_key_index_counts). Return one offender per
    table whose total INCREASED into a duplicate (after > before AND after >= 2) — i.e. a NEWLY
    introduced duplicate unique _key enforcement.

    Regex-INDEPENDENT: it reads the live catalog (duckdb_indexes() + duckdb_constraints()), so it
    catches a 2nd unique _key enforcement however the DDL was written — a separate index (USING art /
    comments — the QA-D bypass), a `_KEY`-cased index (Bypass 2), or an inline UNIQUE / PRIMARY KEY
    constraint (Bypass 1). Pre-existing duplicates (before >= 2, unchanged) do NOT block (no
    false-block). A first enforcement (0 -> 1) does NOT block (not a dup). A genuine N -> N+1 with the
    result >= 2 blocks. `pre_snapshot` is normalized so a missing key reads as 0."""
    pre = {k: int(v) for k, v in (pre_snapshot or {}).items()}
    after = snapshot_key_index_counts(conn)
    out: list[dict] = []
    for table_name, after_n in sorted(after.items()):
        before_n = pre.get(table_name, 0)
        if after_n > before_n and after_n >= 2:
            out.append({"table": table_name, "count": int(after_n),
                        "indexes": _current_dup_index_names(conn, table_name),
                        "before": before_n, "after": int(after_n)})
    return out


def _upsert_tables(conn, tables=None) -> list[str]:
    """Every (in-scope) table that carries a unique INDEX or a UNIQUE/PK CONSTRAINT on _key (the
    ON-CONFLICT(_key) upsert targets), normalized + constraint-aware."""
    flt, params = _table_filter(tables)
    names: set[str] = set()
    try:
        for (t,) in conn.execute(_UPSERT_TABLES_INDEX_SQL.format(table_filter=flt), params).fetchall():
            names.add(t)
    except Exception:
        pass
    try:
        for (t,) in conn.execute(_UPSERT_TABLES_CONSTRAINT_SQL.format(table_filter=flt), params).fetchall():
            names.add(t)
    except Exception:
        pass
    return sorted(names)


def sentinel_upsert_smoketest(conn, tables=None) -> list[dict]:
    """For each in-scope upsert table, BEGIN; INSERT one synthetic row ON CONFLICT (_key) DO UPDATE;
    ROLLBACK — and assert it does NOT raise the "Failed to delete all rows from index" FATAL. Returns
    [{table, error}] for tables that raised the FATAL (empty == clean).

    Defense-in-depth only: on DuckDB v1.5.2/1.5.3 the FATAL no longer self-reproduces, so this is a
    sentinel for a future engine regression, NOT the primary detector. Requires a WRITER connection;
    on a read-only connection it degrades to a no-op (BEGIN/INSERT will error harmlessly and we skip).
    Each table is fully isolated in its own BEGIN/ROLLBACK so a probe never persists a row."""
    failures: list[dict] = []
    for tbl in _upsert_tables(conn, tables):
        # Synthetic sentinel key that cannot collide with real data; the upsert path is exercised by
        # running the same INSERT twice inside the txn so the 2nd one takes the ON CONFLICT branch
        # (DELETE-then-INSERT against the unique index — the exact path that FATALed).
        skey = f"__gate_sentinel__{int(time.time()*1000)}"
        try:
            conn.execute("BEGIN")
        except Exception:
            # read-only connection (or txn already open) — can't smoke-test; skip (structural check
            # is the gate of record). Make sure we don't leave a dangling txn.
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            return failures
        try:
            # Discover the table's columns so we can build a valid INSERT (only _key must be set).
            cols = conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? ORDER BY ordinal_position", [tbl]).fetchall()
            colnames = [c[0] for c in cols]
            if "_key" not in colnames:
                conn.execute("ROLLBACK")
                continue
            collist = ", ".join(f'"{c}"' for c in colnames)
            valexpr = ", ".join(("?" if c == "_key" else "NULL") for c in colnames)
            ins = (f'INSERT INTO "{tbl}" ({collist}) VALUES ({valexpr}) '
                   f'ON CONFLICT (_key) DO UPDATE SET _key = EXCLUDED._key')
            conn.execute(ins, [skey])   # insert sentinel
            conn.execute(ins, [skey])   # 2nd time -> ON CONFLICT DELETE-then-INSERT (the FATAL path)
            conn.execute("ROLLBACK")    # never persist the sentinel
        except Exception as e:  # noqa: BLE001
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            msg = f"{type(e).__name__}: {e}"
            if _FATAL_NEEDLE in msg.lower():
                failures.append({"table": tbl, "error": msg})
            # a non-FATAL error (e.g. NOT NULL on some other column) is NOT a gate failure — the
            # structural check (a) owns correctness; the smoke-test only flags the specific FATAL.
    return failures


# ── core.schema_issue writer + Slack alert + learnings append (only when a writer conn is given) ──
def _record_schema_issue(conn, ddl_file, offenders) -> int:
    """Write one core.schema_issue row per offending table (best-effort; never raises into the gate).
    issue_id has no sequence default on this table, so we hand-assign max+1 (matches the existing
    nightly-drift writer convention)."""
    n = 0
    try:
        base = conn.execute(
            "SELECT COALESCE(max(issue_id), 0) FROM core.schema_issue").fetchone()[0] or 0
    except Exception:
        base = int(time.time())  # table absent / unreadable — fall back to a monotonic-ish id
    for off in offenders:
        base += 1
        detail = (f"Post-apply integrity gate: table {off['table']} has {off['count']} UNIQUE "
                  f"enforcements on (_key) [{', '.join(off['indexes'])}] "
                  f"(unique indexes={off.get('unique_indexes', '?')}, "
                  f"unique/PK constraints={off.get('unique_constraints', '?')}). Two unique "
                  f"enforcements on _key (any mix of unique index + inline UNIQUE/PRIMARY KEY "
                  f"constraint) FATAL the nightly upsert ('Failed to delete all rows from index'). "
                  f"An index/constraint rename MUST be DROP-old + CREATE-new in the same migration. "
                  f"Drop the redundant index/constraint so exactly one unique enforcement on (_key) "
                  f"remains.")
        try:
            conn.execute(
                "INSERT INTO core.schema_issue (issue_id, rule, severity, classification, "
                "table_schema, table_name, column_name, ddl_file, detail, consumers, status, "
                "phase_mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [base, "W1-DUP-UNIQUE-INDEX", "Error", "INTEGRITY", "main", off["table"], "_key",
                 ddl_file, detail, json.dumps(off["indexes"]), "open", "block"])
            n += 1
        except Exception:
            pass  # one bad insert must not stop the others / the gate
    return n


def _slack_alert(text: str) -> bool:
    """Best-effort Slack alert to #cc-sam (mirrors scripts/warehouse_drift_guard.sh post_slack()).
    Gated on CC_SLACK_BOT_TOKEN; a no-token / failure path is a silent no-op (never breaks the gate)."""
    token = os.environ.get("CC_SLACK_BOT_TOKEN", "").strip()
    if not token:
        return False
    channel = os.environ.get("CC_SLACK_CHANNEL", "C0AR0EA21C1")  # #cc-sam
    mention = os.environ.get("CC_SLACK_MENTION", "<@U0AM2CQHW9E>")
    try:
        import urllib.request
        body = json.dumps({"channel": channel, "text": f"{mention} {text}"}).encode("utf-8")
        req = urllib.request.Request(
            "https://slack.com/api/chat.postMessage", data=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8")).get("ok", False)
    except Exception:
        return False


def _append_learning(conn, offenders, ddl_file) -> bool:
    """APPEND a core.review_learnings row when the gate CATCHES an incident, so future deep-reviews
    cite it. Best-effort; degrades silently if the table isn't present yet (DDL staged, not applied)."""
    try:
        tbls = ", ".join(o["table"] for o in offenders)
        lesson = (f"Post-apply integrity gate CAUGHT a duplicate unique index on (_key): {tbls}. "
                  f"Two unique indexes on _key FATAL the nightly upsert. Almost always a bare "
                  f"CREATE UNIQUE INDEX IF NOT EXISTS used as a 'rename' without dropping the old one.")
        rule_text = ("BLOCK any change that leaves >1 UNIQUE index on (_key) for a raw_pipeline_* / "
                     "upsert-target table. An index rename MUST be DROP-old + CREATE-new in the SAME "
                     "migration (expand/contract). Never ship a standalone CREATE UNIQUE INDEX IF NOT "
                     "EXISTS as a rename.")
        conn.execute(
            "INSERT INTO core.review_learnings "
            "(category, lesson, rule_text, example_ddl, source_incident, outcome, severity) "
            "VALUES (?,?,?,?,?,?,?)",
            ["index", lesson, rule_text, None,
             f"post-apply gate W1 on {ddl_file or 'apply-now'}", "caught", "block"])
        return True
    except Exception:
        return False


def run_post_apply_integrity(conn, *, tables=None, ddl_file=None, record=True,
                             smoke_test=True, pre_snapshot=None) -> dict:
    """THE callable wired into the apply path (and reused by the standalone CLI). Runs the dup-unique-
    index check (the gate of record) + the sentinel upsert smoke-test (defense-in-depth) over the
    in-scope tables. On failure (when record=True and `conn` is a writer) records to core.schema_issue,
    appends a learning, and fires a Slack alert. NEVER raises — returns:
        {ok: bool, offenders: [...], smoke_failures: [...], detail: str,
         recorded_issues: int, learning_appended: bool, alerted: bool, mode: str}
    ok=False means the apply must be blocked (status='failed').

    TWO modes:
      * pre_snapshot is None  -> ABSOLUTE mode (standalone / cron audit): block if ANY in-scope table
        has >1 unique _key index. This is the cron guard's gate of record.
      * pre_snapshot is a dict -> CATALOG-DELTA mode (the apply path): block ONLY on a table whose
        unique-on-_key count INCREASED into a duplicate vs the pre-apply snapshot (a NEWLY introduced
        duplicate). Regex-independent (reads the live catalog), so it kills the QA-D USING/comment
        bypass on the apply path, and a PRE-EXISTING dup (none on the authoritative DB) can't false-
        block. `tables` scoping is ignored in delta mode — the snapshot already watches the whole
        upsert blast radius, and the BEFORE/AFTER comparison is what isolates this apply's effect."""
    delta_mode = pre_snapshot is not None
    result = {"ok": True, "offenders": [], "smoke_failures": [], "detail": "integrity gate passed",
              "recorded_issues": 0, "learning_appended": False, "alerted": False,
              "mode": "delta" if delta_mode else "absolute"}
    try:
        if delta_mode:
            offenders = find_newly_introduced_dup_key_indexes(conn, pre_snapshot)
        else:
            offenders = find_duplicate_key_indexes(conn, tables=tables)
    except Exception as e:  # noqa: BLE001 — a probe failure must fail-closed (we can't prove safety)
        result.update(ok=False, detail=f"integrity probe unavailable ({type(e).__name__}: {e}) — "
                                       f"failing closed (cannot prove no duplicate unique _key index)")
        return result
    result["offenders"] = offenders

    smoke_failures = []
    if smoke_test:
        try:
            # In delta mode the smoke-test is scoped to the NEWLY-introduced offender tables only, so a
            # pre-existing dup elsewhere can never even be probed (let alone false-block). In absolute
            # mode it honours the caller's `tables` scope as before.
            smoke_scope = [o["table"] for o in offenders] if delta_mode else tables
            if not delta_mode or smoke_scope:
                smoke_failures = sentinel_upsert_smoketest(conn, tables=smoke_scope)
        except Exception:
            smoke_failures = []  # smoke-test is best-effort defense-in-depth; structural check governs
    result["smoke_failures"] = smoke_failures

    if not offenders and not smoke_failures:
        return result

    # FAILURE — block the apply and (if we hold a writer) record + alert.
    result["ok"] = False
    parts = []
    if offenders:
        label = "NEWLY-INTRODUCED duplicate unique _key index on" if delta_mode \
            else "duplicate unique _key index on"
        parts.append(label + ": " + "; ".join(
            f"{o['table']} ({', '.join(o['indexes'])})"
            + (f" [{o['before']}->{o['after']}]" if delta_mode and 'before' in o else "")
            for o in offenders))
    if smoke_failures:
        parts.append("sentinel upsert FATAL on: "
                     + ", ".join(s["table"] for s in smoke_failures))
    result["detail"] = "POST-APPLY INTEGRITY GATE FAILED — " + " | ".join(parts)

    if record:
        try:
            result["recorded_issues"] = _record_schema_issue(conn, ddl_file, offenders)
        except Exception:
            pass
        try:
            result["learning_appended"] = _append_learning(conn, offenders, ddl_file)
        except Exception:
            pass
        try:
            result["alerted"] = _slack_alert(
                "Warehouse apply BLOCKED by post-apply integrity gate (W1): " + result["detail"]
                + (f" [ddl_file={ddl_file}]" if ddl_file else ""))
        except Exception:
            pass
    return result


# ── standalone CLI (QA: `python moderator_integrity_gate.py --db <path>`; nonzero on failure) ──────
def _main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="W1 post-apply integrity gate (dup unique _key index).")
    ap.add_argument("--db", required=True, help="path to the DuckDB file to check")
    ap.add_argument("--table", action="append", default=None,
                    help="restrict to this table (repeatable); default = all upsert-target tables")
    ap.add_argument("--read-only", action="store_true", default=False,
                    help="open the DB read-only (structural check only; no smoke-test writer)")
    ap.add_argument("--no-smoke-test", action="store_true", default=False,
                    help="skip the sentinel upsert smoke-test (run only the structural check)")
    ap.add_argument("--no-record", action="store_true", default=False,
                    help="do NOT write core.schema_issue / learnings / Slack on failure (QA default)")
    ap.add_argument("--audit", action="store_true", default=False,
                    help="absolute whole-DB check (the cron guard): block if ANY upsert-target table "
                         "has >1 unique index on _key. This is the default for the standalone CLI; the "
                         "flag just makes the contract explicit. (The catalog-DELTA mode is reserved "
                         "for the apply path, which passes a pre-apply snapshot.)")
    ap.add_argument("--json", action="store_true", default=False, help="emit the result as JSON")
    args = ap.parse_args(argv)
    # The standalone CLI ALWAYS runs the absolute check (pre_snapshot stays None below); --audit is an
    # explicit alias for that default and is mutually exclusive with --table only by convention (a
    # scoped absolute check is still valid for spot-checking one table).

    import duckdb
    if not os.path.exists(args.db):
        print(f"GATE ERROR: db not found: {args.db}", file=sys.stderr)
        return 2
    read_only = args.read_only or args.no_smoke_test  # a structural-only run can be read-only
    conn = duckdb.connect(args.db, read_only=read_only)
    try:
        res = run_post_apply_integrity(
            conn, tables=args.table, ddl_file=f"cli:{os.path.basename(args.db)}",
            record=(not args.no_record), smoke_test=(not args.no_smoke_test))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if args.json:
        print(json.dumps(res, indent=2, default=str))
    else:
        if res["ok"]:
            print("GATE PASS — no duplicate unique _key index; no sentinel upsert FATAL.")
        else:
            print("GATE FAIL — " + res["detail"])
            for o in res["offenders"]:
                print(f"  [dup-index] {o['table']}: {o['count']} unique _key indexes "
                      f"[{', '.join(o['indexes'])}]")
            for s in res["smoke_failures"]:
                print(f"  [smoke-FATAL] {s['table']}: {s['error']}")
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(_main())
