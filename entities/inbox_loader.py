"""Warehouse-inbox loader — legacy-pipeline retirement Gates 1b/2a.

Drains upsert batches that OTHER processes (DP-v2 dual-write, droplet capture
jobs) export to a shared drop directory, and lands them in the matching
`raw_pipeline_*` table of the warehouse. This is the warehouse-side half of the
pipeline-supabase ingestion cutover: exporters write batches straight to the
box instead of (only) Supabase, and this loader replaces the pg-mirror pull
table by table as exporters come online.

INBOX CONTRACT (fixed — exporters depend on it):
  * Drop dir: ``/root/warehouse-inbox/<dest_table>/*.parquet`` or ``*.ndjson``
    (override root via WAREHOUSE_INBOX_DIR). ``<dest_table>`` is the raw table
    name with or without the ``raw_pipeline_`` prefix (``campaigns`` and
    ``raw_pipeline_campaigns`` both land in ``raw_pipeline_campaigns``).
  * Each file is ONE upsert batch: the destination table's source columns plus
    ``_exported_at`` (export timestamp; required).
  * Exporters MUST write-then-rename (the loader skips dotfiles, ``*.tmp`` and
    ``*.partial`` so half-written files are never read).
  * The loader upserts by the table's NATURAL KEY (see LOADER_SPECS), then
    moves the file to ``<inbox>/.done/<dest_table>/``.
  * Empty/missing inbox → silent no-op (safe to run before exporters exist).
  * Schema mismatch (unknown dest table, missing key/_exported_at columns,
    file columns not present on an existing destination table, unreadable
    file) → the batch is LEFT IN PLACE and the run fails LOUDLY (phase
    failure / nonzero exit), never silently skipped.

KEY PARITY: for tables the pg mirror (entities/pipeline_mirror.py) also
writes, the ``_key`` expression is reused VERBATIM from pipeline_mirror.SPECS,
so an inbox row and a mirror row for the same natural entity collide on the
same primary key (idempotent during the dual-write phase). New tables without
a mirror equivalent (infra_*, domain_rr_*) define their key here and are
created on first batch (create-if-missing with the file's schema + the key).

Wiring: registers as its own phase ``inbox_loader``, ordered directly AFTER
``pipeline_mirror`` in core.config.PHASE_ORDER so mirror pulls land first and
inbox batches win same-night ties on upsert tables.

Standalone (unit-test / dry-run) mode — never touches the real warehouse:
    python -m entities.inbox_loader --db /tmp/throwaway.duckdb \
        --inbox /tmp/inbox [--dry-run]
`--db` is REQUIRED in CLI mode (the nightly phase handles the real DB under
the writer lock; ad-hoc drains go through
`python -m core.orchestrator --phase inbox_loader`).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from entities.pipeline_mirror import SPECS as MIRROR_SPECS
from entities.pipeline_mirror import _key_concat, _md5_concat

logger = logging.getLogger("entities.inbox_loader")

DEFAULT_INBOX_DIR = "/root/warehouse-inbox"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# DuckDB types we keep verbatim when creating a table from a file's inferred
# schema. Anything else (LIST, STRUCT, MAP, JSON, NULL, ...) is stored VARCHAR —
# the same convention the pg mirror uses for array columns.
_SCALAR_TYPE_RE = re.compile(
    r"^(BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|"
    r"UINTEGER|UBIGINT|FLOAT|DOUBLE|DECIMAL\(\d+,\s*\d+\)|DATE|TIME|"
    r"TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMP_NS|TIMESTAMP_MS|"
    r"TIMESTAMP_S|UUID|VARCHAR)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class LoadSpec:
    """Natural-key + write-mode contract for one destination table."""

    mode: str                      # 'insert' | 'insert_hash' | 'upsert'
    key_sql: str                   # SQL expr over the cast file scan -> _key
    key_cols: tuple[str, ...]      # file columns the key needs (validated)
    hash_cols: tuple[str, ...] | None = None  # insert_hash content fingerprint


def _from_mirror(table: str, key_cols: list[str]) -> LoadSpec:
    """Reuse pipeline_mirror's _key expression VERBATIM (zero key drift)."""
    s = MIRROR_SPECS[table]
    return LoadSpec(
        mode=s.mode,
        key_sql=s.key_sql,
        key_cols=tuple(key_cols),
        hash_cols=tuple(s.hash_cols) if s.hash_cols else None,
    )


# Destination-table registry. Anything NOT listed here is a contract violation
# and fails the run loudly (a new exporter must land its key mapping first).
LOADER_SPECS: dict[str, LoadSpec] = {
    # ---- tables the pg mirror also writes: key exprs shared via _from_mirror
    "campaigns": _from_mirror("campaigns", ["campaign_id"]),
    "campaign_data": _from_mirror("campaign_data", ["campaign_id", "step", "variant"]),
    "campaign_daily_metrics": _from_mirror("campaign_daily_metrics", ["campaign_id", "date"]),
    # variant_copy key includes content_hash computed from subject+body, so the
    # hash inputs are required file columns.
    "variant_copy": _from_mirror("variant_copy", ["campaign_id", "step", "variant", "subject", "body"]),
    "bounce_suppression": _from_mirror("bounce_suppression", ["id"]),
    # Mirror spec ARCHIVED 2026-06-26 (sync stopped; table kept) — key expression
    # kept identical to the archived Spec so inbox rows collide with existing rows.
    "contact_frequency_campaign_daily": LoadSpec(
        mode="upsert",
        key_sql=_key_concat(["campaign_id", "lead_email", "send_date"]),
        key_cols=("campaign_id", "lead_email", "send_date"),
    ),
    "reply_data": _from_mirror("reply_data", ["id"]),
    "lead_events": _from_mirror("lead_events", ["id"]),
    # ---- new tables (no pg-mirror equivalent; loader owns creation).
    # Keys = the exporters' Supabase ON CONFLICT columns (DP-v2 dual-write).
    "infra_accounts": LoadSpec(
        mode="upsert", key_sql="CAST(account_email AS VARCHAR)", key_cols=("account_email",)
    ),
    "infra_account_daily_metrics": LoadSpec(
        mode="upsert",
        key_sql=_key_concat(["account_email", "metric_date"]),
        key_cols=("account_email", "metric_date"),
    ),
    "infra_account_tag_mappings": LoadSpec(
        mode="upsert",
        key_sql=_key_concat(["workspace_slug", "account_email", "tag_id"]),
        key_cols=("workspace_slug", "account_email", "tag_id"),
    ),
    "domain_rr_state": LoadSpec(
        mode="upsert", key_sql="CAST(domain AS VARCHAR)", key_cols=("domain",)
    ),
    # instantly-quota-warden droplet job: append-only per-workspace lead-quota
    # snapshots (upstream PK = bigserial id; see jobs/instantly-quota-warden/
    # migrations/20260511_quota_warden.sql).
    "quota_warden_snapshots": LoadSpec(
        mode="insert", key_sql="CAST(id AS VARCHAR)", key_cols=("id",)
    ),
    # Append-only event log with no upstream id — synthetic key over the full
    # natural row + export time (two identical events at different exports both land).
    "domain_rr_events": LoadSpec(
        mode="insert",
        key_sql=_key_concat([
            "domain", "event_type", "from_status", "to_status",
            "sent_total", "reply_count", "rr_pct", "reason", "_exported_at",
        ]),
        key_cols=("domain", "event_type"),
    ),
}


@dataclass
class TableReport:
    files: int = 0
    rows_read: int = 0
    net_new: int = 0
    total_after: int = 0


@dataclass
class LoadReport:
    per_table: dict[str, TableReport] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def rows_read(self) -> int:
        return sum(t.rows_read for t in self.per_table.values())

    @property
    def net_new(self) -> int:
        return sum(t.net_new for t in self.per_table.values())


def _quote_path(p: Path) -> str:
    return str(p).replace("'", "''")


def _scan_sql(path: Path) -> str:
    if path.suffix == ".parquet":
        return f"read_parquet('{_quote_path(path)}')"
    if path.suffix == ".ndjson":
        return (
            f"read_json_auto('{_quote_path(path)}', format='newline_delimited', "
            f"maximum_object_size=33554432)"
        )
    raise ValueError(f"unsupported file type: {path.name} (want .parquet or .ndjson)")


def _normalize_dest(dirname: str) -> str:
    return dirname[len("raw_pipeline_"):] if dirname.startswith("raw_pipeline_") else dirname


def _file_columns(conn, scan: str) -> dict[str, str]:
    """column -> DuckDB type of the file scan, in file order."""
    rows = conn.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()
    return {r[0]: r[1] for r in rows}


def _table_columns(conn, table: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _create_table(conn, table: str, spec: LoadSpec, file_cols: dict[str, str]) -> None:
    """Create-if-missing from the file's schema + the loader's key/lineage columns."""
    defs = ["_key VARCHAR PRIMARY KEY"]
    for col, typ in file_cols.items():
        if col == "_exported_at":
            continue  # added explicitly below with a fixed type
        kept = typ if _SCALAR_TYPE_RE.match(typ.strip()) else "VARCHAR"
        defs.append(f'"{col}" {kept}')
    if spec.hash_cols:
        defs.append("content_hash VARCHAR")
    defs += ["_exported_at TIMESTAMP", "_loaded_at TIMESTAMP", "_run_id VARCHAR"]
    conn.execute(f"CREATE TABLE {table} ({', '.join(defs)})")
    logger.info("created %s (%d columns) from first inbox batch", table, len(defs))


def _load_file(conn, dest: str, spec: LoadSpec, path: Path, run_id: str, dry_run: bool) -> int:
    """Upsert one batch file into raw_pipeline_<dest>. Returns rows read."""
    table = f"raw_pipeline_{dest}"
    scan = _scan_sql(path)
    file_cols = _file_columns(conn, scan)

    bad_names = [c for c in file_cols if not _IDENT_RE.match(c)]
    if bad_names:
        raise ValueError(f"{path.name}: invalid column names {bad_names}")
    if "_exported_at" not in file_cols:
        raise ValueError(f"{path.name}: missing required _exported_at column")
    missing_keys = [c for c in spec.key_cols if c not in file_cols]
    if missing_keys:
        raise ValueError(f"{path.name}: missing natural-key column(s) {missing_keys} for {table}")

    table_cols = _table_columns(conn, table)
    if not table_cols:
        if dry_run:
            logger.info("[dry-run] would CREATE %s from %s schema", table, path.name)
        else:
            _create_table(conn, table, spec, file_cols)
        table_cols = _table_columns(conn, table)

    data_cols = [c for c in file_cols if c != "_exported_at"]
    if table_cols:
        unknown = [c for c in data_cols if c not in table_cols]
        if unknown:
            raise ValueError(
                f"{path.name}: column(s) {unknown} not present on {table} — "
                f"schema mismatch (add the column(s) to the table or fix the exporter)"
            )

    n_rows = conn.execute(f"SELECT count(*) FROM {scan}").fetchone()[0]
    if dry_run:
        logger.info("[dry-run] %s -> %s: %d rows validated OK", path.name, table, n_rows)
        return n_rows

    # _exported_at lineage lives on the destination too (added lazily so
    # pre-existing mirror tables gain it on first inbox batch).
    if "_exported_at" not in table_cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS _exported_at TIMESTAMP")
        table_cols = _table_columns(conn, table)

    include_hash = bool(spec.hash_cols) and "content_hash" in table_cols

    # Cast every file column to the destination column's type so parquet/ndjson
    # type drift (dates as strings, ints vs bigints, lists) lands cleanly and
    # the _key expression sees the same values the pg mirror computed keys over.
    src_items = [f'CAST("{c}" AS {table_cols[c]}) AS "{c}"' for c in data_cols]
    src_items.append('CAST("_exported_at" AS TIMESTAMP) AS _exported_at')
    hash_select = ""
    if spec.hash_cols:
        hash_select = f", {_md5_concat(list(spec.hash_cols))} AS content_hash"

    target_cols = ["_key"] + [f'"{c}"' for c in data_cols] \
        + (["content_hash"] if include_hash else []) + ["_exported_at", "_loaded_at", "_run_id"]
    proj = ["_key"] + [f'"{c}"' for c in data_cols] \
        + (["content_hash"] if include_hash else []) + ["_exported_at", "now()", "?"]

    sql = (
        f"INSERT INTO {table} ({', '.join(target_cols)}) "
        f"WITH src AS (SELECT {', '.join(src_items)}{hash_select} FROM {scan}), "
        f"keyed AS (SELECT src.*, {spec.key_sql} AS _key FROM src), "
        f"dedup AS (SELECT * FROM keyed "
        f"          QUALIFY row_number() OVER (PARTITION BY _key ORDER BY _exported_at DESC) = 1) "
        f"SELECT {', '.join(proj)} FROM dedup "
    )
    if spec.mode in ("insert", "insert_hash"):
        sql += "ON CONFLICT (_key) DO NOTHING"
    else:
        update_cols = [f'"{c}"' for c in data_cols] \
            + (["content_hash"] if include_hash else []) + ["_exported_at", "_loaded_at", "_run_id"]
        sql += "ON CONFLICT (_key) DO UPDATE SET " + ", ".join(
            f"{c} = excluded.{c}" for c in update_cols
        )

    conn.execute("BEGIN")
    try:
        conn.execute(sql, [run_id])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return n_rows


def _archive(inbox: Path, dest: str, path: Path) -> None:
    done_dir = inbox / ".done" / dest
    done_dir.mkdir(parents=True, exist_ok=True)
    target = done_dir / path.name
    if target.exists():
        target = done_dir / f"{path.stem}-{int(time.time())}{path.suffix}"
    shutil.move(str(path), str(target))


def _prune_done(inbox: Path, dry_run: bool) -> int:
    """Archived batches are load evidence, not a data store — the rows live in
    the warehouse. Prune .done files older than the retention window so the
    high-volume exporters (domain_rr_state alone parks ~600MB/day hourly full
    state) can't fill the disk. Retention days via
    WAREHOUSE_INBOX_DONE_RETENTION_DAYS (default 7; <=0 disables)."""
    retention_days = int(os.environ.get("WAREHOUSE_INBOX_DONE_RETENTION_DAYS", "7"))
    if dry_run or retention_days <= 0:
        return 0
    done = inbox / ".done"
    if not done.is_dir():
        return 0
    cutoff = time.time() - retention_days * 86400
    pruned = 0
    for f in done.rglob("*"):
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                pruned += 1
        except OSError:
            pass  # racing another pruner / permissions — retention is best-effort
    if pruned:
        logger.info(".done retention: pruned %d file(s) older than %dd", pruned, retention_days)
    return pruned


def load_inbox(conn, run_id: str, inbox_dir: str | None = None, dry_run: bool = False) -> LoadReport:
    """Drain every batch in the inbox. Never raises mid-drain: per-file errors are
    collected (file left in place) so healthy tables still land; caller fails
    the run if report.errors is non-empty."""
    inbox = Path(inbox_dir or os.environ.get("WAREHOUSE_INBOX_DIR", DEFAULT_INBOX_DIR))
    report = LoadReport()
    if not inbox.is_dir():
        return report  # no inbox yet — silent no-op by contract

    for entry in sorted(inbox.iterdir()):
        if entry.name.startswith(".") or not entry.is_dir():
            if entry.is_file() and not entry.name.startswith("."):
                report.errors.append(
                    f"{entry.name}: loose file in inbox root — batches must live in "
                    f"<dest_table>/ subdirectories"
                )
            continue
        dest = _normalize_dest(entry.name)
        files = sorted(
            p for p in entry.iterdir()
            if p.is_file() and not p.name.startswith(".")
            and p.suffix in (".parquet", ".ndjson")
            and not p.name.endswith((".tmp", ".partial"))
        )
        if not files:
            continue
        spec = LOADER_SPECS.get(dest)
        if spec is None:
            report.errors.append(
                f"{entry.name}/: no LOADER_SPECS entry for '{dest}' — "
                f"register its natural key in entities/inbox_loader.py before exporting"
            )
            continue

        table = f"raw_pipeline_{dest}"
        tr = report.per_table.setdefault(dest, TableReport())
        before_row = None
        try:
            before_row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
        except Exception:
            before_row = None  # table doesn't exist yet
        before = before_row[0] if before_row else 0

        for path in files:
            try:
                rows = _load_file(conn, dest, spec, path, run_id, dry_run)
                tr.files += 1
                tr.rows_read += rows
                if not dry_run:
                    _archive(inbox, dest, path)
            except Exception as exc:
                report.errors.append(f"{entry.name}/{path.name}: {exc}")
                logger.error("inbox batch FAILED %s/%s: %s", entry.name, path.name, exc)

        if not dry_run and tr.files:
            after = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            tr.net_new = after - before
            tr.total_after = after
        logger.info(
            "%s%s: %d file(s), %d rows read, %+d net new (total %d)",
            "[dry-run] " if dry_run else "", table,
            tr.files, tr.rows_read, tr.net_new, tr.total_after,
        )

    _prune_done(inbox, dry_run)
    return report


def run_inbox_loader(ctx: RunContext) -> PhaseResult:
    report = load_inbox(ctx.db, run_id=ctx.run_id)
    notes = {
        "per_table": {
            t: {"files": r.files, "rows_read": r.rows_read, "net_new": r.net_new,
                "total": r.total_after}
            for t, r in report.per_table.items()
        }
    }
    if report.errors:
        # Fail LOUD: phase logs 'failed' -> orchestrator exits partial (1).
        # Good batches above already landed + were archived; bad files stay put.
        raise RuntimeError(
            f"inbox loader: {len(report.errors)} batch(es) failed "
            f"(landed {report.rows_read} rows from the healthy ones first): "
            + " | ".join(report.errors)
        )
    return PhaseResult(rows_in=report.rows_read, rows_out=report.net_new, notes=notes)


def register(registry: Registry) -> None:
    registry.add_phase("inbox_loader", "all", run_inbox_loader)


# ---------------------------------------------------------------------------
# Standalone CLI (unit-test / dry-run harness). Requires an explicit --db so it
# can never touch the real warehouse by accident; the real DB is only written
# through the orchestrator phase (which holds the writer lock).
# ---------------------------------------------------------------------------

def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse inbox loader (standalone test mode)")
    parser.add_argument("--db", required=True, help="THROWAWAY DuckDB file (never the real warehouse)")
    parser.add_argument("--inbox", default=None, help=f"Inbox dir (default {DEFAULT_INBOX_DIR} / $WAREHOUSE_INBOX_DIR)")
    parser.add_argument("--dry-run", action="store_true", help="Validate + count only; no writes, no file moves")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    import duckdb  # local import: CLI-only dependency path

    conn = duckdb.connect(args.db)
    run_id = f"inboxcli-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    report = load_inbox(conn, run_id=run_id, inbox_dir=args.inbox, dry_run=args.dry_run)
    conn.close()

    for t, r in report.per_table.items():
        print(f"{t}: files={r.files} rows_read={r.rows_read} net_new={r.net_new} total={r.total_after}")
    if report.errors:
        for e in report.errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1
    if not report.per_table:
        print("inbox empty — no-op")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
