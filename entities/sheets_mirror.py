"""Google Sheets snapshot mirror.

CONSUME half of the Sheets ingest (see sources/sheets.py for the full design).
Reads per-tab CSVs from the staging dir and bulk-loads them into raw_sheets_*
tables, one row per sheet row, stored as row_json (JSON array of cell values).

REFERENCE DATA ONLY -- Instantly wins on any conflict. Nothing downstream should
treat these tables as canonical.

Idempotent within a run: DELETE FROM <table> WHERE _run_id = ? then INSERT.
A missing staging CSV is skipped (recorded in detail['skipped']), never fatal --
a stale/un-snapshotted sheet must not break the warehouse build.

NOTE: a 'sheets' phase already exists in core.config.PHASE_ORDER (slot at 04:15,
"Domain Tech Sheet, blacklist sheet"). This module registers under that existing
phase name, so config.py does NOT need editing for the phase to run. It does
still need to be added to the orchestrator's module-discovery list (whatever
imports the entity modules and calls their register()) -- mirror however
entities/pipeline_mirror.py is wired in. Per instructions this file does not
touch config.py.

Staging dir is configured via the SHEETS_STAGING_DIR env var (see
sources/sheets.py:staging_dir); default /root/core/sheets_staging.
"""
from __future__ import annotations

import os

from core.registry import RunContext
from core.sync_run import PhaseResult
from sources import sheets


def _load_tab_csv(db, table, sheet_id, tab, csv_path, run_id) -> int:
    """Idempotently load one staged tab CSV into its raw_sheets_* table."""
    db.execute(f"DELETE FROM {table} WHERE _run_id = ?", [run_id])
    # read_csv with all-VARCHAR is the safe choice: row_json holds embedded JSON
    # (quoted, may contain commas) so we let DuckDB's CSV parser handle quoting.
    db.execute(
        f"""
        INSERT INTO {table} (_sheet_id, _tab, row_index, row_json, _loaded_at, _run_id)
        SELECT ?, ?, CAST(row_index AS INTEGER), row_json, now(), ?
        FROM read_csv(?, header = true, columns = {{'row_index': 'VARCHAR', 'row_json': 'VARCHAR'}})
        """,
        [sheet_id, tab, run_id, csv_path],
    )
    n = db.execute(
        f"SELECT count(*) FROM {table} WHERE _run_id = ?", [run_id]
    ).fetchone()[0]
    return n


def run_sheets_mirror(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    sdir = sheets.staging_dir()
    total = 0
    loaded: dict[str, int] = {}
    skipped: list[dict] = []
    for table, sheet_id, tab, csv_name in sheets.SHEET_TABS:
        csv_path = os.path.join(sdir, csv_name)
        if not os.path.exists(csv_path):
            skipped.append({"table": table, "tab": tab, "reason": "csv_missing",
                            "path": csv_path})
            # A missing CSV means "tab not snapshotted this run". We leave any
            # prior-run rows in place (last-known-good); they carry an older
            # _run_id and can be filtered out by consumers that want current-run.
            continue
        n = _load_tab_csv(db, table, sheet_id, tab, csv_path, ctx.run_id)
        loaded[table] = n
        total += n

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={"staging_dir": sdir, "loaded": loaded, "skipped": skipped},
    )


def register(registry) -> None:
    # Register under the existing 'sheets' phase (config.PHASE_ORDER slot 04:15).
    registry.add_phase("sheets", "sheets_mirror", run_sheets_mirror)
