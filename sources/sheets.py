"""Google Sheets source adapter.

Snapshots trustworthy operational sheets into raw_sheets_* tables. These are
REFERENCE data only -- on any conflict with Instantly, Instantly wins.

WHY CSV-STAGED (read this before changing the design)
-----------------------------------------------------
The droplet runtime has NO Google Sheets credentials and
NO MCP bridge. The only place the sheets are reachable is an interactive Claude
session with the session-connected `google-sheets` MCP. So the ingest is split:

  1. PRODUCE  (interactive, this/any Claude session): pull each tab via the MCP
     tools mcp__google-sheets__get_sheet_data (paging with the `range` param for
     large tabs) and write one CSV per tab under SHEETS_STAGING_DIR. Each CSV has
     exactly two columns: row_index, row_json  (row_json = JSON array of the
     row's cell values, all as text). Helper `write_tab_csv()` below does the
     encoding so the format stays consistent.

  2. CONSUME  (orchestrator, droplet): entities/sheets_mirror.py reads those CSVs
     and bulk-loads them into raw_sheets_* via DuckDB read_csv. No network, no
     credentials -- it just ingests whatever the latest staged snapshot is.

This mirrors the existing split (sources/* = adapter/metadata, entities/* = the
bulk copy), and keeps the messy MCP interaction out of the unattended cron path.

TAB REGISTRY
------------
SHEET_TABS maps (table_name) -> (spreadsheet_id, tab_name, csv_filename). The
entity iterates this list. csv_filename is what the PRODUCE step must write into
SHEETS_STAGING_DIR. A missing CSV is treated as "tab not snapshotted this run"
and skipped (logged in PhaseResult.detail), NOT an error -- a stale sheet should
never break the warehouse build.
"""
from __future__ import annotations

import csv
import json
import os
import re
from typing import Iterable

# Default staging dir on the droplet. Override with SHEETS_STAGING_DIR.
DEFAULT_STAGING_DIR = "/root/core/sheets_staging"

# Spreadsheet IDs are read from the environment (not committed). Only needed if the
# sheets sync is resumed; empty by default.
DOMAIN_TECH_SHEET_ID = os.environ.get("DOMAIN_TECH_SHEET_ID", "")
BLACKLIST_SHEET_ID = os.environ.get("BLACKLIST_SHEET_ID", "")

# (table_name, spreadsheet_id, tab_name, csv_filename)
#
# RETIRED: this backfill-only reference data is no longer synced. The tab registry is
# intentionally empty so the CONSUME step (entities/sheets_mirror.py) is a no-op -- it
# iterates SHEET_TABS, so an empty list means "nothing to snapshot". The raw_sheets_*
# table definitions and the final snapshot data are PRESERVED (not dropped); the table
# constants and write_tab_csv() are kept for reference/restore. To resume syncing, add
# entries of the form (table_name, <sheet-id-const>, tab_name, csv_filename) below.
SHEET_TABS = []


def staging_dir() -> str:
    """Resolve the staging directory: SHEETS_STAGING_DIR env var, else default.

    NOTE: RunContext intentionally carries no free-form config (only run_id, db,
    credentials), so the staging dir is configured via the environment, matching
    how the rest of the warehouse resolves paths (CORE_DB_PATH, CORE_BACKUP_DIR).
    """
    return os.environ.get("SHEETS_STAGING_DIR", DEFAULT_STAGING_DIR)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def write_tab_csv(rows: Iterable[list], csv_path: str) -> int:
    """Encode raw sheet rows (list-of-lists from the MCP) into the staging CSV.

    Output columns: row_index, row_json. row_json is a JSON array of the row's
    cell values, every value coerced to str (None -> ""). Returns rows written.

    Call this from the interactive PRODUCE step after pulling/concatenating all
    pages of a tab via mcp__google-sheets__get_sheet_data. NOTE: strip any MCP
    sentinel/control rows (e.g. "TRUNCATED_FOR_TOKENS...", "PAGED_OK...") before
    passing rows here -- those are transport artifacts, not sheet data.
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    written = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["row_index", "row_json"])
        for idx, row in enumerate(rows):
            cells = ["" if c is None else str(c) for c in (row or [])]
            writer.writerow([idx, json.dumps(cells, ensure_ascii=False)])
            written += 1
    return written


def describe() -> dict:
    """Diagnostic: the tab registry this adapter knows how to mirror."""
    return {
        "staging_dir_default": staging_dir(),
        "tabs": [
            {"table": t, "spreadsheet_id": sid, "tab": tab, "csv": fn}
            for (t, sid, tab, fn) in SHEET_TABS
        ],
    }
