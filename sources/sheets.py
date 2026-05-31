"""Google Sheets source adapter.

Snapshots trustworthy operational sheets into raw_sheets_* tables. These are
REFERENCE data only -- on any conflict with Instantly, Instantly wins.

WHY CSV-STAGED (read this before changing the design)
-----------------------------------------------------
The droplet runtime (renaissance-worker) has NO Google Sheets credentials and
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

DOMAIN_TECH_SHEET_ID = "REDACTED"
BLACKLIST_SHEET_ID = "REDACTED"

# (table_name, spreadsheet_id, tab_name, csv_filename)
#
# Tab names below were confirmed against the LIVE sheets 2026-05-30. The original
# spec's blacklist tab "Master Blacklist" DOES NOT EXIST; the blacklist sheet's
# real tabs are Summary / All Domains / All Blocklisted Domains /
# Unflagged Blocklisted Domains / All Outlook Domains (MailIn). We snapshot the
# two operationally useful ones: "All Domains" (full per-domain inventory w/
# infra, workspaces, tags, blocklist flag, sent/replies) and "All Blocklisted
# Domains" (the actual blacklist tracking, one row per blocklisted domain).
SHEET_TABS = [
    ("raw_sheets_domain_tech_main",
     DOMAIN_TECH_SHEET_ID, "MAIN", "domain_tech__MAIN.csv"),
    ("raw_sheets_domain_tech_domains",
     DOMAIN_TECH_SHEET_ID, "Domains", "domain_tech__Domains.csv"),
    ("raw_sheets_domain_tech_domains_table",
     DOMAIN_TECH_SHEET_ID, "Domains(Table)", "domain_tech__Domains_Table.csv"),
    ("raw_sheets_domain_tech_admin_renaissance",
     DOMAIN_TECH_SHEET_ID, "ADMIN - Renaissance", "domain_tech__ADMIN_Renaissance.csv"),
    ("raw_sheets_blacklist_all_domains",
     BLACKLIST_SHEET_ID, "All Domains", "blacklist__All_Domains.csv"),
    ("raw_sheets_blacklist_blocklisted",
     BLACKLIST_SHEET_ID, "All Blocklisted Domains", "blacklist__All_Blocklisted_Domains.csv"),
]


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
