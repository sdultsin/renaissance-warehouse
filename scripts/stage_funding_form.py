#!/usr/bin/env python3
"""PRODUCE step for the Funding-Form meetings sheet mirror — run on the Mac (has the Google token).

This is the source-of-truth feed for core.meeting from 2026-06-01 onward (WS-E re-platform,
handoffs/2026-06-13-warehouse-audit-residual.md). The droplet has NO Google Sheets credentials,
so the pull MUST run here. Models scripts/stage_otd_billing.py exactly: pull the tab, encode each
row as a row_index/row_json CSV (identical shape to sources/sheets.write_tab_csv so
entities/sheets_mirror.py can consume it), and scp to the droplet staging dir.

Schedule on the Mac BEFORE the droplet meetings refresh (07:00 UTC):
    # launchd / cron, e.g. 06:30 UTC daily:
    <venv>/bin/python3 scripts/stage_funding_form.py

Manual:
    python3 scripts/stage_funding_form.py [--no-scp]

Token: ~/.config/mcp-google-sheets/token.json (full spreadsheets+drive scope).
Droplet staging dir: /root/core/sheets_staging (override via DROPLET_STAGING env).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TOKEN_PATH = os.path.expanduser("~/.config/mcp-google-sheets/token.json")
SHEET_ID = "1vaExhxu319o2CSoQtjWRV49lq5GowOeJzo1_GHXIIqM"
LOCAL_DIR = "/tmp/funding-form-staging"
DROPLET_HOST = "renaissance-worker"
DROPLET_STAGING = os.environ.get("DROPLET_STAGING", "/root/core/sheets_staging")

# (tab_name_in_sheet, csv_filename)  — csv_filename must match sources/sheets.SHEET_TABS
TABS = [
    ("Data", "funding_form__Data.csv"),
]

# Expected header layout (0-based), confirmed 2026-06-13. The consumer (entities/meeting.py)
# extracts by position; this guard fails loud if the sheet's columns are reordered/renamed so
# we never silently mis-attribute meetings. Keep in sync with meeting.py's _FF_COL_* indices.
EXPECTED_HEADER = [
    "Date", "Submission ID", "Submission time", "Channel", "Partner", "Advisor",
    "First Name", "Last Name", "Company", "Email", "Phone", "Annual Revenue",
    "Number of Employees", "Job Title", "Workspace", "Inbox Manager", "Our Email",
    "Campaign Manager", "Campaign Name", "Industry", "Subject Line", "Offer",
]


def write_tab_csv(rows, csv_path):
    """row_index, row_json (JSON array of cell values, all str, None->'')."""
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    n = 0
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["row_index", "row_json"])
        for idx, row in enumerate(rows):
            cells = ["" if c is None else str(c) for c in (row or [])]
            w.writerow([idx, json.dumps(cells, ensure_ascii=False)])
            n += 1
    return n


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-scp", action="store_true", help="write CSVs locally only")
    args = ap.parse_args(argv)

    creds = Credentials.from_authorized_user_file(TOKEN_PATH)
    svc = build("sheets", "v4", credentials=creds)
    api = svc.spreadsheets().values()

    staged = []
    for tab, fname in TABS:
        resp = api.get(spreadsheetId=SHEET_ID, range=tab).execute()
        rows = resp.get("values", [])
        if not rows:
            print(f"  ERROR: tab {tab!r} returned 0 rows — aborting (stale snapshot left in place).",
                  file=sys.stderr)
            return 2
        # Header-layout guard: fail loud on column drift so we never mis-attribute by position.
        header = [str(c).strip() for c in rows[0]]
        if header[:len(EXPECTED_HEADER)] != EXPECTED_HEADER:
            print(f"  ERROR: {tab!r} header drift.\n    expected={EXPECTED_HEADER}\n    got     ={header}",
                  file=sys.stderr)
            return 3
        path = os.path.join(LOCAL_DIR, fname)
        n = write_tab_csv(rows, path)
        print(f"  staged {tab!r}: {n} rows ({n-1} data) -> {path}")
        staged.append(path)

    if args.no_scp:
        print("--no-scp set; skipping upload.")
        return 0

    for path in staged:
        dest = f"{DROPLET_HOST}:{DROPLET_STAGING}/"
        subprocess.run(["scp", path, dest], check=True)
        print(f"  uploaded {os.path.basename(path)} -> {dest}")
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
