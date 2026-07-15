#!/usr/bin/env python3
"""PRODUCE step for the Pre-IPO partner booking sheets — run on the Mac (has the Google token).

Two partner booking desks log Pre-IPO meetings in their own Google Sheets, OUTSIDE the master
Funding-Form sheet (which is Business-Funding-only). This stages them into row_index/row_json CSVs
(identical shape to sources/sheets.write_tab_csv) and scps them to the droplet staging dir so the
warehouse `sheets` phase consumes them on the next run (-> raw_sheets_summit_ventures_leads /
raw_sheets_collins_preipo_leads -> core.meeting partner-sheet branch).

    python3 scripts/stage_partner_booking_sheets.py [--no-scp]

Refresh: re-run after the partners add bookings (wire to a launchd job, like the Funding-Form producer).
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
LOCAL_DIR = os.environ.get("LOCAL_STAGING_DIR", "/tmp/partner-booking-staging")  # [2026-07-14] droplet-run: /root/core/sheets_staging + --no-scp
DROPLET_HOST = "renaissance-worker"
DROPLET_STAGING = os.environ.get("DROPLET_STAGING", "/root/core/sheets_staging")

SUMMIT_SHEET_ID = "1oKlY_2qI-p0oH4d8UAOE3GpceiFU4OIWx1aDAM5RzRY"   # Summit Ventures - Leads (SMS Pre-IPO)
COLLINS_SHEET_ID = "1IZzmCXtbtrpZYbxuU1qkxoOkITL4zmP2In6glpffmYw"  # Collins Investment Partners - Leads (Pre-IPO)

# (spreadsheet_id, tab_name, csv_filename) — csv_filename must match sources/sheets.SHEET_TABS.
# Collins Sheet2 ("Email more info") is a name->email enrichment supplement, NOT a booking source;
# it is intentionally not staged/projected (see DDL header).
JOBS = [
    (SUMMIT_SHEET_ID,  "Sheet1",  "summit_ventures__Sheet1.csv"),
    (COLLINS_SHEET_ID, "Collins", "collins_preipo__Collins.csv"),
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

    # [2026-07-14 creds-rebuild] SA auth; runs on the DROPLET now (Mac OAuth client destroyed)
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_SA_KEY", "/root/.config/gcp-sa/droplet-sheets-sync.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    svc = build("sheets", "v4", credentials=creds)
    api = svc.spreadsheets().values()

    staged = []
    for sheet_id, tab, fname in JOBS:
        resp = api.get(spreadsheetId=sheet_id, range=tab).execute()
        rows = resp.get("values", [])
        path = os.path.join(LOCAL_DIR, fname)
        n = write_tab_csv(rows, path)
        print(f"  staged {tab!r} ({sheet_id[:8]}…): {n} rows -> {path}")
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
