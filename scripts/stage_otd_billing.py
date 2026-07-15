#!/usr/bin/env python3
"""PRODUCE step for the OTD billing sheet mirror — run on the Mac (has the Google token).

Pulls the two tabs of the OTD account statement, encodes each as a row_index/row_json CSV
(identical shape to sources/sheets.write_tab_csv), and scps them to the droplet staging dir
so the warehouse `sheets` phase can consume them on the next run.

Monthly refresh procedure: after OTD sends the new statement, just re-run this script.

    python3 scripts/stage_otd_billing.py [--no-scp]

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
SHEET_ID = "1lRbIk1TvQyBkU9W-aTmI9iKIvY4IcnfWqOPxz4pVTm4"
LOCAL_DIR = os.environ.get("LOCAL_STAGING_DIR", "/tmp/otd-staging")  # [2026-07-14] droplet-run: /root/core/sheets_staging + --no-scp
DROPLET_HOST = "renaissance-worker"
DROPLET_STAGING = os.environ.get("DROPLET_STAGING", "/root/core/sheets_staging")

# (tab_name_in_sheet, csv_filename)  — csv_filename must match sources/sheets.SHEET_TABS
TABS = [
    ("Account Summary", "otd__Account_Summary.csv"),
    ("Charges by Batch", "otd__Charges_by_Batch.csv"),
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

    # [2026-07-14 creds-rebuild] SA auth; droplet-runnable now (Mac OAuth client destroyed)
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(
        os.environ.get("GOOGLE_SA_KEY", "/root/.config/gcp-sa/droplet-sheets-sync.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    svc = build("sheets", "v4", credentials=creds)
    api = svc.spreadsheets().values()

    staged = []
    for tab, fname in TABS:
        resp = api.get(spreadsheetId=SHEET_ID, range=tab).execute()
        rows = resp.get("values", [])
        path = os.path.join(LOCAL_DIR, fname)
        n = write_tab_csv(rows, path)
        print(f"  staged {tab!r}: {n} rows -> {path}")
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
