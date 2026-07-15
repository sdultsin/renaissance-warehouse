#!/usr/bin/env python3
"""Re-runnable exporter for the infra-batch layer — a TRUE MIRROR of the source
Google Sheets.

Reads:
  1. The "Batches - Renaissance" registry (tab "Batches") -> batch_sheet.parquet
     (one row per Sheet batch label; batch-level metadata).
  2. Every "<Workspace> - Email Accounts" sheet's "FINAL DATA" tab (per-account
     rows) -> account_batch.parquet  (the (email, batch) membership facts,
     INCLUDING rg_tag_1 / rg_tag_2).

Writes both parquets to OUTPUT_DIR (default /root/core/build/infra-batch/), then
scripts/build_infra_batch.sql reloads core.* from them. The build is a FULL
DELETE+INSERT reconcile, so each run leaves the warehouse table EQUAL to the
current state of the sheets — adds appear, removes drop. This exporter is the
re-runnable front of that mirror (it replaces the old MANUAL one-off CSV export).

MIRROR semantics: the parquet is rebuilt from scratch every run from whatever the
sheets currently contain. There is no merge-into-previous, no append. An account
removed from every source sheet is absent from the new parquet, and the
DELETE+INSERT in build_infra_batch.sql then drops it from the warehouse table.

Schema parity (must match the live core.sending_account_batch / core.infra_batch
so build_infra_batch.sql's `INSERT ... BY NAME` keeps working):
  account_batch.parquet columns:
     email, batch_key, batch_family, domain, raw_workspace, provider_tag,
     email_tag, offer, first_name, last_name, status_csv, n_source_rows,
     rg_tag_1, rg_tag_2
  batch_sheet.parquet columns:
     batch_label, batch_family, is_replacement, partner, workspace_raw,
     n_domains_sheet, sip_raw, sip_date, warmup_raw, warmup_start_date,
     cold_raw, cold_start_date, warmup_start_qa, billing_raw,
     billing_day_of_month, offer, provider, batch_url, qa_num_accounts,
     qa_started, qa_settings_correct

domain_purchase.parquet is OUT OF SCOPE here (it is sourced from the domain
registry on its own cadence; this exporter never touches it). build_infra_batch.sql
still reads the existing file in place.

Sheet resolution: the registry's "Batch URL" column is PLAIN TEXT (a sheet name,
not a link). We resolve each distinct name to a spreadsheet id via Drive name
search, with an explicit override map (--id-map / EMAIL_ACCOUNTS_ID_MAP) for any
sheet the running OAuth identity can't see by name (shared-drive / not-shared).
Sheets that cannot be resolved are reported and (by default) the run continues but
records a coverage warning; --require-all makes an unresolved KNOWN sheet fatal.

Auth: Google OAuth user token at GOOGLE_SHEETS_TOKEN
(default ~/.config/mcp-google-sheets/token.json), scopes sheets.readonly +
drive.readonly.

Usage:
  .venv/bin/python scripts/export_infra_batch.py \
      [--output-dir /root/core/build/infra-batch] \
      [--id-map sheet_ids.json] [--only "Funding 1,Funding 5"] \
      [--require-all] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config / constants
# ---------------------------------------------------------------------------
REGISTRY_SHEET_ID = "19iB4LLgkXeO6w7EQ0-jD-C_RFvXmmQobhVg5dLe9vFw"
REGISTRY_TAB = "Batches"
DEFAULT_OUTPUT_DIR = "/root/core/build/infra-batch"
DEFAULT_TOKEN = os.path.expanduser(
    os.environ.get("GOOGLE_SHEETS_TOKEN", "~/.config/mcp-google-sheets/token.json")
)
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"
DRIVE_API = "https://www.googleapis.com/drive/v3/files"

# Values in the registry "Batch URL" column that are NOT real sheet names.
NON_SHEET_URLS = {"", "no url", "n/a", "na", "-", "tbd"}

# Final-data column resolution: ordered list of acceptable header aliases per
# logical field. Header names vary across sheets (e.g. "RG# Tag (Tag1)" vs
# "Tag1"); positions are stable, so we ALSO carry a positional fallback index
# (0-based, into the FINAL DATA header row that starts at sheet row 3).
FD_FIELDS = OrderedDict([
    ("domain",      (["domain"], 0)),
    ("first_name",  (["first name", "firstname"], 4)),
    ("last_name",   (["last name", "lastname"], 5)),
    ("email",       (["email", "email address"], 6)),
    ("rg_tag_1",    (["rg# tag (tag1)", "rg tag (tag1)", "tag1", "tag 1", "rg# tag"], 8)),
    ("rg_tag_2",    (["rg#-# (tag2)", "rg-# (tag2)", "tag2", "tag 2"], 9)),
    ("email_tag",   (["email tag (google, outlook or smtp)", "email tag (google/outlook/smtp)",
                      "email tag", "inbox type"], 10)),
    ("provider_tag",(["provider tag", "platform/partner tag", "partner/platform tag",
                      "platform tag", "partner tag"], 11)),
    ("batch_key",   (["batch tag", "batch"], 12)),
    ("raw_workspace",(["workspace"], 13)),
    ("status_csv",  (["status"], 15)),
    ("offer",       (["offer"], 18)),
])


# ---------------------------------------------------------------------------
# Google auth + API helpers
# ---------------------------------------------------------------------------
def _load_token(path: str) -> str:
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
    except ImportError:
        sys.exit("google-auth not installed: pip install google-auth")
    # [2026-07-14 creds-rebuild] service-account auth; `path` arg ignored (old OAuth token destroyed)
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(
        "/root/.config/gcp-sa/droplet-sheets-sync.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly",
                "https://www.googleapis.com/auth/drive.readonly"])
    creds.refresh(Request())
    return creds.token


class GApi:
    def __init__(self, token: str, max_retries: int = 6):
        self.token = token
        self.max_retries = max_retries

    def get(self, url: str):
        last = None
        for attempt in range(self.max_retries):
            req = urllib.request.Request(
                url, headers={"Authorization": f"Bearer {self.token}"}
            )
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                last = e
                if e.code in (429, 500, 502, 503, 504):
                    wait = min(2 ** attempt, 60)
                    sys.stderr.write(
                        f"[export] HTTP {e.code} retry {attempt+1}/{self.max_retries} in {wait}s\n"
                    )
                    time.sleep(wait)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError) as e:
                last = e
                wait = min(2 ** attempt, 60)
                sys.stderr.write(f"[export] net error retry {attempt+1} in {wait}s: {e}\n")
                time.sleep(wait)
        raise RuntimeError(f"GET failed after {self.max_retries} retries: {url} ({last})")

    def values(self, sheet_id: str, a1: str):
        rng = urllib.parse.quote(a1)
        url = (f"{SHEETS_API}/{sheet_id}/values/{rng}"
               "?majorDimension=ROWS&valueRenderOption=UNFORMATTED_VALUE"
               "&dateTimeRenderOption=FORMATTED_STRING")
        return self.get(url).get("values", [])

    def tabs(self, sheet_id: str):
        url = (f"{SHEETS_API}/{sheet_id}"
               "?fields=sheets.properties(title,gridProperties(rowCount,columnCount))")
        return [s["properties"] for s in self.get(url).get("sheets", [])]

    def find_by_name(self, name: str):
        safe = name.replace("'", "\\'")
        q = urllib.parse.quote(
            f"name = '{safe}' and mimeType='application/vnd.google-apps.spreadsheet' "
            "and trashed=false"
        )
        url = (f"{DRIVE_API}?q={q}&fields=files(id,name)&pageSize=20"
               "&corpora=allDrives&includeItemsFromAllDrives=true&supportsAllDrives=true")
        return self.get(url).get("files", [])


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _norm(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_email(s) -> str:
    return _norm(s).lower()


def _to_str_or_none(v):
    s = _norm(v)
    return s if s else None


_DATE_FMTS = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
              "%b %d %Y", "%m/%d/%y"]


def _parse_date(v):
    s = _norm(v)
    if not s:
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_int(v):
    s = _norm(v).replace(",", "")
    if not s:
        return None
    m = re.search(r"-?\d+", s)
    return int(m.group()) if m else None


def _parse_day_of_month(v):
    """'15th' / '15' / 'June 15' -> 15 (1..31), else None."""
    s = _norm(v)
    m = re.search(r"\b([0-3]?\d)\b", s)
    if not m:
        return None
    d = int(m.group(1))
    return d if 1 <= d <= 31 else None


def _batch_family(label: str) -> str:
    """Base label without -R replacement suffix or .N decimal split suffix.
    Mirrors the existing data's family derivation (B54-R->B54, B36.2->B36)."""
    if not label:
        return label
    fam = re.sub(r"-R\b", "", label, flags=re.IGNORECASE)
    fam = re.sub(r"\.\d+[A-Za-z]?$", "", fam)
    return fam.strip()


def _is_replacement(label: str) -> bool:
    return bool(re.search(r"-R\b", label or "", flags=re.IGNORECASE))


# ---------------------------------------------------------------------------
# Registry (batch_sheet.parquet) + sheet inventory
# ---------------------------------------------------------------------------
def read_registry(api: GApi):
    """Return (batch_rows, sheet_names) from the Batches registry tab.

    batch_rows: list of dicts matching batch_sheet.parquet schema.
    sheet_names: ordered dict {sheet_name: count} of distinct Batch-URL targets.
    """
    rows = api.values(REGISTRY_SHEET_ID, f"{REGISTRY_TAB}!A1:Z1010")
    # Header is on sheet row 3 (index 2): two banner rows precede it.
    # Find it defensively by locating the row whose first cell == "Batch".
    header_idx = None
    for i, r in enumerate(rows[:8]):
        if r and _norm(r[0]).lower() == "batch":
            header_idx = i
            break
    if header_idx is None:
        header_idx = 2
    data = rows[header_idx + 1:]

    # Column indexes in the Batch-Info block (from the verified header):
    # A Batch, B Partner/Provider, C Workspace, D # of Domains, E SIP Date,
    # F Warmup Start Date, G Cold Email Start Date, J Billing Date, K Offer,
    # L Email Provider, M Batch URL ; QA block: O Number of accounts,
    # X Warmup start date, V Started, W Settings correct.
    C = dict(batch=0, partner=1, workspace=2, ndom=3, sip=4, warmup=5, cold=6,
             billing=9, offer=10, provider=11, url=12,
             qa_num=14, qa_started=21, qa_settings=22, qa_warmup=23)

    batch_rows = []
    sheet_names: "OrderedDict[str,int]" = OrderedDict()
    seen_labels = set()
    for r in data:
        label = _norm(r[C["batch"]] if len(r) > C["batch"] else "")
        if not label or label.lower() in ("explanation row", "batch"):
            continue
        # Skip obvious non-batch / trailing rows: a real batch row has a label
        # and at least one of provider / url / workspace populated.
        url_raw = _norm(r[C["url"]]) if len(r) > C["url"] else ""
        prov = _norm(r[C["provider"]]) if len(r) > C["provider"] else ""
        ws = _norm(r[C["workspace"]]) if len(r) > C["workspace"] else ""
        if not (url_raw or prov or ws):
            continue
        if label in seen_labels:
            continue
        seen_labels.add(label)

        sip_raw = _norm(r[C["sip"]]) if len(r) > C["sip"] else ""
        warmup_raw = _norm(r[C["warmup"]]) if len(r) > C["warmup"] else ""
        cold_raw = _norm(r[C["cold"]]) if len(r) > C["cold"] else ""
        billing_raw = _norm(r[C["billing"]]) if len(r) > C["billing"] else ""
        qa_warmup_raw = _norm(r[C["qa_warmup"]]) if len(r) > C["qa_warmup"] else ""

        batch_rows.append(dict(
            batch_label=label,
            batch_family=_batch_family(label),
            is_replacement=_is_replacement(label),
            partner=_to_str_or_none(prov),
            workspace_raw=_to_str_or_none(ws),
            n_domains_sheet=_parse_int(r[C["ndom"]] if len(r) > C["ndom"] else None),
            sip_raw=_to_str_or_none(sip_raw),
            sip_date=_parse_date(sip_raw),
            warmup_raw=_to_str_or_none(warmup_raw),
            warmup_start_date=_parse_date(warmup_raw),
            cold_raw=_to_str_or_none(cold_raw),
            cold_start_date=_parse_date(cold_raw),
            warmup_start_qa=_parse_date(qa_warmup_raw),
            billing_raw=_to_str_or_none(billing_raw),
            billing_day_of_month=_parse_day_of_month(billing_raw),
            offer=_to_str_or_none(r[C["offer"]] if len(r) > C["offer"] else None),
            provider=_to_str_or_none(
                r[C["provider"]] if len(r) > C["provider"] else None),
            batch_url=_to_str_or_none(url_raw),
            qa_num_accounts=_to_str_or_none(
                r[C["qa_num"]] if len(r) > C["qa_num"] else None),
            qa_started=_to_str_or_none(
                r[C["qa_started"]] if len(r) > C["qa_started"] else None),
            qa_settings_correct=_to_str_or_none(
                r[C["qa_settings"]] if len(r) > C["qa_settings"] else None),
        ))

        # Sheet inventory: a Batch-URL cell may list multiple sheets via "/".
        if url_raw and url_raw.lower() not in NON_SHEET_URLS:
            for part in url_raw.split("/"):
                name = _norm(part)
                if name and name.lower() not in NON_SHEET_URLS:
                    sheet_names[name] = sheet_names.get(name, 0) + 1

    return batch_rows, sheet_names


# ---------------------------------------------------------------------------
# FINAL DATA reader (account_batch.parquet)
# ---------------------------------------------------------------------------
def _resolve_columns(header_row):
    """Map each FD field -> column index using header aliases first, then the
    positional fallback. Returns (cols, fallback_fields) where fallback_fields is
    the set of fields resolved by POSITION (no header alias matched) — the caller
    warns on these and validates the email column, since a shifted column would
    otherwise silently mirror wrong data into core."""
    hdr = [_norm(c).lower() for c in header_row]
    out = {}
    fallback = set()
    for field, (aliases, pos) in FD_FIELDS.items():
        idx = None
        for a in aliases:
            if a in hdr:
                idx = hdr.index(a)
                break
        if idx is None:
            idx = pos  # positional fallback (positions are stable across sheets)
            fallback.add(field)
        out[field] = idx
    return out, fallback


def _final_data_tab(tabs):
    for t in tabs:
        if _norm(t["title"]).upper() == "FINAL DATA":
            return t
    # tolerate casing/spacing variants
    for t in tabs:
        if _norm(t["title"]).upper().replace(" ", "") == "FINALDATA":
            return t
    return None


def read_email_accounts_sheet(api: GApi, sheet_name: str, sheet_id: str):
    """Read one sheet's FINAL DATA tab -> list of account dicts.
    Returns (rows, stats). stats has counts for QA."""
    tabs = api.tabs(sheet_id)
    fd = _final_data_tab(tabs)
    if fd is None:
        return [], {"sheet": sheet_name, "error": "no FINAL DATA tab",
                    "rows_in": 0, "rows_kept": 0}
    row_count = fd["gridProperties"].get("rowCount", 0)
    # Header at row 3; data rows 4..row_count. Read A..S (covers all fields).
    header = api.values(sheet_id, "FINAL DATA!A3:S3")
    if not header:
        return [], {"sheet": sheet_name, "error": "empty header",
                    "rows_in": 0, "rows_kept": 0}
    cols, fallback = _resolve_columns(header[0])
    if fallback:
        sys.stderr.write(f"[export]   WARN {sheet_name}: columns resolved by POSITION "
                         f"(no header alias matched): {sorted(fallback)}\n")
    # Hard fail this sheet if 'email' couldn't be resolved by header — a shifted
    # email column would mirror garbage. (A positional 'email' is validated below.)
    values = api.values(sheet_id, f"FINAL DATA!A4:S{row_count}")

    # Email-column sanity: ≥90% of non-blank cells in the resolved email column
    # must contain '@'. Guards against a column shift landing on the wrong field.
    ei = cols["email"]
    nonblank = 0
    looks_email = 0
    for r in values[:5000]:
        v = _norm(r[ei]) if ei < len(r) else ""
        if v:
            nonblank += 1
            if "@" in v:
                looks_email += 1
    if nonblank and looks_email / nonblank < 0.9:
        return [], {"sheet": sheet_name,
                    "error": f"email column failed sanity ({looks_email}/{nonblank} "
                             f"have '@') — likely a column shift; refusing this sheet",
                    "rows_in": len(values), "rows_kept": 0}

    out = []
    rows_in = len(values)
    blank_email = 0
    for r in values:
        def cell(field):
            i = cols[field]
            return r[i] if i < len(r) else None
        email = _norm_email(cell("email"))
        if not email:
            blank_email += 1
            continue
        out.append(dict(
            email=email,
            batch_key=_to_str_or_none(cell("batch_key")),
            domain=(_norm(cell("domain")).lower() or None),
            raw_workspace=_to_str_or_none(cell("raw_workspace")),
            provider_tag=_to_str_or_none(cell("provider_tag")),
            email_tag=_to_str_or_none(cell("email_tag")),
            offer=_to_str_or_none(cell("offer")),
            first_name=_to_str_or_none(cell("first_name")),
            last_name=_to_str_or_none(cell("last_name")),
            status_csv=_to_str_or_none(cell("status_csv")),
            rg_tag_1=_to_str_or_none(cell("rg_tag_1")),
            rg_tag_2=_to_str_or_none(cell("rg_tag_2")),
        ))
    return out, {"sheet": sheet_name, "rows_in": rows_in, "rows_kept": len(out),
                 "blank_email": blank_email}


# ---------------------------------------------------------------------------
# Account aggregation -> account_batch.parquet rows
# ---------------------------------------------------------------------------
def build_account_rows(all_account_rows):
    """Collapse to the (email, batch_key) grain matching account_batch.parquet,
    tracking n_source_rows (how many raw sheet rows fed each membership)."""
    agg = OrderedDict()
    for a in all_account_rows:
        key = (a["email"], a["batch_key"])
        if key not in agg:
            bk = a["batch_key"]
            agg[key] = dict(
                email=a["email"],
                batch_key=bk,
                batch_family=_batch_family(bk) if bk else None,
                domain=a["domain"],
                raw_workspace=a["raw_workspace"],
                provider_tag=a["provider_tag"],
                email_tag=a["email_tag"],
                offer=a["offer"],
                first_name=a["first_name"],
                last_name=a["last_name"],
                status_csv=a["status_csv"],
                rg_tag_1=a["rg_tag_1"],
                rg_tag_2=a["rg_tag_2"],
                n_source_rows=0,
            )
        row = agg[key]
        row["n_source_rows"] += 1
        # Backfill any field that's null from a later duplicate row.
        for f in ("domain", "raw_workspace", "provider_tag", "email_tag", "offer",
                  "first_name", "last_name", "status_csv", "rg_tag_1", "rg_tag_2",
                  "batch_family"):
            if row.get(f) is None and a.get(f) is not None:
                row[f] = a[f]
    return list(agg.values())


# ---------------------------------------------------------------------------
# Parquet writers (column order MUST match the live schemas)
# ---------------------------------------------------------------------------
ACCOUNT_COLS = ["email", "batch_key", "batch_family", "domain", "raw_workspace",
                "provider_tag", "email_tag", "offer", "first_name", "last_name",
                "status_csv", "n_source_rows", "rg_tag_1", "rg_tag_2"]
BATCH_COLS = ["batch_label", "batch_family", "is_replacement", "partner",
              "workspace_raw", "n_domains_sheet", "sip_raw", "sip_date",
              "warmup_raw", "warmup_start_date", "cold_raw", "cold_start_date",
              "warmup_start_qa", "billing_raw", "billing_day_of_month", "offer",
              "provider", "batch_url", "qa_num_accounts", "qa_started",
              "qa_settings_correct"]


def _write_parquet(path, rows, cols):
    import pyarrow as pa
    import pyarrow.parquet as pq
    data = {c: [r.get(c) for r in rows] for c in cols}
    # Explicit schema so an all-null column doesn't become the wrong type.
    schema_fields = []
    int_cols = {"n_source_rows", "n_domains_sheet", "billing_day_of_month"}
    bool_cols = {"is_replacement"}
    date_cols = {"sip_date", "warmup_start_date", "cold_start_date",
                 "warmup_start_qa"}
    for c in cols:
        if c in int_cols:
            schema_fields.append((c, pa.int64()))
        elif c in bool_cols:
            schema_fields.append((c, pa.bool_()))
        elif c in date_cols:
            schema_fields.append((c, pa.date32()))
        else:
            schema_fields.append((c, pa.string()))
    table = pa.table(data, schema=pa.schema(schema_fields))
    pq.write_table(table, path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=os.environ.get("INFRA_BATCH_OUTPUT_DIR",
                                                           DEFAULT_OUTPUT_DIR))
    ap.add_argument("--token", default=DEFAULT_TOKEN)
    ap.add_argument("--id-map", default=os.environ.get("EMAIL_ACCOUNTS_ID_MAP"),
                    help="JSON file mapping sheet-name -> spreadsheet id (override "
                         "for sheets not resolvable by Drive name search).")
    ap.add_argument("--only", help="Comma list of sheet-name substrings to limit "
                                    "to (for testing).")
    ap.add_argument("--require-all", action="store_true",
                    help="Fail if any KNOWN registry sheet cannot be resolved/read.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Resolve + count only; do not write parquet.")
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    api = GApi(_load_token(args.token))

    print("[export] reading registry ...")
    batch_rows, sheet_names = read_registry(api)
    print(f"[export] registry: {len(batch_rows)} batch labels, "
          f"{len(sheet_names)} distinct source sheets")

    id_map = {}
    if args.id_map and os.path.exists(args.id_map):
        with open(args.id_map) as f:
            id_map = json.load(f)
        print(f"[export] loaded id-map override: {len(id_map)} entries")

    only = None
    if args.only:
        only = [s.strip().lower() for s in args.only.split(",") if s.strip()]

    # Resolve each sheet name -> id (override map wins; else Drive name search).
    resolved, unresolved = OrderedDict(), []
    for name in sheet_names:
        if only and not any(o in name.lower() for o in only):
            continue
        if name in id_map:
            resolved[name] = id_map[name]
            continue
        hits = api.find_by_name(name)
        if hits:
            resolved[name] = hits[0]["id"]
        else:
            unresolved.append(name)

    print(f"[export] resolved {len(resolved)} sheets; unresolved {len(unresolved)}")
    if unresolved:
        print("[export] UNRESOLVED (need sharing or an --id-map entry):")
        for n in unresolved:
            print(f"           - {n}  ({sheet_names[n]} batches)")
        if args.require_all:
            sys.exit(f"[export] FATAL --require-all: {len(unresolved)} sheets "
                     "unresolved; aborting (no partial mirror).")

    # Read each resolved sheet's FINAL DATA.
    all_rows = []
    per_sheet = []
    failures = []
    t0 = time.time()
    # Sheets that legitimately have no per-account FINAL DATA tab (account-setup
    # sheets like "MailIn Account Setup - Renaissance", "Mina - Information").
    # Their absence of a FINAL DATA tab is expected, NOT a coverage failure.
    NO_FINAL_DATA_OK = ("account setup", "information", "renaissance <>")
    for name, sid in resolved.items():
        try:
            rows, stats = read_email_accounts_sheet(api, name, sid)
            per_sheet.append(stats)
            all_rows.extend(rows)
            err = stats.get("error")
            if err:
                expected = (err == "no FINAL DATA tab"
                            and any(k in name.lower() for k in NO_FINAL_DATA_OK))
                if not expected:
                    failures.append({"sheet": name, "error": err})
                print(f"[export]   {name}: {err}"
                      f"{' (expected — account-setup sheet)' if expected else ''}")
            else:
                print(f"[export]   {name}: {stats.get('rows_kept', 0)} accounts "
                      f"(in={stats.get('rows_in', 0)})")
        except Exception as e:  # noqa: BLE001
            failures.append({"sheet": name, "error": str(e)})
            print(f"[export]   {name}: ERROR {e}")
    print(f"[export] read {len(all_rows)} raw account rows in {time.time()-t0:.1f}s")

    if failures and args.require_all:
        for f in failures:
            print(f"           - {f['sheet']}: {f['error']}")
        sys.exit(f"[export] FATAL --require-all: {len(failures)} sheets failed to "
                 "read (coverage incomplete — refusing a partial mirror).")

    account_rows = build_account_rows(all_rows)
    print(f"[export] collapsed to {len(account_rows)} (email,batch) memberships")

    # ---- safety asserts (loud-fail, never silently promote a broken export) --
    n_emails = len({r["email"] for r in account_rows})
    n_rg1 = sum(1 for r in account_rows if r["rg_tag_1"])
    n_batch = sum(1 for r in account_rows if r["batch_key"])
    print(f"[export] distinct emails={n_emails}  rg_tag_1 filled={n_rg1} "
          f"({100*n_rg1/max(len(account_rows),1):.1f}%)  "
          f"batch_key filled={n_batch} "
          f"({100*n_batch/max(len(account_rows),1):.1f}%)")
    if account_rows and n_rg1 == 0:
        sys.exit("[export] FATAL: 0 rg_tag_1 populated — column mapping broke; "
                 "refusing to write (would wipe RG attribution).")

    # provider-block presence check vs the registry (Google + Outlook).
    reg_providers = {}
    for b in batch_rows:
        p = (b.get("provider") or "").strip().lower()
        if p:
            reg_providers[p] = reg_providers.get(p, 0) + 1
    print(f"[export] registry provider blocks: {reg_providers}")

    if args.dry_run:
        print("[export] dry-run: skipping parquet write.")
        _write_manifest(args.output_dir, batch_rows, account_rows, per_sheet,
                        unresolved, failures, sheet_names, dry_run=True)
        return

    acct_path = os.path.join(args.output_dir, "account_batch.parquet")
    batch_path = os.path.join(args.output_dir, "batch_sheet.parquet")
    _write_parquet(acct_path, account_rows, ACCOUNT_COLS)
    _write_parquet(batch_path, batch_rows, BATCH_COLS)
    print(f"[export] wrote {acct_path} ({len(account_rows)} rows)")
    print(f"[export] wrote {batch_path} ({len(batch_rows)} rows)")
    _write_manifest(args.output_dir, batch_rows, account_rows, per_sheet,
                    unresolved, failures, sheet_names, dry_run=False)


def _write_manifest(output_dir, batch_rows, account_rows, per_sheet, unresolved,
                    failures, sheet_names, dry_run):
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "batch_labels": len(batch_rows),
        "account_memberships": len(account_rows),
        "distinct_emails": len({r["email"] for r in account_rows}),
        "rg_tag_1_filled": sum(1 for r in account_rows if r["rg_tag_1"]),
        "rg_tag_2_filled": sum(1 for r in account_rows if r["rg_tag_2"]),
        "batch_key_filled": sum(1 for r in account_rows if r["batch_key"]),
        "source_sheets_total": len(sheet_names),
        "source_sheets_read": len(per_sheet),
        "unresolved_sheets": unresolved,
        "failed_sheets": failures,
        "per_sheet": per_sheet,
    }
    path = os.path.join(output_dir, "export_manifest.json")
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"[export] wrote manifest {path}")


if __name__ == "__main__":
    main()
