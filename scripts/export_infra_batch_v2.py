#!/usr/bin/env python3
"""Infra-batch exporter v2 — reads ONLY the 3 SHARED sheets (off the 33 unshared
"<Workspace> - Email Accounts" sheets forever; TKT-1 §3/§4-B, 2026-07-03).

Reads (all READ-ONLY, same OAuth token as v1):
  1. Inbox Hub - Renaissance  (1wkrkX_02bdXaj_j-E03vLHIFRw8howadd96LOC4lONo)
       tab "Funding"   — RG tag -> workspace / type / batch / email provider /
                         offer / status (live RG rows; header on row 1)
       tab "Cancelled" — RG tag -> Status ('Cancelled') / workspace / type /
                         batch (header on row 1; NO renewal column — renewal
                         lives in the Cancelling-Accounts sheet, verified 07-03)
  2. Cancelling Accounts - Renaissance (18C-P8XlLB8ndhwfipnEGnYR2W0ED1zGvttcTza7aJW4)
       PER-PARTNER TABS (Shekhar / Avion / Outreach Today / MailIn / Inboxing /
       Cheap Inboxes / Panel (Don't Cancel)) — NOT blocks in one tab. Each tab:
       row 1 = partner name in A1, row 3 = header, two side-by-side blocks:
         "Cancel Tag"        A-D:  Status | Tag | Batch | Renewal Date
         "Cancel Only Inbox" F-J:  Status | Email(domain) | Tag | Batch | Renewal Date
  3. Batches - Renaissance registry (19iB4LLgkXeO6w7EQ0-jD-C_RFvXmmQobhVg5dLe9vFw)
       tab "Batches" — batch metadata (unchanged from v1; parser reused).

Writes to OUTPUT_DIR (default /root/core/build/infra-batch/):
  rg_dim.parquet         — one row per RG/named tag: rg_tag PK -> workspace_name,
                           rg_type, is_cancelled, renewal, partner (the
                           core.rg_tag_dim contract, DDL 1072) + extra
                           enrichment columns consumed directly from the parquet
                           by build_infra_batch_v2.sql (hub_status, batch,
                           batch_family, email_provider, offer, sources).
  batch_sheet_v2.parquet — Batches-registry metadata, SAME columns as v1
                           batch_sheet.parquet (BATCH_COLS superset contract);
                           v1's read_registry is imported and reused verbatim.

MIRROR semantics: rebuilt from scratch every run; build_infra_batch_v2.sql does
the full-replace load under the writer flock. Fail-loud: every fatal path exits
non-zero with a clear message; the calling refresh_infra_batch.sh die()s and
posts the :rotating_light: one-liner via alert_slack.py, and NEVER promotes on
failure (stale-but-correct beats fresh-but-broken).

Usage:
  .venv/bin/python scripts/export_infra_batch_v2.py \
      [--output-dir /root/core/build/infra-batch] \
      [--id-map ids.json] [--require-all] [--dry-run]

--id-map (kept from v1 in spirit): JSON mapping logical sheet name
  ("inbox_hub" | "cancelling_accounts" | "batches_registry") -> spreadsheet id,
  overriding the built-in constants (e.g. if a sheet is ever re-created).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import re
import time
from collections import OrderedDict
from datetime import datetime, timezone

# Reuse v1's auth, API client, registry parser and parsing helpers verbatim —
# single-sourced so the Batches-registry contract can't drift between v1 and v2.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from export_infra_batch import (  # noqa: E402
    BATCH_COLS,
    DEFAULT_TOKEN,
    GApi,
    _batch_family,
    _load_token,
    _norm,
    _to_str_or_none,
    read_registry,
)

# ---------------------------------------------------------------------------
# Config / constants
# ---------------------------------------------------------------------------
DEFAULT_OUTPUT_DIR = "/root/core/build/infra-batch"

SHEET_IDS = {
    "inbox_hub": "1wkrkX_02bdXaj_j-E03vLHIFRw8howadd96LOC4lONo",
    "cancelling_accounts": "18C-P8XlLB8ndhwfipnEGnYR2W0ED1zGvttcTza7aJW4",
    "batches_registry": "19iB4LLgkXeO6w7EQ0-jD-C_RFvXmmQobhVg5dLe9vFw",
}

# Inbox Hub "Funding" tab: header row 1 (index 0). Aliases first, then the
# verified 2026-07-03 positional fallback (same pattern as v1 FD_FIELDS).
HUB_FUNDING_FIELDS = OrderedDict([
    ("rg_tag",         (["tag"], 0)),
    ("status",         (["status"], 5)),
    ("workspace",      (["workspace"], 8)),
    ("rg_type",        (["type"], 9)),
    ("batch",          (["batch"], 17)),
    ("email_provider", (["email"], 18)),
    ("offer",          (["offer"], 22)),
])

# Inbox Hub "Cancelled" tab: header row 1 (index 0); verified 2026-07-03:
# ['OFFER','Tag/Group','Accounts',...,'Status',...,'Workspace',...,'Type',
#  'Technical','Batch',...]. No renewal column here.
HUB_CANCELLED_FIELDS = OrderedDict([
    ("offer",     (["offer"], 0)),
    ("rg_tag",    (["tag/group", "tag"], 1)),
    ("status",    (["status"], 5)),
    ("workspace", (["workspace"], 9)),
    ("rg_type",   (["type"], 12)),
    ("batch",     (["batch"], 14)),
])

# Cancelling-Accounts partner tabs (verified 2026-07-03). Tabs NOT here are
# formatting/archive/technical tabs, deliberately skipped.
PARTNER_TABS = [
    "Shekhar", "Avion", "Outreach Today", "MailIn", "Inboxing",
    "Cheap Inboxes", "Panel (Don't Cancel)",
]
IGNORED_CANCELLING_TABS_PREFIXES = (
    "domain formatting", "nowhere in instantly", "technical sheets",
    "archive",
)

RG_DIM_COLS = ["rg_tag", "workspace_name", "rg_type", "is_cancelled", "renewal",
               "partner", "hub_status", "batch", "batch_family",
               "email_provider", "offer", "sources"]

# Fail-loud floors (a broken parse must never mirror a wiped/partial dim).
MIN_FUNDING_ROWS = 1500       # measured 2,863 on 2026-07-03
MIN_CANCELLED_ROWS = 1000     # measured 2,363 on 2026-07-03
MIN_RG_DIM_ROWS = 2000        # measured ~5,200 distinct tags on 2026-07-03
MIN_PARTNER_TAGS = 500        # measured ~1,900 partner-attributed tags
MIN_BATCH_LABELS = 100        # registry had 165 labels 06-12, ~186 on 07-01


def die(msg: str) -> None:
    sys.exit(f"[export-v2] FATAL: {msg}")


# ---------------------------------------------------------------------------
# Header resolution (aliases first, verified positional fallback second)
# ---------------------------------------------------------------------------
def _resolve_fields(header_row, fields, tab_name):
    hdr = [_norm(c).lower() for c in header_row]
    out, fallback = {}, set()
    for field, (aliases, pos) in fields.items():
        idx = None
        for a in aliases:
            if a in hdr:
                idx = hdr.index(a)
                break
        if idx is None:
            idx = pos
            fallback.add(field)
        out[field] = idx
    if fallback:
        sys.stderr.write(f"[export-v2]   WARN {tab_name}: columns resolved by "
                         f"POSITION (no header alias matched): {sorted(fallback)}\n")
    return out


def _cell(row, idx):
    return row[idx] if idx is not None and idx < len(row) else None


# ---------------------------------------------------------------------------
# Inbox Hub readers
# ---------------------------------------------------------------------------
def _tab_rowcount(api: GApi, sheet_id: str, tab: str) -> int:
    for t in api.tabs(sheet_id):
        if _norm(t["title"]) == tab:
            return t["gridProperties"].get("rowCount", 5000)
    die(f"tab '{tab}' not found on sheet {sheet_id}")


def read_hub_funding(api: GApi, sheet_id: str):
    """-> list of dicts (one per non-blank Tag row) from Inbox Hub 'Funding'."""
    rc = _tab_rowcount(api, sheet_id, "Funding")
    rows = api.values(sheet_id, f"Funding!A1:AC{rc}")
    if not rows:
        die("Inbox Hub Funding tab returned no rows")
    cols = _resolve_fields(rows[0], HUB_FUNDING_FIELDS, "InboxHub/Funding")
    out = []
    for r in rows[1:]:
        tag = _norm(_cell(r, cols["rg_tag"]))
        if not tag:
            continue
        out.append(dict(
            rg_tag=tag,
            workspace_name=_to_str_or_none(_cell(r, cols["workspace"])),
            rg_type=_to_str_or_none(_cell(r, cols["rg_type"])),
            hub_status=_to_str_or_none(_cell(r, cols["status"])),
            batch=_to_str_or_none(_cell(r, cols["batch"])),
            email_provider=_to_str_or_none(_cell(r, cols["email_provider"])),
            offer=_to_str_or_none(_cell(r, cols["offer"])),
        ))
    return out


def read_hub_cancelled(api: GApi, sheet_id: str):
    """-> list of dicts from Inbox Hub 'Cancelled' (membership => cancelled)."""
    rc = _tab_rowcount(api, sheet_id, "Cancelled")
    rows = api.values(sheet_id, f"Cancelled!A1:V{rc}")
    if not rows:
        die("Inbox Hub Cancelled tab returned no rows")
    cols = _resolve_fields(rows[0], HUB_CANCELLED_FIELDS, "InboxHub/Cancelled")
    out = []
    for r in rows[1:]:
        tag = _norm(_cell(r, cols["rg_tag"]))
        if not tag:
            continue
        status = _norm(_cell(r, cols["status"]))
        out.append(dict(
            rg_tag=tag,
            workspace_name=_to_str_or_none(_cell(r, cols["workspace"])),
            rg_type=_to_str_or_none(_cell(r, cols["rg_type"])),
            hub_status=_to_str_or_none(status),
            batch=_to_str_or_none(_cell(r, cols["batch"])),
            offer=_to_str_or_none(_cell(r, cols["offer"])),
            # Membership in the Cancelled tab means cancelled unless the Status
            # cell explicitly says otherwise (1 'Warmup' row on 2026-07-03).
            is_cancelled=("cancel" in status.lower()) if status else True,
        ))
    return out


# ---------------------------------------------------------------------------
# Cancelling-Accounts partner tabs
# ---------------------------------------------------------------------------
def read_partner_tab(api: GApi, sheet_id: str, tab: str):
    """One partner tab -> list of (tag, partner, renewal, cancel_status).

    Layout (verified 2026-07-03): A1 = partner name; row 3 = header with the
    two blocks' columns; data from row 4. Left block = whole-tag cancels
    (Status|Tag|Batch|Renewal Date); right block = per-inbox/domain cancels
    (Status|Email|Tag|Batch|Renewal Date) — many rows per tag.
    """
    rc = _tab_rowcount(api, sheet_id, tab)
    rows = api.values(sheet_id, f"'{tab}'!A1:J{rc}")
    if len(rows) < 3:
        die(f"Cancelling-Accounts tab '{tab}' has no header row 3")
    partner = _norm(rows[0][0]) if rows[0] else ""
    partner = partner or _norm(tab)
    hdr = [_norm(c).lower() for c in rows[2]]

    def occurrences(name):
        return [i for i, c in enumerate(hdr) if c == name]

    tag_i = occurrences("tag")
    st_i = occurrences("status")
    ren_i = occurrences("renewal date")
    email_i = occurrences("email") or occurrences("domain")
    if not tag_i:
        die(f"Cancelling-Accounts tab '{tab}': header row 3 has no 'Tag' column "
            f"(got: {hdr[:12]}) — sheet structure changed, refusing to guess")
    # Left block = first occurrences; right block = occurrences after 'Email'.
    left = dict(status=(st_i[0] if st_i else 0), tag=tag_i[0],
                renewal=(ren_i[0] if ren_i else None))
    right = None
    if email_i:
        e = email_i[0]
        r_tag = [i for i in tag_i if i > e]
        r_st = [i for i in st_i if i >= e - 1]  # right Status sits just before Email
        r_ren = [i for i in ren_i if i > e]
        if r_tag:
            right = dict(status=(r_st[0] if r_st else None), tag=r_tag[0],
                         renewal=(r_ren[0] if r_ren else None))

    out = []
    for r in rows[3:]:
        for block in (left, right):
            if block is None:
                continue
            tag = _norm(_cell(r, block["tag"]))
            if not tag:
                continue
            out.append(dict(
                rg_tag=tag,
                partner=partner,
                renewal=_to_str_or_none(_cell(r, block["renewal"])),
                cancel_status=_to_str_or_none(_cell(r, block["status"])),
            ))
    return out


def read_cancelling_accounts(api: GApi, sheet_id: str, require_all: bool):
    """All partner tabs -> (entries, per_tab_stats, failures). Also warns on
    unrecognized tabs so a NEW partner tab gets noticed instead of silently
    missing from partner attribution."""
    live_tabs = [_norm(t["title"]) for t in api.tabs(sheet_id)]
    known = set(PARTNER_TABS)
    for t in live_tabs:
        tl = t.lower()
        if t not in known and not tl.startswith(IGNORED_CANCELLING_TABS_PREFIXES):
            sys.stderr.write(f"[export-v2]   WARN Cancelling-Accounts has an "
                             f"unrecognized tab '{t}' — a new partner? Not parsed; "
                             f"add it to PARTNER_TABS if it carries cancel tags.\n")
    entries, stats, failures = [], [], []
    for tab in PARTNER_TABS:
        if tab not in live_tabs:
            failures.append({"tab": tab, "error": "tab missing"})
            continue
        try:
            rows = read_partner_tab(api, sheet_id, tab)
            entries.extend(rows)
            stats.append({"tab": tab, "rows": len(rows),
                          "distinct_tags": len({r["rg_tag"] for r in rows})})
            print(f"[export-v2]   Cancelling/{tab}: {len(rows)} tag rows")
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            failures.append({"tab": tab, "error": str(e)})
            print(f"[export-v2]   Cancelling/{tab}: ERROR {e}")
    if failures and require_all:
        die(f"--require-all: {len(failures)} Cancelling-Accounts partner tabs "
            f"failed: {failures} (refusing a partial partner attribution)")
    return entries, stats, failures


# ---------------------------------------------------------------------------
# rg_dim merge
# ---------------------------------------------------------------------------
def build_rg_dim(funding_rows, cancelled_rows, partner_entries):
    """Merge the three sources into one row per tag. Identity precedence:
    Funding (live) > Cancelled tab > Cancelling-Accounts (partner/renewal only).
    First non-null wins within a source (Funding had 70 dup tags on 07-03)."""
    dim: "OrderedDict[str, dict]" = OrderedDict()

    def touch(tag):
        if tag not in dim:
            dim[tag] = dict(rg_tag=tag, workspace_name=None, rg_type=None,
                            is_cancelled=False, renewal=None, partner=None,
                            hub_status=None, batch=None, batch_family=None,
                            email_provider=None, offer=None, sources=[])
        return dim[tag]

    def fill(row, src, fields):
        for f in fields:
            if row.get(f) is None and src.get(f) is not None:
                row[f] = src[f]

    n_dup_funding = 0
    for src in funding_rows:
        row = touch(src["rg_tag"])
        if "funding" in row["sources"]:
            n_dup_funding += 1
        else:
            row["sources"].append("funding")
        fill(row, src, ("workspace_name", "rg_type", "hub_status", "batch",
                        "email_provider", "offer"))

    for src in cancelled_rows:
        row = touch(src["rg_tag"])
        if "cancelled" not in row["sources"]:
            row["sources"].append("cancelled")
        fill(row, src, ("workspace_name", "rg_type", "hub_status", "batch",
                        "offer"))
        if src["is_cancelled"]:
            row["is_cancelled"] = True

    partner_conflicts = 0
    for src in partner_entries:
        row = touch(src["rg_tag"])
        tag_src = f"cancelling:{src['partner']}"
        if tag_src not in row["sources"]:
            row["sources"].append(tag_src)
        if row["partner"] is None:
            row["partner"] = src["partner"]
        elif row["partner"] != src["partner"]:
            partner_conflicts += 1  # first partner tab wins; counted, reported
        if row["renewal"] is None and src["renewal"] is not None:
            row["renewal"] = src["renewal"]

    for row in dim.values():
        row["batch_family"] = _batch_family(row["batch"]) if row["batch"] else None
        row["sources"] = ",".join(row["sources"])
    return list(dim.values()), n_dup_funding, partner_conflicts


# ---------------------------------------------------------------------------
# Parquet writer (explicit schema; all-null columns keep the right type)
# ---------------------------------------------------------------------------
def _write_parquet_v2(path, rows, cols, bool_cols=(), int_cols=(), date_cols=()):
    import pyarrow as pa
    import pyarrow.parquet as pq
    data = {c: [r.get(c) for r in rows] for c in cols}
    fields = []
    for c in cols:
        if c in bool_cols:
            fields.append((c, pa.bool_()))
        elif c in int_cols:
            fields.append((c, pa.int64()))
        elif c in date_cols:
            fields.append((c, pa.date32()))
        else:
            fields.append((c, pa.string()))
    pq.write_table(pa.table(data, schema=pa.schema(fields)), path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=os.environ.get("INFRA_BATCH_OUTPUT_DIR",
                                                           DEFAULT_OUTPUT_DIR))
    ap.add_argument("--token", default=DEFAULT_TOKEN)
    ap.add_argument("--id-map", default=os.environ.get("INFRA_BATCH_V2_ID_MAP"),
                    help="JSON file mapping logical name (inbox_hub | "
                         "cancelling_accounts | batches_registry) -> spreadsheet "
                         "id, overriding the built-in constants.")
    ap.add_argument("--require-all", action="store_true",
                    help="Fail if ANY of the 3 sheets / any partner tab cannot "
                         "be read (no partial mirror).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Read + validate + manifest only; do not write parquet.")
    args = ap.parse_args()

    ids = dict(SHEET_IDS)
    if args.id_map and os.path.exists(args.id_map):
        with open(args.id_map) as f:
            overrides = json.load(f)
        unknown = set(overrides) - set(ids)
        if unknown:
            die(f"--id-map has unknown keys {sorted(unknown)}; valid: {sorted(ids)}")
        ids.update(overrides)
        print(f"[export-v2] id-map overrides applied: {sorted(overrides)}")

    os.makedirs(args.output_dir, exist_ok=True)
    api = GApi(_load_token(args.token))
    t0 = time.time()

    # ── 1. Batches registry (v1 parser reused; batch_sheet_v2 == v1 schema) ──
    print("[export-v2] reading Batches registry ...")
    # read_registry reads the module-level REGISTRY_SHEET_ID constant, which is
    # the same id as ids['batches_registry'] unless overridden via --id-map.
    if ids["batches_registry"] != SHEET_IDS["batches_registry"]:
        import export_infra_batch as v1
        v1.REGISTRY_SHEET_ID = ids["batches_registry"]
    batch_rows, _sheet_names = read_registry(api)
    print(f"[export-v2] registry: {len(batch_rows)} batch labels")
    if len(batch_rows) < MIN_BATCH_LABELS:
        die(f"registry parsed only {len(batch_rows)} batch labels "
            f"(< {MIN_BATCH_LABELS}) — parser/sheet broke, refusing to mirror")

    # ── 2. Inbox Hub ──────────────────────────────────────────────────────────
    print("[export-v2] reading Inbox Hub Funding + Cancelled ...")
    funding_rows = read_hub_funding(api, ids["inbox_hub"])
    cancelled_rows = read_hub_cancelled(api, ids["inbox_hub"])
    print(f"[export-v2] hub: funding={len(funding_rows)} cancelled={len(cancelled_rows)}")
    if len(funding_rows) < MIN_FUNDING_ROWS:
        die(f"Funding tab parsed {len(funding_rows)} rows (< {MIN_FUNDING_ROWS})")
    if len(cancelled_rows) < MIN_CANCELLED_ROWS:
        die(f"Cancelled tab parsed {len(cancelled_rows)} rows (< {MIN_CANCELLED_ROWS})")

    # ── 3. Cancelling-Accounts partner tabs ──────────────────────────────────
    print("[export-v2] reading Cancelling-Accounts partner tabs ...")
    partner_entries, partner_stats, partner_failures = read_cancelling_accounts(
        api, ids["cancelling_accounts"], args.require_all)

    # ── 4. Merge -> rg_dim ────────────────────────────────────────────────────
    rg_dim, n_dup_funding, partner_conflicts = build_rg_dim(
        funding_rows, cancelled_rows, partner_entries)
    n_cancelled = sum(1 for r in rg_dim if r["is_cancelled"])
    n_partner = sum(1 for r in rg_dim if r["partner"])
    n_ws = sum(1 for r in rg_dim if r["workspace_name"])
    n_rg_shape = sum(1 for r in rg_dim if re.match(r"^RG\d", r["rg_tag"]))
    print(f"[export-v2] rg_dim: {len(rg_dim)} tags (RG-shaped {n_rg_shape}) | "
          f"cancelled={n_cancelled} partner={n_partner} workspace={n_ws} | "
          f"funding dups merged={n_dup_funding} partner conflicts={partner_conflicts}")

    # ── 5. Fail-loud floors ───────────────────────────────────────────────────
    if len(rg_dim) < MIN_RG_DIM_ROWS:
        die(f"rg_dim has {len(rg_dim)} tags (< {MIN_RG_DIM_ROWS})")
    if n_cancelled == 0:
        die("rg_dim has 0 cancelled tags — Cancelled-tab parse broke "
            "(would silently un-cancel all capacity)")
    if n_partner < MIN_PARTNER_TAGS:
        die(f"rg_dim has {n_partner} partner-attributed tags (< {MIN_PARTNER_TAGS})")

    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "elapsed_s": round(time.time() - t0, 1),
        "sheet_ids": ids,
        "batch_labels": len(batch_rows),
        "funding_rows": len(funding_rows),
        "cancelled_rows": len(cancelled_rows),
        "rg_dim_tags": len(rg_dim),
        "rg_dim_rg_shaped": n_rg_shape,
        "rg_dim_cancelled": n_cancelled,
        "rg_dim_with_partner": n_partner,
        "rg_dim_with_workspace": n_ws,
        "funding_dup_tags_merged": n_dup_funding,
        "partner_conflicts_first_wins": partner_conflicts,
        "partner_tabs": partner_stats,
        "partner_tab_failures": partner_failures,
    }
    manifest_path = os.path.join(args.output_dir, "export_manifest_v2.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"[export-v2] wrote manifest {manifest_path}")

    if args.dry_run:
        print("[export-v2] dry-run: skipping parquet write.")
        return

    rg_path = os.path.join(args.output_dir, "rg_dim.parquet")
    bs_path = os.path.join(args.output_dir, "batch_sheet_v2.parquet")
    _write_parquet_v2(rg_path, rg_dim, RG_DIM_COLS, bool_cols={"is_cancelled"})
    _write_parquet_v2(
        bs_path, batch_rows, BATCH_COLS,
        bool_cols={"is_replacement"},
        int_cols={"n_domains_sheet", "billing_day_of_month"},
        date_cols={"sip_date", "warmup_start_date", "cold_start_date",
                   "warmup_start_qa"})
    print(f"[export-v2] wrote {rg_path} ({len(rg_dim)} rows)")
    print(f"[export-v2] wrote {bs_path} ({len(batch_rows)} rows)")


if __name__ == "__main__":
    main()
