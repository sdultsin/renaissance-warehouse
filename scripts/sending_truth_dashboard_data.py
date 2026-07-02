#!/usr/bin/env python3
"""Generate the lens-sending-truth cube from the CORRECTED warehouse census (read-only).

THE ONE TRUTH source for sending capacity. Reads `core.account_label` (DDL 95 — the
phantom-free, MX-infra, point-in-time per-account lifecycle census: Active|Warmup, with
A's DDL-1003-healed daily_limit) and joins per-day actuals from `core.sending_account_daily`.
This RETIRES the bespoke `account_truth.duckdb` pipeline that produced the old frozen cube
off a private DB; the dashboard now rides the nightly serving snapshot like every other Lens.

Grain of the emitted cube: one row per (date, workspace, infra, lifecycle, vendor).
Output: a gzip `{schema, dicts, rows, meta}` columnar dict-encoded cube — the exact envelope
the lens-sending-truth app.js consumes (app.js was updated to the lifecycle field set).

Runs ON the droplet against the gated serving snapshot:
    CORE_DB_PATH=/opt/duckdb/warehouse_current.duckdb \
        .venv/bin/python scripts/sending_truth_dashboard_data.py \
        --out /root/portal/dashboards/lens-sending-truth/data.json.gz
or LOCALLY against the read API (no duckdb needed):
    WAREHOUSE_API_URL=... WAREHOUSE_API_TOKEN=... \
        python scripts/sending_truth_dashboard_data.py --api --out /tmp/data.json.gz

Read-only: the only side effect is writing the gzip cube file.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone

CORE_DB_PATH = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")
DEFAULT_OUT = os.environ.get(
    "SENDING_TRUTH_OUT", "/root/portal/dashboards/lens-sending-truth/data.json.gz"
)
# How many days of census history to include. The census is no longer pruned, so this grows
# daily — set generously so the date filter shows real multi-day history (was the old --days 1
# floor that left only 1-2 selectable dates). Census only has what it has; the floor is the
# census depth, not this window.
DEFAULT_DAYS = int(os.environ.get("SENDING_TRUTH_DAYS", "60"))

# String columns that get dict-encoded (low cardinality). Order here = cube column order.
SCHEMA = [
    "date",
    "workspace_slug",
    "workspace_name",
    "infra",          # Google | OTD | Outlook  (MX-based, phantom-free)
    "lifecycle",      # Active | Warmup
    "vendor",
    "ws_active",      # 1 if the workspace is live (core.workspace.is_active), else 0
    "account_count",
    "limit_ge1_accounts",     # accounts with daily_limit >= 1 (real cold capacity)
    "configured_capacity",    # sum(daily_limit) for this segment
    "actual_sends",           # sum(actual_sends) on this date for this segment
    "sent_capped",            # sum(LEAST(actual_sends, daily_limit)) — bounds fulfillment <=1
]
DICT_COLS = {"date", "workspace_slug", "workspace_name", "infra", "lifecycle", "vendor"}


def _build_sql(days: int) -> str:
    # One aggregated pass. census (account_label) carries the live membership + healed
    # daily_limit + lifecycle + MX infra; sending_account_daily carries per-day actuals
    # (account_id IS the lowercased email — clean join, slug-independent).
    return f"""
    WITH al AS (
      SELECT census_date AS date, lower(email) AS email, workspace_slug,
             infra, lifecycle, COALESCE(daily_limit, 0) AS daily_limit,
             COALESCE(NULLIF(vendor, ''), '(untagged)') AS vendor
      FROM core.account_label
      WHERE census_date >= (SELECT max(census_date) FROM core.account_label) - INTERVAL '{days} days'
    ),
    snd AS (
      SELECT date, lower(account_id) AS email, workspace_slug, esp,
             COALESCE(daily_limit, 0) AS daily_limit, COALESCE(actual_sends, 0) AS actual_sends
      FROM core.sending_account_daily
      WHERE date >= (SELECT max(census_date) FROM core.account_label) - INTERVAL '{days} days'
    ),
    joined AS (
      -- census era (>= first census day 2026-06-21): full lifecycle/infra/vendor from account_label
      SELECT al.date, al.workspace_slug, al.infra, al.lifecycle, al.vendor,
             al.daily_limit, COALESCE(snd.actual_sends, 0) AS actual_sends
      FROM al LEFT JOIN snd ON snd.email = al.email AND snd.date = al.date
      UNION ALL
      -- pre-census backfill (< first census day): real per-day Total Emails (daily_limit) + Actual
      -- straight from sending_account_daily. Lifecycle wasn't tracked yet -> '(pre-census)' so the
      -- Active/Warmup split stays honestly blank for these days; infra mapped from esp.
      SELECT snd.date, snd.workspace_slug,
             CASE WHEN snd.esp ILIKE 'google' THEN 'Google'
                  WHEN snd.esp ILIKE 'outlook' OR snd.esp ILIKE 'microsoft' THEN 'Outlook'
                  ELSE 'OTD' END AS infra,
             '(pre-census)' AS lifecycle,
             '(untagged)' AS vendor,
             -- capacity: only the REAL current roster (phantom-free, consistent w/ census-era);
             -- actuals: ALL accounts, so historical sends stay the true total.
             CASE WHEN snd.email IN (SELECT lower(email) FROM core.account_label
                                     WHERE census_date = (SELECT max(census_date) FROM core.account_label))
                  THEN snd.daily_limit ELSE 0 END AS daily_limit,
             snd.actual_sends
      FROM snd
      WHERE snd.date < (SELECT min(census_date) FROM core.account_label)
    )
    SELECT
      CAST(j.date AS VARCHAR)                                         AS date,
      j.workspace_slug,
      COALESCE(w.name, j.workspace_slug)                             AS workspace_name,
      j.infra,
      j.lifecycle,
      j.vendor,
      CASE WHEN COALESCE(w.is_active, FALSE) THEN 1 ELSE 0 END        AS ws_active,
      COUNT(*)                                                        AS account_count,
      COUNT(*) FILTER (WHERE j.daily_limit >= 1)                      AS limit_ge1_accounts,
      CAST(SUM(j.daily_limit) AS BIGINT)                             AS configured_capacity,
      CAST(SUM(j.actual_sends) AS BIGINT)                            AS actual_sends,
      CAST(SUM(LEAST(j.actual_sends, GREATEST(j.daily_limit, 0))) AS BIGINT) AS sent_capped
    FROM joined j
    LEFT JOIN core.workspace w ON w.slug = j.workspace_slug
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY 1, 2, 4, 5, 6
    """


def _workspaces_sql() -> str:
    # Live workspace dim drives the deleted/hidden set (no hardcoded date literal). deleted_at
    # is TIMESTAMPTZ → cast to VARCHAR so the read-API's UDF path doesn't need pytz.
    return """
    SELECT slug, COALESCE(name, slug) AS name,
           CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
           CAST(deleted_at AS VARCHAR) AS deleted_at
    FROM core.workspace
    ORDER BY is_active DESC, slug
    """


# ── query backends ────────────────────────────────────────────────────────────────
def _rows_via_duckdb(sql: str, db: str) -> list[dict]:
    import duckdb

    con = duckdb.connect(db, read_only=True)
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


def _rows_via_api(sql: str) -> list[dict]:
    import urllib.request

    base = os.environ["WAREHOUSE_API_URL"].rstrip("/")
    tok = os.environ["WAREHOUSE_API_TOKEN"]
    req = urllib.request.Request(
        f"{base}/query",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        payload = json.loads(resp.read())
    if "error" in payload:
        raise RuntimeError(f"warehouse API error: {payload['error']}")
    cols = payload["columns"]
    out = [dict(zip(cols, row)) for row in payload["rows"]]
    _rows_via_api.snapshot_id = payload.get("snapshot_id")  # type: ignore[attr-defined]
    return out


# ── cube build ────────────────────────────────────────────────────────────────────
def build(use_api: bool, db: str, days: int) -> dict:
    runner = _rows_via_api if use_api else (lambda sql: _rows_via_duckdb(sql, db))
    seg_rows = runner(_build_sql(days))
    ws_rows = runner(_workspaces_sql())

    # dict-encode the low-cardinality string columns into a shared per-column dictionary.
    dicts: dict[str, list] = {c: [] for c in DICT_COLS}
    dict_index: dict[str, dict] = {c: {} for c in DICT_COLS}

    def enc(col: str, val):
        if val is None:
            val = ""
        idx = dict_index[col].get(val)
        if idx is None:
            idx = len(dicts[col])
            dicts[col].append(val)
            dict_index[col][val] = idx
        return idx

    rows: list[list] = []
    for r in seg_rows:
        row = []
        for c in SCHEMA:
            v = r.get(c)
            row.append(enc(c, v) if c in DICT_COLS else (int(v) if v is not None else 0))
        rows.append(row)

    # per-date roll-ups for app.js defaulting (which dates have real actuals / are weekdays).
    by_date: dict[str, dict] = {}
    for r in seg_rows:
        d = r["date"]
        agg = by_date.setdefault(d, {"actual_sends": 0, "active_capacity": 0})
        agg["actual_sends"] += int(r["actual_sends"] or 0)
        if r["lifecycle"] == "Active":
            agg["active_capacity"] += int(r["configured_capacity"] or 0)

    dates = sorted(by_date.keys())
    active_ws = [w["slug"] for w in ws_rows if w["is_active"]]
    deleted_ws = [
        {"slug": w["slug"], "name": w["name"], "deleted_at": w["deleted_at"]}
        for w in ws_rows
        if not w["is_active"]
    ]

    snap = getattr(_rows_via_api, "snapshot_id", None) if use_api else os.path.basename(
        os.path.realpath(db)
    )

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snap,
        "source": "core.account_label (+ core.sending_account_daily actuals, core.workspace dim)",
        "dates": dates,
        # per-date totals so the app defaults the picker to the latest COMPLETE sending day
        # (skips weekends + the not-yet-loaded intraday actuals date).
        "date_totals": {
            d: {
                "actual_sends": by_date[d]["actual_sends"],
                "active_capacity": by_date[d]["active_capacity"],
            }
            for d in dates
        },
        "active_workspaces": active_ws,   # live set → app hides everything NOT in here by default
        "deleted_workspaces": deleted_ws, # data retained in cube; hidden in UI
    }

    return {"schema": SCHEMA, "dicts": dicts, "rows": rows, "meta": meta,
            "generated_at": meta["generated_at"]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--db", default=CORE_DB_PATH)
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS)
    ap.add_argument("--api", action="store_true",
                    help="read via the warehouse HTTP query API (uses WAREHOUSE_API_URL/TOKEN)")
    ap.add_argument("--json-out", help="ALSO write the raw (uncompressed) JSON here (debug)")
    args = ap.parse_args()

    use_api = args.api or not os.path.exists(args.db)
    if use_api and not os.environ.get("WAREHOUSE_API_URL"):
        sys.stderr.write(
            f"ERROR: {args.db} not found and WAREHOUSE_API_URL not set — cannot read warehouse\n"
        )
        return 1

    cube = build(use_api=use_api, db=args.db, days=args.days)
    blob = json.dumps(cube, separators=(",", ":"), default=str).encode()

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    if args.out.endswith(".gz"):
        with gzip.open(args.out, "wb") as fh:
            fh.write(blob)
    else:
        with open(args.out, "wb") as fh:
            fh.write(blob)
    if args.json_out:
        with open(args.json_out, "wb") as fh:
            fh.write(blob)

    m = cube["meta"]
    sys.stderr.write(
        f"wrote {args.out}: {len(cube['rows'])} segments, dates={m['dates']}, "
        f"active_ws={len(m['active_workspaces'])}, deleted_ws={len(m['deleted_workspaces'])}, "
        f"snapshot={m['snapshot_id']}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
