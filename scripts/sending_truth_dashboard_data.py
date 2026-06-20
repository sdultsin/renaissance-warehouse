#!/usr/bin/env python3
"""Warehouse-native Sending Volume Truth cube — produces the SAME snapshot.json the
Lens "sending-truth" UI consumes ({schema, dicts, rows}), but sourced entirely from the
consolidation warehouse SERVING SNAPSHOT instead of the standalone account_truth.duckdb.

Source tables (read-only, serving snapshot):
  - raw_account_truth_daily_actuals : per account-day inventory + actuals (expected_sends,
        actual_sends, fulfillment, account_status, daily_limit, infra_type, canonical_tag,
        undersend_reason). One row per account per day; latest date = the cube date.
  - core.account_campaign           : live account_email -> campaign mapping. Used to recover
        active_campaign_count PER ACCOUNT, because the baked-in active_campaign_count column
        in the account-truth tables is uniformly 0 in the warehouse (known json_each->unnest
        ingest bug, project_infra_data_truth_c3_20260616). campaign_status=1 => active.

This reproduces dashboard/server.py:classified_sql + export_static_dashboard.py:main in
classification logic (eligibility_bucket / is_eligible / is_campaign_assigned_eligible /
fulfillment_bucket), so the existing sending-truth app.js renders unchanged.

CAVEATS (warehouse vs the standalone Lens source):
  - setup_pending is NOT in the warehouse account-truth table -> treated as false. A small
    number of mid-setup accounts that the standalone tool buckets as 'setup_pending' will fall
    through to the next bucket here. Immaterial to the headline funnel.
  - ramp_replacement classification is dropped (the standalone RAMP_REPLACEMENT_CONDITION keys
    off columns not carried into the warehouse) -> those accounts classify on their base status.
  - active_campaign_count is the LIVE count from core.account_campaign joined onto the latest
    account-truth day (assignment is point-in-time, so this is correct for "today").

Run on the droplet against the serving snapshot:
  CORE_DB_PATH=/opt/duckdb/warehouse_current.duckdb \
    /root/renaissance-warehouse/.venv/bin/python scripts/sending_truth_dashboard_data.py \
    --json-out /root/portal/dashboards/lens-sending-truth/data.json.gz --days 14

A --json-out path ending in .gz (or passing --gzip) writes a gzip-compressed cube
(~13x smaller). The portal repo commits the .gz; the lens-sending-truth app.js fetches
it and inflates client-side via DecompressionStream("gzip"). Keeps the nightly git
commit ~1.3MB instead of ~17.8MB (portal git-history bloat fix).
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from pathlib import Path

import duckdb

DB_DEFAULT = os.environ.get("CORE_DB_PATH", "/opt/duckdb/warehouse_current.duckdb")

# Cube schema — byte-for-byte the columns export_static_dashboard.py emits. First 11 are
# dictionary-encoded client-side (the app.js hydrate() expects exactly this contract).
SCHEMA = [
    "date", "workspace_slug", "workspace_name", "infra_type", "tag", "domain", "campaign",
    "status", "eligibility", "reason", "fulfillment_bucket",
    "account_count", "eligible_account_count", "configured_capacity", "excluded_capacity",
    "eligible_capacity", "campaign_assigned_capacity", "actual_sends", "sent_capped",
    "missing_volume", "zero_send_accounts", "no_campaign_accounts", "bad_status_accounts",
    "zero_limit_accounts",
]
DICT_COLS = set(SCHEMA[:11])


def build_sql(days: int) -> str:
    # active_campaign_count is recovered from the live account_campaign mapping (the baked-in
    # column is all-zero in the warehouse). Joined by lowercased email.
    return """
    WITH ac AS (
      SELECT lower(account_email) AS email,
             count(*) FILTER (WHERE campaign_status = 1) AS active_campaign_count,
             string_agg(DISTINCT campaign_name, ', ') FILTER (WHERE campaign_status = 1) AS active_campaign_names
      FROM core.account_campaign
      GROUP BY 1
    ),
    dedup AS (
      -- The warehouse account-truth table carries ~94k/day duplicate-email rows; we dedupe to
      -- one row per (date,email) (the standalone Lens source is already one row per account).
      --
      -- We DO retain the ~105k/day "Missing Current Inventory" rows (account_status=-999 /
      -- account_status_label='Missing Current Inventory'). They are NOT dropped here anymore:
      -- on 2026-06-17 that churned/MCI bucket carries 303,976 actual_sends (~19%% of the day),
      -- and inner-dropping it made the cube's total actual_sends undercount
      -- core.sending_account_daily by exactly that amount (1,288,551 vs 1,592,527), with the
      -- gap concentrated in the funding-CM workspaces leadership tracks (renaissance-4, koi,
      -- renaissance-5) and the esp=NULL slice (QA-FINDINGS F1/F2/F7). The rows flow into
      -- `classified` where they are already bucketed eligibility='missing_current_inventory'
      -- (is_eligible=false), so their actual_sends count toward volume while the
      -- eligibility/capacity/audit columns are unchanged (expected_sends=0 on these rows).
      -- The UI already lists these in ELIGIBILITY_EXCLUDED/STATUS_EXCLUDED (app.js:48-49).
      SELECT * FROM (
        SELECT t.*, row_number() OVER (
                 PARTITION BY t.date, lower(t.email)
                 ORDER BY t.actual_sends DESC, t.expected_sends DESC, t.daily_limit DESC) AS _rn
        FROM raw_account_truth_daily_actuals t
        WHERE t.date >= (SELECT max(date) FROM raw_account_truth_daily_actuals) - INTERVAL '%d days'
      ) WHERE _rn = 1
    ),
    base AS (
      SELECT t.*, coalesce(ac.active_campaign_count, 0) AS acc_live,
             ac.active_campaign_names AS acc_names
      FROM dedup t
      LEFT JOIN ac ON ac.email = lower(t.email)
    ),
    classified AS (
      SELECT
        date, workspace_slug, workspace_name, infra_type,
        coalesce(nullif(canonical_tag, ''), '(no canonical tag match)') AS tag,
        domain,
        coalesce(nullif(acc_names, ''), '(no active campaign)') AS campaign,
        account_status_label AS status,
        CASE
          WHEN account_status = -999 OR account_status_label = 'Missing Current Inventory'
            THEN 'missing_current_inventory'
          WHEN coalesce(account_status, 0) != 1 THEN 'bad_status'
          WHEN coalesce(daily_limit, 0) = 0 THEN 'daily_limit_zero'
          WHEN acc_live = 0 THEN 'no_active_campaign'
          WHEN coalesce(expected_sends, 0) > 0
            AND coalesce(actual_sends, 0) >= coalesce(expected_sends, 0) * 0.95
            THEN 'fully_utilized'
          ELSE 'assigned_but_undersent'
        END AS eligibility_bucket,
        CASE
          WHEN coalesce(account_status, 0) = 1 AND coalesce(daily_limit, 0) > 0 THEN true
          ELSE false
        END AS is_eligible,
        CASE
          WHEN coalesce(account_status, 0) = 1 AND coalesce(daily_limit, 0) > 0 AND acc_live > 0
            THEN true ELSE false
        END AS is_campaign_assigned_eligible,
        undersend_reason AS reason,
        expected_sends, actual_sends, fulfillment
      FROM base
    ),
    shaped AS (
      SELECT
        date, workspace_slug, workspace_name, infra_type, tag, domain, campaign,
        status,
        eligibility_bucket AS eligibility,
        reason,
        CASE
          WHEN actual_sends = 0 AND expected_sends > 0 THEN 'zero'
          WHEN expected_sends > 0 AND fulfillment < 0.25 THEN 'under25'
          WHEN expected_sends > 0 AND fulfillment < 0.50 THEN 'under50'
          WHEN expected_sends > 0 AND fulfillment < 0.85 THEN 'under85'
          WHEN expected_sends > 0 AND fulfillment >= 0.85 THEN 'ok'
          ELSE 'none'
        END AS fulfillment_bucket,
        count(*) AS account_count,
        count(*) FILTER (WHERE is_eligible) AS eligible_account_count,
        coalesce(sum(expected_sends), 0) AS configured_capacity,
        coalesce(sum(expected_sends) FILTER (WHERE NOT is_eligible), 0) AS excluded_capacity,
        coalesce(sum(expected_sends) FILTER (WHERE is_eligible), 0) AS eligible_capacity,
        coalesce(sum(expected_sends) FILTER (WHERE is_campaign_assigned_eligible), 0) AS campaign_assigned_capacity,
        coalesce(sum(actual_sends), 0) AS actual_sends,
        coalesce(sum(least(greatest(actual_sends, 0), greatest(expected_sends, 0))), 0) AS sent_capped,
        coalesce(sum(CASE WHEN is_eligible THEN greatest(expected_sends - actual_sends, 0)
                          ELSE expected_sends END), 0) AS missing_volume,
        count(*) FILTER (WHERE actual_sends = 0 AND expected_sends > 0) AS zero_send_accounts,
        count(*) FILTER (WHERE eligibility_bucket = 'no_active_campaign') AS no_campaign_accounts,
        count(*) FILTER (WHERE eligibility_bucket = 'bad_status') AS bad_status_accounts,
        count(*) FILTER (WHERE eligibility_bucket = 'daily_limit_zero') AS zero_limit_accounts
      FROM classified
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    )
    SELECT %s
    FROM shaped
    ORDER BY date, workspace_name, infra_type, tag, domain, campaign
    """ % (days, ", ".join(SCHEMA))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_DEFAULT)
    ap.add_argument("--days", type=int, default=1,
                    help="trailing days of snapshots to include (default 1 = the latest snapshot, "
                         "which is the dashboard's default view). Kept at 1 because the warehouse "
                         "account universe is ~1.2M accounts; each extra day adds ~9MB to a "
                         "git-committed cube. Bump if a multi-day pill history is wanted and the "
                         "cube is moved off-repo.")
    ap.add_argument("--json-out", default=None,
                    help="write here; default = stdout. If the path ends in .gz (or --gzip "
                         "is set) the cube is written gzip-compressed.")
    ap.add_argument("--gzip", action="store_true",
                    help="force gzip output even if --json-out lacks a .gz suffix (and gzip "
                         "the stdout stream when no --json-out).")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=True)
    rows = con.execute(build_sql(args.days)).fetchall()
    if not rows:
        sys.stderr.write("ERROR: 0 rows from raw_account_truth_daily_actuals — refusing to write empty cube\n")
        return 1

    # Conservation assert: the cube's SUM(actual_sends) for the snapshot (latest) date must
    # reconcile to core.sending_account_daily for that date — they measure the same thing
    # (total sends that day). The historical undercount (QA-FINDINGS F1) came from inner-dropping
    # the "Missing Current Inventory" bucket before summing; this guard catches any regression of
    # that class. WARN-only (does not return nonzero / block the cube) so the conductor keeps its
    # last-known-good even if the warehouse is mid-backfill or the two surfaces lag differently.
    try:
        _di = SCHEMA.index("date")
        _ai = SCHEMA.index("actual_sends")
        _snap_date = max(r[_di] for r in rows)
        _cube_sends = sum((r[_ai] or 0) for r in rows if r[_di] == _snap_date)
        _wh = con.execute(
            "SELECT coalesce(sum(actual_sends), 0) FROM core.sending_account_daily WHERE date = ?",
            [_snap_date],
        ).fetchone()
        _wh_sends = (_wh[0] or 0) if _wh else 0
        if _wh_sends:
            _drift = abs(_cube_sends - _wh_sends) / _wh_sends
            _msg = (
                "reconcile %s: cube actual_sends=%d vs core.sending_account_daily=%d (drift %.3f%%)\n"
                % (_snap_date, _cube_sends, _wh_sends, _drift * 100.0)
            )
            if _drift > 0.005:
                sys.stderr.write("WARN: " + _msg)
            else:
                sys.stderr.write("OK: " + _msg)
        else:
            sys.stderr.write(
                "WARN: reconcile %s: core.sending_account_daily has 0 sends for the snapshot date "
                "(cube actual_sends=%d) — skipping drift check\n" % (_snap_date, _cube_sends)
            )
    except Exception as exc:  # never let the guard itself break the build
        sys.stderr.write("WARN: reconciliation check skipped (%s: %s)\n" % (type(exc).__name__, exc))

    dicts: dict[str, list[str]] = {name: [] for name in DICT_COLS}
    indexes: dict[str, dict[str, int]] = {name: {} for name in DICT_COLS}
    encoded_rows = []
    for row in rows:
        encoded = []
        for name, value in zip(SCHEMA, row):
            if name not in DICT_COLS:
                encoded.append(value)
                continue
            text = "" if value is None else str(value)
            lookup = indexes[name]
            if text not in lookup:
                lookup[text] = len(dicts[name])
                dicts[name].append(text)
            encoded.append(lookup[text])
        encoded_rows.append(encoded)

    payload = {"schema": SCHEMA, "dicts": dicts, "rows": encoded_rows}
    out = json.dumps(payload, separators=(",", ":"), default=str)
    raw = out.encode("utf-8")
    # gzip when the destination is a .gz path or --gzip is set. mtime=0 + fixed level keeps the
    # bytes deterministic for a given cube -> git commits a new blob only when the DATA changed,
    # not on every nightly run (a timestamped gzip header would churn the repo regardless).
    gz = args.gzip or (args.json_out is not None and args.json_out.endswith(".gz"))
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        if gz:
            with open(args.json_out, "wb") as fh:
                with gzip.GzipFile(filename="", mode="wb", fileobj=fh, mtime=0, compresslevel=9) as f:
                    f.write(raw)
            wrote = Path(args.json_out).stat().st_size
            sys.stderr.write("wrote %s rows=%d json_bytes=%d gz_bytes=%d\n" % (args.json_out, len(rows), len(raw), wrote))
        else:
            Path(args.json_out).write_bytes(raw)
            sys.stderr.write("wrote %s rows=%d bytes=%d\n" % (args.json_out, len(rows), len(raw)))
    else:
        buf = sys.stdout.buffer
        if gz:
            with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0, compresslevel=9) as f:
                f.write(raw)
        else:
            buf.write(raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
