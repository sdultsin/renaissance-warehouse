#!/usr/bin/env python3
"""Per-workspace data-accuracy QA gate (handoff 2026-06-18-data-accuracy-fix, issues B1-B6,B10).

Asserts the invariants the DDL-81 serving views + the entities/meeting.py reply-backfill must hold,
so the per-workspace email-performance numbers stay trustworthy and any regression fails loud.

Checks (all read-only; never writes the warehouse):
  1. RECONCILIATION (B2)  Σ v_workspace_perf_30d.meetings_attributed (incl the '(unattributed)' row)
                          == the total email meetings in core.meeting for the same 30d window. The
                          per-workspace sum must account for EVERY email meeting — no silent drops.
  2. NO STALE CODENAME (B3) no row of v_workspace_perf_30d.workspace is an old codename
                          (Koi and Destroy / Prospects Power / Tariffs + Funding / the literal
                          'Renaissance 2/4/5'); names come from core.workspace (current Instantly).
  3. SHEET ATTRIBUTION (B2) sheet (>=Jun-1) channel='Email' meetings are >= 99% campaign-attributed
                          after the email-reply backfill (was ~95% on name-match alone).
  4. NO (various) (A2)     v_workspace_daily renders all real workspaces (>= 20 distinct, incl the
                          unkeyed warm-leads/the-dyad/outlook-2/renaissance-6/7) — none collapse.
  5. EVERY ROW NAMED (B4)  no v_workspace_perf_30d row has a NULL/blank workspace label (the dim join
                          recovered the ~5% NULL-workspace_id daily-metrics rows).
  6. STALE MATVIEW GONE (B5) analytics.v_meeting_enriched does not exist (dropped in the re-platform;
                          must never be reintroduced as an attractive stale trap).

Exit code: 0 = all green, 1 = at least one FAIL.

Usage:
    python scripts/qa_workspace_accuracy.py            # uses core.config.DB_PATH (read-only)
    python scripts/qa_workspace_accuracy.py --db /path/to/snapshot.duckdb
"""

from __future__ import annotations

import argparse
import sys

import duckdb

try:  # repo context (nightly runs python -m); fall back to the canonical path when run standalone
    from core.config import DB_PATH
except ModuleNotFoundError:
    DB_PATH = "/root/core/warehouse.duckdb"

# Old codenames that must NEVER surface in a serving view (B3). Lowercased compare.
FORBIDDEN_NAMES = {
    "koi and destroy", "prospects power", "tariffs + funding",
    "renaissance 2", "renaissance 4", "renaissance 5",
}
EMAIL_FILTER = (
    "((m.source='sheet' AND m.channel='Email') OR (m.source<>'sheet' AND NOT "
    "regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),"
    "'sendivo|\\bsms\\b|whatsapp|iskra')))"
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="DuckDB path (default: core.config.DB_PATH)")
    ap.add_argument("--min-sheet-attr", type=float, default=99.0, help="min sheet email attribution %%")
    args = ap.parse_args(argv)

    db_path = args.db or str(DB_PATH)
    con = duckdb.connect(db_path, read_only=True)
    fails: list[str] = []
    oks: list[str] = []

    # 1. RECONCILIATION (B2)
    view_sum = con.execute(
        "SELECT COALESCE(sum(meetings_attributed),0) FROM main.v_workspace_perf_30d"
    ).fetchone()[0]
    direct = con.execute(
        f"""
        WITH win AS (SELECT max(date) w_end, max(date)-29 w_start
                     FROM main.raw_pipeline_campaign_daily_metrics)
        SELECT count(*) FROM core.meeting m, win
        WHERE CAST(m.posted_at AS DATE) BETWEEN win.w_start AND win.w_end AND {EMAIL_FILTER}
        """
    ).fetchone()[0]
    if view_sum == direct:
        oks.append(f"1 RECONCILIATION: Σ per-workspace meetings ({view_sum}) == core.meeting window total ({direct})")
    else:
        fails.append(f"1 RECONCILIATION BROKEN: view sum {view_sum} != core.meeting total {direct}")

    # 2. NO STALE CODENAME (B3)
    names = [r[0] for r in con.execute(
        "SELECT DISTINCT workspace FROM main.v_workspace_perf_30d WHERE workspace IS NOT NULL"
    ).fetchall()]
    bad = sorted({n for n in names if n.strip().lower() in FORBIDDEN_NAMES})
    if not bad:
        oks.append(f"2 NO STALE CODENAME: {len(names)} workspace names, none are old codenames")
    else:
        fails.append(f"2 STALE CODENAME surfaced: {bad}")

    # 3. SHEET ATTRIBUTION (B2)
    total, attr = con.execute(
        """
        SELECT count(*),
               count(*) FILTER (WHERE campaign_id IS NOT NULL)
        FROM core.meeting WHERE source='sheet' AND channel='Email'
        """
    ).fetchone()
    pct = round(100.0 * attr / total, 2) if total else 100.0
    if pct >= args.min_sheet_attr:
        oks.append(f"3 SHEET ATTRIBUTION: {pct}% >= {args.min_sheet_attr}% ({attr}/{total})")
    else:
        fails.append(f"3 SHEET ATTRIBUTION too low: {pct}% < {args.min_sheet_attr}% ({attr}/{total})")

    # 4. NO (various) collapse (A2)
    n_ws = con.execute(
        "SELECT count(DISTINCT workspace) FROM main.v_workspace_daily WHERE workspace <> '(unattributed)'"
    ).fetchone()[0]
    if n_ws >= 20:
        oks.append(f"4 NO (various): {n_ws} distinct workspaces render")
    else:
        fails.append(f"4 WORKSPACES COLLAPSED: only {n_ws} distinct (expected >= 20)")

    # 5. EVERY ROW NAMED (B4)
    n_blank = con.execute(
        "SELECT count(*) FROM main.v_workspace_perf_30d WHERE workspace IS NULL OR trim(workspace)=''"
    ).fetchone()[0]
    if n_blank == 0:
        oks.append("5 EVERY ROW NAMED: no NULL/blank workspace labels")
    else:
        fails.append(f"5 BLANK WORKSPACE rows: {n_blank}")

    # 6. STALE MATVIEW GONE (B5)
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name='v_meeting_enriched'"
    ).fetchone()[0]
    if exists == 0:
        oks.append("6 STALE MATVIEW GONE: analytics.v_meeting_enriched absent")
    else:
        fails.append("6 STALE MATVIEW REINTRODUCED: v_meeting_enriched exists (drop it)")

    con.close()

    print("=== qa_workspace_accuracy ===")
    for o in oks:
        print(f"  OK   {o}")
    for f in fails:
        print(f"  FAIL {f}")
    print(f"=== {len(oks)} passed, {len(fails)} failed ===")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
