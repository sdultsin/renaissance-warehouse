#!/usr/bin/env python3
"""Load Instantly per-workspace lead-credit quota into core.instantly_credit (DDL 70).

The ONE portal datum with no warehouse source (GENERATOR-NOTES.md §1.4 / Phase-0 §3 GAP #1).
scripts/portal_credits.py pulls the figures from the Instantly billing API (read-only) and emits
JSON; this script PERSISTS that JSON into the warehouse so the credit pool becomes servable AND
gets a daily trend (keyed on snapshot_date).

Pipeline:
    python scripts/portal_credits.py > /tmp/portal_credits.json   # API pull (read-only)
    python scripts/load_instantly_credit.py --in /tmp/portal_credits.json   # warehouse upsert
  or just pipe / let this script run the puller itself:
    python scripts/load_instantly_credit.py                       # runs portal_credits.py internally

Single-writer: take the warehouse writer window (no core.* writes 03:30-05:45 UTC).

JUNK-ROW EXCLUSION (per task): the "The Eagles" workspace is a Free-Trial account with lim=250 and
an absurd pct_used (~38570%); it is NOT a production funding workspace. It is dropped here (by
plan='Free Trial' AND by name) so it never pollutes the credit tile. Any other Free-Trial row is
dropped on the same rule (defensive, not just the one name).

Idempotent: UPSERT keyed (snapshot_date, workspace) — re-running same day overwrites that day's row.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import subprocess
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.load_instantly_credit")

_PULLER = REPO_ROOT / "scripts" / "portal_credits.py"


def _is_junk(row: dict) -> bool:
    """True for the 'The Eagles' free-trial junk row (and any other free-trial row)."""
    plan = (row.get("plan") or "").strip().lower()
    ws = (row.get("workspace") or "").strip().lower()
    if plan == "free trial":
        return True
    if ws.startswith("the eagles"):
        return True
    return False


def load_payload(payload: dict) -> dict:
    snapshot_date = payload.get("date") or dt.date.today().isoformat()
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-credits")
    rows = payload.get("rows") or []
    kept, dropped = [], []
    for r in rows:
        (dropped if _is_junk(r) else kept).append(r)

    conn = db_module.connect()
    conn.execute("BEGIN")
    try:
        # idempotent: clear today's rows then re-insert (UPSERT by full-day replace)
        conn.execute("DELETE FROM core.instantly_credit WHERE snapshot_date = ?", [snapshot_date])
        for r in kept:
            conn.execute(
                """
                INSERT INTO core.instantly_credit
                  (snapshot_date, workspace, env_key, used, lim, remaining, pct_used, plan, _run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [snapshot_date, (r.get("workspace") or "").strip(), r.get("env_key"),
                 r.get("used"), r.get("lim"), r.get("remaining"), r.get("pct_used"),
                 r.get("plan"), run_id],
            )
        # org-wide credit pool (single row/day)
        pool = payload.get("credit_pool")
        conn.execute("DELETE FROM core.instantly_credit_pool WHERE snapshot_date = ?", [snapshot_date])
        if pool:
            conn.execute(
                """
                INSERT INTO core.instantly_credit_pool
                  (snapshot_date, organization, plan, total_credits, available_credits,
                   used_credits, pct_used, _run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [snapshot_date, pool.get("organization"), pool.get("plan"),
                 pool.get("total_credits"), pool.get("available_credits"),
                 pool.get("used_credits"), pool.get("pct_used"), run_id],
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        n_total = conn.execute(
            "SELECT count(*) FROM core.instantly_credit WHERE snapshot_date = ?", [snapshot_date]
        ).fetchone()[0]
        conn.close()
    result = {
        "snapshot_date": snapshot_date,
        "loaded": len(kept),
        "dropped_junk": [d.get("workspace") for d in dropped],
        "table_rows_today": n_total,
        "pool_loaded": bool(payload.get("credit_pool")),
        "errors": payload.get("errors") or [],
    }
    logger.info("core.instantly_credit: loaded=%d dropped_junk=%s pool=%s",
                result["loaded"], result["dropped_junk"], result["pool_loaded"])
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default=None,
                    help="portal_credits.py JSON file; if omitted, runs portal_credits.py")
    ap.add_argument("--db", default=None)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.infile:
        payload = json.loads(Path(args.infile).read_text())
    else:
        out = subprocess.run([sys.executable, str(_PULLER)], capture_output=True, text=True)
        if out.returncode != 0:
            logger.error("portal_credits.py failed: %s", out.stderr)
            return 1
        payload = json.loads(out.stdout)

    result = load_payload(payload)
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
