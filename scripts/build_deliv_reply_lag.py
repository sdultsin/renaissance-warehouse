#!/usr/bin/env python3
"""Build the deliverability REPLY-LAG monitor (send -> first-reply latency).

Monitor 1 of the D2 safe-monitors (deliverability/Samuel deep-dive):
  1. core.deliv_reply_lag        — one row per (thread, first prospect reply) with the
                                    send->first-reply latency (workspace-aware).
  2. core.deliv_reply_lag_daily  — daily SNAPSHOT (per workspace per reply_date:
                                    count + median + p25/p75/p90 of lag_minutes).

Monitor 2 (human-vs-auto reply tile) is pure views over raw_pipeline_campaign_daily_metrics
(no build step needed) — see sql/ddl/<NN>_deliv_monitors.sql.

WHY this latency (vs DDL 69 core.sla_reply_time): DDL 69 measures OUR response speed to a
prospect (a CM handling SLA). THIS measures the PROSPECT's send->reply lag — a leading
DELIVERABILITY indicator: when mail soft-folders, prospects see it later and reply later, so
this lag drifts up before bounce/RR fully collapse.

SOURCE (verified read-only 2026-06-18, snapshot warehouse_20260618_064119_971):
  main.raw_pipeline_conversation_messages
    ue_type=1 (direction 'ue_1')           = our automated campaign sends (28.5M rows; carries step)
    ue_type=2 (direction 'inbound')        = prospect inbound replies (860k)
    ue_type=3 (direction 'outbound_manual')= our manual/AIM replies (NOT used here)
  workspace_id in this source = the SLUG (durable; survives campaign deletion), same as DDL 69.

DEFINITION: for each thread, take the prospect's FIRST inbound reply (min ts over ue_type=2),
then the LAST of our automated sends (ue_type=1) at or before that reply ts — i.e. the message
the prospect replied to. lag = first_reply_ts - send_ts (minutes, >= 0). reply_date = the
prospect-reply UTC day (the day we attribute the lag to). Threads with a reply but no prior
ue_type=1 send (e.g. AIM-only / manual-seeded threads) are dropped — no send anchor to measure.

DESIGN: full rebuild of the fact each run (cheap; one row per replied thread). The daily
snapshot is UPSERTed for a trailing window (default 14d) because late-arriving replies change
recent days; older days are immutable once threads close.

Single-writer: runs in the nightly AFTER the orchestrator releases the writer lock (same slot
as build_sla_reply_time.py). The in-process warehouse-writer lock (core/db.py) serializes it.

Usage:
    python scripts/build_deliv_reply_lag.py                 # full fact rebuild + 14d snapshot
    python scripts/build_deliv_reply_lag.py --snapshot-days 60
    python scripts/build_deliv_reply_lag.py --snapshot-all  # re-snapshot all history (backfill)
    python scripts/build_deliv_reply_lag.py --db /path/to/warehouse.duckdb
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.build_deliv_reply_lag")

# Resolve the DDL file by glob so the renumber (placeholder -> confirmed NN) doesn't break this.
_DDL_CANDIDATES = sorted((REPO_ROOT / "sql" / "ddl").glob("*_deliv_monitors.sql"))
_CONV = "main.raw_pipeline_conversation_messages"


def _ddl_path() -> Path:
    if not _DDL_CANDIDATES:
        raise FileNotFoundError("no sql/ddl/*_deliv_monitors.sql found")
    return _DDL_CANDIDATES[-1]


def build(db, snapshot_days: int | None, run_id: str) -> None:
    # --- apply DDL (idempotent). In prod setup_db applies it version-tracked; here we
    # read the file so the script is runnable standalone. The fact tables are
    # CREATE TABLE IF NOT EXISTS, the views CREATE OR REPLACE — execute whole. ---------
    ddl_text = _ddl_path().read_text()
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(ddl_text)

    # --- 1. response-level fact (full rebuild) -------------------------------
    db.execute("DELETE FROM core.deliv_reply_lag")
    db.execute(
        f"""
        INSERT INTO core.deliv_reply_lag
            (thread_id, workspace_slug, first_reply_ts, send_ts, lag_minutes,
             reply_date, _built_at, _run_id)
        WITH first_reply AS (   -- prospect's FIRST inbound reply per thread
            SELECT thread_id,
                   ANY_VALUE(workspace_id)        AS workspace_slug,
                   min(message_timestamp)         AS first_reply_ts
            FROM {_CONV}
            WHERE ue_type = 2 AND message_timestamp IS NOT NULL AND thread_id IS NOT NULL
            GROUP BY thread_id
        ),
        anchored AS (           -- our LAST automated send at/before that reply
            SELECT f.thread_id, f.workspace_slug, f.first_reply_ts,
                   max(s.message_timestamp) AS send_ts
            FROM first_reply f
            JOIN {_CONV} s
              ON s.thread_id = f.thread_id
             AND s.ue_type = 1
             AND s.message_timestamp IS NOT NULL
             AND s.message_timestamp <= f.first_reply_ts
            GROUP BY f.thread_id, f.workspace_slug, f.first_reply_ts
        )
        SELECT
            thread_id,
            workspace_slug,
            first_reply_ts,
            send_ts,
            date_diff('minute', send_ts, first_reply_ts)   AS lag_minutes,
            CAST(first_reply_ts AS DATE)                    AS reply_date,
            now()                                           AS _built_at,
            ?                                               AS _run_id
        FROM anchored
        WHERE date_diff('minute', send_ts, first_reply_ts) >= 0
        """,
        [run_id],
    )

    total = db.execute("SELECT count(*) FROM core.deliv_reply_lag").fetchone()[0]
    n_ws = db.execute(
        "SELECT count(DISTINCT workspace_slug) FROM core.deliv_reply_lag"
    ).fetchone()[0]
    logger.info("core.deliv_reply_lag: %d replied threads across %d workspaces", total, n_ws)

    # --- 2. daily snapshot (UPSERT trailing window; or full backfill) --------
    where_window = ""
    params: list = [run_id]
    if snapshot_days is not None:
        cutoff = (dt.datetime.now(dt.timezone.utc).date()
                  - dt.timedelta(days=snapshot_days)).isoformat()
        where_window = "AND reply_date >= ?"
        params.append(cutoff)
        db.execute("DELETE FROM core.deliv_reply_lag_daily WHERE reply_date >= ?", [cutoff])
    else:
        db.execute("DELETE FROM core.deliv_reply_lag_daily")

    db.execute(
        f"""
        INSERT INTO core.deliv_reply_lag_daily
            (reply_date, workspace_slug, n_replies, median_lag_min,
             p25_lag_min, p75_lag_min, p90_lag_min, _snapshot_at, _run_id)
        SELECT
            reply_date,
            workspace_slug,
            count(*)                              AS n_replies,
            median(lag_minutes)                   AS median_lag_min,
            quantile_cont(lag_minutes, 0.25)      AS p25_lag_min,
            quantile_cont(lag_minutes, 0.75)      AS p75_lag_min,
            quantile_cont(lag_minutes, 0.90)      AS p90_lag_min,
            now()                                 AS _snapshot_at,
            ?                                     AS _run_id
        FROM core.deliv_reply_lag
        WHERE lag_minutes IS NOT NULL
          AND workspace_slug IS NOT NULL
          AND reply_date IS NOT NULL
          {where_window}
        GROUP BY reply_date, workspace_slug
        """,
        params,
    )
    snap = db.execute("SELECT count(*) FROM core.deliv_reply_lag_daily").fetchone()[0]
    logger.info("core.deliv_reply_lag_daily: %d (workspace x day) snapshot rows", snap)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="warehouse.duckdb path (default = config)")
    ap.add_argument("--snapshot-days", type=int, default=14,
                    help="re-snapshot the trailing N days (default 14; covers late replies)")
    ap.add_argument("--snapshot-all", action="store_true",
                    help="re-snapshot the entire history (one-time backfill)")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-dlag")
    snapshot_days = None if args.snapshot_all else args.snapshot_days

    conn = db_module.connect(Path(args.db) if args.db else None)
    conn.execute("BEGIN")
    try:
        build(conn, snapshot_days, run_id)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
