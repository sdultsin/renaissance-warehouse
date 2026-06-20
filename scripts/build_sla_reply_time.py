#!/usr/bin/env python3
"""STAGED — DO NOT WIRE INTO nightly.sh YET.

Build the SLA reply-time metric:
  1. core.sla_reply_time         — response-level fact (one row per response-pair),
                                    WORKSPACE-AWARE (slug carried from the source).
  2. core.sla_reply_time_daily   — daily SNAPSHOT (per workspace per day:
                                    count + avg + median + q25 + q50 + q75).

Runs AFTER the canonical `iam_response_time` phase (it reads core.iam_response_time and
main.raw_pipeline_conversation_messages). Single-writer: take the warehouse writer window
(no core.* writes 03:30-05:45 UTC; coordinate with `orchestrator`).

GRAIN / SOURCE (verified 2026-06-14, read-only):
  * core.iam_response_time (DDL 50) is already one row per prospect reply (the response-pair
    grain): id, email_id, campaign_id, lead_email, thread_id, thread_reply_number,
    prospect_replied_at, iam_responded_at, response_minutes, response_bucket.
    834,590 rows; 135,250 answered (16%); 699,340 unanswered. thread_reply_number = seq.
  * It has NO workspace column. We carry workspace_id (= the SLUG) from
    main.raw_pipeline_conversation_messages joined on m.id = irt.email_id (834,590/834,590
    join). NOT via core.campaign (8.5% match — campaigns get deleted; the slug persists).
  * All timestamps are TIMESTAMPTZ / UTC (DB TimeZone = Etc/UTC). reply_date = UTC date.

PERCENTILE RULE: the daily snapshot is the TREND source (percentiles can't be averaged
across days). For weekly/monthly/custom spans, recompute from core.sla_reply_time via the
sla_reply_time_rollup() macro / v_sla_reply_time_rollup_period view (DDL 68 placeholder) —
never average the daily percentile columns.

DESIGN NOTE — full rebuild vs incremental: the response-level fact is rebuilt in full each
run (cheap: ~135k answered rows; mirrors the iam_response_time entity's own DELETE+INSERT).
The daily snapshot is UPSERTed for a trailing window (default 14 days) because late-arriving
IM responses change recent days' percentiles; older days are immutable once their threads
close, so we don't re-snapshot the whole history every night.

Usage:
    python scripts/build_sla_reply_time.py                  # full fact rebuild + 14d snapshot
    python scripts/build_sla_reply_time.py --snapshot-days 60
    python scripts/build_sla_reply_time.py --snapshot-all   # re-snapshot entire history (backfill)
    python scripts/build_sla_reply_time.py --db /path/to/warehouse.duckdb
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.build_sla_reply_time")

# DDL renumbered placeholder-68 -> 69 at apply time (post-cutover writer window 2026-06-14).
_DDL = REPO_ROOT / "sql" / "ddl" / "69_sla_reply_time.sql"

_CONV = "main.raw_pipeline_conversation_messages"


def build(db, snapshot_days: int | None, run_id: str) -> None:
    # --- apply DDL (idempotent CREATE IF NOT EXISTS / OR REPLACE) -------------
    # In production this is applied by setup_db (version-tracked); here we read the
    # file directly so the script is runnable standalone. Strip the @@INDEXES@@ marker:
    # build the fact table UNINDEXED, bulk-insert, then build indexes (same DuckDB ART
    # bulk-insert workaround the iam_response_time entity uses).
    ddl_text = _DDL.read_text()
    table_ddl, _, index_and_rest = ddl_text.partition("-- @@INDEXES@@")
    db.execute("CREATE SCHEMA IF NOT EXISTS core")

    # --- 1. response-level fact (full rebuild) -------------------------------
    db.execute("DROP TABLE IF EXISTS core.sla_reply_time")
    db.execute(table_ddl)  # creates the unindexed core.sla_reply_time
    db.execute(
        f"""
        INSERT INTO core.sla_reply_time
            (response_id, thread_id, workspace_slug, campaign_id, lead_email,
             seq_in_thread, prospect_msg_ts, our_reply_ts, response_latency_minutes,
             reply_date, _built_at, _run_id)
        SELECT
            irt.id                                   AS response_id,
            irt.thread_id,
            m.workspace_id                           AS workspace_slug,  -- source col holds the SLUG
            irt.campaign_id,
            irt.lead_email,
            irt.thread_reply_number                  AS seq_in_thread,
            irt.prospect_replied_at                  AS prospect_msg_ts,
            irt.iam_responded_at                     AS our_reply_ts,
            irt.response_minutes                     AS response_latency_minutes,
            CAST(irt.prospect_replied_at AS DATE)    AS reply_date,
            now()                                    AS _built_at,
            ?                                        AS _run_id
        FROM core.iam_response_time irt
        JOIN {_CONV} m
          ON m.id = irt.email_id AND m.ue_type = 2
        """,
        [run_id],
    )
    # index the now-populated table (the part of the DDL after @@INDEXES@@ also contains
    # the snapshot table + views; execute it whole — all statements are IF NOT EXISTS /
    # OR REPLACE so it is safe to run every time).
    db.execute(index_and_rest)

    total = db.execute("SELECT count(*) FROM core.sla_reply_time").fetchone()[0]
    answered = db.execute(
        "SELECT count(*) FROM core.sla_reply_time WHERE response_latency_minutes IS NOT NULL"
    ).fetchone()[0]
    n_ws = db.execute(
        "SELECT count(DISTINCT workspace_slug) FROM core.sla_reply_time "
        "WHERE response_latency_minutes IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "core.sla_reply_time: %d rows (%d answered) across %d workspaces",
        total, answered, n_ws,
    )

    # --- 2. daily snapshot (UPSERT trailing window; or full backfill) --------
    where_window = ""
    params: list = [run_id]
    if snapshot_days is not None:
        cutoff = (dt.datetime.now(dt.timezone.utc).date()
                  - dt.timedelta(days=snapshot_days)).isoformat()
        where_window = "AND reply_date >= ?"
        params.append(cutoff)
        # remove the days we are about to re-snapshot so the UPSERT is clean
        db.execute(
            f"DELETE FROM core.sla_reply_time_daily WHERE reply_date >= ?", [cutoff]
        )
    else:
        db.execute("DELETE FROM core.sla_reply_time_daily")  # full backfill

    db.execute(
        f"""
        INSERT INTO core.sla_reply_time_daily
            (reply_date, workspace_slug, n_responses, avg_latency_min,
             median_latency_min, q25_latency_min, q50_latency_min, q75_latency_min,
             _snapshot_at, _run_id)
        SELECT
            reply_date,
            workspace_slug,
            count(*)                                       AS n_responses,
            avg(response_latency_minutes)                  AS avg_latency_min,
            median(response_latency_minutes)               AS median_latency_min,
            quantile_cont(response_latency_minutes, 0.25)  AS q25_latency_min,
            quantile_cont(response_latency_minutes, 0.50)  AS q50_latency_min,
            quantile_cont(response_latency_minutes, 0.75)  AS q75_latency_min,
            now()                                          AS _snapshot_at,
            ?                                              AS _run_id
        FROM core.sla_reply_time
        WHERE response_latency_minutes IS NOT NULL
          AND workspace_slug IS NOT NULL
          AND reply_date IS NOT NULL
          {where_window}
        GROUP BY reply_date, workspace_slug
        """,
        params,
    )
    snap_rows = db.execute("SELECT count(*) FROM core.sla_reply_time_daily").fetchone()[0]
    logger.info("core.sla_reply_time_daily: %d (workspace x day) snapshot rows", snap_rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="warehouse.duckdb path (default = config)")
    ap.add_argument("--snapshot-days", type=int, default=14,
                    help="re-snapshot the trailing N days (default 14; covers late IM replies)")
    ap.add_argument("--snapshot-all", action="store_true",
                    help="re-snapshot the entire history (one-time backfill)")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-slart")
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
