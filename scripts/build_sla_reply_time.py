#!/usr/bin/env python3
"""Build the canonical SLA reply-time metric — the §6 spec-true business-minute clock.

  1. core.sla_reply_time         — response-level fact, ONE row per thread's FIRST prospect
                                   reply (seq_in_thread=1), WORKSPACE-AWARE. Carries the
                                   §6 business-minute latency + clock-open bucket date,
                                   materialized so the warehouse and the daily report share
                                   ONE definition (DR-7).
  2. core.sla_reply_time_daily   — daily SNAPSHOT keyed on CLOCK_OPEN_DATE (count + avg +
                                   median + q25/q50/q75 of business-minute latency).

WIRED into nightly.sh (runs AFTER the orchestrator releases the writer lock; core/db.py's
in-process writer flock serializes it). Reads core.email_message (nightly-synced).

SOURCE / DEFINITION (DR-7, validated against deliverables/2026-07-02-sla-scrutiny/FINDINGS.md,
reconciled 2026-07-03 — this REPLACES the DDL-69 iam_response_time wall-clock build):
  * Base = core.email_message: ue_type 2 = inbound prospect reply, ue_type 3 = our reply.
  * FIRST prospect reply only: seq = row_number() OVER (PARTITION BY thread_id, workspace_id
    ORDER BY message_at, message_id); keep seq=1. (Load-bearing — FINDINGS §1: counting every
    reply halves R1's median 28->9.)
  * Our reply = min(ue_type=3 message_at) matched on thread_id AND workspace_id, message_at >
    the first prospect reply. Unanswered rows are KEPT (our_reply_ts / biz_latency NULL) so
    answer-rate is derivable; the daily snapshot + rollups aggregate only answered rows.
  * biz_latency_minutes = BUSINESS MINUTES accrued only inside 12:00-20:00 ET Mon-Fri
    (DST-correct, zoneinfo). clock_open_date = the ET date the SLA clock OPENS (off-window /
    weekend arrivals open at the next window). These two functions ARE the validated §6 clamp
    (ported verbatim from render_daily.py PR #151); the report now READS these columns rather
    than recomputing them, so the two can never diverge.
  * GRAIN = THREAD (thread_id, workspace_id), NOT lead_email — the validated reference is
    thread-grain (§6 reads seq_in_thread=1 here and reproduces its numbers with zero delta).
    Lead-grain dedup is the optional v_sla_reply_time_lead_grain view (DDL 1070 #5).

Usage:
    python scripts/build_sla_reply_time.py                  # full fact rebuild + FULL daily snapshot
    python scripts/build_sla_reply_time.py --snapshot-days 14   # restrict the daily snapshot to trailing 14d
    python scripts/build_sla_reply_time.py --db /path/to/warehouse.duckdb

The daily snapshot DEFAULTS to a full-history rebuild from the (fully-rebuilt) fact — the fact is
cheap (~120k first-reply rows) and its late replies are always re-incorporated, so a full re-snapshot
is uniformly clock_open_date-bucketed with no accumulation/discontinuity. For arbitrary spans use the
sla_reply_time_rollup() macro / v_sla_reply_time_rollup_period view (recomputes percentiles from the
fact — you cannot average the daily percentile columns).
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

_DDL = REPO_ROOT / "sql" / "ddl" / "1070_sla_reply_time_bizminutes.sql"
_EMAIL = "core.email_message"

# ── THE validated §6 SLA clock (ported verbatim from render_daily.py, PR #151). ──────────────
# core.sla_reply_time is now the single home for this clamp; render_daily.py §6 reads the
# materialized biz_latency_minutes / clock_open_date it produces. Keep these bit-identical.
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    ET_TZ = None


def _biz_minutes(p, r, tz):
    """Business minutes accrued only inside 12:00-20:00 ET Mon-Fri between aware datetimes
    p (first prospect reply) and r (our first reply after it). DST-correct via zoneinfo."""
    if r <= p:
        return 0.0
    d = p.astimezone(tz).date()
    end = r.astimezone(tz).date()
    tot = 0.0
    while d <= end:
        if d.isoweekday() <= 5:
            o = dt.datetime.combine(d, dt.time(12), tzinfo=tz)
            c = dt.datetime.combine(d, dt.time(20), tzinfo=tz)
            lo = p if p > o else o
            hi = r if r < c else c
            if hi > lo:
                tot += (hi - lo).total_seconds() / 60.0
        d += dt.timedelta(days=1)
    return tot


def _clock_open_date(p, tz):
    """The ET date the thread's SLA clock OPENS. A weekday arrival before 20:00 ET opens that
    day; a post-8pm or weekend arrival opens the next Mon-Fri day."""
    e = p.astimezone(tz)
    d = e.date()
    if e.isoweekday() <= 5 and e < dt.datetime.combine(d, dt.time(20), tzinfo=tz):
        return d
    d += dt.timedelta(days=1)
    while d.isoweekday() > 5:
        d += dt.timedelta(days=1)
    return d


def _selfcheck(tz) -> None:
    """Fail-LOUD guard on the ported §6 clamp. Since render_daily.py §6 no longer computes the clamp
    inline (it reads this build's materialized output), there is no live cross-check that the two
    agree — so a future accidental edit to _biz_minutes / _clock_open_date could silently ship wrong
    SLA numbers. These fixtures pin the validated PR #151 behavior; a divergence hard-fails the
    nightly build (never a quiet wrong number). [DR-7 single-definition integrity]"""
    U = dt.timezone.utc
    def D(s): return dt.datetime.fromisoformat(s).replace(tzinfo=U)
    cases = [
        # (p, r, expect_biz, expect_clock_open)   times are UTC; ET is UTC-4 (EDT) on these dates
        ("2026-06-25T16:00:00", "2026-06-25T16:45:00", 45.0, dt.date(2026, 6, 25)),  # in-window 12:00->12:45 ET
        ("2026-06-27T22:00:00", "2026-06-29T17:05:00", 65.0, dt.date(2026, 6, 29)),  # Sat arrival -> Mon 12:00->13:05 ET
        ("2026-06-27T22:00:00", "2026-06-29T16:05:00",  5.0, dt.date(2026, 6, 29)),  # Sat -> Mon 12:05pm ET = 5 min
        ("2026-06-25T16:00:00", "2026-06-25T15:00:00",  0.0, dt.date(2026, 6, 25)),  # answer before p -> 0
    ]
    for ps, rs, eb, eco in cases:
        p, r = D(ps), D(rs)
        gb = _biz_minutes(p, r, tz)
        gco = _clock_open_date(p, tz)
        if abs(gb - eb) > 1e-6 or gco != eco:
            raise RuntimeError(
                f"SLA clamp self-check FAILED for p={ps} r={rs}: got biz={gb} clock_open={gco}, "
                f"expected biz={eb} clock_open={eco} — the ported §6 clamp has DIVERGED from PR #151. "
                f"Refusing to build (would silently ship wrong SLA numbers).")


def build(db, snapshot_days: int | None, run_id: str) -> None:
    if ET_TZ is None:
        raise RuntimeError("zoneinfo unavailable — refusing to build the SLA clock in a wrong fixed offset")
    tz = ET_TZ
    utc = dt.timezone.utc
    _selfcheck(tz)   # pin the validated §6 clamp before materializing anything

    # --- apply DDL (idempotent; DROP+CREATE fact/snapshot, OR REPLACE views/macro) -----------
    ddl_text = _DDL.read_text()
    table_ddl, _, index_and_rest = ddl_text.partition("-- @@INDEXES@@")
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(table_ddl)      # DROP+CREATE the unindexed core.sla_reply_time

    # --- 1. pull first-reply pairs (seq=1) across ALL workspaces (canonical) ------------------
    # No truncation cap here (box-local DuckDB), so unanswered firsts are kept. `ours` spans
    # full history so an old first-reply still finds its true earliest ue_type=3 answer.
    pairs = db.execute(
        f"""
        WITH inbound AS (
          SELECT thread_id, workspace_id AS ws, lead_email, campaign_id, message_at AS p_ts,
                 row_number() OVER (PARTITION BY thread_id, workspace_id
                                    ORDER BY message_at, message_id) AS seq
          FROM {_EMAIL}
          WHERE ue_type=2 AND thread_id IS NOT NULL),
        ours AS (SELECT thread_id, workspace_id AS ws, message_at AS r_ts
                 FROM {_EMAIL}
                 WHERE ue_type=3 AND thread_id IS NOT NULL)
        SELECT i.thread_id, i.ws, i.lead_email, i.campaign_id,
               epoch_ms(i.p_ts) AS p_ms,
               epoch_ms((SELECT min(o.r_ts) FROM ours o
                         WHERE o.thread_id=i.thread_id AND o.ws=i.ws AND o.r_ts > i.p_ts)) AS r_ms
        FROM inbound i
        WHERE i.seq=1
        """
    ).fetchall()

    now_ts = dt.datetime.now(tz=utc)
    rows = []
    for thread_id, ws, lead_email, campaign_id, p_ms, r_ms in pairs:
        if p_ms is None:
            continue
        p = dt.datetime.fromtimestamp(float(p_ms) / 1000.0, tz=utc)
        clock_open = _clock_open_date(p, tz)
        reply_date = p.date()
        if r_ms is None:
            our_ts = biz_lat = raw_lat = None
        else:
            r = dt.datetime.fromtimestamp(float(r_ms) / 1000.0, tz=utc)
            our_ts = r
            biz_lat = _biz_minutes(p, r, tz)
            raw_lat = (r - p).total_seconds() / 60.0
        rows.append((
            f"{thread_id}|{ws}",           # response_id (one first-reply pair per thread+ws)
            thread_id, ws, campaign_id, lead_email,
            1,                             # seq_in_thread
            p, our_ts,
            biz_lat, raw_lat, raw_lat,     # biz_latency, raw_latency, response_latency (back-compat alias)
            clock_open, reply_date,
            now_ts, run_id,
        ))

    db.executemany(
        """INSERT INTO core.sla_reply_time
           (response_id, thread_id, workspace_slug, campaign_id, lead_email, seq_in_thread,
            prospect_msg_ts, our_reply_ts, biz_latency_minutes, raw_latency_minutes,
            response_latency_minutes, clock_open_date, reply_date, _built_at, _run_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    db.execute(index_and_rest)   # snapshot table DROP+CREATE + indexes + views + macro

    total = db.execute("SELECT count(*) FROM core.sla_reply_time").fetchone()[0]
    answered = db.execute(
        "SELECT count(*) FROM core.sla_reply_time WHERE biz_latency_minutes IS NOT NULL"
    ).fetchone()[0]
    n_ws = db.execute(
        "SELECT count(DISTINCT workspace_slug) FROM core.sla_reply_time "
        "WHERE biz_latency_minutes IS NOT NULL"
    ).fetchone()[0]
    logger.info("core.sla_reply_time: %d first-reply rows (%d answered) across %d workspaces",
                total, answered, n_ws)

    # --- 2. daily snapshot (bucketed on clock_open_date; business-minute stats) ---------------
    if snapshot_days is not None:
        cutoff = (dt.datetime.now(dt.timezone.utc).date()
                  - dt.timedelta(days=snapshot_days)).isoformat()
        db.execute("DELETE FROM core.sla_reply_time_daily WHERE clock_open_date >= ?", [cutoff])
        where_window, params = "AND clock_open_date >= ?", [run_id, cutoff]
    else:
        db.execute("DELETE FROM core.sla_reply_time_daily")
        where_window, params = "", [run_id]

    db.execute(
        f"""
        INSERT INTO core.sla_reply_time_daily
            (clock_open_date, workspace_slug, n_responses, avg_latency_min,
             median_latency_min, q25_latency_min, q50_latency_min, q75_latency_min,
             _snapshot_at, _run_id)
        SELECT
            clock_open_date,
            workspace_slug,
            count(*)                                     AS n_responses,
            avg(biz_latency_minutes)                     AS avg_latency_min,
            median(biz_latency_minutes)                  AS median_latency_min,
            quantile_cont(biz_latency_minutes, 0.25)     AS q25_latency_min,
            quantile_cont(biz_latency_minutes, 0.50)     AS q50_latency_min,
            quantile_cont(biz_latency_minutes, 0.75)     AS q75_latency_min,
            now()                                        AS _snapshot_at,
            ?                                            AS _run_id
        FROM core.sla_reply_time
        WHERE seq_in_thread = 1
          AND biz_latency_minutes IS NOT NULL
          AND workspace_slug IS NOT NULL
          AND clock_open_date IS NOT NULL
          {where_window}
        GROUP BY clock_open_date, workspace_slug
        """,
        params,
    )
    snap_rows = db.execute("SELECT count(*) FROM core.sla_reply_time_daily").fetchone()[0]
    logger.info("core.sla_reply_time_daily: %d (workspace x clock-open-day) snapshot rows", snap_rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None, help="warehouse.duckdb path (default = config)")
    ap.add_argument("--snapshot-days", type=int, default=None,
                    help="restrict the daily snapshot to the trailing N clock-open days "
                         "(default: full-history rebuild — the fact is cheap + fully rebuilt each run)")
    ap.add_argument("--snapshot-all", action="store_true",
                    help="force a full-history daily snapshot (the default; kept for explicitness)")
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
