"""core.inbox_date_history — append-only record of every change to an inbox's creation date and
warm-up start, reconstructed by comparing consecutive core.account_census days.

WHY. Suppliers fix a broken mailbox by deleting and re-adding it, which resets both timestamps in
Instantly. Over the last 30 days that is 60,388 creation-date changes and 47,714 warm-up-start
changes, and every warm-up-start change came with a creation-date change — so every one is a re-add,
not an edit. Instantly is right about the mailbox that exists NOW; what gets destroyed is what the
date used to be. This phase is what stops that loss.

APPEND-ONLY. Never UPDATE, never DELETE. Idempotency is enforced by a UNIQUE index on
(email, field, detected_on); the anti-join below is a cheap pre-filter, not the guarantee, and the
SELECT is DISTINCT so a single run cannot self-duplicate either. Running it twice on the same day is
a no-op; running it after a gap backfills every day it missed.

FAIL-SAFE. Skips cleanly if the table is missing (DDL 1149 not applied) and never raises out of the
phase, so one history mirror cannot take the nightly down. Schema: sql/ddl/1149_inbox_date_history.sql
[2026-07-21, David]
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.inbox_date_history")

# One row per (email, field) whenever the value differs from that email's PREVIOUS census day.
# LAG over census_date is the whole comparison — no self-join, so it stays cheap on a ~12M-row census.
# `IS DISTINCT FROM` (not <>) so a NULL -> value transition counts and a NULL -> NULL does not.
_DIFF_SQL = """
WITH c AS (
    SELECT email, workspace_slug, census_date,
           timestamp_created        AS created,
           timestamp_warmup_start   AS ws,
           lag(timestamp_created)      OVER (PARTITION BY email ORDER BY census_date) AS prev_created,
           lag(timestamp_warmup_start) OVER (PARTITION BY email ORDER BY census_date) AS prev_ws
    FROM core.account_census
),
chg AS (
    SELECT email, workspace_slug, 'created' AS field,
           prev_created AS old_value, created AS new_value, census_date AS detected_on
    FROM c WHERE prev_created IS NOT NULL AND created IS DISTINCT FROM prev_created
    UNION ALL
    SELECT email, workspace_slug, 'warmup_start',
           prev_ws, ws, census_date
    FROM c WHERE prev_ws IS NOT NULL AND ws IS DISTINCT FROM prev_ws
)
SELECT DISTINCT chg.* FROM chg
LEFT JOIN core.inbox_date_history h
       ON h.email = chg.email AND h.field = chg.field AND h.detected_on = chg.detected_on
WHERE h.email IS NULL
"""


_CENSUS_COLS = ("email", "workspace_slug", "census_date",
                "timestamp_created", "timestamp_warmup_start")


def _table_exists(conn) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'core' AND table_name = 'inbox_date_history'").fetchone()[0] > 0


def _census_ready(conn) -> str:
    """Name the missing column loudly instead of failing with a generic binder error.

    Without this, a rename in core.account_census would leave the phase throwing something opaque and
    quietly producing zero history — the exact silent-stall failure mode this table exists to prevent.
    (All five names verified live against core.account_census on 2026-07-21.)"""
    have = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='core' AND table_name='account_census'").fetchall()}
    missing = [c for c in _CENSUS_COLS if c not in have]
    return ", ".join(missing)


def run_inbox_date_history(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn):
        logger.error("inbox_date_history SKIP: table missing (ddl 1149 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})

    missing = _census_ready(conn)
    if missing:
        logger.error("inbox_date_history SKIP: core.account_census is missing column(s): %s", missing)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "census_cols:" + missing})

    before = conn.execute("SELECT count(*) FROM core.inbox_date_history").fetchone()[0]
    conn.execute("BEGIN")
    try:
        # INSERT-ONLY, never UPDATE, never DELETE — that is the append-only contract. Three layers,
        # cheapest first: the anti-join skips what we already hold, DISTINCT stops a single run
        # self-duplicating, and ON CONFLICT DO NOTHING makes even a concurrent run a clean no-op
        # instead of a unique violation. DO NOTHING, never DO UPDATE: a history row that can be
        # rewritten is not history.
        conn.execute(
            "INSERT INTO core.inbox_date_history "
            "(email, workspace_slug, field, old_value, new_value, detected_on, _loaded_at, _run_id) "
            "SELECT email, workspace_slug, field, old_value, new_value, detected_on, now(), ? "
            "FROM (" + _DIFF_SQL + ") ON CONFLICT DO NOTHING", [ctx.run_id])
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    after = conn.execute("SELECT count(*) FROM core.inbox_date_history").fetchone()[0]
    added = after - before
    logger.info("inbox_date_history: +%d rows (total %d).", added, after)
    return PhaseResult(rows_in=added, rows_out=after,
                       notes={"added": added, "total": after})


def register(registry: Registry) -> None:
    # portal_core is PASS A, which is where the Data-Hub-facing core tables land AND — critically —
    # it runs AFTER account_census in the same pass, so the day's census row exists to compare
    # against. Putting it in PASS B would have meant history lagged a day behind the data it reads,
    # and PASS B is exactly the pass that has been failing.
    registry.add_phase("portal_core", "inbox_date_history", run_inbox_date_history)
