"""
core.account_first_cold_send — per-inbox FIRST real campaign send (= go-live), rebuilt nightly.

EARLIEST real send per inbox = LEAST across two sources: (1) the ue_type=1 ("sent from campaign" / cold) rows of
the OLD (retired, frozen <=2026-06-23) main.raw_pipeline_conversation_messages + the NEW (fresh, daily)
main.raw_instantly_email_message; and (2) core.sending_account_daily (Instantly's COMPLETE send feed), which the
message-log misses for ~22k live inboxes (e.g. Outreach Today). LEAST(earliest), not COALESCE(prefer-msg): the two
sources agree to within ~0 days where both exist (verified 2026-07-07: of ~1.04M inboxes present in both, median
gap 0d, ZERO differ by >20d), so taking the earliest never drags in a stale warm-up date — it only recovers the
true first send when one source happened to see it sooner. This is the REAL go-live moment — unlike the old
account_label.cold_start / v_inbox_overview.go_live definition, which counted WARM-UP sends (warm-up inflates
actual_sends, so ~59k warming MilkBox falsely showed a go_live). Feeds v_inbox_overview.go_live (gated to
Active-tagged inboxes). Schema: sql/ddl/<N>_account_first_cold_send.sql.

Full rebuild each run (DELETE + INSERT) = idempotent. Garbage-date floor 2025-01-01 (the old log carries bogus
2001 timestamps). Graceful: skips cleanly if the table or both source logs are absent. Built 2026-07-07.
"""
from __future__ import annotations
import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.account_first_cold_send")


def _table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0] > 0


def run_account_first_cold_send(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn, "core", "account_first_cold_send"):
        logger.error("account_first_cold_send SKIP: table missing (ddl not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})

    parts = []
    if _table_exists(conn, "main", "raw_pipeline_conversation_messages"):
        parts.append(
            "SELECT lower(trim(eaccount)) AS email, CAST(message_timestamp AS TIMESTAMPTZ) AS ts "
            "FROM main.raw_pipeline_conversation_messages "
            "WHERE ue_type = 1 AND eaccount IS NOT NULL AND message_timestamp >= TIMESTAMP '2025-01-01'")
    if _table_exists(conn, "main", "raw_instantly_email_message"):
        parts.append(
            "SELECT lower(trim(eaccount)) AS email, CAST(message_at AS TIMESTAMPTZ) AS ts "
            "FROM main.raw_instantly_email_message "
            "WHERE ue_type = 1 AND eaccount IS NOT NULL AND message_at >= TIMESTAMP '2025-01-01'")
    if not parts:
        logger.error("account_first_cold_send SKIP: no send-log source table present.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_sendlog"})

    msg_cte = ("msg AS (SELECT email, min(ts) AS ts FROM (" + " UNION ALL ".join(parts) + ") GROUP BY 1)")
    # sending_account_daily = Instantly's COMPLETE send feed; the message-log misses ~22k live inboxes (e.g. OTD).
    if _table_exists(conn, "core", "sending_account_daily"):
        sad_cte = ("sad AS (SELECT lower(account_id) AS email, CAST(min(date) AS TIMESTAMPTZ) AS ts "
                   "FROM core.sending_account_daily WHERE actual_sends>0 AND date >= DATE '2025-01-01' GROUP BY 1)")
    else:
        sad_cte = "sad AS (SELECT CAST(NULL AS VARCHAR) AS email, CAST(NULL AS TIMESTAMPTZ) AS ts WHERE FALSE)"
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM core.account_first_cold_send")
        conn.execute(f"""
            INSERT INTO core.account_first_cold_send BY NAME
            WITH {msg_cte}, {sad_cte}, u AS (SELECT email FROM msg UNION SELECT email FROM sad)
            -- LEAST = EARLIEST real send across both sources. DuckDB least() IGNORES NULLs, so a
            -- msg-only or sad-only inbox keeps its single date; where both exist it takes the earlier.
            -- Chosen over COALESCE(msg,sad) so we never discard an earlier real send the cold-log
            -- happened to miss (the two sources sit within ~0 days of each other — see module docstring).
            SELECT u.email, LEAST(msg.ts, sad.ts) AS first_cold_send_at, now() AS _loaded_at, '{ctx.run_id}' AS _run_id
            FROM u LEFT JOIN msg USING(email) LEFT JOIN sad USING(email)
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = conn.execute("SELECT count(*) FROM core.account_first_cold_send").fetchone()[0]
    logger.info("core.account_first_cold_send <- %d inboxes (first real campaign send)", n)
    return PhaseResult(rows_in=n, rows_out=n, notes={"inboxes": n, "sources": len(parts)})


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "account_first_cold_send", run_account_first_cold_send)
