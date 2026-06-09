"""Compute IM (Inbox Manager) response times — builds core.iam_response_time.

One row per prospect reply, with the time delta to the next IM manual response
in the same thread.

Source: main.raw_pipeline_conversation_messages — the full prospect<->IM thread
backfill (both directions, ~10 months deep). This supersedes the thin
raw_instantly_sent_email pull (which only covered a few days from the
email_type=sent endpoint).
  - ue_type=2  → inbound prospect replies   (the message we measure FROM)
  - ue_type=3  → outbound_manual IM replies  (the response we measure TO)

Coverage: response time is computed when a matching IM manual reply (ue_type=3)
exists in the same thread AFTER the prospect reply timestamp. Replies with no
later IM response get iam_responded_at = NULL (response_bucket='no_response').

thread_reply_number = rank of this prospect reply within its thread (1=first).
Rebuilt fully each run (DELETE + INSERT). Idempotent.

Registers under 'canonical' phase.
Schema: sql/ddl/50_iam_response_time.sql.
"""

from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.iam_response_time")

_DDL = REPO_ROOT / "sql" / "ddl" / "50_iam_response_time.sql"

_CONV = "main.raw_pipeline_conversation_messages"

_RESPONSE_BUCKET_SQL = """\
CASE
  WHEN iam_responded_at IS NULL THEN 'no_response'
  WHEN response_minutes < 1    THEN '<1min'
  WHEN response_minutes < 5    THEN '1-5min'
  WHEN response_minutes < 30   THEN '5-30min'
  WHEN response_minutes < 120  THEN '30min-2h'
  WHEN response_minutes < 1440 THEN '2h-24h'
  ELSE '>24h'
END"""


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    inbound_count = db.execute(
        f"SELECT count(*) FROM {_CONV} WHERE ue_type = 2 AND thread_id IS NOT NULL"
    ).fetchone()[0]
    outbound_count = db.execute(
        f"SELECT count(*) FROM {_CONV} WHERE ue_type = 3 AND thread_id IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        "conversation_messages: inbound(ue2)=%d outbound_manual(ue3)=%d",
        inbound_count,
        outbound_count,
    )

    # Full rebuild. Insert into an UNINDEXED table, then bulk-build indexes:
    # inserting ~800k rows into a table with live ART indexes throws DuckDB
    # "Invalid argument" (same bug family as the raw_instantly_sent_email incident).
    table_ddl, _, index_ddl = _DDL.read_text().partition("-- @@INDEXES@@")
    db.execute("DROP TABLE IF EXISTS core.iam_response_time")
    db.execute(table_ddl)

    db.execute(
        f"""
        INSERT INTO core.iam_response_time
            (id, email_id, campaign_id, lead_email, thread_id, thread_reply_number,
             prospect_replied_at, iam_responded_at, response_minutes, response_bucket,
             synced_at)
        WITH
        -- Distinct inbound prospect replies (dedupe exact dup rows from raw sync)
        inbound_src AS (
            SELECT DISTINCT
                id                          AS email_id,
                campaign_id,
                lower(trim(lead_email))     AS lead_email,
                thread_id,
                message_timestamp           AS prospect_replied_at
            FROM {_CONV}
            WHERE ue_type = 2
              AND thread_id IS NOT NULL
              AND lead_email IS NOT NULL
              AND message_timestamp IS NOT NULL
        ),
        inbound AS (
            SELECT
                email_id,
                campaign_id,
                lead_email,
                thread_id,
                prospect_replied_at,
                ROW_NUMBER() OVER (
                    PARTITION BY thread_id
                    ORDER BY prospect_replied_at, email_id
                )                           AS thread_reply_number
            FROM inbound_src
        ),
        -- IM manual outbound replies (ue_type=3)
        outbound AS (
            SELECT thread_id, message_timestamp AS resp_ts
            FROM {_CONV}
            WHERE ue_type = 3
              AND thread_id IS NOT NULL
              AND message_timestamp IS NOT NULL
        ),
        -- Next IM manual reply after each prospect reply in the same thread
        with_iam AS (
            SELECT
                ib.email_id,
                ib.campaign_id,
                ib.lead_email,
                ib.thread_id,
                ib.thread_reply_number,
                ib.prospect_replied_at,
                MIN(o.resp_ts)              AS iam_responded_at
            FROM inbound ib
            LEFT JOIN outbound o
                ON o.thread_id = ib.thread_id
               AND o.resp_ts > ib.prospect_replied_at
            GROUP BY
                ib.email_id, ib.campaign_id, ib.lead_email, ib.thread_id,
                ib.thread_reply_number, ib.prospect_replied_at
        )
        SELECT
            email_id || '_' || thread_reply_number               AS id,
            email_id,
            campaign_id,
            lead_email,
            thread_id,
            thread_reply_number,
            prospect_replied_at,
            iam_responded_at,
            CASE
                WHEN iam_responded_at IS NULL THEN NULL
                ELSE CAST(
                    EXTRACT(EPOCH FROM (iam_responded_at - prospect_replied_at)) / 60
                    AS INTEGER
                )
            END                                                  AS response_minutes,
            {_RESPONSE_BUCKET_SQL}                               AS response_bucket,
            now()                                                AS synced_at
        FROM with_iam
        """
    )

    # Bulk-build indexes on the now-populated table (avoids the incremental-insert
    # ART index bug; this is the same path the earlier index-corruption fix used).
    db.execute(index_ddl)

    total = db.execute("SELECT count(*) FROM core.iam_response_time").fetchone()[0]
    stats = db.execute(
        """
        SELECT
            count(*)                                              AS total_pairs,
            count(*) FILTER (WHERE iam_responded_at IS NOT NULL) AS with_response,
            count(*) FILTER (WHERE iam_responded_at IS NULL)     AS no_response,
            round(median(response_minutes)
                  FILTER (WHERE response_minutes IS NOT NULL))   AS median_min,
            count(DISTINCT lead_email)                           AS distinct_leads
        FROM core.iam_response_time
        """
    ).fetchone()

    logger.info(
        "core.iam_response_time: %d rows | with_response=%d no_response=%d "
        "median_min=%s distinct_leads=%d",
        *stats,
    )

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={
            "total_pairs": stats[0],
            "with_response": stats[1],
            "no_response": stats[2],
            "median_response_min": float(stats[3]) if stats[3] else None,
            "distinct_leads": stats[4],
            "inbound_available": inbound_count,
            "outbound_available": outbound_count,
        },
    )


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "iam_response_time", run)
