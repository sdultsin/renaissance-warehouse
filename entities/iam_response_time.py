"""Compute IAM response times — builds core.iam_response_time.

One row per prospect reply, with the time delta to the next IAM manual response
in the same thread. Source:
  - raw_instantly_email: prospect replies with thread_id (Instantly-source)
  - raw_instantly_sent_email: manual IAM outbound replies (ue_type=3)

Coverage:
  - Instantly-source replies (have thread_id): response time computed when a
    matching IAM sent email exists in the same thread after the reply timestamp.
  - Pipeline-source replies (no thread_id available): get iam_responded_at=NULL.

thread_reply_number = rank of this prospect reply within its thread (1=first).
Rebuilt fully each run (DELETE + INSERT, like core.reply). Idempotent.

Registers under 'canonical' phase, must run AFTER f_reply_canonical.
Schema: sql/ddl/50_iam_response_time.sql.
"""

from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.iam_response_time")

_DDL = REPO_ROOT / "sql" / "ddl" / "50_iam_response_time.sql"

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
    db.execute(_DDL.read_text())

    # Check whether raw_instantly_sent_email has any data.
    sent_count = db.execute(
        "SELECT count(*) FROM raw_instantly_sent_email"
    ).fetchone()[0]
    logger.info("raw_instantly_sent_email rows: %d", sent_count)

    db.execute("DELETE FROM core.iam_response_time")

    db.execute(
        f"""
        INSERT INTO core.iam_response_time
            (id, email_id, campaign_id, lead_email, thread_id, thread_reply_number,
             prospect_replied_at, iam_responded_at, response_minutes, response_bucket,
             synced_at)
        WITH
        -- Prospect replies with thread_id (from direct-Instantly source)
        instantly_replies AS (
            SELECT
                rie.email_id,
                rie.campaign_id,
                lower(trim(rie.lead_email))                          AS lead_email,
                rie.thread_id,
                rie.reply_timestamp                                  AS prospect_replied_at,
                ROW_NUMBER() OVER (
                    PARTITION BY rie.thread_id
                    ORDER BY rie.reply_timestamp
                )                                                    AS thread_reply_number
            FROM raw_instantly_email rie
            WHERE rie.thread_id IS NOT NULL
              AND rie.lead_email IS NOT NULL
        ),
        -- Next IAM manual reply after each prospect reply in the same thread
        with_iam AS (
            SELECT
                ir.email_id,
                ir.campaign_id,
                ir.lead_email,
                ir.thread_id,
                ir.thread_reply_number,
                ir.prospect_replied_at,
                MIN(se.sent_timestamp)                               AS iam_responded_at
            FROM instantly_replies ir
            LEFT JOIN raw_instantly_sent_email se
                ON se.thread_id = ir.thread_id
               AND se.sent_timestamp > ir.prospect_replied_at
            GROUP BY
                ir.email_id, ir.campaign_id, ir.lead_email, ir.thread_id,
                ir.thread_reply_number, ir.prospect_replied_at
        ),
        -- Pipeline-source replies have no thread_id; include as no_response rows
        -- using reply_id as email_id so DoD row count roughly matches core.reply
        pipeline_replies AS (
            SELECT
                r.reply_id   AS email_id,
                r.campaign_id,
                r.lead_email,
                NULL         AS thread_id,
                1            AS thread_reply_number,
                r.reply_timestamp AS prospect_replied_at,
                NULL::TIMESTAMPTZ AS iam_responded_at
            FROM core.reply r
            WHERE r.source = 'pipeline'
        ),
        combined AS (
            SELECT * FROM with_iam
            UNION ALL
            SELECT * FROM pipeline_replies
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
        FROM combined
        """
    )

    total = db.execute("SELECT count(*) FROM core.iam_response_time").fetchone()[0]
    stats = db.execute(
        """
        SELECT
            count(*)                                              AS total_pairs,
            count(*) FILTER (WHERE iam_responded_at IS NOT NULL) AS with_response,
            count(*) FILTER (WHERE iam_responded_at IS NULL)     AS no_response,
            round(avg(response_minutes)
                  FILTER (WHERE response_minutes IS NOT NULL))   AS avg_min,
            count(*) FILTER (WHERE thread_id IS NOT NULL)        AS with_thread_id
        FROM core.iam_response_time
        """
    ).fetchone()

    logger.info(
        "core.iam_response_time: %d rows | with_response=%d no_response=%d "
        "avg_min=%s with_thread_id=%d",
        *stats,
    )

    return PhaseResult(
        rows_in=total,
        rows_out=total,
        notes={
            "total_pairs": stats[0],
            "with_response": stats[1],
            "no_response": stats[2],
            "avg_response_min": float(stats[3]) if stats[3] else None,
            "with_thread_id": stats[4],
            "sent_rows_available": sent_count,
        },
    )


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "iam_response_time", run)
