"""core.meeting canonical entity — resolved from raw_pipeline_meetings_booked_raw.

Slack is the source of truth for meetings (per Sam). Calendly + Close are v1.5.
Registers under the 'canonical' phase. Idempotent full rebuild each run.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.meeting")

RAW = "raw_pipeline_meetings_booked_raw"


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "meeting", run_meeting)


def run_meeting(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    # Idempotent full rebuild — core.meeting is a pure projection of the raw table.
    db.execute("DELETE FROM core.meeting")
    db.execute(
        f"""
        INSERT INTO core.meeting
          (meeting_id, source, source_event_id, posted_at, partner, campaign_id,
           campaign_name_raw, cm, match_method, match_confidence, is_duplicate_of,
           cost_per_meeting_usd_estimated, raw_text)
        SELECT
          COALESCE(channel_id,'') || ':' || COALESCE(message_ts,'') || ':' ||
            COALESCE(CAST(line_index AS VARCHAR),'')  AS meeting_id,
          'slack'              AS source,
          CAST(id AS VARCHAR)  AS source_event_id,
          posted_at,
          partner,
          campaign_id,
          campaign_name_raw,
          NULL                 AS cm,            -- filled from campaign join below
          match_method,
          match_confidence,
          NULL                 AS is_duplicate_of,
          NULL                 AS cost_per_meeting_usd_estimated,  -- v3 derivation
          raw_text
        FROM {RAW}
        WHERE id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY COALESCE(channel_id,'') || ':' || COALESCE(message_ts,'') || ':' ||
            COALESCE(CAST(line_index AS VARCHAR),'')
          ORDER BY posted_at
        ) = 1
        """
    )

    # CM attribution: campaign join first (authoritative), regex on raw_text as fallback.
    db.execute(
        """
        UPDATE core.meeting AS m
        SET cm = c.cm
        FROM core.campaign c
        WHERE m.campaign_id = c.campaign_id AND m.cm IS NULL AND c.cm IS NOT NULL
        """
    )
    db.execute(
        r"""
        UPDATE core.meeting
        SET cm = upper(regexp_extract(raw_text, '\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\b', 1))
        WHERE cm IS NULL
          AND raw_text IS NOT NULL
          AND regexp_extract(raw_text, '\b(SAM|SAMUEL|LEO|IDO|EYVER|TOUKIR|TOMER|LUCAS|MAX)\b', 1) <> ''
        """
    )

    n = db.execute("SELECT count(*) FROM core.meeting").fetchone()[0]
    logger.info("core.meeting rebuilt: %d rows", n)
    return PhaseResult(rows_in=n, rows_out=n, notes={"source": RAW})
