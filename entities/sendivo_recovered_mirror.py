"""Sendivo recovered-outbound mirror WITH the SLA discriminator columns (DDL 1077, ITEM-3).

Mirrors comms.sendivo_outbound_recovered (the worker's nightly Sendivo-log recovery,
08:15/13:30 UTC) into raw_sendivo_outbound_recovered carrying sub_account_name +
blast_id + campaign_name. The existing v1 mirror (entities/sendivo_outbound_mirror.py ->
raw_comms_sendivo_outbound) reads a PG view that exposes only phone10/message/sent_at/
status_group — without sub_account there is no desk attribution, and without blast_id a
reply-type (conversational) send cannot be told from a scheduled blast step, which is
exactly the split the SMS reply-time SLA (core.sla_reply_time_smswa) needs:
  * blast_id IS NULL  = reply-type send (AIM or manual IM answer)  <- the SLA response side
  * blast_id NOT NULL = cold-blast step (never a "response")
Verified 2026-07-03 (ITEM3-SLA coverage audit): 1,087/1,087 known AIM sends appear as
blast_id-NULL rows; 97.6% of SMS bookings 06-28..07-01 show a prior reply-type send.

Design (same conventions as sendivo_outbound_mirror, except the watermark):
  * INCREMENTAL by recovered_at (the source's DISCOVERY timestamp, NOT sent_at) with a
    small overlap window; first run (empty target) pulls full history once (~2.2M rows).
    sent_at would silently miss deep back-fills — the worker's recovery job anti-join-
    backfills rows whose sent_at is days old (PR #179), and those must still be picked up.
  * Append-only raw layer, one _run_id per pull; v_sendivo_outbound_recovered dedupes on
    sendivo_log_id (source-unique) so overlap re-pulls are idempotent for queries.
  * Registered in the comms_mirror phase -> runs inside the orchestrator's serialized
    writer window; never opens a second writer.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.registry import PhaseResult

if TYPE_CHECKING:
    from core.registry import RunContext

logger = logging.getLogger(__name__)

RAW_TABLE = "raw_sendivo_outbound_recovered"
PG_TABLE = "pg.comms.sendivo_outbound_recovered"

# Re-pull a small overlap before the high-water mark to absorb clock skew between the
# worker and the warehouse. Watermark = recovered_at (discovery time), so even a deep
# sent_at back-fill is picked up the run after it lands. Dedupe view keeps this idempotent.
OVERLAP = "interval '6 hours'"


def _run(ctx: "RunContext") -> PhaseResult:
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn = ctx.db

    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        high_water = conn.execute(
            f"SELECT max(recovered_at) FROM {RAW_TABLE}"
        ).fetchone()[0]

        if high_water is None:
            where = ""
            logger.info("%s empty -> full history pull", RAW_TABLE)
        else:
            # Literal timestamp from our OWN warehouse (not user input); postgres_scanner
            # pushes the predicate down to PG.
            where = f"WHERE recovered_at > (TIMESTAMP '{high_water}' - {OVERLAP})"
            logger.info("%s incremental: recovered_at > %s - overlap", RAW_TABLE, high_water)

        conn.execute(
            f"""
            INSERT INTO {RAW_TABLE}
              (sendivo_log_id, phone10, sent_at, recovered_at, sub_account_name,
               campaign_name, blast_id, message_content, _loaded_at, _run_id)
            SELECT sendivo_log_id, phone10, sent_at, recovered_at, sub_account_name,
                   campaign_name, blast_id, message_content, now(), ?
            FROM {PG_TABLE}
            {where}
            """,
            [ctx.run_id],
        )
        n = conn.execute(
            f"SELECT count(*) FROM {RAW_TABLE} WHERE _run_id = ?", [ctx.run_id]
        ).fetchone()[0]
        logger.info("mirrored comms.sendivo_outbound_recovered -> %s: %d rows", RAW_TABLE, n)

        conn.execute("DETACH pg")
    except Exception:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass
        raise

    return PhaseResult(rows_in=n, rows_out=n, notes={"incremental": high_water is not None})


def register(registry) -> None:
    # Same phase as the comms mirrors so it runs in the serialized writer window.
    registry.add_phase("comms_mirror", "sendivo_recovered_mirror", _run)
