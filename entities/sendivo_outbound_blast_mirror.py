"""Sendivo per-message blast mirror (booking->blast attribution, 2026-06-29).

Mirrors the BLAST-CARRYING outbound messages from comms.sendivo_outbound_recovered
(comms Supabase: phone10 / blast_id / blast_name / sent_at, ~50k rows since
2026-06-26 when Larry added blast_id to /sms/logs) into the warehouse table
raw_sendivo_outbound_message. This is the (phone10 x blast_id) grain that
core.v_sms_booking_attribution needs to derive the originating (first-reply)
blast for each SMS booking -- replacing Sendivo's unreliable last-reply
deals_won and removing the manual vendor-export dependency.

Design mirrors entities/sendivo_outbound_mirror.py:
  * INCREMENTAL by sent_at (high-water - overlap); first run pulls full history.
  * Append-only raw layer (_run_id tagged); v_sendivo_outbound_blast de-dups.
  * Single-writer-safe: runs in the existing "comms_mirror" phase (serialized
    writer window), so it adds ZERO contention to the real-time path.
  * Uses the same postgres ATTACH the v1 comms mirror uses.

Only rows with a non-null blast_id are mirrored (the attribution-relevant
subset). Pre-2026-06-26 messages have no blast_id and are skipped -- that
history is gated on the Larry blast_id backfill (when it lands, the same
incremental pull picks the newly-tagged rows up automatically).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.registry import PhaseResult

if TYPE_CHECKING:
    from core.registry import RunContext

logger = logging.getLogger(__name__)

RAW_TABLE = "raw_sendivo_outbound_message"
PG_SRC = "pg.comms.sendivo_outbound_recovered"
# Absorb late-arriving / out-of-order rows; the append-only raw + de-dup view make it idempotent.
OVERLAP = "interval '6 hours'"


def _run(ctx: "RunContext") -> PhaseResult:
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn = ctx.db

    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        high_water = conn.execute(f"SELECT max(sent_at) FROM {RAW_TABLE}").fetchone()[0]

        if high_water is None:
            where = "WHERE blast_id IS NOT NULL"
            logger.info("%s empty -> full blast history pull", RAW_TABLE)
        else:
            # Interpolating a literal timestamp from our OWN warehouse (not user input);
            # postgres_scanner pushes the predicate down to PG.
            where = (
                f"WHERE blast_id IS NOT NULL "
                f"AND sent_at > (TIMESTAMP '{high_water}' - {OVERLAP})"
            )
            logger.info("%s incremental: sent_at > %s - overlap", RAW_TABLE, high_water)

        conn.execute(
            f"""
            INSERT INTO {RAW_TABLE}
              (sendivo_log_id, phone10, blast_id, blast_name, campaign_name,
               sub_account_name, sent_at, _loaded_at, _run_id)
            SELECT sendivo_log_id, phone10, blast_id, blast_name, campaign_name,
                   sub_account_name, sent_at, now(), ?
            FROM {PG_SRC}
            {where}
            """,
            [ctx.run_id],
        )
        n = conn.execute(
            f"SELECT count(*) FROM {RAW_TABLE} WHERE _run_id = ?", [ctx.run_id]
        ).fetchone()[0]
        logger.info("mirrored %s blast messages -> %s: %d rows", PG_SRC, RAW_TABLE, n)

        conn.execute("DETACH pg")
    except Exception:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass
        raise

    return PhaseResult(rows_in=n, rows_out=n, notes={"incremental": high_water is not None})


def register(registry) -> None:
    # Same phase as the v1 comms mirror so it runs in the serialized writer window.
    registry.add_phase("comms_mirror", "sendivo_outbound_blast_mirror", _run)
