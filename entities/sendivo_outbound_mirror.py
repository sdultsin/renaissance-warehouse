"""Sendivo outbound-blast mirror (R6 gap-close, 2026-06-09).

Mirrors the CLEANED Sendivo outbound-SMS stream — comms.v_sendivo_outbound_message
(phone10 / message / sent_at / status_group, ~1.55M rows) — into the warehouse
table raw_comms_sendivo_outbound. This is the Sendivo "original blast" body + the
full outbound cadence, which the v1 comms mirror (entities/comms_mirror.py)
intentionally left out (it skips the 6.37M-row raw comms.webhook_receipt as
noise). Without this, the Sendivo original message + full-thread outbound side
is NOT warehouse-native — the exact R6 gap this build closes.

Design:
  * INCREMENTAL by sent_at. Each run pulls only view rows with
    sent_at > (max sent_at already in the warehouse) minus a small overlap
    window, so the nightly run never re-scans the 6.37M webhook_receipt rows.
    First run (empty target) pulls the full history once.
  * Append-only at the raw layer (matches the raw_* convention): each pull is
    tagged with _run_id. The DDL ships v_comms_sendivo_outbound which de-dups
    on (phone10, sent_at, message) for queries.
  * Single-writer-safe: runs INSIDE the orchestrator's serialized writer
    (registered in the existing "comms_mirror" phase, ~03:45 UTC), so it adds
    ZERO contention to the real-time Close path and never opens a second writer.
  * Uses the same postgres_scanner ATTACH the comms mirror uses; reads through
    the PG view (postgres_scanner can read PG views).

This is async/batch only — nothing here touches the worker's request path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.registry import PhaseResult

if TYPE_CHECKING:
    from core.registry import RunContext

logger = logging.getLogger(__name__)

RAW_TABLE = "raw_comms_sendivo_outbound"
PG_VIEW = "pg.comms.v_sendivo_outbound_message"

# Re-pull a small overlap before the high-water mark to absorb late-arriving /
# out-of-order webhook rows. The append-only raw layer + de-dup view make the
# overlap idempotent for queries.
OVERLAP = "interval '6 hours'"


def _run(ctx: "RunContext") -> PhaseResult:
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn = ctx.db

    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        high_water = conn.execute(
            f"SELECT max(sent_at) FROM {RAW_TABLE}"
        ).fetchone()[0]

        if high_water is None:
            where = ""
            logger.info("%s empty -> full history pull", RAW_TABLE)
        else:
            # Interpolating a literal timestamp from our OWN warehouse (not user
            # input); postgres_scanner pushes the predicate down to PG.
            where = (
                f"WHERE sent_at > (TIMESTAMP '{high_water}' - {OVERLAP})"
            )
            logger.info("%s incremental: sent_at > %s - overlap", RAW_TABLE, high_water)

        conn.execute(
            f"""
            INSERT INTO {RAW_TABLE} (phone10, message, sent_at, status_group, _loaded_at, _run_id)
            SELECT phone10, message, sent_at, status_group, now(), ?
            FROM {PG_VIEW}
            {where}
            """,
            [ctx.run_id],
        )
        n = conn.execute(
            f"SELECT count(*) FROM {RAW_TABLE} WHERE _run_id = ?", [ctx.run_id]
        ).fetchone()[0]
        logger.info("mirrored comms.v_sendivo_outbound_message -> %s: %d rows", RAW_TABLE, n)

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
    registry.add_phase("comms_mirror", "sendivo_outbound_mirror", _run)
