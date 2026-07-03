"""Sendivo per-message blast mirror (booking->blast attribution, 2026-06-29).

Mirrors the BLAST-CARRYING outbound messages from comms.sendivo_outbound_recovered
(comms Supabase: phone10 / blast_id / blast_name / sent_at) into the warehouse
table raw_sendivo_outbound_message. This is the (phone10 x blast_id) grain that
core.v_sms_booking_attribution needs to derive the originating (first-reply)
blast for each SMS booking -- replacing Sendivo's unreliable last-reply
deals_won and removing the manual vendor-export dependency.

Design:
  * COMPLETENESS anti-join (NOT high-water): each run mirrors every blast-carrying
    recovered row whose sendivo_log_id is not already in the warehouse. First run
    backfills the FULL history; later runs pull only genuinely-new ids.
  * Append-only raw layer (_run_id tagged); v_sendivo_outbound_blast de-dups.
  * Single-writer-safe: runs in the existing "comms_mirror" phase (serialized
    writer window), so it adds ZERO contention to the real-time path.
  * Uses the same postgres ATTACH the v1 comms mirror uses.

WHY anti-join and not sent_at high-water (flag warehouse-flags #16, 2026-07-02):
the source table was renamed comms.sendivo_outbound_log -> sendivo_outbound_recovered
and Larry BACKFILLED blast_id onto historical rows (blast_id now spans 2026-05-18..).
A sent_at high-water can only ever move FORWARD, so it (a) silently missed the whole
back-catalogue that gained a blast_id after the high-water had already advanced, and
(b) stalled the mirror at 2026-06-30 when the 07-01 recovered rows landed after that
night's run. The anti-join on sendivo_log_id is order-independent: it picks up any
newly-recovered or newly-blast-tagged row regardless of its sent_at, so the mirror
converges to complete and can never silently freeze. recovered is a bounded (~1.2M-row)
table, so the nightly full projection is cheap in the comms_mirror window.

Only rows with a non-null blast_id are mirrored (the attribution-relevant subset;
no-blast rows carry no attribution signal and v_sendivo_outbound_blast filters them).
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


def _run(ctx: "RunContext") -> PhaseResult:
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn = ctx.db

    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        pre = conn.execute(f"SELECT count(*) FROM {RAW_TABLE}").fetchone()[0]
        logger.info("%s completeness sync: %d rows already mirrored", RAW_TABLE, pre)

        # Completeness anti-join: mirror every blast-carrying recovered row not yet in the
        # warehouse (by sendivo_log_id). Order-independent, so a newly-recovered OR newly-
        # blast-tagged historical row is always picked up and the mirror can never freeze.
        # NOT EXISTS is null-safe (unlike NOT IN); the append-only raw + de-dup view keep it idempotent.
        conn.execute(
            f"""
            INSERT INTO {RAW_TABLE}
              (sendivo_log_id, phone10, blast_id, blast_name, campaign_name,
               sub_account_name, sent_at, _loaded_at, _run_id)
            SELECT r.sendivo_log_id, r.phone10, r.blast_id, r.blast_name, r.campaign_name,
                   r.sub_account_name, r.sent_at, now(), ?
            FROM {PG_SRC} r
            WHERE r.blast_id IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM {RAW_TABLE} w WHERE w.sendivo_log_id = r.sendivo_log_id
              )
            """,
            [ctx.run_id],
        )
        n = conn.execute(
            f"SELECT count(*) FROM {RAW_TABLE} WHERE _run_id = ?", [ctx.run_id]
        ).fetchone()[0]
        logger.info("mirrored %s blast messages -> %s: %d new rows", PG_SRC, RAW_TABLE, n)

        conn.execute("DETACH pg")
    except Exception:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass
        raise

    return PhaseResult(rows_in=n, rows_out=n, notes={"backfill": pre == 0, "new_rows": n})


def register(registry) -> None:
    # Same phase as the v1 comms mirror so it runs in the serialized writer window.
    registry.add_phase("comms_mirror", "sendivo_outbound_blast_mirror", _run)
