"""core.{provider,deliverability,batch}_history — daily AGGREGATE snapshots for the Data Hub over-time graphs.

Once per nightly run, appends ONE aggregate row per provider / per workspace / per batch from
core.v_inbox_overview (~150 rows/night). Registered in the 'canonical' phase (v_inbox_overview is a view
over already-populated core tables, so it reflects the current fleet whatever the intra-phase order).

Idempotent per snapshot_date: for each table it DELETEs today's rows then re-INSERTs, so a same-day re-run
replaces cleanly (plain append -> no PK / ART-abort risk). FAIL-SOFT by design: a missing source/table or any
error is logged and skipped per-table and the function NEVER raises — one bad table can't abort the nightly.

Forward-only: provider(supplier), deliverability (SPF/DKIM/DMARC/blacklist), and batch are not date-partitioned
anywhere else in the warehouse (the census keeps status/ESP only; the DNS sweep keeps latest, not per-day), so
this is the origin of that history. Schema: sql/ddl/1087_rollup_history.sql. Built 2026-07-07.
"""
from __future__ import annotations
import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.rollup_history")

# (table, INSERT ... BY NAME SELECT) — every SELECT ends with `now() AS _loaded_at, ? AS _run_id`
# (the ? is bound to ctx.run_id). Grouping/definitions mirror the Hub's live rollups exactly so the
# history lines up with the on-demand tables (Google Panel split via tags; provider junk excluded;
# batch_key '' / 'Unmapped' excluded).
_CAPTURES = [
    ("provider_history", """
        INSERT INTO core.provider_history BY NAME (
          SELECT current_date AS snapshot_date,
                 CASE WHEN tags ILIKE '%Google Panel%' THEN 'Google Panel' ELSE provider END AS provider,
                 count(*)                                            AS n_total,
                 count(*) FILTER (WHERE stage='Live')                AS n_live,
                 count(*) FILTER (WHERE stage='Warming')             AS n_warming,
                 count(*) FILTER (WHERE stage='Disconnected')        AS n_disc,
                 count(*) FILTER (WHERE stage='Banned')              AS n_banned,
                 count(DISTINCT domain)                              AS n_domains,
                 coalesce(sum(daily_limit) FILTER (WHERE stage='Live'), 0) AS live_capacity,
                 now() AS _loaded_at, ? AS _run_id
          FROM core.v_inbox_overview
          WHERE provider IS NOT NULL AND provider NOT IN ('', 'Unmapped', '(pending)')
          GROUP BY 1, 2)"""),
    ("deliverability_history", """
        INSERT INTO core.deliverability_history BY NAME (
          SELECT current_date AS snapshot_date, workspace_slug,
                 count(*)                                                                        AS n_total,
                 round(100.0 * count(*) FILTER (WHERE has_spf)   / nullif(count(*), 0))          AS spf_pct,
                 round(100.0 * count(*) FILTER (WHERE has_dkim)  / nullif(count(*), 0))          AS dkim_pct,
                 round(100.0 * count(*) FILTER (WHERE has_dmarc) / nullif(count(*), 0))          AS dmarc_pct,
                 round(100.0 * count(*) FILTER (WHERE has_mx)    / nullif(count(*), 0))          AS mx_pct,
                 count(*) FILTER (WHERE blacklisted)                                             AS blacklisted,
                 now() AS _loaded_at, ? AS _run_id
          FROM core.v_inbox_overview
          GROUP BY 1, 2)"""),
    ("batch_history", """
        INSERT INTO core.batch_history BY NAME (
          SELECT current_date AS snapshot_date, batch_key,
                 count(*)                                            AS n_total,
                 count(*) FILTER (WHERE stage='Live')                AS n_live,
                 count(*) FILTER (WHERE stage='Warming')             AS n_warming,
                 count(*) FILTER (WHERE stage='Disconnected')        AS n_disc,
                 round(100.0 * count(*) FILTER (WHERE go_live IS NOT NULL) / nullif(count(*), 0)) AS pct_live,
                 now() AS _loaded_at, ? AS _run_id
          FROM core.v_inbox_overview
          WHERE batch_key IS NOT NULL AND batch_key NOT IN ('', 'Unmapped')
          GROUP BY 1, 2)"""),
]


def _table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0] > 0


def run_rollup_history(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn, "core", "v_inbox_overview"):
        logger.error("rollup_history SKIP: core.v_inbox_overview missing.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_source"})

    total = 0
    captured = {}
    for tbl, sql in _CAPTURES:
        if not _table_exists(conn, "core", tbl):
            logger.error("rollup_history SKIP core.%s: table missing (ddl 1087 not applied yet).", tbl)
            continue
        try:
            conn.execute("BEGIN")
            conn.execute(f"DELETE FROM core.{tbl} WHERE snapshot_date = current_date")
            conn.execute(sql, [ctx.run_id])
            conn.execute("COMMIT")
            n = conn.execute(
                f"SELECT count(*) FROM core.{tbl} WHERE snapshot_date = current_date"
            ).fetchone()[0]
            captured[tbl] = n
            total += n
            logger.info("core.%s +%d aggregate rows for today", tbl, n)
        except Exception as exc:  # fail-soft: never abort the nightly for a history append
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            logger.warning("rollup_history core.%s FAILED (fail-soft, nightly continues): %s", tbl, exc)

    return PhaseResult(rows_in=total, rows_out=total, notes={"captured": captured, "total_rows": total})


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "rollup_history", run_rollup_history)
