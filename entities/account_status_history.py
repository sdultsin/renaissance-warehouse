"""
core.account_status_history — append-only lifecycle change-log (built 2026-07-06).

Schema: sql/ddl/1080_account_status_history.sql. Registers under the existing 'canonical' phase (runs after
the 'account_census' promote + account_error), so today's census + error rows already exist. Reads the
LATEST census (status/warmup) LEFT JOIN core.account_error (the disconnect reason) and appends ONE row for
each inbox whose (status_label, warmup_status_label, error_string) CHANGED vs its most-recent prior row — or
that we have never recorded. NEVER deletes.

This is the compact, complete lifecycle history (connected / disconnected / warming / paused + the disconnect
REASON) that neither the 15-day-blind census-of-status nor the reason-less poll parquets could give us — the
gap exposed by the 2026-07-06 MilkBox-IMAP investigation. Idempotent: a re-run on the same census_date with
an unchanged census inserts nothing (last == cur -> no change), so no PK / ART-abort risk.

Join hygiene (moderator gate, 2026-07-06): both sides of the census<->error join are lowercased explicitly
(census already stores lower(email), but we make the invariant explicit so a future census change can't
silently NULL every reason). The prev-state lookup breaks same-day ties by observed_at then _loaded_at so
prev_* is deterministic.
"""
from __future__ import annotations
import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.account_status_history")


def _table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0] > 0


def run_account_status_history(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn, "core", "account_status_history"):
        logger.error("account_status_history SKIP: table missing (ddl not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_table"})
    if not _table_exists(conn, "core", "account_census"):
        logger.error("account_status_history SKIP: core.account_census missing.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_census"})

    census_date = conn.execute("SELECT max(census_date) FROM core.account_census").fetchone()[0]
    if census_date is None:
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "empty_census"})

    has_err = _table_exists(conn, "core", "account_error")
    err_cte = (
        "err AS (SELECT lower(email) AS email, COALESCE(workspace_uuid, '') AS workspace_uuid, any_value(error_string) AS error_string "
        "FROM core.account_error GROUP BY lower(email), COALESCE(workspace_uuid, ''))"
        if has_err else
        "err AS (SELECT CAST(NULL AS VARCHAR) AS email, CAST(NULL AS VARCHAR) AS workspace_uuid, CAST(NULL AS VARCHAR) AS error_string WHERE FALSE)"
    )

    sql = f"""
    WITH {err_cte},
    cen AS (
        SELECT lower(email) AS email, COALESCE(workspace_uuid, '') AS workspace_uuid,
               any_value(workspace_slug)      AS workspace_slug,
               any_value(status_label)        AS status_label,
               any_value(warmup_status_label) AS warmup_status_label,
               any_value(daily_limit)         AS daily_limit,
               any_value(provider_code)       AS provider_code,
               max(census_date)               AS observed_date,
               max(snapshot_at)               AS observed_at
        FROM core.account_census
        WHERE census_date = DATE '{census_date}'
        GROUP BY lower(email), COALESCE(workspace_uuid, '')
    ),
    cur AS (
        SELECT cen.*, err.error_string
        FROM cen LEFT JOIN err USING (email, workspace_uuid)
    ),
    last AS (
        SELECT email, workspace_uuid, status_label, warmup_status_label, error_string
        FROM (SELECT email, workspace_uuid, status_label, warmup_status_label, error_string,
                     row_number() OVER (PARTITION BY email, workspace_uuid
                                        ORDER BY observed_date DESC, observed_at DESC NULLS LAST,
                                                 _loaded_at DESC) AS rn
              FROM core.account_status_history) WHERE rn = 1
    )
    SELECT cur.email, cur.workspace_uuid, cur.workspace_slug, cur.status_label, cur.warmup_status_label,
           cur.error_string, cur.daily_limit, cur.provider_code, cur.observed_date, cur.observed_at,
           last.status_label       AS prev_status_label,
           last.warmup_status_label AS prev_warmup_label,
           last.error_string       AS prev_error_string,
           (last.email IS NULL)    AS is_first_seen,
           'census'                AS source,
           now()                   AS _loaded_at,
           '{ctx.run_id}'          AS _run_id
    FROM cur LEFT JOIN last USING (email, workspace_uuid)
    WHERE last.email IS NULL
       OR cur.status_label       IS DISTINCT FROM last.status_label
       OR cur.warmup_status_label IS DISTINCT FROM last.warmup_status_label
       OR cur.error_string       IS DISTINCT FROM last.error_string
    """
    try:
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO core.account_status_history BY NAME ({sql})")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = conn.execute(
        "SELECT count(*) FROM core.account_status_history WHERE observed_date = ? AND _run_id = ?",
        [census_date, ctx.run_id],
    ).fetchone()[0]
    total = conn.execute("SELECT count(*) FROM core.account_status_history").fetchone()[0]
    logger.info("core.account_status_history +%d change rows for %s (history total=%d)", n, census_date, total)
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"census_date": str(census_date), "change_rows": n, "total_rows": total})


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "account_status_history", run_account_status_history)
