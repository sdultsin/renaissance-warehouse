"""
WS4 — core.account_label entity (in-warehouse nightly rebuild).

Schema: sql/ddl/106_ws4_account_label.sql.
Registers under the existing 'canonical' phase (runs 05:30, AFTER 'account_census' promote at 04:02,
so core.account_census for today's census_date is already populated). No PHASE_ORDER edit needed.

Idempotent: rebuilds the LATEST census_date partition of core.account_label each nightly run from tables
that already exist. DELETE-by-partition + INSERT (same pattern as entities/account_census.py).

LIFECYCLE (D1, BINARY): Active = EVER sent cold (a core.sending_account_daily row with actual_sends>0,
looked back as far as the cold history allows) | Warmup. lifecycle_confidence ∈ {confident, uncertain}.
This is the SQL equivalent of gen_account_label_and_uncertain_v2.py (validated 220,427/0/92,559 offline;
re-simulated live this session = 220,328/0/94,559 over the 2026-06-21 census of 314,887 — drift is the
documented B' census-advance behavior, NOT a bug).

SOFT DEP — core.account_mx_resolution: VERIFIED ABSENT live this session AND absent from the droplet repo.
The provider_code=1 MX waterfall is therefore conditionally joined ONLY when the table exists; absent, pc=1
falls back to OTD (the verified 174,594/174,594 outcome). _table_exists() gates the join so the entity never
errors on a missing soft dependency.
"""
from __future__ import annotations
import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger(__name__)


def _table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()[0] > 0


def run_account_label(ctx: RunContext) -> PhaseResult:
    conn = ctx.db

    # Guard: census must exist + be populated (it is registered earlier, but graceful-skip if a partial
    # run somehow reaches canonical before promote landed census).
    if not _table_exists(conn, "core", "account_census"):
        logger.error("account_label SKIP: core.account_census missing (promote phase not run).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no_account_census"})

    census_date = conn.execute(
        "SELECT max(census_date) FROM core.account_census"
    ).fetchone()[0]
    if census_date is None:
        logger.error("account_label SKIP: core.account_census has no rows.")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "empty_census"})

    has_mx = _table_exists(conn, "core", "account_mx_resolution")
    # Soft MX waterfall for provider_code=1; absent -> OTD fallback (verified outcome).
    mx_join = (
        "LEFT JOIN core.account_mx_resolution mx "
        "ON mx.domain = split_part(cen.email,'@',2) AND cen.provider_code = 1"
        if has_mx else ""
    )
    mx_infra = "mx.infra" if has_mx else "CAST(NULL AS VARCHAR)"

    # COLD-ever producer (independent of the census poll). lower(account_id)=account_id verified, but keep
    # lower() explicit for safety.
    select_sql = f"""
    WITH cen AS (
        SELECT census_date,
               lower(email)            AS email,
               workspace_slug,
               provider_code,
               status,
               warmup_status,
               warmup_status_label,
               CAST(stat_warmup_score AS DOUBLE) AS warmup_score,
               daily_limit,
               timestamp_created,
               timestamp_warmup_start
        FROM core.account_census
        WHERE census_date = DATE '{census_date}'
    ),
    cold AS (
        SELECT lower(account_id)                               AS email,
               MIN(date)                                       AS cold_start,
               MAX(date)                                       AS last_cold_send_date,
               SUM(actual_sends)                               AS total_cold_sends_ever,
               COUNT(*) FILTER (WHERE actual_sends > 0)        AS cold_send_days
        FROM core.sending_account_daily
        WHERE actual_sends > 0
        GROUP BY 1
    ),
    vnd AS (
        SELECT lower(account_email) AS email, any_value(vendor_category) AS vendor_category
        FROM core.sending_account_vendor
        GROUP BY 1
    )
    SELECT
        cen.census_date,
        cen.email,
        cen.workspace_slug,
        CASE WHEN cen.provider_code = 2 THEN 'Google'
             WHEN cen.provider_code = 3 THEN 'Outlook'
             ELSE COALESCE({mx_infra}, 'OTD') END                         AS infra,
        CASE WHEN cen.provider_code = 2 THEN 'provider_code=2'
             WHEN cen.provider_code = 3 THEN 'provider_code=3'
             WHEN {mx_infra} IS NOT NULL THEN 'mx_resolution'
             ELSE 'otd_fallback' END                                      AS infra_source,
        COALESCE(vnd.vendor_category, '(pending)')                        AS vendor,
        CASE WHEN vnd.vendor_category IS NOT NULL THEN 'sending_account_vendor'
             ELSE '(pending)' END                                         AS vendor_source,
        CASE WHEN cold.email IS NOT NULL AND cen.daily_limit > 0 THEN 'Active' ELSE 'Warmup' END  AS lifecycle,
        -- BINARY D1: cold-ever => confident Active; else uncertain (the strict-D1 outcome:
        -- confident-Warmup=0; every non-cold account is genuinely ambiguous within the cold window).
        CASE WHEN cold.email IS NOT NULL THEN 'confident' ELSE 'uncertain' END
                                                                          AS lifecycle_confidence,
        CASE WHEN cold.email IS NOT NULL THEN 'cold_send_history'
             WHEN cen.daily_limit > 0 AND cen.warmup_status IN (1,0) THEN 'capacity_only_no_cold'
             WHEN cen.warmup_status = -1 AND cen.daily_limit > 0 THEN 'warmup_banned_dl_pos_no_cold'
             WHEN cen.timestamp_warmup_start IS NULL THEN 'no_warmup_start_no_cold'
             ELSE 'unclassified_no_cold' END                             AS lifecycle_basis,
        cold.cold_start,
        cold.last_cold_send_date,
        COALESCE(cold.total_cold_sends_ever, 0)                          AS total_cold_sends_ever,
        COALESCE(cold.cold_send_days, 0)                                 AS cold_send_days,
        cen.provider_code,
        cen.warmup_status,
        cen.warmup_score,
        cen.daily_limit,
        cen.timestamp_created,
        cen.timestamp_warmup_start,
        CASE WHEN cold.email IS NOT NULL THEN NULL
             WHEN cen.daily_limit > 0 AND cen.warmup_status IN (1,0)
                  THEN 'capacity_assigned_no_cold_send_in_window'
                       || CASE WHEN cen.timestamp_created IS NOT NULL
                               AND CAST(cen.timestamp_created AS DATE) < DATE '2026-05-26'
                          THEN ';created_before_cold_window_blindspot' ELSE '' END
             WHEN cen.warmup_status = -1 AND cen.daily_limit > 0
                  THEN 'warmup_banned_with_send_capacity_no_cold_send'
             WHEN cen.timestamp_warmup_start IS NULL
                  THEN 'no_warmup_start_and_no_cold_send'
                       || CASE WHEN cen.timestamp_created IS NOT NULL
                               AND CAST(cen.timestamp_created AS DATE) < DATE '2026-05-26'
                          THEN ';created_before_cold_window_blindspot' ELSE '' END
             ELSE 'no_cold_send_and_ambiguous_warmup_signal' END         AS reason_uncertain,
        (cen.timestamp_created IS NOT NULL
            AND CAST(cen.timestamp_created AS DATE) < DATE '2026-05-26')  AS created_before_cold_window,
        now()                                                            AS _resolved_at
    FROM cen
    LEFT JOIN cold ON cold.email = cen.email
    LEFT JOIN vnd  ON vnd.email  = cen.email
    {mx_join}
    """

    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM core.account_label WHERE census_date = ?", [census_date])
        conn.execute(f"INSERT INTO core.account_label BY NAME ({select_sql})")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = conn.execute(
        "SELECT count(*) FROM core.account_label WHERE census_date = ?", [census_date]
    ).fetchone()[0]
    logger.info("core.account_label <- %d rows for census_date %s (mx_resolution=%s)",
                n, census_date, "present" if has_mx else "absent->OTD_fallback")
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"census_date": str(census_date), "mx_resolution_present": has_mx})


def register(registry: Registry) -> None:
    # Ride the existing 'canonical' phase (runs after 'account_census' promote). discover_and_register()
    # auto-imports entities/*.py exposing register(registry); no nightly.sh / PHASE_ORDER edit needed.
    registry.add_phase("canonical", "account_label", run_account_label)
