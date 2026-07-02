"""account_census: promote the live /accounts poll parquet -> core.account_census.

Immutable, per-DATE census of LIVE Instantly accounts. One row per
(census_date, workspace_uuid, lower(email)). Source = the hourly poller parquet at
/root/core/live_accounts/accounts_live_<ts>.parquet (poll_live_accounts.py, cron 7 * * * *).
REPLACES the account_truth-derived inflated core.sending_account inventory.
"""
from __future__ import annotations
import logging, os
from datetime import datetime, timezone
from pathlib import Path
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.account_census")
CENSUS_DIR = Path(os.environ.get("LIVE_ACCOUNTS_DIR", "/root/core/live_accounts"))

def _pick_parquet_for_today() -> Path:
    """Latest accounts_live_<ts>.parquet whose ts date == today UTC (one canonical/date)."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    cands = sorted(CENSUS_DIR.glob(f"accounts_live_{today}T*.parquet"))
    if not cands:
        allp = sorted(CENSUS_DIR.glob("accounts_live_*.parquet"))
        if not allp:
            raise FileNotFoundError(f"No accounts_live_*.parquet in {CENSUS_DIR}")
        return allp[-1]  # fall back to newest; census_date reflects ITS snapshot_at (never invents a date)
    return cands[-1]

def run_account_census(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    snap = _pick_parquet_for_today()
    logger.info("account_census source: %s", snap)

    # Probe columns so a pre-change parquet (no workspace_uuid/warmup_limit) still ingests.
    cols = {r[0] for r in conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{snap}')").fetchall()}
    ws_uuid = "workspace_uuid" if "workspace_uuid" in cols else (
        "organization" if "organization" in cols else "CAST(NULL AS VARCHAR)")
    wlimit  = "warmup_limit" if "warmup_limit" in cols else "CAST(NULL AS INTEGER)"

    target_date = conn.execute(
        f"SELECT CAST(snapshot_at AS DATE) FROM read_parquet('{snap}') LIMIT 1").fetchone()[0]

    # FAIL-LOUD guard: if the chosen parquet has no workspace_uuid (pre-change poller still running),
    # do NOT silently write a UUID-less census (which would pass the de-inflation gate while being empty
    # / unnamed). Skip with a clear error so freshness alarms.
    if ws_uuid.startswith("CAST(NULL"):
        logger.error("account_census SKIP: parquet %s lacks workspace_uuid/organization "
                     "(poller 3a not deployed yet). No rows written for %s.", snap.name, target_date)
        return PhaseResult(rows_in=0, rows_out=0,
                           notes={"skipped": "no_workspace_uuid", "snapshot": snap.name})

    select_sql = f"""
        SELECT
            CAST(snapshot_at AS DATE)                       AS census_date,
            {ws_uuid}                                       AS workspace_uuid,
            lower(email)                                    AS email,
            split_part(lower(email), '@', 2)                AS domain,
            workspace_slug,
            CAST(provider_code AS INTEGER)                  AS provider_code,
            CAST(daily_limit AS DOUBLE)                     AS daily_limit,
            CAST(warmup_status AS INTEGER)                  AS warmup_status,
            CASE CAST(warmup_status AS INTEGER) WHEN 1 THEN 'active'
                 WHEN 0 THEN 'paused' WHEN -1 THEN 'banned' END AS warmup_status_label,
            CAST({wlimit} AS INTEGER)                       AS warmup_limit,
            CAST(stat_warmup_score AS INTEGER)              AS stat_warmup_score,
            CAST(status AS INTEGER)                         AS status,
            CASE CAST(status AS INTEGER) WHEN 1 THEN 'active' WHEN 2 THEN 'paused'
                 WHEN -1 THEN 'connection_error' WHEN -2 THEN 'soft_bounce'
                 WHEN -3 THEN 'sending_error' END           AS status_label,
            CAST(setup_pending AS BOOLEAN)                  AS setup_pending,
            CAST(timestamp_created AS TIMESTAMPTZ)          AS timestamp_created,
            CAST(timestamp_warmup_start AS TIMESTAMPTZ)     AS timestamp_warmup_start,
            CAST(timestamp_updated AS TIMESTAMPTZ)          AS timestamp_updated,
            CAST(snapshot_at AS TIMESTAMPTZ)                AS snapshot_at,
            'instantly_api'                                 AS source,
            '{snap.name}'                                   AS _snapshot_file,
            now()                                           AS _loaded_at,
            '{ctx.run_id}'                                  AS _run_id
        FROM read_parquet('{snap}')
        WHERE email IS NOT NULL AND {ws_uuid} IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY CAST(snapshot_at AS DATE), {ws_uuid}, lower(email)
            ORDER BY snapshot_at DESC) = 1
    """
    try:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM core.account_census WHERE census_date = ?", [target_date])
        conn.execute(f"INSERT INTO core.account_census BY NAME ({select_sql})")
        # LAST-GOOD CARRY-FORWARD [2026-07-01]: if the hourly poller could not fetch a workspace's
        # /accounts (Instantly 500s — e.g. Funding 1 / renaissance-4, whose /accounts 500s on a
        # specific page even at limit=10), it is ABSENT from today's parquet. Rather than let the
        # fleet silently shrink (propagating to core.inbox / v_inbox_overview / the portal), carry its
        # most-recent prior census rows forward so the census stays complete — fresh where Instantly
        # serves it, last-good where it can't. Fires ONLY for still-active workspaces that are
        # essentially absent today (<10% of their prior count), so a genuine retirement or a real
        # shrink is never resurrected.
        prior_date = conn.execute(
            "SELECT max(census_date) FROM core.account_census WHERE census_date < ?", [target_date]
        ).fetchone()[0]
        if prior_date is not None:
            gaps = conn.execute("""
                WITH cur AS (SELECT workspace_uuid, count(*) AS n FROM core.account_census
                             WHERE census_date = ? GROUP BY 1),
                     pri AS (SELECT workspace_uuid, count(*) AS n FROM core.account_census
                             WHERE census_date = ? GROUP BY 1)
                SELECT pri.workspace_uuid, pri.n
                FROM pri
                LEFT JOIN cur USING (workspace_uuid)
                JOIN core.workspace w ON w.workspace_id = pri.workspace_uuid AND w.is_active
                WHERE COALESCE(cur.n, 0) < pri.n * 0.10
            """, [target_date, prior_date]).fetchall()
            for wsid, prior_n in gaps:
                conn.execute("DELETE FROM core.account_census WHERE census_date = ? AND workspace_uuid = ?",
                             [target_date, wsid])
                conn.execute("INSERT INTO core.account_census BY NAME "
                             "SELECT * REPLACE (CAST(? AS DATE) AS census_date) FROM core.account_census "
                             "WHERE census_date = ? AND workspace_uuid = ?", [target_date, prior_date, wsid])
                logger.warning("account_census CARRY-FORWARD: active workspace %s absent from today's "
                               "poll (<10%% of prior %d) — reused last-good rows from %s",
                               wsid, prior_n, prior_date)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    n = conn.execute(
        "SELECT count(*) FROM core.account_census WHERE census_date = ?", [target_date]).fetchone()[0]
    logger.info("core.account_census <- %d rows for %s from %s", n, target_date, snap.name)
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"snapshot": snap.name, "census_date": str(target_date)})

def register(registry: Registry) -> None:
    registry.add_phase("account_census", "promote", run_account_census)
