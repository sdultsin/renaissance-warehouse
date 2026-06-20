"""SPEC D: Sending Data-Quality monitors.

Registers under the 'derived' phase (runs after canonical). Handles:
  D3 — ingest account_truth daily actuals CSV into raw + canonical.
  D4 — set _snapshot_date on core.sending_account from raw_account_truth_accounts.
  D2 — tag coverage gaps (deferred until SPEC A lands core.sending_account_tag).

D1 + D5 are pure views (created by DDL 31_sending_dq_monitors.sql) — no ingest code needed.
"""
from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.sending_dq")

# Where account_truth daily actuals CSVs live.
ACTUALS_DIR = Path(
    os.environ.get(
        "ACCOUNT_TRUTH_ACTUALS_DIR",
        "/root/Renaissance/deliverables/2026-05-27-instantly-account-truth/outputs",
    )
)
ACTUALS_GLOB = "account_truth_daily_*.csv"


def _find_actuals_csvs() -> list[Path]:
    """Return all daily actuals CSVs sorted by date (filename)."""
    return sorted(ACTUALS_DIR.glob(ACTUALS_GLOB))


def run_sending_dq(ctx: RunContext) -> PhaseResult:
    """D3: ingest daily actuals + D4: set freshness signal."""
    db = ctx.db
    total_rows = 0

    # --- D4: Add _snapshot_date column if missing, then populate ---
    try:
        db.execute(
            "ALTER TABLE core.sending_account ADD COLUMN _snapshot_date DATE"
        )
        logger.info("Added _snapshot_date column to core.sending_account")
    except Exception:
        pass  # Column already exists

    # Set _snapshot_date from the latest raw snapshot's _loaded_at
    db.execute("""
        UPDATE core.sending_account
        SET _snapshot_date = (
            SELECT CAST(_loaded_at AS DATE)
            FROM raw_account_truth_accounts
            ORDER BY _loaded_at DESC
            LIMIT 1
        )
        WHERE _snapshot_date IS NULL
           OR _snapshot_date < (
               SELECT CAST(_loaded_at AS DATE)
               FROM raw_account_truth_accounts
               ORDER BY _loaded_at DESC
               LIMIT 1
           )
    """)
    snapshot_date = db.execute(
        "SELECT MAX(_snapshot_date) FROM core.sending_account"
    ).fetchone()[0]
    logger.info("D4: core.sending_account._snapshot_date = %s", snapshot_date)

    # --- D3: Ingest daily actuals CSVs ---
    csvs = _find_actuals_csvs()
    if not csvs:
        logger.warning("No daily actuals CSVs found in %s", ACTUALS_DIR)
        return PhaseResult(rows_in=0, rows_out=0, notes={"d4_snapshot_date": str(snapshot_date)})

    # Find which dates are already loaded
    existing_dates = set()
    try:
        rows = db.execute(
            "SELECT DISTINCT date FROM raw_account_truth_daily_actuals"
        ).fetchall()
        existing_dates = {r[0] for r in rows}
    except Exception:
        pass  # Table might not exist yet (first run before DDL)

    new_csvs = []
    for csv_path in csvs:
        # Extract date from filename: account_truth_daily_YYYY-MM-DD.csv
        stem = csv_path.stem  # account_truth_daily_2026-05-30
        date_str = stem.replace("account_truth_daily_", "")
        try:
            import datetime
            d = datetime.date.fromisoformat(date_str)
            if d not in existing_dates:
                new_csvs.append((csv_path, date_str))
        except ValueError:
            continue

    if not new_csvs:
        logger.info("D3: All %d daily actuals CSVs already loaded", len(csvs))
        return PhaseResult(
            rows_in=0, rows_out=0,
            notes={"d4_snapshot_date": str(snapshot_date), "csvs_total": len(csvs)},
        )

    for csv_path, date_str in new_csvs:
        logger.info("D3: Loading %s", csv_path.name)
        # Use DuckDB's native CSV reader for speed
        db.execute(f"""
            INSERT INTO raw_account_truth_daily_actuals
            SELECT
                CAST(date AS DATE) AS date,
                workspace_slug,
                workspace_name,
                email,
                domain,
                infra_type,
                TRY_CAST(provider_code AS INTEGER) AS provider_code,
                TRY_CAST(account_status AS INTEGER) AS account_status,
                account_status_label,
                TRY_CAST(daily_limit AS INTEGER) AS daily_limit,
                TRY_CAST(expected_sends AS INTEGER) AS expected_sends,
                TRY_CAST(actual_sends AS INTEGER) AS actual_sends,
                TRY_CAST(delta AS INTEGER) AS delta,
                TRY_CAST(fulfillment AS DOUBLE) AS fulfillment,
                TRY_CAST(active_campaign_count AS INTEGER) AS active_campaign_count,
                canonical_tag,
                undersend_reason,
                warning_flags,
                now() AS _loaded_at,
                '{ctx.run_id}' AS _run_id
            FROM read_csv('{csv_path}',
                header=true,
                auto_detect=true,
                ignore_errors=true,
                columns={{
                    'date': 'VARCHAR',
                    'workspace_slug': 'VARCHAR',
                    'workspace_name': 'VARCHAR',
                    'email': 'VARCHAR',
                    'domain': 'VARCHAR',
                    'infra_type': 'VARCHAR',
                    'provider_code': 'VARCHAR',
                    'account_status': 'VARCHAR',
                    'account_status_label': 'VARCHAR',
                    'setup_pending': 'VARCHAR',
                    'warmup_status': 'VARCHAR',
                    'daily_limit': 'VARCHAR',
                    'expected_sends': 'VARCHAR',
                    'actual_sends': 'VARCHAR',
                    'delta': 'VARCHAR',
                    'fulfillment': 'VARCHAR',
                    'all_tags': 'VARCHAR',
                    'canonical_tag_count': 'VARCHAR',
                    'canonical_tag': 'VARCHAR',
                    'canonical_tags_matched': 'VARCHAR',
                    'active_campaign_count': 'VARCHAR',
                    'campaign_count': 'VARCHAR',
                    'active_campaign_names': 'VARCHAR',
                    'all_campaign_names': 'VARCHAR',
                    'undersend_reason': 'VARCHAR',
                    'warning_flags': 'VARCHAR',
                    'created_at': 'VARCHAR',
                    'updated_at': 'VARCHAR'
                }})
        """)
        n = db.execute(
            f"SELECT count(*) FROM raw_account_truth_daily_actuals WHERE date = '{date_str}'"
        ).fetchone()[0]
        total_rows += n
        logger.info("  -> %d rows for %s", n, date_str)

    # Rebuild canonical daily table from raw (dedup: one row per date+email, prefer highest actual_sends)
    db.execute("DELETE FROM core.sending_account_daily")
    db.execute("""
        INSERT INTO core.sending_account_daily
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY date, email
                    ORDER BY actual_sends DESC NULLS LAST
                ) AS rn
            FROM raw_account_truth_daily_actuals
        )
        SELECT
            date,
            email AS account_id,
            workspace_slug,
            CASE infra_type
                WHEN 'Google' THEN 'google'
                WHEN 'Outlook' THEN 'outlook'
                WHEN 'OTD' THEN 'otd'
                ELSE NULL
            END AS esp,
            daily_limit,
            expected_sends,
            actual_sends,
            delta,
            fulfillment,
            active_campaign_count
        FROM deduped
        WHERE rn = 1
    """)

    # [2026-06-16 infra-data-truth] ESP BACKFILL. The CSV's infra_type is 'Missing Current Inventory'
    # for accounts that sent but aren't in the (cached) account_truth inventory -> esp=NULL above
    # (~85k rows/day, ~43k of them sending), which broke the by-infra sending split. core.sending_account_vendor
    # (DDL 72, 100% mapped) is the warehouse vendor truth; project it to esp. Verified mapping (1:1 where
    # both known): MailIn->outlook, Outreach Today->otd, Reseller->google, Cheap Inboxes->outlook (all
    # provider_code=3). Recovers ~91% of esp-null rows; the residual (~5-8k/day) are accounts absent from
    # the vendor table (brand-new / deleted-workspace) and stay NULL. Idempotent; no-op once esp is set.
    try:
        db.execute("""
            UPDATE core.sending_account_daily AS sad
            SET esp = CASE v.vendor_category
                          WHEN 'MailIn'         THEN 'outlook'
                          WHEN 'Outreach Today' THEN 'otd'
                          WHEN 'Reseller'       THEN 'google'
                          WHEN 'Cheap Inboxes'  THEN 'outlook'
                      END
            FROM core.sending_account_vendor AS v
            WHERE sad.esp IS NULL
              AND lower(v.account_email) = lower(sad.account_id)
              AND v.vendor_category IN ('MailIn', 'Outreach Today', 'Reseller', 'Cheap Inboxes')
        """)
        backfilled = db.execute(
            "SELECT count(*) FROM core.sending_account_daily WHERE esp IS NOT NULL"
        ).fetchone()[0]
        logger.info("D3: esp backfilled from sending_account_vendor; %d rows now have esp", backfilled)
    except Exception as exc:  # core.sending_account_vendor may not exist on a fresh DB -> skip, non-fatal
        logger.warning("D3: esp backfill skipped (%s)", exc)

    canonical_rows = db.execute("SELECT count(*) FROM core.sending_account_daily").fetchone()[0]
    logger.info("D3: core.sending_account_daily rebuilt with %d rows", canonical_rows)

    return PhaseResult(
        rows_in=total_rows,
        rows_out=canonical_rows,
        notes={
            "d4_snapshot_date": str(snapshot_date),
            "csvs_loaded": len(new_csvs),
            "csvs_total": len(csvs),
        },
    )


def register(registry: Registry) -> None:
    registry.add_phase("derived", "sending_dq", run_sending_dq)
