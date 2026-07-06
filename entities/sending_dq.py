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
    # NOTE: even when no NEW csv is found we must still fall through to D6 (reassert
    # capacity from core.sending_account) + the canonical rebuild, so the table self-
    # heals against the upstream generator's "-999 / Missing Current Inventory" zeroing
    # on every run, not only when a new day's CSV lands.
    csvs = _find_actuals_csvs()
    if not csvs:
        logger.warning("No daily actuals CSVs found in %s", ACTUALS_DIR)

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
        logger.info("D3: All %d daily actuals CSVs already loaded (reassert + rebuild still run)", len(csvs))

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

    # --- D6: reassert capacity for generator-misclassified rows ---------------
    # The upstream account-truth generator stamps any sender it cannot match to its
    # (stale/incomplete) `account_inventory` snapshot as account_status=-999 /
    # infra_type='Missing Current Inventory' / daily_limit=0 / expected_sends=0.
    # That silently zeros real, active sending accounts (e.g. F2 Google reseller:
    # ~3,307 active accts reported as 0 cold capacity while physically sending ~45k/day).
    # core.sending_account already carries the correct ESP + daily_limit for these.
    # Reassert it here so the fact never reports 0 capacity for an account core knows
    # is active with a real ESP and limit. Runs over the WHOLE table every nightly
    # (heals history + new), idempotent (only touches -999 rows). Conservative: leaves
    # retired / esp-unresolved / warming (daily_limit<=0) rows untouched -> no overcount.
    # Runs BEFORE the canonical rebuild so the rebuild's infra_type->esp mapping picks up
    # the healed Google/Outlook/OTD rows (the downstream vendor esp-backfill still applies).
    reasserted = _reassert_capacity_from_core(db)

    # Rebuild canonical daily table from raw (dedup: one row per date+email, prefer highest actual_sends).
    # [2026-07-06] Two-part fix for a recurring nightly failure that left core.sending_account_daily EMPTY
    # (row_floors=0 / "no fresh data" / QA-stale alerts). The table has a composite PK (date, account_id),
    # and DuckDB's in-memory ART index for that PK CANNOT spill to temp; the old single 55M-row DELETE-all +
    # INSERT-all pinned ~7.4GiB building that index in one commit and OOM'd at the 8GB memory_limit. Worse,
    # an OOM-killed commit left the PK index ORPHANED from the (deleted) rows — a persistent corruption that
    # survives DELETE/TRUNCATE/CHECKPOINT, so every subsequent rebuild then failed with a phantom
    # "duplicate key" on an empty table. Fix:
    #   (1) Recreate the table fresh from its OWN current DDL (CREATE OR REPLACE) — this drops the corrupt PK
    #       index and rebuilds it clean every run (self-healing), preserves the PK/NOT-NULL contract, and
    #       reads the live schema so a future migration is picked up automatically. Dependent views survive
    #       CREATE OR REPLACE and re-bind to the same name.
    #   (2) Populate PER DATE, each day in its own transaction — bounds each commit's PK-index delta to ~1
    #       day (~1.4M rows) so it fits far under the 8GB limit. Same dedup result as the global rebuild
    #       (the window PARTITIONs by (date, email), so restricting the source to one date is equivalent).
    db.execute("SET preserve_insertion_order=false")
    _ddl = db.execute(
        "SELECT sql FROM duckdb_tables() "
        "WHERE schema_name='core' AND table_name='sending_account_daily'"
    ).fetchone()[0]
    assert _ddl.strip().upper().startswith("CREATE TABLE"), f"unexpected DDL: {_ddl[:40]!r}"
    db.execute(_ddl.replace("CREATE TABLE", "CREATE OR REPLACE TABLE", 1))
    raw_dates = [r[0] for r in db.execute(
        "SELECT DISTINCT date FROM raw_account_truth_daily_actuals ORDER BY date"
    ).fetchall()]
    for _d in raw_dates:
        db.execute("BEGIN")
        try:
            db.execute("""
                INSERT INTO core.sending_account_daily
                WITH deduped AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY date, email
                            ORDER BY actual_sends DESC NULLS LAST
                        ) AS rn
                    FROM raw_account_truth_daily_actuals
                    WHERE date = ?
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
            """, [_d])
            db.execute("COMMIT")
        except Exception:
            db.execute("ROLLBACK")
            raise

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

    # [2026-06-27 esp-census-residual] ESP BACKFILL pass 2 (from the census). The vendor backfill
    # (D3 above) recovers ~91% of esp-NULL sending rows, but a ~5-8k/day residual stays NULL: accounts
    # that sent (infra_type='Missing Current Inventory' / account_status=-999) AND are absent from
    # core.sending_account_vendor (DDL 72) -> no vendor projection. Measured 2026-06-27: 603 such
    # accounts / 8,139 sends on 06-25, 100% present in core.account_label (DDL 95, the phantom-free
    # MX-infra census) as Active OTD. account_label IS the canonical infra resolver the Sending-Truth
    # lens (scripts/sending_truth_dashboard_data.py) and the report's Section-4 join already trust, so
    # project its infra to esp for whatever the vendor pass left NULL. This closes the esp-NULL hole to
    # ~0 (100%-or-flag) so esp-grouped reads (e.g. derived.v_sending_volume_daily) match the
    # account_label-join truth — killing the exact class of "by-infra split disagrees with Grace" bug.
    # Latest census per email (MX-infra is stable per account). Idempotent; no-op once esp is set.
    # Live-now twin: sql/ddl/1029_esp_backfill_from_census.sql (byte-identical UPDATE body).
    # CASE arms stay in lockstep with the inner `WHERE infra IN (...)`; the ELSE sad.esp is
    # defence-in-depth (a no-op, never a NULL overwrite, if the two lists ever drift).
    try:
        db.execute("""
            UPDATE core.sending_account_daily AS sad
            SET esp = CASE lab.infra
                          WHEN 'OTD'     THEN 'otd'
                          WHEN 'Google'  THEN 'google'
                          WHEN 'Outlook' THEN 'outlook'
                          ELSE sad.esp
                      END
            FROM (
                SELECT lower(email) AS email,
                       arg_max(infra, census_date) AS infra
                FROM core.account_label
                WHERE infra IN ('OTD', 'Google', 'Outlook')
                GROUP BY lower(email)
            ) AS lab
            WHERE sad.esp IS NULL
              AND lab.email = lower(sad.account_id)
        """)
        residual = db.execute(
            "SELECT count(*) FROM core.sending_account_daily WHERE esp IS NULL AND actual_sends > 0"
        ).fetchone()[0]
        logger.info("D3b: esp backfilled from account_label census; %d esp-NULL sending rows remain", residual)
    except Exception as exc:  # core.account_label may not exist on a fresh DB -> skip, non-fatal
        logger.warning("D3b: esp census backfill skipped (%s)", exc)

    canonical_rows = db.execute("SELECT count(*) FROM core.sending_account_daily").fetchone()[0]
    logger.info("D3: core.sending_account_daily rebuilt with %d rows", canonical_rows)

    return PhaseResult(
        rows_in=total_rows,
        rows_out=canonical_rows,
        notes={
            "d4_snapshot_date": str(snapshot_date),
            "csvs_loaded": len(new_csvs),
            "csvs_total": len(csvs),
            "rows_reasserted_from_core": reasserted,
        },
    )


def _reassert_capacity_from_core(db) -> int:
    """D6: heal generator '-999 / Missing Current Inventory' rows from core.sending_account.

    For every raw_account_truth_daily_actuals row the upstream generator zeroed
    (account_status = -999), if core.sending_account knows that email (same workspace)
    as an ACTIVE account with a resolved ESP and a real daily_limit (>0), overwrite the
    capacity columns with core's truth. Idempotent: re-running only re-touches rows that
    are still -999 (a healed row is account_status=1 and no longer matches). Returns the
    number of rows reasserted.

    provider_code mapping mirrors the generator's PROVIDER_LABELS: 1=OTD, 2=Google, 3=Outlook.
    expected_sends := daily_limit (full configured cold capacity for an active account).

    The mappings + warning_flags handling here are kept byte-identical to
    sql/ddl/1003_reassert_account_truth_capacity.sql (the live-apply twin). The source is
    deduped to one row per (workspace_slug, lower(email)) so the UPDATE...FROM pick is
    deterministic (verified 0 dupes 2026-06-23; the dedup makes it provably stable).
    """
    before = db.execute(
        "SELECT count(*) FROM raw_account_truth_daily_actuals WHERE account_status = -999"
    ).fetchone()[0]
    db.execute("""
        UPDATE raw_account_truth_daily_actuals AS f
        SET infra_type = CASE sa.esp
                WHEN 'google' THEN 'Google'
                WHEN 'outlook' THEN 'Outlook'
                WHEN 'otd' THEN 'OTD'
                ELSE f.infra_type END,
            provider_code = CASE sa.esp
                WHEN 'otd' THEN 1
                WHEN 'google' THEN 2
                WHEN 'outlook' THEN 3
                ELSE f.provider_code END,
            account_status = 1,
            account_status_label = 'Active (reasserted from core.sending_account)',
            daily_limit = sa.daily_limit,
            expected_sends = sa.daily_limit,
            delta = sa.daily_limit - COALESCE(f.actual_sends, 0),
            fulfillment = CASE WHEN sa.daily_limit > 0
                THEN COALESCE(f.actual_sends, 0)::DOUBLE / sa.daily_limit END,
            warning_flags = CASE
                WHEN COALESCE(f.warning_flags, '') LIKE '%reasserted_from_core%' THEN f.warning_flags
                ELSE NULLIF(TRIM(BOTH ';' FROM
                    COALESCE(f.warning_flags, '') || ';reasserted_from_core'), '') END
        FROM (
            SELECT workspace_slug,
                   LOWER(email)               AS email_lc,
                   arg_max(esp, daily_limit)  AS esp,
                   max(daily_limit)           AS daily_limit
            FROM core.sending_account
            WHERE is_active AND esp IS NOT NULL AND daily_limit > 0
            GROUP BY workspace_slug, LOWER(email)
        ) sa
        WHERE f.account_status = -999
          AND LOWER(f.email) = sa.email_lc
          AND f.workspace_slug = sa.workspace_slug
    """)
    after = db.execute(
        "SELECT count(*) FROM raw_account_truth_daily_actuals WHERE account_status = -999"
    ).fetchone()[0]
    reasserted = before - after
    logger.info(
        "D6: reasserted capacity from core.sending_account for %d rows (-999 remaining: %d)",
        reasserted, after,
    )
    return reasserted


def register(registry: Registry) -> None:
    registry.add_phase("derived", "sending_dq", run_sending_dq)
