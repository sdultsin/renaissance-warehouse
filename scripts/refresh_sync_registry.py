#!/usr/bin/env python3
"""Track E — seed + refresh core.sync_registry (the freshness backbone).

Run AFTER the nightly orchestrator + all mirrors finish (so it observes the
freshest state), then run scripts/warehouse_qa.py to fail-loud on SLA breach.

What it does, idempotently, every run:
  1. Auto-discovers every `raw_*` table in the warehouse and upserts a registry
     row for it — so no feed can silently lack a row (Track E DoD anti-join = 0).
  2. Registers a curated set of core/derived decision tables that exist.
  3. Applies the cadence POLICY (expected_cadence + sla_hours + send-sensitivity).
  4. Refreshes STATE per row: last_synced_at = max(freshness_column),
     last_biz_date = max(biz_date_column), row_count, last_row_delta.

Standalone:
    python scripts/refresh_sync_registry.py            # uses config.DB_PATH (writer)
    python scripts/refresh_sync_registry.py --db ./x.duckdb

The orchestrator/nightly hold no lock once finished; for manual runs the caller
should wrap this in `flock /root/core/warehouse.write.lock` (single-writer rule).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logger = logging.getLogger("scripts.refresh_sync_registry")

# Priority order for detecting the "when was this last synced" column.
FRESH_PRIORITY = [
    "_loaded_at", "_mirrored_at", "_synced_at", "synced_at", "loaded_at",
    "_inserted_at", "_last_seen_at", "last_seen_at", "_resolved_at",
    "captured_at", "checked_at", "run_timestamp",
    "scan_timestamp", "created_at", "timestamp", "_snapshot_date",
    "snapshot_date", "date",
]
# Priority order for the business-date column (what date the data is ABOUT).
BIZ_PRIORITY = ["date", "snapshot_date", "_snapshot_date", "biz_date", "day", "event_date"]

CADENCE_SLA_HOURS = {"daily": 36, "weekly": 192, "periodic": 192, "once": None, "retired": None}

# Append-only feeds that grow on a send-day — row_delta=0 there is suspicious.
SEND_SENSITIVE = {
    "raw_account_truth_daily_actuals", "raw_instantly_email",
    "core.campaign_daily", "core.sending_account_daily",
}

# Explicit per-table overrides (full control for the special cases).
OVERRIDES: dict[str, dict] = {
    "raw_instantly_campaign_marker_tag": dict(
        cadence="retired", status="empty", source="instantly",
        notes="tag-mappings endpoint not in public REST (known empty)"),
    "raw_pipeline_conversation_messages": dict(
        cadence="retired", status="retired", source="pipeline_supabase",
        notes="obsolete; replaced by raw_instantly_email"),
    "raw_pipeline_campaign_data__prehash_legacy": dict(
        cadence="retired", status="retired", source="pipeline_supabase",
        notes="legacy/superseded"),
    "raw_pipeline_variant_copy": dict(
        cadence="once", source="pipeline_supabase",
        notes="launched variant copy — never changes (VARIANT_COPY_SYNC_ONCE)"),
    "raw_blacklist_check": dict(
        cadence="periodic", source="dns", freshness_column="checked_at"),
    # Scope A 2026-06-09: upgraded from the F3 one-time frozen snapshot to a NIGHTLY
    # mirror (entities/im_bookings.py, 'im_bookings' phase). The frozen 2026-05-31
    # snapshot is preserved alongside exactly ONE live snapshot; staleness alerts apply.
    "raw_im_bookings": dict(
        cadence="daily", status="active", source="darcy_portal",
        freshness_column="_loaded_at",
        notes="nightly portal mirror since 2026-06-09 (entities/im_bookings.py); "
              "frozen 2026-05-31 snapshot preserved alongside the live one"),
    # 2026-06-11 (meetings outage postmortem): a successful mirror run that pulls 0 new
    # rows must still alert when the DATA goes stale — the Jun 4-10 outage showed
    # sync-ran freshness alone masks an upstream death. Scraper lands D+1, the 07:00 UTC
    # meetings refresh pulls it same morning -> 2-day biz SLA.
    "raw_pipeline_meetings_booked_raw": dict(
        biz_date_column="posted_at", biz_sla_days=2,
        notes="meetings ground truth (Slack scrape via pipeline); biz-recency SLA 2d"),
}

# Curated core/derived decision tables to register IF they exist (raw_ are auto).
# (name, source, owner_phase, cadence, freshness_column, biz_date_column, biz_sla_days, notes)
CORE_FEEDS = [
    ("core.campaign_daily",        "instantly_step_api", "derived",   "daily",    "_loaded_at", "date", None, "Track H per-campaign daily"),
    ("core.sending_account_daily", "account_truth",      "account_truth", "daily", "_loaded_at", "date", None, "Track G infra daily"),
    # biz date = posted_at (when the meeting was POSTED, not when we synced) — the Jun 4-10
    # outage had last_synced_at current while the newest meeting was 4 days old.
    ("core.meeting",               "slack_scrape",       "canonical", "daily",    "_last_seen_at", "posted_at", 2, "booked meetings; biz-recency SLA 2d"),
    ("core.opportunity",           "instantly",          "canonical", "daily",    "_resolved_at",  None, None, "opportunities"),
    ("core.domain_registry",       "registrar+dns",      "canonical", "periodic", "_loaded_at", None,   None, "Track I domain master"),
    ("core.sending_account",       "account_truth",      "canonical", "daily",    "last_seen_at", None, None, "canonical inbox"),
]


def _classify_raw(name: str) -> tuple[str, str | None, str]:
    """(source, owner_phase, cadence) for a raw_ table by prefix."""
    if name.startswith("raw_instantly_"):   return ("instantly", "instantly", "daily")
    # pipeline-supabase is being retired and its source is intermittently down; the
    # warehouse (not pipeline) is now the canonical sink. Track as 'periodic' (8d SLA)
    # so a multi-day death still alerts, without daily noise about the known outage.
    if name.startswith("raw_pipeline_"):     return ("pipeline_supabase", "pipeline_mirror", "periodic")
    if name.startswith("raw_comms_"):        return ("comms", "comms_mirror", "daily")
    if name.startswith("raw_sendivo_"):      return ("sendivo", "sendivo", "daily")
    if name.startswith("raw_account_truth_"):return ("account_truth", "account_truth", "daily")
    if name.startswith("raw_sheets_"):       return ("sheets", "sheets", "periodic")
    if name.startswith("raw_cc_"):           return ("d1_cc", "cc_mirror", "daily")
    if name in ("raw_dns_sweep_domain", "raw_recipient_mx"):
        return ("dns", "dns_sweep", "periodic")
    if name == "raw_registrar_domains":      return ("registrar", "sheets", "periodic")
    if name == "raw_cloudflare_zones":       return ("cloudflare", "sheets", "periodic")
    return ("other", None, "daily")


def _pick(cols: set[str], priority: list[str]) -> str | None:
    for c in priority:
        if c in cols:
            return c
    return None


def _columns_by_table(conn) -> dict[str, set[str]]:
    rows = conn.execute(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema IN ('main', 'core', 'derived')
        """
    ).fetchall()
    out: dict[str, set[str]] = {}
    for schema, table, col in rows:
        key = table if schema == "main" else f"{schema}.{table}"
        out.setdefault(key, set()).add(col)
    return out


def _upsert_policy(conn, name, schema, source, owner_phase, cadence,
                   fresh_col, biz_col, send_sensitive, status, notes,
                   biz_sla_days=None) -> None:
    sla = CADENCE_SLA_HOURS.get(cadence)
    conn.execute(
        """
        INSERT INTO core.sync_registry
          (name, table_schema, source, owner_phase, expected_cadence, sla_hours,
           freshness_column, biz_date_column, biz_sla_days, is_send_sensitive, status, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (name) DO UPDATE SET
          table_schema = excluded.table_schema,
          source = excluded.source,
          owner_phase = excluded.owner_phase,
          expected_cadence = excluded.expected_cadence,
          sla_hours = excluded.sla_hours,
          freshness_column = excluded.freshness_column,
          biz_date_column = excluded.biz_date_column,
          biz_sla_days = excluded.biz_sla_days,
          is_send_sensitive = excluded.is_send_sensitive,
          status = excluded.status,
          notes = COALESCE(excluded.notes, core.sync_registry.notes)
        """,
        [name, schema, source, owner_phase, cadence, sla, fresh_col, biz_col,
         biz_sla_days, send_sensitive, status, notes],
    )


def seed(conn) -> int:
    cols_by = _columns_by_table(conn)
    n = 0

    # 1. Every raw_ table (auto-discovered).
    raw_tables = [k for k in cols_by if k.startswith("raw_")]
    for name in sorted(raw_tables):
        cols = cols_by[name]
        source, owner_phase, cadence = _classify_raw(name)
        status = "active"
        notes = None
        ov = OVERRIDES.get(name, {})
        source = ov.get("source", source)
        cadence = ov.get("cadence", cadence)
        status = ov.get("status", status)
        notes = ov.get("notes", notes)
        fresh_col = ov.get("freshness_column") or _pick(cols, FRESH_PRIORITY)
        biz_col = ov.get("biz_date_column") or _pick(cols, BIZ_PRIORITY)
        send_sensitive = name in SEND_SENSITIVE
        _upsert_policy(conn, name, "main", source, owner_phase, cadence,
                       fresh_col, biz_col, send_sensitive, status, notes,
                       biz_sla_days=ov.get("biz_sla_days"))
        n += 1

    # 2. Curated core/derived decision tables that exist.
    for name, source, owner_phase, cadence, fresh_col, biz_col, biz_sla, notes in CORE_FEEDS:
        if name not in cols_by:
            continue
        cols = cols_by[name]
        schema = name.split(".", 1)[0]
        # validate declared columns actually exist; else auto-detect.
        if fresh_col not in cols:
            fresh_col = _pick(cols, FRESH_PRIORITY)
        if biz_col and biz_col not in cols:
            biz_col = _pick(cols, BIZ_PRIORITY)
        send_sensitive = name in SEND_SENSITIVE
        _upsert_policy(conn, name, schema, source, owner_phase, cadence,
                       fresh_col, biz_col, send_sensitive, "active", notes,
                       biz_sla_days=biz_sla)
        n += 1

    return n


def _phase_last_success(conn, phase: str | None):
    """Fallback freshness for canonical/derived tables that carry no row-level
    load timestamp: the last successful orchestrator phase run for that phase.
    Reuses the existing core.sync_run_phase instrumentation."""
    if not phase:
        return None
    try:
        return conn.execute(
            "SELECT max(ended_at) FROM core.sync_run_phase "
            "WHERE phase_name = ? AND status = 'success'",
            [phase],
        ).fetchone()[0]
    except Exception:
        return None


def refresh_state(conn) -> int:
    rows = conn.execute(
        "SELECT name, table_schema, freshness_column, biz_date_column, row_count, owner_phase "
        "FROM core.sync_registry"
    ).fetchall()
    refreshed = 0
    for name, schema, fresh_col, biz_col, prev_count, owner_phase in rows:
        # Physical reference: raw_ names are bare (main); core/derived are qualified already.
        ref = name
        try:
            cnt = conn.execute(f"SELECT count(*) FROM {ref}").fetchone()[0]
        except Exception as exc:  # table vanished
            logger.warning("refresh: %s count failed: %s", name, exc)
            continue
        last_synced = None
        last_biz = None
        if fresh_col:
            try:
                last_synced = conn.execute(
                    f"SELECT TRY_CAST(max({fresh_col}) AS TIMESTAMPTZ) FROM {ref}"
                ).fetchone()[0]
            except Exception as exc:
                logger.warning("refresh: %s max(%s) failed: %s", name, fresh_col, exc)
        # Fallback for canonical/derived tables with no usable row-level timestamp:
        # use the last successful orchestrator phase run for their owner_phase.
        if last_synced is None:
            last_synced = _phase_last_success(conn, owner_phase)
        if biz_col:
            try:
                last_biz = conn.execute(
                    f"SELECT TRY_CAST(max({biz_col}) AS DATE) FROM {ref}"
                ).fetchone()[0]
            except Exception:
                pass
        delta = None if prev_count is None else (cnt - prev_count)
        conn.execute(
            """
            UPDATE core.sync_registry SET
              prev_row_count = row_count,
              row_count = ?,
              last_row_delta = ?,
              last_synced_at = ?,
              last_biz_date = ?,
              last_checked_at = now()
            WHERE name = ?
            """,
            [cnt, delta, last_synced, last_biz, name],
        )
        refreshed += 1
    return refreshed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed + refresh core.sync_registry")
    parser.add_argument("--db", type=str, default=None, help="Override DB path")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    db_path = Path(args.db) if args.db else None
    conn = db_module.connect(db_path)

    # Ensure the registry DDL exists (idempotent) even if setup_db wasn't re-run.
    ddl = REPO_ROOT / "sql" / "ddl" / "38_sync_registry.sql"
    if ddl.exists():
        conn.execute(ddl.read_text())

    conn.execute("BEGIN")
    try:
        seeded = seed(conn)
        refreshed = refresh_state(conn)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    total = conn.execute("SELECT count(*) FROM core.sync_registry").fetchone()[0]
    stale = conn.execute(
        "SELECT count(*) FROM v_warehouse_freshness WHERE is_stale"
    ).fetchone()[0]
    logger.info("sync_registry: seeded=%d refreshed=%d total=%d stale=%d",
                seeded, refreshed, total, stale)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
