"""Slim mirror of pipeline-supabase analytical tables into DuckDB.

v1.1 rewrite (2026-05-30): uses DuckDB's `postgres_scanner` extension to bulk-copy
direct from Postgres -> DuckDB. Drops the per-row executemany pattern that hung
Agent B's v1 attempt on lead_events / bounce_suppression. New runtime: ~5 sec for
the 4 small tables, ~30-60 sec for all 8 including bounce_suppression (220k rows)
and lead_events (90d subset). Memory stays trivial — DuckDB streams.

For each table in ``SLIM_TABLES``:
    1. Compose: SELECT <cols, casting arrays to VARCHAR>, now(), :run_id FROM pg.public.<table> WHERE ...
    2. DELETE FROM raw_pipeline_<table> WHERE _run_id = :run_id (idempotent within a run)
    3. INSERT INTO raw_pipeline_<table> (cols, _loaded_at, _run_id) <the SELECT>

Wrapped in BEGIN/COMMIT per table so partial failures roll back cleanly.

Prior runs are preserved (we never delete rows from a different _run_id).
"""

from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.pipeline_mirror")


# (source_table, where_clause). where=None means full mirror.
# Note: spec said `timestamp >= ...` for reply_data + lead_events; actual source
# columns are `reply_timestamp` and `event_timestamp`. We use the real names.
SLIM_TABLES: list[tuple[str, str | None]] = [
    ("campaigns", None),
    ("campaign_data", None),
    ("campaign_daily_metrics", "date >= current_date - interval '90 days'"),
    ("meetings_booked_raw", None),
    ("reply_data", "reply_timestamp >= current_date - interval '90 days'"),
    ("lead_events", "event_timestamp >= current_date - interval '90 days'"),
    ("variant_copy", None),
    ("bounce_suppression", None),
]


# Explicit column lists per raw table (NOT including the trailing _loaded_at, _run_id which
# we append in the INSERT). Order matches sql/ddl/04_pipeline_mirror.sql.
RAW_COLUMNS: dict[str, list[str]] = {
    "campaigns": [
        "campaign_id", "workspace_id", "workspace_name", "name", "status", "cm_name",
        "industry", "bounced_count", "contacted_count", "leads_count", "completed_count",
        "unsubscribed_count", "instantly_created_at", "synced_at", "tags", "lead_source",
        "rg_batch_ids", "segment", "timestamp_updated", "daily_limit", "product",
        "excluded_from_analysis", "exclusion_reason", "infra_type",
    ],
    "campaign_data": [
        "campaign_id", "campaign_name", "workspace_id", "workspace_name", "cm_name",
        "segment", "product", "infra_type", "status", "date_launched", "daily_limit",
        "lead_source", "tags", "excluded_from_analysis", "exclusion_reason", "step",
        "variant", "emails_sent", "replies", "opportunities", "analytics_sequence_started",
        "leads_closed", "e_op", "reply_rate", "close_rate", "campaign_score", "subject",
        "body", "subject_preview", "body_preview", "signature", "v_disabled", "synced_at",
        "meetings_booked", "rg_batch_tags", "pair_tag", "sender_tags", "other_tags",
        "total_leads", "leads_completed", "leads_bounced", "leads_unsubscribed",
        "lead_sequence_started",
    ],
    "campaign_daily_metrics": [
        "campaign_id", "date", "sent", "contacted", "new_leads_contacted", "opened",
        "unique_opened", "replies", "unique_replies", "replies_automatic",
        "unique_replies_automatic", "clicks", "unique_clicks", "opportunities",
        "unique_opportunities", "synced_at", "workspace_id", "workspace_name",
    ],
    "meetings_booked_raw": [
        "id", "channel_id", "channel_name", "partner", "message_ts", "line_index",
        "posted_by", "posted_at", "raw_text", "booking_number", "campaign_name_raw",
        "campaign_id", "match_method", "match_confidence", "synced_at",
        "posted_by_slack_id", "raw_line",
    ],
    "reply_data": [
        "id", "campaign_id", "lead_email", "reply_text", "reply_timestamp",
        "workspace_id", "intent", "from_name", "subject", "synced_at", "step", "variant",
    ],
    "lead_events": [
        "id", "lead_email", "campaign_id", "event_type", "workspace_id",
        "event_timestamp", "event_data", "synced_at",
    ],
    "variant_copy": [
        "campaign_id", "step", "variant", "subject", "body", "synced_at",
        "body_resolved", "subject_resolved", "v_disabled", "body_unspintaxed",
        "subject_unspintaxed",
    ],
    "bounce_suppression": [
        "id", "email", "domain", "bounce_type", "first_bounced_at", "last_seen_at",
        "workspaces_seen", "source_campaigns", "raw_reason", "lead_first_name",
        "lead_last_name", "lead_company", "created_at",
    ],
}


# Columns that are Postgres ARRAY types and need CAST to VARCHAR for DuckDB.
# Empirically verified via duckdb_columns() against postgres_scanner ATTACH.
ARRAY_COLUMNS: dict[str, set[str]] = {
    "campaigns": {"tags", "rg_batch_ids"},
    "campaign_data": {"tags", "rg_batch_tags", "sender_tags", "other_tags"},
    "bounce_suppression": {"workspaces_seen", "source_campaigns"},
}


def _select_expr(table: str, col: str) -> str:
    """Wrap array columns in CAST; pass everything else through. Output is a
    qualified SELECT-list expression, e.g. ``CAST(tags AS VARCHAR) AS tags``."""
    if col in ARRAY_COLUMNS.get(table, ()):
        return f"CAST({col} AS VARCHAR) AS {col}"
    return col


def _build_insert_sql(table: str, where: str | None) -> str:
    cols = RAW_COLUMNS[table]
    select_list = ", ".join(_select_expr(table, c) for c in cols)
    target_cols = ", ".join(cols + ["_loaded_at", "_run_id"])
    where_sql = f"WHERE {where}" if where else ""
    # ? placeholder for _run_id binds at execute() time
    return (
        f"INSERT INTO raw_pipeline_{table} ({target_cols}) "
        f"SELECT {select_list}, now(), ? "
        f"FROM pg.public.{table} {where_sql}"
    )


def run_pipeline_mirror(ctx: RunContext) -> PhaseResult:
    pg_url = ctx.credentials.require("PIPELINE_SUPABASE_DB_URL")
    conn = ctx.db

    # Load + attach Postgres source (idempotent across reruns in the same session).
    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    # Detach if already attached so we get a fresh handle (no harm if not attached).
    try:
        conn.execute("DETACH pg")
    except Exception:
        pass
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")

    rows_total = 0
    per_table: dict[str, int] = {}

    try:
        for table, where in SLIM_TABLES:
            logger.info("mirroring %s (where=%s)", table, where or "<full>")
            insert_sql = _build_insert_sql(table, where)
            conn.execute("BEGIN")
            try:
                conn.execute(
                    f"DELETE FROM raw_pipeline_{table} WHERE _run_id = ?", [ctx.run_id]
                )
                conn.execute(insert_sql, [ctx.run_id])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            n = conn.execute(
                f"SELECT count(*) FROM raw_pipeline_{table} WHERE _run_id = ?",
                [ctx.run_id],
            ).fetchone()[0]
            rows_total += n
            per_table[table] = n
            logger.info("  %s -> %d rows", table, n)
    finally:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass

    return PhaseResult(
        rows_in=rows_total,
        rows_out=rows_total,
        notes={"per_table": per_table},
    )


def register(registry: Registry) -> None:
    registry.add_phase("pipeline_mirror", "all", run_pipeline_mirror)
