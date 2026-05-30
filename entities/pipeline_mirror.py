"""Phase 2 Track B: slim mirror of pipeline-supabase analytical tables into DuckDB.

For each table in ``SLIM_TABLES``:
    1. ``SELECT *`` (plus an optional WHERE) from public.<table> on pipeline-supabase,
       streamed via a server-side cursor so RAM stays bounded.
    2. DELETE FROM raw_pipeline_<table> WHERE _run_id = <current run> (idempotency within a run).
    3. INSERT the fresh batch in chunks with _loaded_at = now() and _run_id = ctx.run_id.

Prior runs are preserved. The 8 raw_pipeline_* DDL lives in sql/ddl/04_pipeline_mirror.sql.

ARRAY columns (e.g. campaigns.tags) and jsonb columns (lead_events.event_data) come back
from psycopg2 as Python lists / dicts. We serialize them to JSON strings before insert
because the DuckDB columns are typed VARCHAR.

Memory:
    The big tables here (lead_events ~1.4M rows / 90d, reply_data ~380k, bounce_suppression
    ~220k) blow Python past 15GB if loaded all at once. So we stream from Postgres in
    chunks via a server-side cursor and flush each chunk straight to DuckDB.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.pipeline_supabase import PipelineSupabase

logger = logging.getLogger("entities.pipeline_mirror")


# (source_table, where_clause). where=None means full mirror.
# v1 scope: only the 4 tables needed for workspace/campaign/meeting canonical entities.
# Deferred to v1.5 (currently hang on large JSONB columns; see entities/pipeline_mirror.py
# git history): reply_data, lead_events, variant_copy, bounce_suppression. Bring back
# with row batching + COPY instead of executemany when we need them.
SLIM_TABLES: list[tuple[str, str | None]] = [
    ("campaigns", None),
    ("campaign_data", None),
    ("campaign_daily_metrics", "date >= current_date - interval '90 days'"),
    ("meetings_booked_raw", None),
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

# Stream from Postgres + flush to DuckDB in chunks this big. 5k is small enough to keep
# RAM low and big enough that round-trip overhead stays negligible.
CHUNK_SIZE = 5000


def _coerce_value(value: Any) -> Any:
    """JSON-encode lists/dicts (Postgres ARRAY / jsonb -> VARCHAR). Pass everything else through."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    return value


def _flush_chunk(
    conn: duckdb.DuckDBPyConnection,
    insert_sql: str,
    cols: list[str],
    chunk: list[dict[str, Any]],
    loaded_at: datetime,
    run_id: str,
) -> None:
    if not chunk:
        return
    batch = [
        tuple(_coerce_value(row.get(c)) for c in cols) + (loaded_at, run_id)
        for row in chunk
    ]
    conn.executemany(insert_sql, batch)


def _write_raw_streaming(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    table: str,
    rows_iter: Iterable[dict[str, Any]],
) -> int:
    """Idempotent within a run: DELETE WHERE _run_id = ?; INSERT streamed batch.

    Wraps the entire (delete + chunked insert) in one DuckDB transaction so a partial
    failure rolls back cleanly.

    Returns rows inserted.
    """
    raw_table = f"raw_pipeline_{table}"
    cols = RAW_COLUMNS[table]
    all_cols = cols + ["_loaded_at", "_run_id"]
    placeholders = ", ".join(["?"] * len(all_cols))
    col_list = ", ".join(all_cols)
    insert_sql = f"INSERT INTO {raw_table} ({col_list}) VALUES ({placeholders})"
    loaded_at = datetime.now(timezone.utc)

    conn.execute("BEGIN")
    written = 0
    try:
        conn.execute(f"DELETE FROM {raw_table} WHERE _run_id = ?", [run_id])
        chunk: list[dict[str, Any]] = []
        for row in rows_iter:
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                _flush_chunk(conn, insert_sql, cols, chunk, loaded_at, run_id)
                written += len(chunk)
                chunk = []
        if chunk:
            _flush_chunk(conn, insert_sql, cols, chunk, loaded_at, run_id)
            written += len(chunk)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return written


def run_pipeline_mirror(ctx: RunContext) -> PhaseResult:
    db_url = ctx.credentials.require("PIPELINE_SUPABASE_DB_URL")
    pg = PipelineSupabase(db_url)

    rows_total = 0
    per_table: dict[str, int] = {}

    try:
        for table, where in SLIM_TABLES:
            logger.info("mirroring %s (where=%s)", table, where or "<full>")
            rows_iter = pg.iter_table(table, where_clause=where, chunk_size=CHUNK_SIZE)
            written = _write_raw_streaming(ctx.db, ctx.run_id, table, rows_iter)
            rows_total += written
            per_table[table] = written
            logger.info("  %s -> %d rows", table, written)
    finally:
        pg.close()

    return PhaseResult(
        rows_in=rows_total,
        rows_out=rows_total,
        notes={"per_table": per_table},
    )


def register(registry: Registry) -> None:
    registry.add_phase("pipeline_mirror", "all", run_pipeline_mirror)
