"""Slim mirror of pipeline-supabase analytical tables into DuckDB.

v2 rewrite (2026-06-02, spec 15): per-table SYNC MODES instead of
append-a-full-snapshot-every-run. Each raw table has a surrogate
`_key VARCHAR PRIMARY KEY` and one of three write strategies:

  * insert       — immutable events. ON CONFLICT (_key) DO NOTHING.
  * insert_hash  — copy. _key includes a content fingerprint, so identical copy
                   is never re-written and edited copy lands as a new row.
  * upsert       — mutable dimension/daily. ON CONFLICT (_key) DO UPDATE.

FREEZE-ON-DELETE invariant: we only ever INSERT/UPSERT keys present in the
current pull. Nothing deletes keys absent from the source, so a campaign deleted
upstream keeps its last-known rows frozen — no blanks, no errors.

Read-side: immutable event tables pull incrementally by a timestamp watermark
(stop re-scanning millions of rows); daily metrics pull a 45-day window; the rest
pull full (small). See spec 15 for the full rationale + acceptance tests.

Schema (the _key column + copy content_hash) lives in sql/ddl/04_pipeline_mirror.sql
(fresh installs). The original insert_hash live migration was
scripts/migrate_pipeline_mirror_v2.py (built from this module's SPECS so keys can't
drift). The `_key` / `content_hash` SQL expressions here MUST match those used by the
live migrations.

Amendment 2026-06-06 (spec 15): campaign_data moved insert_hash -> upsert (its daily
metric rollup was frozen by the content-hashed key). The live re-key migration is
sql/ddl/36_campaign_data_upsert_rekey.sql; copy-version history stays in variant_copy.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.pipeline_mirror")


# ---------------------------------------------------------------------------
# Per-table sync configuration. Column lists match sql/ddl/04_pipeline_mirror.sql
# (excluding the trailing _key / content_hash / _loaded_at / _run_id which we add).
# ---------------------------------------------------------------------------

@dataclass
class Spec:
    mode: str                              # 'insert' | 'insert_hash' | 'upsert'
    key_sql: str                           # expression over `src` producing _key
    columns: list[str] = field(default_factory=list)
    array_columns: set[str] = field(default_factory=set)
    hash_cols: list[str] | None = None     # insert_hash: cols fingerprinted into content_hash
    watermark_col: str | None = None       # incremental pull timestamp
    watermark_overlap: str = "2 days"
    window_sql: str | None = None          # extra WHERE on the source pull


def _md5_concat(cols: list[str]) -> str:
    """md5 over coalesced, unit-separated columns — stable content fingerprint."""
    parts = " || CHR(31) || ".join(f"coalesce(CAST({c} AS VARCHAR), '')" for c in cols)
    return f"md5({parts})"


def _key_concat(cols: list[str]) -> str:
    parts = " || '|' || ".join(f"coalesce(CAST({c} AS VARCHAR), '')" for c in cols)
    return f"md5({parts})"


SPECS: dict[str, Spec] = {
    "campaigns": Spec(
        mode="upsert",
        key_sql="campaign_id",
        columns=[
            "campaign_id", "workspace_id", "workspace_name", "name", "status", "cm_name",
            "industry", "bounced_count", "contacted_count", "leads_count", "completed_count",
            "unsubscribed_count", "instantly_created_at", "synced_at", "tags", "lead_source",
            "rg_batch_ids", "segment", "timestamp_updated", "daily_limit", "product",
            "excluded_from_analysis", "exclusion_reason", "infra_type",
        ],
        array_columns={"tags", "rg_batch_ids"},
    ),
    "campaign_data": Spec(
        # UPSERT (spec 15 amendment 2026-06-06): campaign_data carries the daily
        # metric rollup (emails_sent / opportunities / v_disabled / total_leads …)
        # alongside copy. insert_hash froze metrics because copy rarely changes, so
        # the content-hashed _key never moved and ON CONFLICT DO NOTHING discarded
        # every new metric snapshot — raw_pipeline_campaign_data (and the __ALL__
        # rollup built off it) went stale. Switch to upsert on the natural key
        # (campaign_id|step|variant), dropping content_hash from the key so metric
        # updates land in place. Copy-version HISTORY is unaffected here and remains
        # fully covered by the separate variant_copy mirror (still insert_hash).
        mode="upsert",
        key_sql=_key_concat(["campaign_id", "step", "variant"]),
        columns=[
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
        array_columns={"tags", "rg_batch_tags", "sender_tags", "other_tags"},
    ),
    "campaign_daily_metrics": Spec(
        mode="upsert",
        key_sql=_key_concat(["campaign_id", "date"]),
        window_sql="date >= current_date - 45",
        columns=[
            "campaign_id", "date", "sent", "contacted", "new_leads_contacted", "opened",
            "unique_opened", "replies", "unique_replies", "replies_automatic",
            "unique_replies_automatic", "clicks", "unique_clicks", "opportunities",
            "unique_opportunities", "synced_at", "workspace_id", "workspace_name",
        ],
    ),
    "meetings_booked_raw": Spec(
        mode="insert",
        key_sql="CAST(id AS VARCHAR)",
        watermark_col="posted_at",
        columns=[
            "id", "channel_id", "channel_name", "partner", "message_ts", "line_index",
            "posted_by", "posted_at", "raw_text", "booking_number", "campaign_name_raw",
            "campaign_id", "match_method", "match_confidence", "synced_at",
            "posted_by_slack_id", "raw_line",
        ],
    ),
    "reply_data": Spec(
        mode="insert",
        key_sql="CAST(id AS VARCHAR)",
        watermark_col="reply_timestamp",
        columns=[
            "id", "campaign_id", "lead_email", "reply_text", "reply_timestamp",
            "workspace_id", "intent", "from_name", "subject", "synced_at", "step", "variant",
        ],
    ),
    "reply_intent_classifications": Spec(
        mode="upsert",
        key_sql=_key_concat(["source_table", "source_id"]),
        watermark_col="classified_at",
        columns=[
            "source_table", "source_id", "workspace_id", "campaign_id", "lead_email",
            "sender_email", "recipient_email", "reply_timestamp", "intent", "intent_source",
            "is_auto_reply", "auto_reply_source", "auto_reply_confidence",
            "classifier_version", "classified_at",
        ],
    ),
    "reply_auto_reconciliation": Spec(
        mode="upsert",
        key_sql=_key_concat(["date", "campaign_id"]),
        columns=[
            "date", "campaign_id", "aggregate_unique_auto", "row_level_auto",
            "coverage_pct", "source_notes", "checked_at",
        ],
    ),
    "lead_events": Spec(
        mode="insert",
        key_sql="CAST(id AS VARCHAR)",
        watermark_col="event_timestamp",
        columns=[
            "id", "lead_email", "campaign_id", "event_type", "workspace_id",
            "event_timestamp", "event_data", "synced_at",
        ],
    ),
    "variant_copy": Spec(
        mode="insert_hash",
        key_sql=_key_concat(["campaign_id", "step", "variant", "content_hash"]),
        hash_cols=["subject", "body"],
        columns=[
            "campaign_id", "step", "variant", "subject", "body", "synced_at",
            "body_resolved", "subject_resolved", "v_disabled", "body_unspintaxed",
            "subject_unspintaxed",
        ],
    ),
    "bounce_suppression": Spec(
        mode="upsert",
        key_sql="CAST(id AS VARCHAR)",
        watermark_col="last_seen_at",
        watermark_overlap="7 days",
        columns=[
            "id", "email", "domain", "bounce_type", "first_bounced_at", "last_seen_at",
            "workspaces_seen", "source_campaigns", "raw_reason", "lead_first_name",
            "lead_last_name", "lead_company", "created_at",
        ],
        array_columns={"workspaces_seen", "source_campaigns"},
    ),
    # Full Instantly email thread bodies (~17.1M rows, growing). Immutable email
    # events: a sent/received message never mutates in place, so mode='insert'
    # keyed on the stable Instantly message id (`id`, text NOT NULL) is the right
    # grain — one row per message. thread_id/workspace_id are denormalized columns
    # for BI joins, not part of the key (id is globally unique on its own). No
    # content_hash: bodies are write-once. Incremental pull by message_timestamp so
    # nightly runs scan only the recent tail, never the full 17M every night.
    #
    # LINEAGE: source today is pipeline-supabase public.conversation_messages,
    # which itself mirrors Instantly Unibox conversations. Post pipeline-supabase
    # retirement, swap this mirror's source to a direct Instantly conversation
    # sync (per-workspace Unibox/emails endpoints) feeding the same
    # raw_pipeline_conversation_messages shape — the warehouse contract is the
    # table, not the upstream Supabase.
    "conversation_messages": Spec(
        mode="insert",
        key_sql="CAST(id AS VARCHAR)",
        watermark_col="message_timestamp",
        columns=[
            "id", "thread_id", "campaign_id", "workspace_id", "lead_email",
            "sender_email", "sender_name", "recipient_email", "recipient_name",
            "direction", "ue_type", "body_text", "body_html", "subject",
            "message_timestamp", "step_raw", "step", "variant", "is_unread",
            "interest_status", "ai_interest_value", "content_preview", "eaccount",
            "subsequence_id", "synced_at",
        ],
    ),
}


def _src_select(spec: Spec) -> str:
    """SELECT list for the `src` CTE: source columns (array-casted) + content_hash."""
    items = []
    for c in spec.columns:
        if c in spec.array_columns:
            items.append(f"CAST({c} AS VARCHAR) AS {c}")
        else:
            items.append(c)
    if spec.hash_cols:
        items.append(f"{_md5_concat(spec.hash_cols)} AS content_hash")
    return ", ".join(items)


def _build_sql(table: str, spec: Spec) -> str:
    """Compose the full ON CONFLICT insert/upsert for one table. `?` binds _run_id."""
    has_hash = bool(spec.hash_cols)

    # Source WHERE: watermark (incremental) and/or window.
    conds: list[str] = []
    if spec.watermark_col:
        wm = spec.watermark_col
        conds.append(
            f"({wm} >= (SELECT coalesce(max({wm}), TIMESTAMP '1970-01-01') "
            f"FROM raw_pipeline_{table}) - INTERVAL '{spec.watermark_overlap}' "
            f"OR {wm} IS NULL)"
        )
    if spec.window_sql:
        conds.append(spec.window_sql)
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    # Target + outer projection.
    target_cols = ["_key"] + spec.columns + (["content_hash"] if has_hash else []) + ["_loaded_at", "_run_id"]
    proj = [f"{spec.key_sql} AS _key"] + spec.columns + (["content_hash"] if has_hash else []) + ["now()", "?"]

    sql = (
        f"INSERT INTO raw_pipeline_{table} ({', '.join(target_cols)}) "
        f"WITH src AS (SELECT {_src_select(spec)} FROM pg.public.{table} {where}) "
        f"SELECT {', '.join(proj)} FROM src "
    )

    if spec.mode in ("insert", "insert_hash"):
        sql += "ON CONFLICT (_key) DO NOTHING"
    elif spec.mode == "upsert":
        update_cols = spec.columns + (["content_hash"] if has_hash else []) + ["_loaded_at", "_run_id"]
        set_list = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
        sql += f"ON CONFLICT (_key) DO UPDATE SET {set_list}"
    else:  # pragma: no cover
        raise ValueError(f"unknown mode {spec.mode}")
    return sql


def run_pipeline_mirror(ctx: RunContext) -> PhaseResult:
    pg_url = ctx.credentials.require("PIPELINE_SUPABASE_DB_URL")
    conn = ctx.db

    # PIPELINE_MIRROR_ONLY=tbl1,tbl2 restricts the run to named SPECS tables.
    # Used by scripts/meetings_refresh.sh (07:00 UTC) to pull just meetings_booked_raw
    # without re-mirroring everything; unknown names fail loudly rather than no-op.
    only_env = os.environ.get("PIPELINE_MIRROR_ONLY", "").strip()
    specs = SPECS
    if only_env:
        wanted = [t.strip() for t in only_env.split(",") if t.strip()]
        unknown = [t for t in wanted if t not in SPECS]
        if unknown:
            raise ValueError(f"PIPELINE_MIRROR_ONLY names unknown tables: {unknown}")
        specs = {t: SPECS[t] for t in wanted}
        logger.info("PIPELINE_MIRROR_ONLY -> mirroring only %s", wanted)

    conn.execute("INSTALL postgres")
    conn.execute("LOAD postgres")
    try:
        conn.execute("DETACH pg")
    except Exception:
        pass
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")

    rows_total = 0
    per_table: dict[str, dict] = {}
    try:
        for table, spec in specs.items():
            logger.info("mirroring %s (mode=%s, watermark=%s)", table, spec.mode, spec.watermark_col or "-")
            before = conn.execute(f"SELECT count(*) FROM raw_pipeline_{table}").fetchone()[0]
            conn.execute("BEGIN")
            try:
                conn.execute(_build_sql(table, spec), [ctx.run_id])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            after = conn.execute(f"SELECT count(*) FROM raw_pipeline_{table}").fetchone()[0]
            written = after - before  # net new rows (in-place upserts show 0)
            per_table[table] = {"total": after, "new": written}
            rows_total += max(written, 0)
            logger.info("  %s -> %d total (%+d new)", table, after, written)
    finally:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass

    return PhaseResult(rows_in=rows_total, rows_out=rows_total, notes={"per_table": per_table})


def register(registry: Registry) -> None:
    registry.add_phase("pipeline_mirror", "all", run_pipeline_mirror)
