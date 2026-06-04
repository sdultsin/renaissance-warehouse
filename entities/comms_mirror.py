"""comms-orchestration bulk mirror entity.

Mirrors the analytically-valuable tables from the comms-orchestration Postgres
(Sendivo SMS + warm-call + AIM) into ``raw_comms_*`` tables using DuckDB's
``postgres_scanner`` extension. Same v1 approach as ``pipeline_mirror.py``: a
small set of high-value tables copied wholesale on each run (full-refresh per
table, idempotent by ``_run_id``).

Append-only at the warehouse layer: every run writes a fresh snapshot tagged
with the ``_run_id`` so history is kept, but a given run is idempotent
(re-running deletes that run's rows first).

Source schemas differ per table: most live in ``comms``; ``ai_decision_log``
lives in ``audit``. Each entry carries its source schema so the SELECT can
qualify it correctly (``pg.<schema>.<table>``).

jsonb, enum (USER-DEFINED), and ARRAY (text[]) columns are CAST to VARCHAR in
the SELECT (see ``_CAST_TO_VARCHAR``) so the raw layer stays flat text, matching
the pipeline_mirror ARRAY pattern.

Tables mirrored:
  comms.brand              -> raw_comms_brand
  comms.conversation       -> raw_comms_conversation
  comms.message            -> raw_comms_message
  comms.suppression        -> raw_comms_suppression
  comms.escalation         -> raw_comms_escalation
  comms.call_opportunity   -> raw_comms_call_opportunity
  comms.phone_enrichment   -> raw_comms_phone_enrichment
  comms.instantly_message  -> raw_comms_instantly_message
  audit.ai_decision_log    -> raw_comms_ai_decision_log

Explicitly NOT mirrored (per Sam): comms.close_sync, comms.gbc_application,
comms.webhook_receipt, comms.app_link_check, config.* tables.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.registry import PhaseResult

if TYPE_CHECKING:
    from core.registry import RunContext

logger = logging.getLogger(__name__)

# Tables to mirror: (source_schema, pg_table, raw_table)
_TABLES: list[tuple[str, str, str]] = [
    ("comms", "brand", "raw_comms_brand"),
    ("comms", "conversation", "raw_comms_conversation"),
    # message: SMS inbound+outbound (content=body, ai_decision_id, direction). Joined to
    # conversation for prospect phone/email; feeds derived.reply_intent (Workstream I).
    ("comms", "message", "raw_comms_message"),
    ("comms", "suppression", "raw_comms_suppression"),
    ("comms", "escalation", "raw_comms_escalation"),
    ("comms", "call_opportunity", "raw_comms_call_opportunity"),
    ("comms", "phone_enrichment", "raw_comms_phone_enrichment"),
    ("comms", "instantly_message", "raw_comms_instantly_message"),
    ("audit", "ai_decision_log", "raw_comms_ai_decision_log"),
]

# Columns that must be CAST to VARCHAR for postgres_scanner -> DuckDB. Covers
# Postgres jsonb, USER-DEFINED enum, and ARRAY (text[]) source columns so the
# raw layer stays flat text. Keyed by raw table name. Verified 2026-05-30
# against the live source schema (comms MCP list_tables verbose).
_CAST_TO_VARCHAR: dict[str, set[str]] = {
    # jsonb + enum on conversation
    "raw_comms_conversation": {"state", "metadata", "last_proposed_slots"},
    # jsonb on phone_enrichment
    "raw_comms_phone_enrichment": {"raw_response"},
    # text[] + jsonb on instantly_message
    "raw_comms_instantly_message": {"to_emails", "raw_payload"},
    # enums on ai_decision_log
    "raw_comms_ai_decision_log": {"state_before", "state_after"},
}


def _build_select(
    ctx: "RunContext",
    source_schema: str,
    pg_table: str,
    raw_table: str,
) -> str:
    """Build the INSERT...SELECT for one table, CASTing jsonb/enum/array cols to text.

    Columns are read from the (already-created) warehouse raw table so the
    SELECT column list always matches the DDL.
    """
    cast_cols = _CAST_TO_VARCHAR.get(raw_table, set())
    cols = [r[1] for r in ctx.db.execute(f"PRAGMA table_info('{raw_table}')").fetchall()]
    # Drop our bookkeeping cols; we add them explicitly.
    data_cols = [c for c in cols if c not in ("_loaded_at", "_run_id")]
    select_exprs = []
    for c in data_cols:
        if c in cast_cols:
            select_exprs.append(
                f"CAST(pg.{source_schema}.{pg_table}.{c} AS VARCHAR) AS {c}"
            )
        else:
            select_exprs.append(c)
    select_clause = ",\n            ".join(select_exprs)
    insert_cols = ",\n            ".join(data_cols)
    sql = f"""
        INSERT INTO {raw_table} ({insert_cols}, _loaded_at, _run_id)
        SELECT
            {select_clause},
            now(),
            ?
        FROM pg.{source_schema}.{pg_table}
    """
    return sql


def _run_comms_mirror(ctx: "RunContext") -> PhaseResult:
    """Mirror comms-orchestration tables into raw_comms_* via postgres_scanner."""
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn = ctx.db

    # Ensure postgres_scanner is available + attach the comms pg database.
    conn.execute("INSTALL postgres; LOAD postgres;")
    # DuckDB cannot parameterize ATTACH — interpolate the URL literally (it comes
    # from our own .env, not user input; same pattern as pipeline_mirror.py).
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        total = 0
        for source_schema, pg_table, raw_table in _TABLES:
            conn.execute(f"DELETE FROM {raw_table} WHERE _run_id = ?", [ctx.run_id])
            sql = _build_select(ctx, source_schema, pg_table, raw_table)
            conn.execute(sql, [ctx.run_id])
            n = conn.execute(
                f"SELECT count(*) FROM {raw_table} WHERE _run_id = ?", [ctx.run_id]
            ).fetchone()[0]
            logger.info(
                "mirrored %s.%s -> %s: %d rows", source_schema, pg_table, raw_table, n
            )
            total += n
        conn.execute("DETACH pg")
    except Exception:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass
        raise
    return PhaseResult(rows_in=total, rows_out=total, notes={"tables": len(_TABLES)})


def register(registry) -> None:
    registry.add_phase("comms_mirror", "comms_mirror", _run_comms_mirror)
