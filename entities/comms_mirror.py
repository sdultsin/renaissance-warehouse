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
  comms.close_sync         -> raw_comms_close_sync        (WS-E gap-close, 2026-06-08)
  comms.gbc_application    -> raw_comms_gbc_application    (WS-E gap-close, 2026-06-08)
  comms.app_link_check     -> raw_comms_app_link_check     (WS-E gap-close, 2026-06-08)

Explicitly NOT mirrored: comms.webhook_receipt and config.* tables.
  * comms.webhook_receipt (~6.18M rows) is a raw pre-processing webhook log —
    intentionally excluded as noise (no analytic value; the processed signal
    already lands in the other mirrored tables). Do NOT add it here.

WS-E (Spec 16 §WS-E, 2026-06-08) closed the close_sync / gbc_application /
app_link_check gap. Their DDL is additive-only in sql/ddl/47_comms_mirror_gaps.sql
(the v1 tables in 16_comms_mirror.sql are untouched — additive invariant §3).
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
    # editable $/credit rate table — dollarizes phone-enrichment spend (feeds
    # derived.enrichment_cost). Small + stable; full-refresh like the rest.
    ("comms", "enrichment_vendor_pricing", "raw_comms_enrichment_vendor_pricing"),
    ("audit", "ai_decision_log", "raw_comms_ai_decision_log"),
    # ── WS-E gap-close (Spec 16, 2026-06-08): the three remaining analytic tables.
    # DDL in sql/ddl/47_comms_mirror_gaps.sql. webhook_receipt stays OUT (noise).
    ("comms", "close_sync", "raw_comms_close_sync"),
    ("comms", "gbc_application", "raw_comms_gbc_application"),
    ("comms", "app_link_check", "raw_comms_app_link_check"),
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
    # ── WS-E gap-close: jsonb columns on the three new tables (2026-06-08).
    "raw_comms_close_sync": {"request_payload", "response_payload"},
    "raw_comms_gbc_application": {"raw_payload", "suppression_log"},
    "raw_comms_app_link_check": {"raw_response"},
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
