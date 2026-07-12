"""comms-orchestration bulk mirror entity.

Mirrors the analytically-valuable tables from the comms-orchestration Postgres
(Sendivo SMS + warm-call + AIM) into ``raw_comms_*`` tables using DuckDB's
``postgres_scanner`` extension: a small set of high-value tables copied
wholesale on each run.

REPLACE-style at the warehouse layer (2026-07-01, warehouse-flags#12): each run
atomically DELETEs the whole raw table and INSERTs a fresh full snapshot, so a
table always holds exactly ONE snapshot (one row per source id) and re-running
is idempotent. The original design deleted only the current ``_run_id``'s rows
("history is kept"), which meant every nightly run APPENDED a full table copy —
tables grew to 12-34 stacked snapshots and naive aggregates (credit spend, opp
counts) read ~3-30x inflated. Nothing ever consumed the old snapshots (every
downstream reader filtered to the latest ``_run_id``), so history was dropped.
DELETE + INSERT run in one transaction per table: a mid-run crash can never
leave a table empty or half-loaded for readers.

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
  comms.lead_application   -> raw_comms_lead_application   (Lumara apply-form, 2026-06-11)
  comms.campaign_blast_template -> raw_comms_campaign_blast_template (coverage-audit gap #3, 2026-07-09)
  comms.meeting_reminder   -> raw_comms_meeting_reminder        (coverage-audit gap #3, 2026-07-09)
  comms.sendivo_outbound_log -> raw_comms_sendivo_outbound_log  (coverage-audit gap #3, 2026-07-09)
  config.kill_switch       -> raw_comms_kill_switch             (coverage-audit gap #3, 2026-07-09)
  config.worker_config     -> raw_comms_worker_config           (coverage-audit gap #3; value REDACTED for secret keys)
  config.brand_followup_cap -> raw_comms_brand_followup_cap     (coverage-audit gap #3, 2026-07-09)
  config.iskra_watchdog_state -> raw_comms_iskra_watchdog_state (coverage-audit gap #3, 2026-07-09)
  comms.instantly_lead_state_event -> raw_comms_instantly_lead_state_event (opp-state ledger, 2026-07-12)

SECURITY (config.worker_config): its ``value`` column holds live secrets
(worker_secret, cc_slack_bot_token). ``_COLUMN_EXPRS`` swaps ``value`` for
'<redacted>' on secret-pattern keys in the SELECT so secret material never
lands in the warehouse (which is served broadly via the read-only query API).

Explicitly NOT mirrored: comms.webhook_receipt, comms.alert_throttle,
comms.iskra_conversation.
  * comms.webhook_receipt (~6.18M rows) is a raw pre-processing webhook log —
    intentionally excluded as noise (no analytic value; the processed signal
    already lands in the other mirrored tables). Do NOT add it here.
  * comms.alert_throttle is ops throttle state (no analytic value);
    comms.iskra_conversation is a rebuildable poll cache of an API that is
    already mirrored richer (raw_iskra_conversations).

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
    # ── Lumara apply-form applications (2026-06-11): the AIM app-link funnel's
    # conversion event. Webform → worker /apply/webhook → this table → partner
    # CRM (GBC; Ken round-robin planned). DDL in sql/ddl/56_lead_application_mirror.sql.
    ("comms", "lead_application", "raw_comms_lead_application"),
    # ── Mirror-coverage-audit gap #3 (2026-07-09): the remaining small comms-hub
    # tables. DDL in sql/ddl/1092_comms_mirror_gap_ops_config.sql. All tiny —
    # full-refresh REPLACE like the rest.
    # SMS cold-blast copy provenance (feeds Close "Original Message" fallback):
    ("comms", "campaign_blast_template", "raw_comms_campaign_blast_template"),
    # Reminder/no-show funnel (T-1h email / T-30m SMS latches + call-window stamps):
    ("comms", "meeting_reminder", "raw_comms_meeting_reminder"),
    # Standalone /sms/logs reconciliation mirror w/ blast_id enrichment:
    ("comms", "sendivo_outbound_log", "raw_comms_sendivo_outbound_log"),
    # config.* ops/config forensics (worker_config value is REDACTED for secret
    # keys via _COLUMN_EXPRS — see SECURITY note in the module docstring):
    ("config", "kill_switch", "raw_comms_kill_switch"),
    ("config", "worker_config", "raw_comms_worker_config"),
    ("config", "brand_followup_cap", "raw_comms_brand_followup_cap"),
    ("config", "iskra_watchdog_state", "raw_comms_iskra_watchdog_state"),
    # ── Instantly opp-state LEDGER (comms migs 042/043, 2026-07-12): append-only
    # history of lead interest-state changes swept every ~30 min from the
    # Instantly API (which keeps NO event history — relabels overwrite state, so
    # this ledger is the only faithful record; daily-opp / ever-opp derived
    # views build on it warehouse-side). DDL in
    # sql/ddl/1099_instantly_lead_state_event_mirror.sql. No jsonb/enum/array
    # columns → no casts. Full-refresh REPLACE like the rest (source is
    # append-only, so the snapshot only grows).
    ("comms", "instantly_lead_state_event", "raw_comms_instantly_lead_state_event"),
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
    # ── lead_application (2026-06-11): id is uuid, raw is jsonb.
    "raw_comms_lead_application": {"id", "raw"},
    # ── coverage-audit gap #3 (2026-07-09): jsonb columns on the new tables.
    "raw_comms_meeting_reminder": {"metadata"},
    "raw_comms_sendivo_outbound_log": {"raw"},
}

# Per-table column-expression overrides for the mirror SELECT. Unlike
# _CAST_TO_VARCHAR (a plain CAST), these swap the whole column expression —
# used to REDACT secret material so it never lands in the warehouse (which is
# served broadly via the read-only query API). Expressions run DuckDB-side over
# the attached pg table; unqualified column refs are unambiguous (single-table
# SELECT). Keyed by raw table name, then column name.
_COLUMN_EXPRS: dict[str, dict[str, str]] = {
    # config.worker_config holds live secrets in `value` (worker_secret,
    # cc_slack_bot_token). Keep key names + non-secret values (worker_url,
    # iskra_push_enabled, …) — redact anything secret-shaped, incl. future keys.
    "raw_comms_worker_config": {
        "value": (
            "CASE WHEN lower(key) LIKE '%secret%' OR lower(key) LIKE '%token%' "
            "OR lower(key) LIKE '%password%' OR lower(key) LIKE '%api_key%' "
            "OR lower(key) LIKE '%apikey%' OR lower(key) LIKE '%credential%' "
            "THEN '<redacted>' ELSE value END"
        ),
    },
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
    expr_cols = _COLUMN_EXPRS.get(raw_table, {})
    cols = [r[1] for r in ctx.db.execute(f"PRAGMA table_info('{raw_table}')").fetchall()]
    # Drop our bookkeeping cols; we add them explicitly.
    data_cols = [c for c in cols if c not in ("_loaded_at", "_run_id")]
    select_exprs = []
    for c in data_cols:
        if c in expr_cols:
            select_exprs.append(f"{expr_cols[c]} AS {c}")
        elif c in cast_cols:
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
            sql = _build_select(ctx, source_schema, pg_table, raw_table)
            # Atomic REPLACE (warehouse-flags#12): wipe the previous snapshot and
            # load the new one in a single transaction. The old per-_run_id DELETE
            # appended a full copy every run (12-34x duplication, inflated SUMs).
            conn.execute("BEGIN")
            try:
                conn.execute(f"DELETE FROM {raw_table}")
                conn.execute(sql, [ctx.run_id])
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            # Post-replace the table IS this run's snapshot.
            n = conn.execute(f"SELECT count(*) FROM {raw_table}").fetchone()[0]
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
