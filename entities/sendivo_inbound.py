"""Sendivo INBOUND replies, recovered from comms.webhook_receipt (spec 14 granular addendum).

Phase 'sendivo', ingest 'inbound'. The comms-orchestration worker drops ~81%+ of inbound replies
(stale number→brand registry → 'unknown sender_number'), but it RETAINS every raw webhook in
comms.webhook_receipt.raw_payload. We parse those payloads directly — so the warehouse has the full
reply stream (and per-campaign reply/opt-out attribution via the sending number) independent of the
broken worker. Full-refresh per run (134k rows, cheap), idempotent by _run_id.

Payload shape (webhook_type='sendivo_inbound'):
  {"event":"inbound_message","data":{"to","from","message","conversation_id","sub_account_name",
    "received_at","message_id","contact":{"email","first_name","last_name","phone_number"}}}
"""
from __future__ import annotations

import logging

from core.registry import PhaseResult, Registry, RunContext

logger = logging.getLogger("entities.sendivo_inbound")

_OPT_OUT_RE = "^(stop|stopall|unsubscribe|unsub|end|quit|cancel|optout|opt out|opt-out|remove)"

_DDL = """
CREATE TABLE IF NOT EXISTS raw_sendivo_inbound (
    inbound_message_id VARCHAR, received_at TIMESTAMPTZ, prospect_number VARCHAR,
    our_number VARCHAR, message VARCHAR, is_opt_out BOOLEAN, sub_account_name VARCHAR,
    sendivo_conversation_id BIGINT, contact_email VARCHAR, contact_first_name VARCHAR,
    contact_last_name VARCHAR, webhook_receipt_id BIGINT, processed_by_worker BOOLEAN,
    _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR);
"""


def register(registry: Registry) -> None:
    registry.add_phase("sendivo", "inbound", run_sendivo_inbound)


def run_sendivo_inbound(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id
    pg_url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
    conn.execute(_DDL)

    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")
    try:
        conn.execute("DELETE FROM raw_sendivo_inbound WHERE _run_id = ?", [run_id])
        conn.execute(
            f"""
            INSERT INTO raw_sendivo_inbound
            SELECT
              json_extract_string(p, '$.data.message_id'),
              try_cast(json_extract_string(p, '$.data.received_at') AS TIMESTAMPTZ),
              json_extract_string(p, '$.data.from'),
              json_extract_string(p, '$.data.to'),
              json_extract_string(p, '$.data.message'),
              regexp_matches(lower(trim(coalesce(json_extract_string(p, '$.data.message'), ''))), '{_OPT_OUT_RE}'),
              json_extract_string(p, '$.data.sub_account_name'),
              try_cast(json_extract_string(p, '$.data.conversation_id') AS BIGINT),
              json_extract_string(p, '$.data.contact.email'),
              json_extract_string(p, '$.data.contact.first_name'),
              json_extract_string(p, '$.data.contact.last_name'),
              wid, processed, now(), ?
            FROM (
              SELECT id AS wid, processed, CAST(raw_payload AS VARCHAR) AS p
              FROM pg.comms.webhook_receipt
              WHERE webhook_type = 'sendivo_inbound'
            )
            """,
            [run_id],
        )
        n = conn.execute(
            "SELECT count(*) FROM raw_sendivo_inbound WHERE _run_id = ?", [run_id]
        ).fetchone()[0]
        conn.execute("DETACH pg")
    except Exception:
        try:
            conn.execute("DETACH pg")
        except Exception:
            pass
        raise

    logger.info("sendivo inbound: %d rows", n)
    return PhaseResult(rows_in=n, rows_out=n, notes={"inbound_rows": n})
