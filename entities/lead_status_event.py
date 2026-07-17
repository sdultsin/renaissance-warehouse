"""main.raw_lead_status_event — nightly append of NON-REPLY lead status events (DDL 1124).

Two sources, both already nightly-fresh by the `derived` phase:

  meeting_booked (R34a) — core.v_meeting_truth (portal im_bookings SoT via
      core.meeting_rebuilt + core.meeting; all eras, all channels, deduped
      email/phone+day). event_ts follows the 1114 convention:
      COALESCE(meeting_date::TIMESTAMPTZ, posted_at). Rows without a recoverable
      lead_email are NOT ingested (they cannot join the per-lead ledger — the
      meetings lane still counts them; honest gap, ~42 rows all-time).

  call_logged (R26) — core.call (a full DELETE+INSERT rebuild of raw_close_call
      every `close` phase; raw_close_call is UPSERT-on-id and never pruned, and
      its incremental watermark started from a FULL pull — verified 2026-07-17
      against the Close API: zero call activities exist before 2026-06-01, and
      raw min(date_created)=2026-06-01, so raw == the account's complete history)
      + core.v_call_outcome_final for the outcome. lead_email resolves for ~78%
      of calls (Close lead fetch); phone_e164 is carried on ~100% so phone-grain
      matching stays possible. If raw were ever truncated upstream, the anti-join
      grain means already-appended events are RETAINED here (append-only ledger).

Idempotent: anti-join on the uniqueness grain (event_type, source, source_ref).
The FIRST run is the full backfill; every later run appends the increment.
Append-only: upstream deletions never delete events.

Run via the orchestrator only:
      --phase derived --ingest lead_status_event
(NEVER `python -m entities.lead_status_event` — entities are orchestrator phases
with no __main__; a bare -m run exits 0 having done nothing.)
"""

from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.lead_status_event")

# CONTRACT NOTE (gate annotate_consumer): the INSERT column lists below reference
# main.raw_lead_status_event columns (event_ts et al.) created by DDL 1124, which
# ships IN THE SAME change-set and applies before this entity's first nightly run.
# The run() guard additionally SKIPs (loudly) until the table exists — no drift window.

_MEETING_LOAD = """
INSERT INTO main.raw_lead_status_event
    (event_id, event_type, workspace_slug, lead_email, phone_e164, event_ts,
     source, source_ref, outcome, detail, _run_id)
SELECT
    md5('meeting_booked|v_meeting_truth|' || mt.meeting_key),
    'meeting_booked',
    mt.workspace_slug,
    lower(mt.lead_email),
    NULL,
    COALESCE(CAST(mt.meeting_date AS TIMESTAMPTZ), mt.posted_at),
    'v_meeting_truth',
    mt.meeting_key,
    NULL,
    CAST(to_json(struct_pack(
        channel := mt.channel_norm,
        era := mt.era,
        offer := mt.offer,
        is_ours := mt.is_ours,
        in_funding_scope := mt.in_funding_scope,
        posted_at := mt.posted_at,
        meeting_date := mt.meeting_date,
        partner := mt.partner
    )) AS VARCHAR),
    $run_id
FROM core.v_meeting_truth mt
ANTI JOIN main.raw_lead_status_event t
  ON t.event_type = 'meeting_booked'
 AND t.source     = 'v_meeting_truth'
 AND t.source_ref = mt.meeting_key
WHERE mt.lead_email IS NOT NULL AND mt.lead_email <> ''
QUALIFY row_number() OVER (PARTITION BY mt.meeting_key ORDER BY mt.posted_at) = 1
"""

_CALL_LOAD = """
INSERT INTO main.raw_lead_status_event
    (event_id, event_type, workspace_slug, lead_email, phone_e164, event_ts,
     source, source_ref, outcome, detail, _run_id)
SELECT
    md5('call_logged|close|' || c.call_id),
    'call_logged',
    NULL,
    lower(NULLIF(c.lead_email, '')),
    c.phone_e164,
    c.occurred_at,
    'close',
    c.call_id,
    COALESCE(o.outcome_class, c.disposition),
    CAST(to_json(struct_pack(
        direction := c.direction,
        duration_seconds := c.duration_seconds,
        user_name := c.user_name,
        caller_name := c.caller_name,
        source_campaign := c.source_campaign,
        source_channel := c.source_channel,
        disposition := c.disposition,
        outcome_source := o.outcome_source
    )) AS VARCHAR),
    $run_id
FROM core.call c
LEFT JOIN (
    SELECT *
    FROM core.v_call_outcome_final
    QUALIFY row_number() OVER (PARTITION BY call_id ORDER BY resolved_at DESC) = 1
) o ON o.call_id = c.call_id
ANTI JOIN main.raw_lead_status_event t
  ON t.event_type = 'call_logged'
 AND t.source     = 'close'
 AND t.source_ref = c.call_id
WHERE c.call_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY c.call_id ORDER BY c.occurred_at) = 1
"""


def _exists(conn, schema: str, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
    )


def run(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _exists(conn, "main", "raw_lead_status_event"):
        logger.warning("lead_status_event SKIP: main.raw_lead_status_event missing (DDL 1124 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "table missing"})

    notes: dict = {}
    before = conn.execute("SELECT count(*) FROM main.raw_lead_status_event").fetchone()[0]

    # meeting_booked (R34a)
    if _exists(conn, "core", "v_meeting_truth"):
        conn.execute(_MEETING_LOAD, {"run_id": ctx.run_id})
        after_m = conn.execute("SELECT count(*) FROM main.raw_lead_status_event").fetchone()[0]
        notes["meeting_booked_added"] = after_m - before
    else:
        after_m = before
        notes["meeting_booked_added"] = "skipped: core.v_meeting_truth missing"
        logger.warning("lead_status_event: core.v_meeting_truth missing — meeting events skipped this run.")

    # call_logged (R26)
    if _exists(conn, "core", "call") and _exists(conn, "core", "v_call_outcome_final"):
        conn.execute(_CALL_LOAD, {"run_id": ctx.run_id})
        after_c = conn.execute("SELECT count(*) FROM main.raw_lead_status_event").fetchone()[0]
        notes["call_logged_added"] = after_c - after_m
    else:
        after_c = after_m
        notes["call_logged_added"] = "skipped: core.call / v_call_outcome_final missing"
        logger.warning("lead_status_event: core.call or v_call_outcome_final missing — call events skipped this run.")

    mix = dict(conn.execute(
        "SELECT event_type, count(*) FROM main.raw_lead_status_event GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall())
    total = after_c
    loaded = total - before
    notes.update({"table_rows": total, "event_mix": mix})
    logger.info("lead_status_event: +%d events (table now %d). mix: %s", loaded, total, mix)
    return PhaseResult(rows_in=loaded, rows_out=loaded, notes=notes)


def register(registry: Registry) -> None:
    registry.add_phase("derived", "lead_status_event", run)
