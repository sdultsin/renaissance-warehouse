"""WS-G — core.conversion_event (Spec 16, BI / Lead-Intent layer).

ONE unifying row per appointment / booking across every conversion agent and channel.
Rebuilt DELETE+INSERT (idempotent full rebuild) from two feeders:

  • core.meeting                       → agent='im',          type='meeting_booked'
  • core.call ⋈ core.call_outcome      → agent='warm_caller', type='appointment_set'
        WHERE core.call_outcome.outcome_class = 'answered_appt_set'

The agent / channel / type dims are FREE-TEXT VARCHAR (see sql/ddl/45_conversion_event.sql),
so SMS-AIM v1/v2 slot in later by inserting a new string — NO DDL change (DoD item).

lead_key (WS-F core.lead) is joined ONLY if core.lead exists at run time; otherwise it is
left NULL and the row still carries lead_email / phone_e164, so the spine join is a pure
backfill later. lead_email / phone_e164 are themselves NULL where the feeder lacks them
(core.meeting carries no contact identity in v1 — see assumptions below).

ORDERING (within the 'canonical' phase, run order = sorted module filename):
  • core.call / core.call_outcome are built in the EARLIER 'close' phase → always ready.
  • core.meeting is built by entities/meeting.py, ALSO in 'canonical'. 'conversion_event'
    sorts BEFORE 'meeting' (c < m), so on a SINGLE fresh build this reads core.meeting from
    the PRIOR run (DELETE+INSERT rebuilds persist across runs → eventually consistent). For
    strict first-build correctness, conversion_event must run AFTER meeting. We tolerate the
    current order (warehouse is nightly + idempotent) and FLAG it for the parent: to make it
    strictly first-run-correct, register/run this after entities/meeting.py (e.g. rename to
    sort after 'meeting', mirroring entities/domain.py's documented rename pattern).

Registers under the existing 'canonical' phase — core/config.py PHASE_ORDER is untouched.
Schema lives in sql/ddl/45_conversion_event.sql.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.conversion_event")

_DDL = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "45_conversion_event.sql"


def _table_exists(db, schema: str, name: str) -> bool:
    row = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchone()
    return bool(row and row[0])


def _channel_from_call_source(source_channel_expr: str) -> str:
    """SQL CASE mapping core.call.source_channel → conversion_event.source_channel.

    Close stores the lead's origin in a custom field (Instantly | Sendivo). Map:
      Instantly → cold_email,  Sendivo → sms,  anything else → lower(trim()) as-is
      (free-text dim: a new origin needs no code change to PASS THROUGH, only to MAP nicely).
    """
    return f"""
        CASE
          WHEN lower(trim(COALESCE({source_channel_expr}, ''))) = 'instantly' THEN 'cold_email'
          WHEN lower(trim(COALESCE({source_channel_expr}, ''))) = 'sendivo'   THEN 'sms'
          WHEN trim(COALESCE({source_channel_expr}, '')) = ''                 THEN NULL
          ELSE lower(trim({source_channel_expr}))
        END
    """


def rebuild(db, now: datetime) -> dict:
    """DELETE+INSERT rebuild of core.conversion_event from the two feeders. Returns notes."""
    db.execute(_DDL.read_text())  # idempotent CREATE IF NOT EXISTS

    has_lead = _table_exists(db, "core", "lead")
    has_meeting = _table_exists(db, "core", "meeting")
    has_call = _table_exists(db, "core", "call")
    has_call_outcome = _table_exists(db, "core", "call_outcome")

    db.execute("DELETE FROM core.conversion_event")

    n_im = 0
    n_wc = 0

    # ── Feeder 1: IM meetings (core.meeting) → agent='im', type='meeting_booked' ──
    # ASSUMPTION (flagged): core.meeting (v1, Slack success-channel scrape) is the IM
    # booked-meeting feed and carries NO per-meeting lead email/phone → lead_email /
    # phone_e164 NULL here. source = always 'slack' (channel of the *post*, not the lead's
    # outreach channel); the meetings it scrapes are IM cold-email bookings → source_channel
    # is best-effort 'cold_email'. occurred_at = posted_at (the only meeting timestamp).
    if has_meeting:
        db.execute(
            """
            INSERT INTO core.conversion_event
              (event_id, lead_key, lead_email, phone_e164, source_channel,
               conversion_agent, conversion_type, occurred_at, campaign_id,
               warm_caller_id, resolved_at)
            SELECT
              md5('im:' || meeting_id)  AS event_id,
              NULL                      AS lead_key,
              NULL                      AS lead_email,
              NULL                      AS phone_e164,
              'cold_email'              AS source_channel,
              'im'                      AS conversion_agent,
              'meeting_booked'          AS conversion_type,
              posted_at                 AS occurred_at,
              campaign_id,
              NULL                      AS warm_caller_id,
              ?                         AS resolved_at
            FROM core.meeting
            """,
            [now],
        )
        n_im = db.execute("SELECT count(*) FROM core.conversion_event WHERE conversion_agent='im'").fetchone()[0]
    else:
        logger.warning("core.meeting absent — skipping IM meeting feeder")

    # ── Feeder 2: warm-caller appts (core.call ⋈ core.call_outcome) ──────────────
    # outcome_class='answered_appt_set' marks a warm-caller appointment. source_channel
    # mapped from core.call.source_channel (Instantly→cold_email, Sendivo→sms). occurred_at
    # = call.occurred_at. warm_caller_id from core.call (the MVP 'ALL' aggregate today;
    # per-rep split is a later backfill — spec §1).
    if has_call and has_call_outcome:
        db.execute(
            f"""
            INSERT INTO core.conversion_event
              (event_id, lead_key, lead_email, phone_e164, source_channel,
               conversion_agent, conversion_type, occurred_at, campaign_id,
               warm_caller_id, resolved_at)
            SELECT
              md5('warm_caller:' || c.call_id)  AS event_id,
              NULL                              AS lead_key,
              c.lead_email,
              c.phone_e164,
              {_channel_from_call_source('c.source_channel')} AS source_channel,
              'warm_caller'                     AS conversion_agent,
              'appointment_set'                 AS conversion_type,
              c.occurred_at,
              c.source_campaign                 AS campaign_id,
              c.warm_caller_id,
              ?                                 AS resolved_at
            FROM core.call c
            JOIN core.call_outcome o ON o.call_id = c.call_id
            WHERE o.outcome_class = 'answered_appt_set'
            """,
            [now],
        )
        n_wc = db.execute(
            "SELECT count(*) FROM core.conversion_event WHERE conversion_agent='warm_caller'"
        ).fetchone()[0]
    else:
        logger.warning("core.call / core.call_outcome absent — skipping warm-caller feeder")

    # ── lead_key backfill (only if the WS-F lead spine exists) ───────────────────
    # Join core.lead by email first, then phone. Left NULL if core.lead is absent
    # (sibling workstream not yet landed) → pure backfill, no schema change. TODO(WS-F):
    # confirm core.lead column names (assumed: lead_key, lead_email, phone_e164).
    lead_joined = 0
    if has_lead:
        try:
            db.execute(
                """
                UPDATE core.conversion_event AS ce
                SET lead_key = l.lead_key
                FROM core.lead l
                WHERE ce.lead_key IS NULL
                  AND ce.lead_email IS NOT NULL
                  AND lower(trim(ce.lead_email)) = lower(trim(l.email))
                """
            )
            db.execute(
                """
                UPDATE core.conversion_event AS ce
                SET lead_key = l.lead_key
                FROM core.lead l
                WHERE ce.lead_key IS NULL
                  AND ce.phone_e164 IS NOT NULL
                  AND ce.phone_e164 = l.phone_e164
                """
            )
            lead_joined = db.execute(
                "SELECT count(*) FROM core.conversion_event WHERE lead_key IS NOT NULL"
            ).fetchone()[0]
        except Exception as exc:  # core.lead exists but column shape differs — tolerate.
            logger.warning("core.lead present but lead_key join failed (%s) — leaving lead_key NULL", exc)
            lead_joined = 0

    total = db.execute("SELECT count(*) FROM core.conversion_event").fetchone()[0]
    null_channel = db.execute(
        "SELECT count(*) FROM core.conversion_event WHERE source_channel IS NULL"
    ).fetchone()[0]
    return {
        "im_meeting_booked": n_im,
        "warm_caller_appt_set": n_wc,
        "total": total,
        "lead_key_joined": lead_joined,
        "lead_spine_present": has_lead,
        "null_source_channel": null_channel,
    }


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    now = datetime.now(timezone.utc)
    notes = rebuild(db, now)
    logger.info("core.conversion_event rebuilt: %s", notes)
    return PhaseResult(rows_in=notes["total"], rows_out=notes["total"], notes=notes)


def register(registry: Registry) -> None:
    # Ride the existing 'canonical' phase — no PHASE_ORDER edit needed. Must run AFTER the
    # lead spine (WS-F) + close-phase feeders; see the ORDERING note in the module docstring.
    registry.add_phase("canonical", "conversion_event", run)
