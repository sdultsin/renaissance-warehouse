"""WS-G — core.conversion_event (Spec 16, BI / Lead-Intent layer).

ONE unifying row per appointment / booking across every conversion agent and channel.
Rebuilt DELETE+INSERT (idempotent full rebuild) from three feeders:

  • core.meeting                       → agent='im',          type='meeting_booked'
        feeder='slack_meeting' — the original Slack-scrape count feed; NO lead identity.
  • core.call ⋈ core.v_call_outcome_final → agent='warm_caller', type='appointment_set'
        WHERE outcome_class = 'answered_appt_set'
        feeder='close_call'. W1i (2026-06-26): the FINAL outcome view coalesces the rep
        note-regex booking marker ('booked via call', deterministic $0 floor) > the LLM
        transcript pass (core.call_outcome_llm) > the disposition base (core.call_outcome).
        classify_outcome() never emits 'answered_appt_set', so before W1i this feeder was
        always 0 — every call appointment was invisible to the conversion fact.
  • raw_im_bookings (latest snapshot)  → agent='im',          type='meeting_booked'
        feeder='portal_im_bookings' — Scope A (2026-06-09): the identity-bearing feed from
        Darcy's bookings portal (~99.9% email, ~97% phone). This is what makes per-lead
        conversion analysis possible (response-time × conversion etc.).

⚠ COUNTING RULE (see sql/ddl/54_conversion_event_feeder.sql): the same physical IM booking
can appear via BOTH 'slack_meeting' and 'portal_im_bookings' (no row-level join key exists
between the feeds). Never count(*) across feeders for "total meetings" — filter feeder.
Identity joins are safe: only portal rows carry lead_email/phone.

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
_DDL_FEEDER = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "54_conversion_event_feeder.sql"
# W1i (2026-06-26): the warm-caller appt-set refinement layer — core.call_outcome_llm (the
# LLM transcript pass output) + core.v_call_outcome_final (note-regex 'booked via call' >
# LLM > disposition). Feeder 2 reads the FINAL outcome from this view so call appointments
# (which classify_outcome never emits) finally land in core.conversion_event. Idempotent.
_DDL_CALL_OUTCOME_LLM = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "1024_call_outcome_llm.sql"


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
    db.execute(_DDL_FEEDER.read_text())  # idempotent ADD COLUMN feeder + backfill
    db.execute(_DDL_CALL_OUTCOME_LLM.read_text())  # W1i: core.call_outcome_llm + v_call_outcome_final (idempotent)

    has_lead = _table_exists(db, "core", "lead")
    has_meeting = _table_exists(db, "core", "meeting")
    has_call = _table_exists(db, "core", "call")
    has_call_outcome = _table_exists(db, "core", "call_outcome")
    has_portal = _table_exists(db, "main", "raw_im_bookings")

    # [2026-07-21] Do NOT delete THROUGH the secondary indexes — that is what has been killing the
    # nightly. On 2026-07-21 23:06 this exact DELETE raised
    #   "Invalid Input Error: Failed to delete all rows from index. Only deleted 627 out of 901 rows"
    # which poisons the DuckDB connection, so the orchestrator aborts the whole run with "no further
    # phases will run". rollup_history lives LATER in this same `canonical` phase, which is why
    # core.provider_history / deliverability_history / batch_history froze at 2026-07-19. The same
    # error has hit `core.canonical.domain` too — 12 occurrences in logs/nightly.log.
    #
    # It is NOT inherent to the pattern: a scratch table built from the same 89,898 rows with the
    # same four indexes DELETEs fine, and survived 12 consecutive DELETE+INSERT cycles. So it is
    # accumulated ART state in the live build DB. Rather than repair that state by hand, drop the
    # secondary indexes for the rebuild and recreate them after — the rebuild then never deletes
    # through an index, AND every run leaves freshly-built indexes, so the corruption cannot
    # accumulate again. These are non-unique lookup indexes: dropping them touches no data and
    # rebuilding 89k rows is trivial.
    #
    # NOTE THE SCHEMA QUALIFIER. DuckDB namespaces indexes: `DROP INDEX ix_core_conv_event_agent`
    # raises "does not exist! Did you mean core.ix_...", and the IF EXISTS form SILENTLY DOES
    # NOTHING — a repair written the obvious way reports success and changes nothing.
    # DuckDB is ASYMMETRIC here and it is a trap: DROP INDEX *requires* the schema qualifier
    # (`core.ix_…`), while CREATE INDEX *rejects* it ("Parser Error: syntax error at or near .")
    # and takes the schema from the table. Get that wrong and you drop all four and cannot put
    # them back — caught by the cycle test, not by reading. So keep BOTH forms.
    _IDX = [("ix_core_conv_event_agent",    "conversion_agent"),
            ("ix_core_conv_event_channel",  "source_channel"),
            ("ix_core_conv_event_occurred", "occurred_at"),
            ("ix_core_conv_event_campaign", "campaign_id")]
    _dropped = []
    for _name, _col in _IDX:
        try:
            db.execute(f"DROP INDEX core.{_name}")     # qualified: required by DROP
            _dropped.append((_name, _col))             # bare: required by CREATE
        except Exception as _e:      # already absent is fine; anything else must not abort the run
            logger.info("conversion_event: index %s not dropped (%s)", _name, str(_e)[:80])

    db.execute("DELETE FROM core.conversion_event")

    n_im = 0
    n_wc = 0
    n_portal = 0

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
               warm_caller_id, resolved_at, feeder)
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
              ?                         AS resolved_at,
              'slack_meeting'           AS feeder
            FROM core.meeting
            """,
            [now],
        )
        n_im = db.execute("SELECT count(*) FROM core.conversion_event WHERE feeder='slack_meeting'").fetchone()[0]
    else:
        logger.warning("core.meeting absent — skipping IM meeting feeder")

    # ── Feeder 1b (Scope A, 2026-06-09): portal bookings (raw_im_bookings) ───────
    # The identity-bearing IM booking feed: Darcy's CEO-Portal im_bookings table, mirrored
    # nightly by entities/im_bookings.py (latest snapshot only — the frozen 2026-05-31
    # snapshot is a strict subset historically). Carries prospect email (~99.9%) + phone
    # (~97%). occurred_at = portal `date` parsed + CLAMPED (the portal has date-entry typos,
    # e.g. 2027-05-28): outside [2024-01-01, today+14d] → NULL (row kept; identity joins
    # don't need the date). phone normalized to E.164 best-effort (US default).
    # campaign_id left NULL: the portal `campaign` is a display NAME, not an Instantly
    # campaign id — resolving names across workspaces is unreliable; the raw table keeps it.
    if has_portal:
        db.execute(
            """
            INSERT INTO core.conversion_event
              (event_id, lead_key, lead_email, phone_e164, source_channel,
               conversion_agent, conversion_type, occurred_at, campaign_id,
               warm_caller_id, resolved_at, feeder)
            SELECT
              md5('im_portal:' || id)              AS event_id,
              NULL                                 AS lead_key,
              nullif(lower(trim(email)), '')       AS lead_email,
              CASE
                WHEN length(regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 10
                  THEN '+1' || regexp_replace(phone, '[^0-9]', '', 'g')
                WHEN length(regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g')) = 11
                     AND regexp_replace(phone, '[^0-9]', '', 'g') LIKE '1%'
                  THEN '+' || regexp_replace(phone, '[^0-9]', '', 'g')
                ELSE NULL
              END                                  AS phone_e164,
              'cold_email'                         AS source_channel,
              'im'                                 AS conversion_agent,
              'meeting_booked'                     AS conversion_type,
              CASE
                WHEN try_cast(date AS DATE) BETWEEN DATE '2024-01-01'
                     AND current_date + INTERVAL 14 DAY
                  THEN try_cast(date AS TIMESTAMPTZ)
              END                                  AS occurred_at,
              NULL                                 AS campaign_id,
              NULL                                 AS warm_caller_id,
              ?                                    AS resolved_at,
              'portal_im_bookings'                 AS feeder
            FROM raw_im_bookings
            WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM raw_im_bookings)
            """,
            [now],
        )
        n_portal = db.execute(
            "SELECT count(*) FROM core.conversion_event WHERE feeder='portal_im_bookings'"
        ).fetchone()[0]
    else:
        logger.warning("raw_im_bookings absent — skipping portal booking feeder")

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
               warm_caller_id, resolved_at, feeder)
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
              ?                                 AS resolved_at,
              'close_call'                      AS feeder
            FROM core.call c
            JOIN core.v_call_outcome_final o ON o.call_id = c.call_id
            WHERE o.outcome_class = 'answered_appt_set'
            """,
            [now],
        )
        n_wc = db.execute(
            "SELECT count(*) FROM core.conversion_event WHERE conversion_agent='warm_caller'"
        ).fetchone()[0]

        # W1i: fill core.warm_caller.appt_set_calls (DDL 42 left it NULL — "WS-G fills the
        # appt-set signal"). Per-rep rows count that user's appt-set calls; the 'ALL' aggregate
        # = the channel total. Final outcome = note-regex 'booked via call' > LLM > disposition.
        try:
            db.execute(
                """
                UPDATE core.warm_caller w SET appt_set_calls = COALESCE((
                    SELECT count(*)
                    FROM core.call c
                    JOIN core.v_call_outcome_final o ON o.call_id = c.call_id
                    WHERE o.outcome_class = 'answered_appt_set'
                      AND (w.warm_caller_id = 'ALL' OR c.user_id = w.user_id)
                ), 0)
                """
            )
        except Exception as exc:  # never let the rollup update abort the conversion rebuild
            logger.warning("warm_caller.appt_set_calls fill skipped (%s)", exc)
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

    # Put the secondary indexes back, freshly built from the rows just inserted. Recreating is what
    # makes this self-healing: even if the ART had drifted, the next run starts from a clean one.
    # Never let this abort the phase — the DATA is already correct at this point, and a missing
    # lookup index is a performance issue, not a correctness one ([[feedback_no_breaking_guards]]).
    for _name, _col in _dropped:
        try:
            db.execute(f"CREATE INDEX IF NOT EXISTS {_name} ON core.conversion_event ({_col})")
        except Exception as _e:  # noqa: BLE001
            logger.error("conversion_event: could NOT recreate index %s (%s) — data is fine, "
                         "lookups on that column will be slower until the next run.",
                         _name, str(_e)[:120])
    if _dropped:
        logger.info("conversion_event: rebuilt %d secondary index(es) after the swap", len(_dropped))

    total = db.execute("SELECT count(*) FROM core.conversion_event").fetchone()[0]
    null_channel = db.execute(
        "SELECT count(*) FROM core.conversion_event WHERE source_channel IS NULL"
    ).fetchone()[0]
    return {
        "im_meeting_booked_slack": n_im,
        "im_meeting_booked_portal": n_portal,
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
