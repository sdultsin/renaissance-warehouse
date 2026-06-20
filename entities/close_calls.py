"""WS-B — Close warm-call ingest (structured, no transcript).

Pulls call activity from Close (incremental by date_updated) into raw_close_call, then
rebuilds core.call / core.warm_caller / core.call_outcome from raw. lead_email /
phone_e164 / source_campaign / source_channel are resolved by fetching the Close lead
(GET /lead/{id}/) — leads are cached so each lead is fetched at most once per run.

Runs in the `close` phase. Idempotent: raw is UPSERT-on-id; the three core tables are
DELETE+INSERT rebuilds from raw. core.call_transcript is created (by the DDL) but left
empty for WS-H. Outcome classification is DETERMINISTIC (no LLM here).

See sql/ddl/42_close_calls.sql.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.close import CloseClient, CloseError

logger = logging.getLogger("entities.close_calls")

_DDL = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "42_close_calls.sql"

# Close lead custom-field ids for attribution. These are account-specific CRM field
# ids — sourced from env so they are not hard-coded in a public repo. Attribution
# degrades gracefully (falls back to the friendly custom dict) if they are unset.
_CF_CAMPAIGN = os.environ.get("CLOSE_CF_CAMPAIGN", "")
_CF_SOURCE = os.environ.get("CLOSE_CF_SOURCE", "")

_RAW_COLS = [
    "id", "_type", "lead_id", "contact_id", "direction", "disposition", "status",
    "duration", "recording_duration", "recording_url", "has_recording", "voicemail_url",
    "voicemail_duration", "outcome_id", "outcome_reason", "note", "note_html", "cost",
    "local_phone", "remote_phone", "phone", "user_id", "user_name", "source",
    "date_created", "date_answered", "date_updated", "organization_id",
    "api_response_raw", "_loaded_at", "_run_id",
]
_RAW_PLACEHOLDERS = ", ".join("?" for _ in _RAW_COLS)
_RAW_UPDATE_SET = ", ".join(f"{c} = excluded.{c}" for c in _RAW_COLS if c != "id")
_RAW_UPSERT_SQL = (
    f"INSERT INTO raw_close_call ({', '.join(_RAW_COLS)}) "
    f"VALUES ({_RAW_PLACEHOLDERS}) "
    f"ON CONFLICT (id) DO UPDATE SET {_RAW_UPDATE_SET}"
)

# Separators that end a caller name in the note prefix.
_CALLER_SEP = re.compile(r"[\s\-—–:|,;]+")

# Tokens that look like names but are outcome keywords, not caller names.
_NOT_NAMES = frozenset({
    "not", "no", "yes", "dnc", "wrong", "remove", "interested", "appointment",
    "booked", "scheduled", "schedule", "set", "callback", "voicemail", "vm",
    "answer", "answered", "busy", "left", "message", "number", "hang", "hung",
    "follow", "call", "called", "calling", "this", "cant", "cannot", "dead",
    "grabbed", "for", "send", "sent", "spoke", "spoke", "talked", "says",
    "said", "will", "wants", "asked", "need", "needs", "seems", "very",
    "already", "still", "just", "said", "good", "bad", "great", "maybe",
})


# ── pure helpers (unit-testable; no DB) ───────────────────────────────────────

def parse_caller_name(note: str | None) -> str | None:
    """Extract the warm caller's first name from the start of a Close call note.

    Callers are instructed to begin every note with their first name.
    Handles bare names ("Jamie"), name + separator + details ("Elle - set appt"),
    and case variants ("jamie", "ELLE"). Returns None for blank notes or tokens
    that don't look like a name (non-alpha or single character).
    """
    if not note:
        return None
    token = _CALLER_SEP.split(note.strip(), maxsplit=1)[0]
    if not token or not token.isalpha() or len(token) < 2:
        return None
    if token.lower() in _NOT_NAMES:
        return None
    return token.title()


def _to_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _parse_dt(v):
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
    except (TypeError, ValueError):
        return None


def resolve_lead_attrs(lead: dict | None) -> dict:
    """Extract (lead_email, phone_e164, source_campaign, source_channel) from a Close lead.

    A Close lead carries contacts[].emails[].email, contacts[].phones[].phone, and BOTH a
    `custom` dict ({'Campaign':…,'Source':…}) and flattened `custom.cf_*` keys. We read the
    flattened cf_* keys (stable across renames) and fall back to the friendly dict.
    Sendivo leads are phone-only → email stays None.
    """
    if not lead:
        return {"lead_email": None, "phone_e164": None,
                "source_campaign": None, "source_channel": None}

    email = None
    phone = None
    for ct in lead.get("contacts") or []:
        for e in ct.get("emails") or []:
            if e.get("email"):
                email = e["email"].strip().lower()
                break
        if email:
            break
    for ct in lead.get("contacts") or []:
        for p in ct.get("phones") or []:
            if p.get("phone"):
                phone = p["phone"]
                break
        if phone:
            break

    custom = lead.get("custom") if isinstance(lead.get("custom"), dict) else {}
    campaign = lead.get(f"custom.{_CF_CAMPAIGN}") or custom.get("Campaign")
    channel = lead.get(f"custom.{_CF_SOURCE}") or custom.get("Source")
    return {
        "lead_email": email,
        "phone_e164": phone,
        "source_campaign": campaign,
        "source_channel": channel,
    }


def classify_outcome(disposition: str | None,
                     voicemail_duration: int | None, voicemail_url: str | None) -> str:
    """Disposition-only outcome_class. Note is ground truth — not parsed here.

    Three classes only:
      voicemail   — vm-left disposition or voicemail_url/voicemail_duration present
      no_answer   — no-answer, busy, error, canceled
      answered    — anything else (connected in some way)
    """
    disp = (disposition or "").strip().lower()
    vm = (voicemail_duration or 0) > 0 or bool(voicemail_url)

    if disp in ("vm-left", "voicemail") or vm:
        return "voicemail"
    if disp in ("no-answer", "no_answer", "busy", "failed", "error", "canceled", "cancelled"):
        return "no_answer"
    return "answered"


# ── DB transform (shared by run() and the local test) ─────────────────────────

def upsert_raw(conn, call: dict, run_id: str, now: datetime) -> None:
    vals = [
        call.get("id"),
        call.get("_type"),
        call.get("lead_id"),
        call.get("contact_id"),
        call.get("direction"),
        call.get("disposition"),
        call.get("status"),
        _to_int(call.get("duration")),
        _to_int(call.get("recording_duration")),
        call.get("recording_url"),
        bool(call.get("has_recording")),
        call.get("voicemail_url"),
        _to_int(call.get("voicemail_duration")),
        call.get("outcome_id"),
        call.get("outcome_reason"),
        call.get("note"),
        call.get("note_html"),
        call.get("cost"),
        call.get("local_phone"),
        call.get("remote_phone"),
        call.get("phone"),
        call.get("user_id"),
        call.get("user_name"),
        call.get("source"),
        _parse_dt(call.get("date_created")),
        _parse_dt(call.get("date_answered")),
        _parse_dt(call.get("date_updated")),
        call.get("organization_id"),
        json.dumps(call),
        now,
        run_id,
    ]
    conn.execute(_RAW_UPSERT_SQL, vals)


def rebuild_core(conn, lead_cache: dict, now: datetime) -> dict:
    """DELETE+INSERT rebuild of core.call / core.warm_caller / core.call_outcome from raw.

    lead_cache maps lead_id -> resolved attrs dict (from resolve_lead_attrs). Returns a
    notes dict with attribution coverage stats.
    """
    raw = conn.execute(
        """
        SELECT id, lead_id, direction, disposition, duration, has_recording,
               recording_url, cost, date_created, date_answered, note, note_html,
               voicemail_duration, voicemail_url, user_id, user_name, remote_phone, phone
        FROM raw_close_call
        """
    ).fetchall()
    cols = ["id", "lead_id", "direction", "disposition", "duration", "has_recording",
            "recording_url", "cost", "date_created", "date_answered", "note", "note_html",
            "voicemail_duration", "voicemail_url", "user_id", "user_name", "remote_phone", "phone"]

    call_rows = []
    outcome_rows = []
    # warm_caller aggregation: per real user + a global 'ALL'.
    agg: dict[str, dict] = {}

    def _bump(key, user_id, user_name, connected):
        d = agg.setdefault(key, {"user_id": user_id, "user_name": user_name,
                                 "calls": 0, "connected": 0})
        d["calls"] += 1
        if connected:
            d["connected"] += 1
        # keep a name if we have one
        if user_name and not d.get("user_name"):
            d["user_name"] = user_name

    campaign_present = 0
    for r in raw:
        rec = dict(zip(cols, r))
        attrs = lead_cache.get(rec["lead_id"]) or resolve_lead_attrs(None)
        occurred = rec["date_answered"] or rec["date_created"]
        phone = attrs["phone_e164"] or rec["remote_phone"] or rec["phone"]
        connected = (rec["disposition"] or "").lower() == "answered"
        uid = rec["user_id"] or "unknown"

        call_rows.append([
            rec["id"], rec["lead_id"], attrs["lead_email"], phone,
            "ALL", rec["user_id"], rec["user_name"], parse_caller_name(rec["note"]),
            rec["direction"], rec["disposition"],
            _to_int(rec["duration"]), bool(rec["has_recording"]), rec["recording_url"],
            _to_float(rec["cost"]), occurred, attrs["source_campaign"],
            attrs["source_channel"], now,
        ])
        if attrs["source_campaign"]:
            campaign_present += 1

        oc = classify_outcome(rec["disposition"], rec["voicemail_duration"], rec["voicemail_url"])
        outcome_rows.append([rec["id"], oc, rec["note"], now])

        _bump("ALL", None, None, connected)
        _bump(uid, rec["user_id"], rec["user_name"], connected)

    # Rebuild core.call
    conn.execute("DELETE FROM core.call")
    if call_rows:
        conn.executemany(
            "INSERT INTO core.call (call_id, close_lead_id, lead_email, phone_e164, "
            "warm_caller_id, user_id, user_name, caller_name, direction, disposition, "
            "duration_seconds, has_recording, recording_url, cost, occurred_at, "
            "source_campaign, source_channel, resolved_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            call_rows,
        )

    # Rebuild core.call_outcome
    conn.execute("DELETE FROM core.call_outcome")
    if outcome_rows:
        conn.executemany(
            "INSERT INTO core.call_outcome (call_id, outcome_class, note, resolved_at) "
            "VALUES (?,?,?,?)",
            outcome_rows,
        )

    # Rebuild core.warm_caller
    conn.execute("DELETE FROM core.warm_caller")
    wc_rows = []
    for key, d in agg.items():
        calls = d["calls"]
        connected = d["connected"]
        rate = (connected / calls) if calls else None
        wc_rows.append([
            key, d["user_id"], d["user_name"], calls, connected, rate, None, now,
        ])
    if wc_rows:
        conn.executemany(
            "INSERT INTO core.warm_caller (warm_caller_id, user_id, user_name, calls, "
            "connected_calls, connect_rate, appt_set_calls, resolved_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            wc_rows,
        )

    return {
        "core_calls": len(call_rows),
        "warm_callers": len(wc_rows),
        "campaign_attributed": campaign_present,
        "campaign_attributed_pct": round(campaign_present / len(call_rows), 4) if call_rows else None,
    }


# ── phase entrypoint ──────────────────────────────────────────────────────────

def register(registry: Registry) -> None:
    registry.add_phase("close", "close_calls", run)


def run(ctx: RunContext) -> PhaseResult:
    api_key = ctx.credentials.optional("CLOSE_API_KEY")
    if not api_key:
        logger.warning("No CLOSE_API_KEY — skipping Close call ingest")
        return PhaseResult(notes={"reason": "no_key"})

    conn = ctx.db
    conn.execute(_DDL.read_text())

    # Incremental watermark = max date_updated already stored.
    row = conn.execute("SELECT max(date_updated) FROM raw_close_call").fetchone()
    since = row[0] if row and row[0] is not None else None
    if since is not None and since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    logger.info("Close ingest watermark since=%s", since.isoformat() if since else "(full backfill)")

    now = datetime.now(timezone.utc)
    rows_in = 0
    lead_ids: set[str] = set()
    failures: list[dict] = []

    try:
        with CloseClient(api_key) as client:
            for call in client.iter_calls(since=since):
                if not call.get("id"):
                    continue
                upsert_raw(conn, call, ctx.run_id, now)
                rows_in += 1
                if call.get("lead_id"):
                    lead_ids.add(call["lead_id"])

            # Resolve every lead referenced by ANY call in raw (not just this pull), so a
            # rebuild after an incremental pull still attributes older calls. Cache per id.
            all_lead_ids = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT lead_id FROM raw_close_call WHERE lead_id IS NOT NULL"
                ).fetchall()
            ]
            lead_cache: dict[str, dict] = {}
            orphans = 0
            for lid in all_lead_ids:
                lead = client.get_lead(lid)
                if lead is None:
                    orphans += 1
                lead_cache[lid] = resolve_lead_attrs(lead)
    except CloseError as exc:
        logger.error("Close API error: %s", exc)
        failures.append({"error": str(exc)[:300]})
        return PhaseResult(rows_in=rows_in, rows_out=0,
                           notes={"failures": failures, "since": since.isoformat() if since else None})

    stats = rebuild_core(conn, lead_cache, now)
    notes = {
        "since": since.isoformat() if since else None,
        "new_or_updated_calls": rows_in,
        "leads_resolved": len(lead_cache),
        "orphan_leads": orphans,
        **stats,
        "failures": failures,
    }
    logger.info("Close ingest: %s", notes)
    return PhaseResult(rows_in=rows_in, rows_out=stats["core_calls"], notes=notes)
