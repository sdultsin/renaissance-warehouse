"""WS-H — local Whisper transcription of Close call recordings (Spec 16, BI/Lead-Intent).

Fills core.call_transcript (created empty by sql/ddl/42_close_calls.sql) by running
faster-whisper LOCALLY on the droplet — no paid transcription API. Incremental and
idempotent: each run picks core.call rows that have a recording and no transcript yet,
capped per run so it never runs forever.

PII / disk safety (spec 16 §6):
  - Transcript text is PII and lives ONLY in the warehouse (droplet-only, git-ignored).
    Nothing here is written to any git-tracked path.
  - Audio is downloaded to /tmp and DELETED immediately after each transcription (in a
    `finally`), so the 80%-full droplet never accumulates MP3s.
  - The whisper model loads ONCE per run; compute is CPU-niced so the nightly run stays
    polite under load.

Download note: Close's `recording_url` (GET {recording_url} with HTTP basic
`-u "$CLOSE_API_KEY:"`) returns a 302 to a pre-signed S3 URL — we MUST follow the
redirect. httpx (like curl) strips the Authorization header on the cross-host hop to S3,
so the Close basic auth is only ever sent to api.close.com.

Resilience: each call is wrapped in try/except — a failed download/transcription is logged
and skipped, never aborting the phase.

Registration: rides the existing `close` phase (no core/config.py edit). It MUST run AFTER
close_calls so core.call exists/is fresh. ⚠ Phase registration order is alphabetical by
module filename (orchestrator discovers via sorted glob), and "call_transcription" sorts
BEFORE "close_calls". That only affects same-run freshness: on the very first run core.call
is built by close_calls *after* this entity, so this entity simply sees 0 new rows that run
and catches them up the next run (it is incremental). The DDL is CREATE IF NOT EXISTS, so
the table always exists and this never errors. If the parent wants same-run coverage, run
the `close` phase twice, or have close_calls call into this after its rebuild.

TODO (parent / future WS): enrich core.call_outcome where needs_llm = true (the
'answered_other' rows) by feeding the matching core.call_transcript.transcript to an LLM to
refine outcome_class (e.g. answered_appt_set / answered_not_interested / objection). That is
a separate LLM pass (mirror entities/reply_intent_llm.py) keyed on call_id — described here,
not built, to keep this entity API-free and deterministic in cost.

See sql/ddl/42_close_calls.sql (core.call, core.call_transcript).
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.call_transcription")

_DDL = Path(__file__).resolve().parent.parent / "sql" / "ddl" / "42_close_calls.sql"

# faster-whisper config. 'base' int8 on CPU is fast + adequate for short warm-call audio;
# override with CALL_TRANSCRIBE_MODEL (e.g. 'small') if quality matters more than speed.
_DEFAULT_MODEL = "base"
_DEFAULT_MAX_PER_RUN = 200
_DOWNLOAD_TIMEOUT = 120.0


def _max_per_run(creds) -> int:
    raw = creds.optional("CALL_TRANSCRIBE_MAX_PER_RUN")
    try:
        return max(0, int(raw)) if raw else _DEFAULT_MAX_PER_RUN
    except (TypeError, ValueError):
        return _DEFAULT_MAX_PER_RUN


def _model_name(creds) -> str:
    return creds.optional("CALL_TRANSCRIBE_MODEL") or _DEFAULT_MODEL


def _be_nice() -> None:
    """Lower CPU priority so transcription does not starve the nightly run (spec §6)."""
    try:
        os.nice(15)
    except (OSError, AttributeError):  # best-effort; not fatal
        pass


def _download_recording(api_key: str, recording_url: str, dest: Path) -> None:
    """Download the Close recording (authed, following the 302 to pre-signed S3) to `dest`.

    httpx strips the Authorization header on a cross-host redirect, so the Close basic auth
    only reaches api.close.com — S3 is hit anonymously with its own pre-signed query.
    """
    with httpx.Client(
        auth=(api_key, ""),
        headers={"User-Agent": "renaissance-warehouse/1.0"},
        timeout=_DOWNLOAD_TIMEOUT,
        follow_redirects=True,
    ) as client:
        with client.stream("GET", recording_url) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_bytes(64 * 1024):
                    fh.write(chunk)


def run(ctx: RunContext) -> PhaseResult:
    api_key = ctx.credentials.optional("CLOSE_API_KEY")
    if not api_key:
        logger.warning("No CLOSE_API_KEY — skipping call transcription")
        return PhaseResult(notes={"reason": "no_key"})

    conn = ctx.db
    conn.execute(_DDL.read_text())  # idempotent; guarantees core.call + core.call_transcript exist

    max_per_run = _max_per_run(ctx.credentials)
    if max_per_run == 0:
        logger.info("CALL_TRANSCRIBE_MAX_PER_RUN=0 — transcription disabled this run")
        return PhaseResult(notes={"reason": "disabled", "max_per_run": 0})

    # Incremental: recorded calls with no transcript yet. Cap per run so it never runs forever.
    todo = conn.execute(
        """
        SELECT call_id, recording_url, duration_seconds
        FROM core.call
        WHERE has_recording
          AND recording_url IS NOT NULL
          AND call_id NOT IN (SELECT call_id FROM core.call_transcript)
        ORDER BY occurred_at DESC
        LIMIT ?
        """,
        [max_per_run],
    ).fetchall()

    if not todo:
        logger.info("call_transcription: nothing to do (all recorded calls already transcribed)")
        return PhaseResult(rows_in=0, rows_out=0, notes={"pending": 0})

    # Load the whisper model ONCE per run (heavy). Import lazily so the module imports even
    # where faster-whisper isn't installed (e.g. local dev / AST check).
    model_name = _model_name(ctx.credentials)
    try:
        _be_nice()
        from faster_whisper import WhisperModel

        model = WhisperModel(model_name, device="cpu", compute_type="int8")
    except Exception as exc:  # noqa: BLE001 — missing dep / model download failure is fatal-for-run, not crash
        logger.error("call_transcription: could not load whisper model %r: %s", model_name, exc)
        return PhaseResult(rows_in=len(todo), rows_out=0,
                           notes={"error": "model_load_failed", "detail": str(exc)[:300]})

    done = 0
    failures: list[dict] = []
    for call_id, recording_url, dur in todo:
        # Each audio file is its own temp path in /tmp; deleted in `finally` no matter what.
        fd, tmp_path = tempfile.mkstemp(suffix=".mp3", prefix="ws_h_")
        os.close(fd)
        tmp = Path(tmp_path)
        try:
            _download_recording(api_key, recording_url, tmp)

            # Transcribe to memory (no DB lock held during heavy compute) ...
            segments, info = model.transcribe(str(tmp), language="en")
            text = " ".join(s.text for s in segments).strip()
            lang = getattr(info, "language", "en") or "en"
            audio_dur = int(round(getattr(info, "duration", 0) or (dur or 0)))

            # ... then INSERT the finished transcript (quick write).
            conn.execute(
                """
                INSERT INTO core.call_transcript
                    (call_id, transcript, model, lang, duration_seconds, transcribed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (call_id) DO UPDATE SET
                    transcript = excluded.transcript,
                    model = excluded.model,
                    lang = excluded.lang,
                    duration_seconds = excluded.duration_seconds,
                    transcribed_at = excluded.transcribed_at
                """,
                [call_id, text, model_name, lang, audio_dur, datetime.now(timezone.utc)],
            )
            done += 1
        except Exception as exc:  # noqa: BLE001 — one bad call must never abort the phase
            logger.warning("call_transcription: %s failed (%s) — skipping", call_id, str(exc)[:200])
            failures.append({"call_id": call_id, "error": str(exc)[:200]})
        finally:
            # Delete the audio immediately (disk-safe; PII not left on disk).
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    pending_after = conn.execute(
        """
        SELECT count(*) FROM core.call
        WHERE has_recording AND recording_url IS NOT NULL
          AND call_id NOT IN (SELECT call_id FROM core.call_transcript)
        """
    ).fetchone()[0]

    notes = {
        "model": model_name,
        "max_per_run": max_per_run,
        "attempted": len(todo),
        "transcribed": done,
        "failed": len(failures),
        "pending_after": pending_after,
        "failures": failures[:20],
    }
    logger.info("call_transcription: %s", {k: v for k, v in notes.items() if k != "failures"})
    return PhaseResult(rows_in=len(todo), rows_out=done, notes=notes)


def register(registry: Registry) -> None:
    # Ride the existing `close` phase (no PHASE_ORDER edit). MUST follow close_calls so
    # core.call exists/is fresh — see module docstring on alphabetical discovery ordering.
    registry.add_phase("close", "call_transcription", run)
