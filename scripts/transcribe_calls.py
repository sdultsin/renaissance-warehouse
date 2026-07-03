#!/usr/bin/env python3
"""Standalone daily transcription of ALL pending Close call recordings (Spec 16, WS-H).

WHY STANDALONE (not a nightly phase): transcription is slow I/O + CPU (download each
recording, run faster-whisper). If it ran inside the orchestrator it would hold the single
DuckDB writer lock for the whole batch, blocking Lens/dashboards and the nightly window.
This job instead:
  1. reads the pending list READ-ONLY (no write lock),
  2. downloads + whispers each call holding NO warehouse connection,
  3. opens the writer only in short bursts to flush a batch of finished transcripts.

So the warehouse writer is free during the heavy work; reads keep working.

Processes EVERYTHING pending (no per-run cap) so we stay caught up daily. Volume is small
(~100-300 calls/day, ~1-1.5 audio-hours), so a full pass is minutes of compute. Idempotent:
only calls with a recording and no transcript yet are picked up; re-running is safe.

PERMANENTLY-UNAVAILABLE RECORDINGS (the 2026-07-02 W-3 fix):
  core.call is rebuilt nightly from raw_close_call, which we NEVER delete. So a call that had
  has_recording=true when we synced it stays in the pending set forever even after it is DELETED
  in Close — its recording endpoint then 404s ("Activity not found") on every run, re-erroring
  nightly and keeping the coverage watchdog's gap permanently open (this is exactly the "0/69,
  all Close 404s" residue). We now TOMBSTONE such calls: on a recording 404 we probe the Close
  activity endpoint — if the activity is gone (deleted), or the call is older than the grace
  window and the recording still 404s (it never materialized), we write a sentinel row into
  core.call_transcript (transcript='', model='unavailable_404'). That drops the call out of the
  pending set (NOT IN call_transcript) and closes the watchdog gap, WITHOUT polluting downstream:
  classify_call_outcomes.py filters `length(trim(transcript)) > 0`, so sentinels are skipped, and
  the residue is auditable via `WHERE model = 'unavailable_404'`. A FRESH call whose recording is
  merely still processing (activity present, age < grace) is left pending and retried next run.

LOCK-ROBUSTNESS (the 2026-06-16 hardening — see handoffs/2026-06-16-call-transcription-backfill.md):
  DuckDB is single-writer (flock); in this build a *read-only* open ALSO fails while a writer
  holds the lock. The original job died on the very first flush when it collided with the
  06:00-07:30 cron stack (kpi/sendivo/sms-watchdog) or a long nightly — losing the whole run's
  whisper work while recordings kept piling up (silent gap: Jun-12 329/0, Jun-15 279/0).
  Now BOTH the pending-read and every flush are wrapped in lock-aware retry-with-backoff: a
  finished batch of transcripts is NEVER dropped because the writer was momentarily busy — it
  waits (niced, in memory) and commits when the writer frees. A coverage watchdog
  (scripts/transcribe_coverage_watchdog.py) independently alerts #cc-sam if a gap ever persists.

Cron (UTC) — 23:30, clear of the nightly writer window (nightly moved to 05:30 and can run
10+ hours when an upstream fetch hangs — it held the lock through the old 08:30 slot, which is
what stalled transcripts 06-28→07-01):
    30 23 * * *  cd /root/renaissance-warehouse && .venv/bin/python scripts/transcribe_calls.py >> logs/transcribe.log 2>&1
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import db as db_module
from core.config import DB_PATH
from core.credentials import load_credentials
from entities.call_transcription import _download_recording  # reuse the authed 302->S3 download

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transcribe_calls")

_MODEL = os.environ.get("CALL_TRANSCRIBE_MODEL", "base")
# Flush in SHORT bursts so each write-lock hold is brief (good citizen vs other writers).
_FLUSH_EVERY = int(os.environ.get("CALL_TRANSCRIBE_FLUSH_EVERY", "25"))

# Sentinel for a recording that will NEVER transcribe (deleted in Close / never materialized).
# Written as a call_transcript row with an EMPTY transcript so it drops out of the pending set
# without being treated as real text downstream (classify filters length>0). Auditable via model.
_UNAVAILABLE_MODEL = "unavailable_404"
# A recording still 404ing this long after the call almost certainly never materialized (Close's
# processing lag is minutes, not days) — tombstone it even if the activity itself still exists.
_UNAVAILABLE_GRACE_H = int(os.environ.get("CALL_TRANSCRIBE_UNAVAILABLE_GRACE_H", "48"))
_CLOSE_ACTIVITY_URL = "https://api.close.com/api/v1/activity/call/{call_id}/"

# Lock-aware retry budget. Per read/flush we retry with exponential backoff up to this wall-clock
# budget before giving up — generous enough to outlast any legitimate writer hold (the longest
# observed is the ~1h sendivo multi-day heal). If a single flush can't land inside the budget the
# untranscribed calls simply stay pending and next run retries (idempotent) — the watchdog pages.
_LOCK_MAX_WAIT_S = int(os.environ.get("CALL_TRANSCRIBE_LOCK_MAX_WAIT_S", "3600"))
_BACKOFF_START_S = float(os.environ.get("CALL_TRANSCRIBE_BACKOFF_START_S", "5"))
_BACKOFF_CAP_S = float(os.environ.get("CALL_TRANSCRIBE_BACKOFF_CAP_S", "120"))

# The live DB is read-via-the-snapshot's purpose is safety; for the PENDING list we want the
# FRESHEST truth (the newest recordings are exactly what we must catch), so we read the live DB
# read-only WITH retry, and only fall back to the (possibly hours-stale) serving snapshot if the
# live DB is unreachable for the whole budget.
_SNAPSHOT = Path(os.environ.get("CALL_TRANSCRIBE_SNAPSHOT", "/opt/duckdb/warehouse_current.duckdb"))

_LOCK_MARKERS = (
    "could not set lock",
    "conflicting lock",
    "database is locked",
    # core/db.py in-process flock timeout (RuntimeError, 2026-06 hardening) — without this
    # marker the flush CRASHED the whole run instead of deferring (2026-06-30/07-01 stall).
    "could not acquire warehouse writer lock",
)

_PENDING_SQL = """
    SELECT call_id, recording_url, duration_seconds, occurred_at
    FROM core.call
    WHERE has_recording AND recording_url IS NOT NULL
      AND call_id NOT IN (SELECT call_id FROM core.call_transcript)
    ORDER BY occurred_at DESC
"""

_PENDING_COUNT_SQL = """
    SELECT count(*) FROM core.call
    WHERE has_recording AND recording_url IS NOT NULL
      AND call_id NOT IN (SELECT call_id FROM core.call_transcript)
"""

_INSERT = """
    INSERT INTO core.call_transcript (call_id, transcript, model, lang, duration_seconds, transcribed_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (call_id) DO UPDATE SET
        transcript = excluded.transcript, model = excluded.model, lang = excluded.lang,
        duration_seconds = excluded.duration_seconds, transcribed_at = excluded.transcribed_at
"""


def _is_lock_error(exc: Exception) -> bool:
    s = str(exc).lower()
    return any(m in s for m in _LOCK_MARKERS)


def _permanently_gone_reason(api_key: str, call_id: str, occurred_at, exc: Exception) -> str | None:
    """Return a tombstone reason if this recording will NEVER transcribe, else None.

    Only a recording-endpoint 404 is a candidate (a network blip / timeout / whisper error is
    transient — keep it pending). For a 404 we confirm PERMANENCE so we never tombstone a recording
    that is merely still processing right after the call:
      - 'deleted'      — the Close activity itself is gone (activity endpoint 404): unrecoverable.
      - 'never_landed' — activity still there but the recording has 404'd for > grace hours: it
                         never materialized (Close's processing lag is minutes, not days).
    A fresh call (age < grace) whose activity still exists returns None → stays pending, retried.
    """
    resp = getattr(exc, "response", None)
    if not isinstance(exc, httpx.HTTPStatusError) or resp is None or resp.status_code != 404:
        return None
    # Is the underlying Close activity gone entirely? (the dominant case — deleted-in-Close calls)
    try:
        with httpx.Client(auth=(api_key, ""), timeout=30.0,
                          headers={"User-Agent": "renaissance-warehouse/1.0"}) as c:
            a = c.get(_CLOSE_ACTIVITY_URL.format(call_id=call_id))
        if a.status_code == 404:
            return "deleted"
    except Exception as probe_exc:  # noqa: BLE001 — probe failure must not crash the run
        logger.info("activity-existence probe failed for %s (%s) — falling back to age rule",
                    call_id, str(probe_exc)[:80])
    # Activity present (or probe inconclusive) but the recording has been 404ing past the grace
    # window → it never landed. Guard against a missing/naive occurred_at.
    if occurred_at is None:
        return None
    if occurred_at.tzinfo is None:
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - occurred_at > timedelta(hours=_UNAVAILABLE_GRACE_H):
        return "never_landed"
    return None


def _flush(rows: list) -> bool:
    """Open the writer briefly, insert the batch, close (releases the lock immediately).

    Lock-aware: on the DuckDB single-writer lock we retry with exponential backoff up to
    _LOCK_MAX_WAIT_S so a finished whisper batch is NEVER lost to a momentary writer collision.
    Returns True if committed, False only if the lock budget was exhausted (caller keeps the rows
    pending for the next run). A NON-lock error is a real bug → re-raised.
    """
    if not rows:
        return True
    deadline = time.monotonic() + _LOCK_MAX_WAIT_S
    backoff = _BACKOFF_START_S
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = db_module.connect(DB_PATH)
            try:
                conn.executemany(_INSERT, rows)
            finally:
                conn.close()
            if attempt > 1:
                logger.info("flushed %d transcripts after %d attempts (writer was busy)", len(rows), attempt)
            else:
                logger.info("flushed %d transcripts", len(rows))
            return True
        except Exception as exc:  # noqa: BLE001
            if not _is_lock_error(exc):
                raise  # schema/PK/disk — surface it, don't silently swallow
            if time.monotonic() >= deadline:
                logger.error("flush GAVE UP after %ds of writer-lock contention — %d transcripts kept "
                             "pending for next run: %s", _LOCK_MAX_WAIT_S, len(rows), str(exc)[:120])
                return False
            wait = min(backoff, _BACKOFF_CAP_S)
            logger.info("writer locked (flush attempt %d) — retry in %.0fs", attempt, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, _BACKOFF_CAP_S)


def _load_pending() -> list:
    """Freshest pending list, lock-aware. Live DB read-only with retry; fall back to the serving
    snapshot only if the live DB is locked for the whole budget (snapshot may lag hours)."""
    deadline = time.monotonic() + _LOCK_MAX_WAIT_S
    backoff = _BACKOFF_START_S
    attempt = 0
    while True:
        attempt += 1
        try:
            ro = db_module.connect(DB_PATH, read_only=True)
            try:
                return ro.execute(_PENDING_SQL).fetchall()
            finally:
                ro.close()
        except Exception as exc:  # noqa: BLE001
            if not _is_lock_error(exc):
                raise
            if time.monotonic() >= deadline:
                if _SNAPSHOT.exists():
                    logger.warning("live DB locked for %ds — reading pending from serving snapshot "
                                   "(may lag): %s", _LOCK_MAX_WAIT_S, _SNAPSHOT)
                    ro = db_module.connect(_SNAPSHOT, read_only=True)
                    try:
                        return ro.execute(_PENDING_SQL).fetchall()
                    finally:
                        ro.close()
                raise
            wait = min(backoff, _BACKOFF_CAP_S)
            logger.info("live DB locked (pending-read attempt %d) — retry in %.0fs", attempt, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, _BACKOFF_CAP_S)


def _pending_count() -> int | None:
    """Best-effort post-run pending count (live RO, single try — never blocks the run end)."""
    try:
        ro = db_module.connect(DB_PATH, read_only=True)
        try:
            return int(ro.execute(_PENDING_COUNT_SQL).fetchone()[0])
        finally:
            ro.close()
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    creds = load_credentials()
    api_key = creds.require("CLOSE_API_KEY")

    # 1) pending list — freshest live truth, lock-aware (no write lock held during whisper loop)
    todo = _load_pending()
    if not todo:
        logger.info("nothing pending — all recorded calls transcribed")
        return 0
    logger.info("pending: %d recorded calls to transcribe (model=%s, flush_every=%d)",
                len(todo), _MODEL, _FLUSH_EVERY)

    try:
        os.nice(15)
    except Exception:
        pass
    from faster_whisper import WhisperModel
    model = WhisperModel(_MODEL, device="cpu", compute_type="int8")

    batch: list = []
    done = failed = lost = tombstoned = 0
    t0 = time.monotonic()
    for i, (call_id, rec_url, dur, occurred_at) in enumerate(todo, 1):
        fd, tmp_name = tempfile.mkstemp(suffix=".mp3")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            _download_recording(api_key, rec_url, tmp)
            segments, info = model.transcribe(str(tmp), language="en")
            text = " ".join(s.text for s in segments).strip()
            batch.append([
                call_id, text, _MODEL, getattr(info, "language", "en"),
                float(getattr(info, "duration", 0) or 0), datetime.now(timezone.utc),
            ])
            done += 1
        except Exception as exc:  # one bad call must never sink the run
            reason = _permanently_gone_reason(api_key, call_id, occurred_at, exc)
            if reason:
                # Recording will never transcribe → sentinel row so it leaves the pending set
                # (stops the nightly re-404 + closes the coverage watchdog's permanent gap).
                batch.append([call_id, "", _UNAVAILABLE_MODEL, "xx", 0.0, datetime.now(timezone.utc)])
                tombstoned += 1
                logger.info("tombstoned call=%s (%s) — recording permanently unavailable", call_id, reason)
            else:
                failed += 1
                logger.warning("transcribe failed call=%s: %s", call_id, str(exc)[:160])
        finally:
            try:
                tmp.unlink()
            except Exception:
                pass
        if len(batch) >= _FLUSH_EVERY:
            if _flush(batch):
                rate = i / max(time.monotonic() - t0, 1e-9) * 60.0
                logger.info("progress %d/%d (%.0f calls/min)", i, len(todo), rate)
            else:
                lost += len(batch)  # lock budget exhausted; these stay pending for next run
            batch = []
    if not _flush(batch):
        lost += len(batch)

    pending_after = _pending_count()
    logger.info(
        "transcription complete: transcribed=%d tombstoned(unavailable)=%d failed=%d "
        "deferred(lock)=%d total=%d pending_after=%s",
        done, tombstoned, failed, lost, len(todo),
        "unknown" if pending_after is None else pending_after,
    )
    # Non-zero exit only if we deferred completed work to a writer-lock timeout (visible in cron mail).
    return 2 if lost else 0


if __name__ == "__main__":
    raise SystemExit(main())
