#!/usr/bin/env python3
"""W1i (RevOps funnel deep-dive) — LLM transcript -> call-outcome refinement (Fix-1b).

Standalone daily pass that reads core.call_transcript for ANSWERED calls and refines the
outcome into core.call_outcome_llm:
    answered_appt_set | answered_not_interested | objection | answered_other
plus objection_type, a rate_quoted coaching flag, sentiment, a one-line summary, confidence.

WHY this exists: classify_outcome() (entities/close_calls.py) is disposition-only and never
emits 'answered_appt_set', so core.conversion_event's warm-caller feeder always matched 0 and
core.warm_caller.appt_set_calls stayed NULL — the call channel was structurally absent from
the warehouse conversion fact. This pass (plus the deterministic note-regex floor in
sql/ddl/1024_call_outcome_llm.sql) lights it up. core.v_call_outcome_final coalesces
note-booking > LLM(this) > disposition; conversion_event reads the view.

PROVIDER: same setup as the Sendivo opportunity classifier (/root/sms-sentiment-bi) — qwen
(qwen/qwen-2.5-72b-instruct) via OpenRouter's OpenAI-compatible endpoint. Key from env:
OPENROUTER_API_KEY_POSITIVE_REPLY_RATE (the Sendivo-classifier key) then OPENROUTER_API_KEY.

WHY STANDALONE (mirrors scripts/transcribe_calls.py): an LLM batch over thousands of calls
must NOT hold the single DuckDB writer lock. We read the pending list READ-ONLY, call the API
holding NO warehouse connection, and open the writer only in short bursts to flush a batch.
Both the pending-read and every flush are lock-aware (retry-with-backoff) so a finished batch
is never lost to a momentary writer collision. DuckDB IS the checkpoint: idempotent — only
calls without a core.call_outcome_llm row at the current CLASSIFIER_VERSION are picked up, so
re-running is safe and resumes where it left off. No PII file is written (transcripts go ONLY
to the API, exactly like the Sendivo reply classifier; labels+summary go to DuckDB).

Backfill once, then a small daily incremental. Suggested cron (UTC, after the 08:30
transcription cron, clear of the 06:00-07:30 writer stack):
    30 9 * * * cd /root/renaissance-warehouse && .venv/bin/python scripts/classify_call_outcomes.py >> logs/classify_call_outcomes.log 2>&1
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import db as db_module
from core.config import DB_PATH
from core.credentials import load_credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("classify_call_outcomes")

CLASSIFIER_MODEL = os.environ.get("CALL_OUTCOME_MODEL", "qwen/qwen-2.5-72b-instruct")
CLASSIFIER_VERSION = int(os.environ.get("CALL_OUTCOME_VERSION", "1"))
_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
_KEY_CANDIDATES = ("OPENROUTER_API_KEY_POSITIVE_REPLY_RATE", "OPENROUTER_API_KEY")

_BATCH_SIZE = int(os.environ.get("CALL_OUTCOME_BATCH_SIZE", "15"))
_FLUSH_EVERY = int(os.environ.get("CALL_OUTCOME_FLUSH_EVERY", "30"))
_TRANSCRIPT_CHARS = int(os.environ.get("CALL_OUTCOME_TRANSCRIPT_CHARS", "4000"))

_LOCK_MAX_WAIT_S = int(os.environ.get("CALL_OUTCOME_LOCK_MAX_WAIT_S", "3600"))
_BACKOFF_START_S = float(os.environ.get("CALL_OUTCOME_BACKOFF_START_S", "5"))
_BACKOFF_CAP_S = float(os.environ.get("CALL_OUTCOME_BACKOFF_CAP_S", "120"))
_SNAPSHOT = Path(os.environ.get("CALL_OUTCOME_SNAPSHOT", "/opt/duckdb/warehouse_current.duckdb"))
_LOCK_MARKERS = ("could not set lock", "conflicting lock", "database is locked")

_OUTCOME_ENUM = {"answered_appt_set", "answered_not_interested", "objection", "answered_other"}
_OBJECTION_ENUM = {"price", "terms", "trust", "timing", "already_have", "no_need", "not_dm", "other"}
_SENTIMENTS = {"positive", "neutral", "negative"}

# Only ANSWERED calls that have a transcript and no label at the current version. (no_answer /
# voicemail have no conversation to mine.) DuckDB-as-checkpoint = the NOT EXISTS clause.
_PENDING_SQL = f"""
    SELECT t.call_id, t.transcript
    FROM core.call_transcript t
    JOIN core.call_outcome o ON o.call_id = t.call_id
    WHERE o.outcome_class = 'answered'
      AND t.transcript IS NOT NULL AND length(trim(t.transcript)) > 0
      AND NOT EXISTS (
        SELECT 1 FROM core.call_outcome_llm l
        WHERE l.call_id = t.call_id AND l.classifier_version = {CLASSIFIER_VERSION}
      )
    ORDER BY t.transcribed_at DESC
"""

_INSERT = """
    INSERT INTO core.call_outcome_llm
      (call_id, outcome_class, objection_type, rate_quoted, sentiment, summary,
       confidence, classifier_model, classifier_version, classified_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (call_id) DO UPDATE SET
      outcome_class = excluded.outcome_class, objection_type = excluded.objection_type,
      rate_quoted = excluded.rate_quoted, sentiment = excluded.sentiment,
      summary = excluded.summary, confidence = excluded.confidence,
      classifier_model = excluded.classifier_model,
      classifier_version = excluded.classifier_version, classified_at = excluded.classified_at
"""

_SYSTEM_PROMPT = """You classify the OUTCOME of a warm sales call. The caller is an appointment \
SETTER for a business-funding company (they pitch as "Big Think Capital" and try to BOOK the \
prospect into a later call with a funding advisor). The input is a (sometimes rough, \
auto-transcribed) recording of one call. For each call return one rich outcome record.

outcome_class MUST be exactly one of:
  answered_appt_set       - an appointment/booking WAS set, or the prospect clearly agreed to a
                            specific time / to meet the advisor.
  answered_not_interested - connected to the prospect but they declined / not interested / hung
                            up on the pitch / "not looking" / asked to be removed.
  objection               - a real conversation with an unresolved objection or stall and NO
                            booking (asked to be emailed instead, "who are you", price/terms
                            pushback, "already working with someone", "call me later").
  answered_other          - connected but none of the above: voicemail greeting reached, wrong
                            number, gatekeeper only, language barrier, too short to tell, IVR.

Also return:
  objection_type - the PRIMARY objection if any, one of "price","terms","trust","timing",
                   "already_have","no_need","not_dm","other", else null.
                   (price=rate/cost too high; terms=repayment cadence/structure; trust=who are
                   you/scam/how got my info; timing=not now/later; already_have=using another
                   funder/broker; no_need=don't need capital; not_dm=wrong person/refers on.)
  rate_quoted    - did the CALLER state a specific rate/APR/price/factor on THIS call (e.g.
                   "8% APR", "0.8% per month")? true/false. (A pre-booking coaching signal.)
  sentiment      - "positive" | "neutral" | "negative" (the prospect's disposition).
  summary        - one short line (max ~140 chars): what happened. No PII beyond a first name.
  confidence     - 0.0-1.0.

Return ONLY a JSON array, one object per call, in the SAME ORDER as the input, each exactly:
{"id": <int index>, "outcome_class": "...", "objection_type": "..."|null, "rate_quoted": \
true/false, "sentiment": "...", "summary": "...", "confidence": 0.0}
No prose, no markdown fences — just the JSON array."""


def _is_lock_error(exc: Exception) -> bool:
    s = str(exc).lower()
    return any(m in s for m in _LOCK_MARKERS)


def _clean(text: str | None) -> str:
    if not text:
        return ""
    t = re.sub(r"\s+", " ", text).strip()
    return t[:_TRANSCRIPT_CHARS]


def _resolve_key(creds) -> str | None:
    for k in _KEY_CANDIDATES:
        v = creds.optional(k) or os.environ.get(k)
        if v:
            return v
    return None


def _coerce(obj: dict) -> dict | None:
    oc = obj.get("outcome_class")
    if oc not in _OUTCOME_ENUM:
        return None
    ot = obj.get("objection_type")
    if ot in ("", "null", "none", None) or ot not in _OBJECTION_ENUM:
        ot = None
    sent = obj.get("sentiment")
    if sent not in _SENTIMENTS:
        sent = "neutral"
    try:
        conf = min(max(float(obj.get("confidence")), 0.0), 1.0)
    except (TypeError, ValueError):
        conf = None
    return {
        "outcome_class": oc,
        "objection_type": ot,
        "rate_quoted": bool(obj.get("rate_quoted")),
        "sentiment": sent,
        "summary": (obj.get("summary") or "")[:500],
        "confidence": conf,
    }


def _call_qwen(client: httpx.Client, key: str, batch: list[dict]) -> dict[int, dict]:
    lines = [json.dumps({"id": j, "transcript": _clean(r["transcript"])}) for j, r in enumerate(batch)]
    user_msg = "Classify these calls:\n" + "\n".join(lines)
    resp = client.post(
        _OPENROUTER_URL,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": CLASSIFIER_MODEL, "temperature": 0, "max_tokens": 6000,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        },
        timeout=180,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.lstrip().startswith("json"):
            raw = raw.lstrip()[4:]
    # tolerate prose around the array
    m = re.search(r"\[.*\]", raw, re.S)
    arr = json.loads(m.group(0) if m else raw)
    out: dict[int, dict] = {}
    for obj in arr:
        if not isinstance(obj, dict):
            continue
        idx = obj.get("id")
        if not isinstance(idx, int) or not (0 <= idx < len(batch)):
            continue
        rec = _coerce(obj)
        if rec is not None:
            out[idx] = rec
    return out


def _flush(rows: list) -> bool:
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
            logger.info("flushed %d outcome rows%s", len(rows),
                        f" after {attempt} attempts (writer was busy)" if attempt > 1 else "")
            return True
        except Exception as exc:  # noqa: BLE001
            if not _is_lock_error(exc):
                raise
            if time.monotonic() >= deadline:
                logger.error("flush GAVE UP after %ds writer-lock contention — %d rows kept pending: %s",
                             _LOCK_MAX_WAIT_S, len(rows), str(exc)[:120])
                return False
            wait = min(backoff, _BACKOFF_CAP_S)
            logger.info("writer locked (flush attempt %d) — retry in %.0fs", attempt, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, _BACKOFF_CAP_S)


def _load_pending(limit: int | None) -> list:
    sql = _PENDING_SQL + (f"\n    LIMIT {int(limit)}" if limit else "")
    deadline = time.monotonic() + _LOCK_MAX_WAIT_S
    backoff = _BACKOFF_START_S
    attempt = 0
    while True:
        attempt += 1
        try:
            ro = db_module.connect(DB_PATH, read_only=True)
            try:
                return ro.execute(sql).fetchall()
            finally:
                ro.close()
        except Exception as exc:  # noqa: BLE001
            if not _is_lock_error(exc):
                raise
            if time.monotonic() >= deadline:
                if _SNAPSHOT.exists():
                    logger.warning("live DB locked %ds — reading pending from serving snapshot", _LOCK_MAX_WAIT_S)
                    ro = db_module.connect(_SNAPSHOT, read_only=True)
                    try:
                        return ro.execute(sql).fetchall()
                    finally:
                        ro.close()
                raise
            wait = min(backoff, _BACKOFF_CAP_S)
            logger.info("live DB locked (pending-read attempt %d) — retry in %.0fs", attempt, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, _BACKOFF_CAP_S)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="cap calls this run (default: all pending)")
    ap.add_argument("--smoke", action="store_true", help="classify 3 calls, print results, write nothing")
    args = ap.parse_args()

    creds = load_credentials()
    key = _resolve_key(creds)
    if not key:
        logger.error("no OpenRouter key (%s) — cannot classify", _KEY_CANDIDATES)
        return 1

    todo = _load_pending(3 if args.smoke else args.limit)
    if not todo:
        logger.info("nothing pending — all answered+transcribed calls classified at v%d", CLASSIFIER_VERSION)
        return 0
    logger.info("pending: %d answered+transcribed calls (model=%s, v%d, batch=%d)",
                len(todo), CLASSIFIER_MODEL, CLASSIFIER_VERSION, _BATCH_SIZE)

    rows = [{"call_id": r[0], "transcript": r[1]} for r in todo]
    now_iso = datetime.now(timezone.utc)
    batch_out: list = []
    classified = failed_batches = lost = 0
    t0 = time.monotonic()

    with httpx.Client(headers={"User-Agent": "renaissance-warehouse/1.0"}) as client:
        for start in range(0, len(rows), _BATCH_SIZE):
            chunk = rows[start:start + _BATCH_SIZE]
            results = None
            for attempt in range(5):
                try:
                    results = _call_qwen(client, key, chunk)
                    break
                except Exception as exc:  # noqa: BLE001 — retry transient, never abort the run
                    logger.warning("batch @%d attempt %d failed (%s)", start, attempt + 1, str(exc)[:160])
                    time.sleep(min(2 ** attempt, 30))
            if results is None:
                failed_batches += 1
                continue
            for j, rec in results.items():
                cid = chunk[j]["call_id"]
                if args.smoke:
                    print(json.dumps({"call_id": cid, **rec}, ensure_ascii=False))
                    classified += 1
                    continue
                batch_out.append([
                    cid, rec["outcome_class"], rec["objection_type"], rec["rate_quoted"],
                    rec["sentiment"], rec["summary"], rec["confidence"],
                    CLASSIFIER_MODEL, CLASSIFIER_VERSION, now_iso,
                ])
                classified += 1
            if not args.smoke and len(batch_out) >= _FLUSH_EVERY:
                if _flush(batch_out):
                    rate = (start + len(chunk)) / max(time.monotonic() - t0, 1e-9) * 60.0
                    logger.info("progress %d/%d (%.0f calls/min)", start + len(chunk), len(rows), rate)
                else:
                    lost += len(batch_out)
                batch_out = []

    if not args.smoke and not _flush(batch_out):
        lost += len(batch_out)

    logger.info("call-outcome classify done: classified=%d failed_batches=%d deferred(lock)=%d total=%d",
                classified, failed_batches, lost, len(rows))
    return 2 if lost else 0


if __name__ == "__main__":
    raise SystemExit(main())
