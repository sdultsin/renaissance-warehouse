"""Rich LLM reply-intent classifier (Spec 16 — BI/Lead-Intent layer, WS-C, object: ReplyIntent).

Classifies NEW core.reply rows into core.reply_intent — a rich, multi-signal, versioned
intent record that SUPERSEDES the dead raw_pipeline_reply_intent_classifications (a
deterministic keyword/native-label heuristic, frozen ~2026-06-07, part of the pipeline-Supabase
retirement). We never depend on that table going forward.

Model: claude-haiku-4-5 (Haiku-class, cheap, batched).
Enum (spec 16 §6), fixed + versioned — primary_intent is ALWAYS one of:
  interested · info_request · objection_price · objection_timing · objection_trust ·
  objection_no_need · not_decision_maker · unsubscribe · auto_reply · hostile · neutral_other

Incremental + idempotent:
  * Only classifies core.reply rows lacking a core.reply_intent row at the CURRENT
    classifier_version. Re-running at the same version classifies 0.
  * Bumping CLASSIFIER_VERSION forces a full re-classify.
  * is_auto_reply=true rows are labelled deterministically (primary_intent='auto_reply') with
    NO API call — only human replies hit the LLM.

Robustness: a classifier/API failure on a batch LOGS and CONTINUES — it never raises out of the
phase and never aborts the nightly. Heavy work (the API calls) happens outside any long-held
writer lock; results are staged in memory and UPSERTed in one pass.

⚠ PII: reply_text + lead_email go to the Anthropic API for classification only; nothing is
written to a git-tracked file.

Registers under the `intent` phase (parent adds the slot to core/config.py PHASE_ORDER).
Schema = sql/ddl/43_reply_intent.sql.
"""
from __future__ import annotations

import json
import logging
import os

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.reply_intent_llm")

_DDL = REPO_ROOT / "sql" / "ddl" / "43_reply_intent.sql"

CLASSIFIER_MODEL = "claude-haiku-4-5"
CLASSIFIER_VERSION = 1

# Anthropic key lookup order (warehouse repo has no helper; key lives in the parent .env as
# ANTHROPIC_KEY — not ANTHROPIC_API_KEY).
_KEY_CANDIDATES = ("ANTHROPIC_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_KEY_RENAISSANCE_OVERVIEW_CHATBOT")

_PRIMARY_ENUM = {
    "interested", "info_request", "objection_price", "objection_timing", "objection_trust",
    "objection_no_need", "not_decision_maker", "unsubscribe", "auto_reply", "hostile",
    "neutral_other",
}
_SENTIMENTS = {"positive", "neutral", "negative"}

# How many replies per LLM call, and how many replies to attempt per nightly run (bound cost;
# the backfill drains over a few nights, then it's a small incremental every night).
_BATCH_SIZE = int(os.environ.get("REPLY_INTENT_BATCH_SIZE", "20"))
_MAX_PER_RUN = int(os.environ.get("REPLY_INTENT_MAX_PER_RUN", "4000"))

_INTENT_COLS = [
    "reply_id", "primary_intent", "intent_tags", "sentiment", "is_question", "is_objection",
    "objection_type", "is_unsubscribe", "is_referral", "is_wrong_person", "summary",
    "classifier_model", "classifier_version", "confidence", "classified_at",
]
_UPDATE_SET = ", ".join(f"{c} = excluded.{c}" for c in _INTENT_COLS if c != "reply_id")
_UPSERT_SQL = (
    f"INSERT INTO core.reply_intent ({', '.join(_INTENT_COLS)}) "
    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "
    f"ON CONFLICT (reply_id) DO UPDATE SET {_UPDATE_SET}"
)

_SYSTEM_PROMPT = """You classify inbound replies to B2B cold-outreach emails (the sender offers \
business funding / merchant cash advance). For each reply, return a rich intent record.

primary_intent MUST be exactly one of:
  interested          - wants to talk / move forward / a clear positive
  info_request        - asks to send details / rates / how it works (and not already clearly interested)
  objection_price     - too expensive / rates / fees push-back
  objection_timing    - not now / call me later / next quarter
  objection_trust     - who are you / is this a scam / how did you get my info
  objection_no_need   - we're funded / don't need it / not looking
  not_decision_maker  - wrong person / refers you elsewhere / "talk to my partner/CFO"
  unsubscribe         - remove me / stop / take me off / hard no
  auto_reply          - out-of-office / autoresponder / bounce / undeliverable
  hostile             - angry / spam complaint / abusive
  neutral_other       - acknowledgement, unclear, smalltalk, anything else

Also return:
  intent_tags        - array of secondary labels from the SAME enum (may be empty)
  sentiment          - "positive" | "neutral" | "negative"
  is_question        - did the lead ask anything?
  is_objection       - did the lead push back?
  objection_type     - one of "price","timing","trust","already_have","no_need","not_dm" or null
  is_unsubscribe     - remove/stop request?
  is_referral        - points you to another person?
  is_wrong_person    - not the decision maker?
  summary            - one short line: what they said
  confidence         - 0.0-1.0

Return ONLY a JSON array, one object per reply, in the SAME ORDER as the input, each:
{"id": <int index>, "primary_intent": "...", "intent_tags": [...], "sentiment": "...", \
"is_question": true/false, "is_objection": true/false, "objection_type": "..."|null, \
"is_unsubscribe": true/false, "is_referral": true/false, "is_wrong_person": true/false, \
"summary": "...", "confidence": 0.0}
No prose, no markdown fences — just the JSON array."""


def register(registry: Registry) -> None:
    registry.add_phase("intent", "reply_intent_llm", run)


def _clean(text: str | None, limit: int = 1500) -> str:
    if not text:
        return ""
    # strip obvious HTML tags + collapse whitespace; keep it cheap.
    import re
    t = re.sub(r"<[^>]+>", " ", text)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:limit]


def _coerce(obj: dict) -> dict | None:
    """Validate + normalise one model output object. Returns None if unusable."""
    pi = obj.get("primary_intent")
    if pi not in _PRIMARY_ENUM:
        return None
    tags = obj.get("intent_tags") or []
    tags = [t for t in tags if isinstance(t, str) and t in _PRIMARY_ENUM]
    sent = obj.get("sentiment")
    if sent not in _SENTIMENTS:
        sent = "neutral"
    ot = obj.get("objection_type")
    if ot in ("", "null", "none"):
        ot = None
    conf = obj.get("confidence")
    try:
        conf = float(conf)
        conf = min(max(conf, 0.0), 1.0)
    except (TypeError, ValueError):
        conf = None
    return {
        "primary_intent": pi,
        "intent_tags": tags,
        "sentiment": sent,
        "is_question": bool(obj.get("is_question")),
        "is_objection": bool(obj.get("is_objection")),
        "objection_type": ot,
        "is_unsubscribe": bool(obj.get("is_unsubscribe")),
        "is_referral": bool(obj.get("is_referral")),
        "is_wrong_person": bool(obj.get("is_wrong_person")),
        "summary": (obj.get("summary") or "")[:500],
        "confidence": conf,
    }


def _classify_batch(client, batch: list[dict]) -> dict[int, dict]:
    """Send one batch of replies to the LLM. Returns {batch_index -> coerced record}.
    Any failure raises (caller catches per-batch)."""
    lines = []
    for j, r in enumerate(batch):
        subj = _clean(r["subject"], 200)
        body = _clean(r["reply_text"])
        lines.append(json.dumps({"id": j, "step": r["step"], "subject": subj, "reply": body}))
    user_msg = "Classify these replies:\n" + "\n".join(lines)

    resp = client.messages.create(
        model=CLASSIFIER_MODEL,
        max_tokens=4096,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text").strip()
    # tolerate accidental code fences
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.lstrip().startswith("json"):
            raw = raw.lstrip()[4:]
    arr = json.loads(raw)
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


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent

    # --- deterministic pass: auto-replies get labelled with NO API call ---
    # (incremental: only rows missing an intent at the current version)
    db.execute(
        f"""
        INSERT INTO core.reply_intent
        ({', '.join(_INTENT_COLS)})
        SELECT r.reply_id, 'auto_reply', CAST([] AS VARCHAR[]), 'neutral',
               false, false, NULL, false, false, false,
               'autoresponder / out-of-office / bounce', ?, ?, 1.0, now()
        FROM core.reply r
        LEFT JOIN core.reply_intent i
          ON i.reply_id = r.reply_id AND i.classifier_version = ?
        WHERE r.is_auto_reply = true AND i.reply_id IS NULL
        ON CONFLICT (reply_id) DO UPDATE SET
          primary_intent = excluded.primary_intent,
          classifier_model = excluded.classifier_model,
          classifier_version = excluded.classifier_version,
          confidence = excluded.confidence,
          classified_at = excluded.classified_at
        """,
        [CLASSIFIER_MODEL, CLASSIFIER_VERSION, CLASSIFIER_VERSION],
    )
    auto_labelled = db.execute(
        "SELECT count(*) FROM core.reply_intent WHERE classifier_version = ? "
        "AND primary_intent = 'auto_reply' AND classifier_model = ?",
        [CLASSIFIER_VERSION, CLASSIFIER_MODEL],
    ).fetchone()[0]

    # --- LLM pass: human replies lacking an intent at the current version ---
    pending = db.execute(
        """
        SELECT r.reply_id, r.subject, r.reply_text, r.step
        FROM core.reply r
        LEFT JOIN core.reply_intent i
          ON i.reply_id = r.reply_id AND i.classifier_version = ?
        WHERE coalesce(r.is_auto_reply, false) = false
          AND i.reply_id IS NULL
        ORDER BY r.reply_timestamp DESC NULLS LAST
        LIMIT ?
        """,
        [CLASSIFIER_VERSION, _MAX_PER_RUN],
    ).fetchall()

    if not pending:
        logger.info("No human replies pending classification at version %d", CLASSIFIER_VERSION)
        return PhaseResult(
            notes={"auto_labelled": auto_labelled, "classified": 0, "reason": "nothing_pending"}
        )

    # Lazy import so the module imports cleanly on hosts without the SDK (droplet venv until
    # the parent deploys `anthropic`). Skip-with-log if missing/unconfigured — never raise.
    try:
        import anthropic  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        logger.error("anthropic SDK unavailable (%s) — skipping LLM classify, %d pending", exc, len(pending))
        return PhaseResult(notes={"auto_labelled": auto_labelled, "classified": 0, "error": "no_sdk"})

    api_key = next((ctx.credentials.optional(k) for k in _KEY_CANDIDATES if ctx.credentials.optional(k)), None)
    if not api_key:
        logger.error("No Anthropic key (%s) — skipping LLM classify, %d pending", _KEY_CANDIDATES, len(pending))
        return PhaseResult(notes={"auto_labelled": auto_labelled, "classified": 0, "error": "no_key"})

    client = anthropic.Anthropic(api_key=api_key)

    rows = [{"reply_id": r[0], "subject": r[1], "reply_text": r[2], "step": r[3]} for r in pending]
    classified = 0
    batch_failures = 0

    for start in range(0, len(rows), _BATCH_SIZE):
        batch = rows[start:start + _BATCH_SIZE]
        try:
            results = _classify_batch(client, batch)
        except Exception as exc:  # noqa: BLE001  — a batch failure must never abort the phase
            batch_failures += 1
            logger.error("batch @%d failed (%s): %s", start, type(exc).__name__, str(exc)[:200])
            continue
        for j, rec in results.items():
            r = batch[j]
            try:
                db.execute(
                    _UPSERT_SQL,
                    [
                        r["reply_id"], rec["primary_intent"], rec["intent_tags"], rec["sentiment"],
                        rec["is_question"], rec["is_objection"], rec["objection_type"],
                        rec["is_unsubscribe"], rec["is_referral"], rec["is_wrong_person"],
                        rec["summary"], CLASSIFIER_MODEL, CLASSIFIER_VERSION, rec["confidence"],
                    ],
                )
                classified += 1
            except Exception as exc:  # noqa: BLE001
                logger.error("upsert failed for %s: %s", r["reply_id"], str(exc)[:200])

    remaining = db.execute(
        """
        SELECT count(*) FROM core.reply r
        LEFT JOIN core.reply_intent i
          ON i.reply_id = r.reply_id AND i.classifier_version = ?
        WHERE coalesce(r.is_auto_reply, false) = false AND i.reply_id IS NULL
        """,
        [CLASSIFIER_VERSION],
    ).fetchone()[0]

    logger.info(
        "reply_intent: auto=%d, classified=%d (batch_failures=%d), still_pending=%d",
        auto_labelled, classified, batch_failures, remaining,
    )

    return PhaseResult(
        rows_in=len(rows),
        rows_out=classified + auto_labelled,
        notes={
            "auto_labelled": auto_labelled,
            "classified": classified,
            "batch_failures": batch_failures,
            "still_pending": remaining,
            "classifier_version": CLASSIFIER_VERSION,
            "model": CLASSIFIER_MODEL,
        },
    )
