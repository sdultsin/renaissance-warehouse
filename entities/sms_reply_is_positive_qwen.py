"""SMS qwen strict-positive + human/auto labels — durable warehouse table (persistence layer).

Re-materializes derived.sms_reply_is_positive_qwen from the out-of-band seed JSONL on EVERY nightly
rebuild, so the offline-qwen SMS reply classifications survive the nightly DROP/rebuild and reach the
gated serving snapshot. Exact mirror of entities/reply_is_positive_qwen.py (the email table), for SMS.

WHY: the table is first loaded as a one-off CREATE OR REPLACE TABLE into the primary; nothing in the
DDL/sources would rebuild it, so the next nightly would lose it and it would never reach the read-API
serving copy. This entity is the fix — same out-of-band-seed pattern as entities/partner_feedback.py
and entities/reply_is_positive_qwen.py (loads a local seed file the public repo never commits).

NOT a re-classification: the labels already exist in the seed file. This is pure persistence — we
never call the LLM here. v_omni_sms_performance (DDL 91) reads is_positive/is_human from this table,
replacing the not-opt-out INTERIM proxy.

Pipeline (idempotent, every nightly):
  seed JSONL --> CREATE OR REPLACE TABLE derived.sms_reply_is_positive_qwen (one row / inbound_message_id)

Seed file (gitignored — *.jsonl + seed_data/ in .gitignore; local/box only):
  seed_data/sms-reply-is-positive-qwen/sms_seed.jsonl
Each line: {"reply_id", "is_positive", "is_human", "reason", "received_at", "model"}.
classified_at is added at load (watermark).

Resilience: if the seed file is absent (a fresh clone without the operator's seed data) we log + skip
rather than DROP an existing table or abort the nightly — exactly like reply_is_positive_qwen /
partner_feedback. The DDL (sql/ddl/90_sms_reply_is_positive_qwen.sql) guarantees the empty table shape
exists regardless.

Integrity: we assert rows-committed == rows-in-seed (attempted-vs-committed) and log both, so a
silently-truncated load can never pass as healthy.

Registers under the existing `derived` phase — core/config.py PHASE_ORDER is untouched.
Schema = sql/ddl/90_sms_reply_is_positive_qwen.sql.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.sms_reply_is_positive_qwen")

_DDL = REPO_ROOT / "sql" / "ddl" / "90_sms_reply_is_positive_qwen.sql"

# Seed location. Default = the canonical box seed dir; overridable via env for local dev / a
# different drop path. Falls back to the work-dir results path if the canonical seed is absent
# (covers the box state before the seed is moved into seed_data/).
_DEFAULT_SEED = REPO_ROOT / "seed_data" / "sms-reply-is-positive-qwen" / "sms_seed.jsonl"
_FALLBACK_SEEDS = [
    Path("/root/sms-sentiment-bi/results/sms_seed.jsonl"),
    Path(os.path.expanduser("~/sms-sentiment-bi/results/sms_seed.jsonl")),
]


def _resolve_seed() -> Path | None:
    env = os.environ.get("SMS_REPLY_IS_POSITIVE_QWEN_SEED")
    candidates = ([Path(env)] if env else []) + [_DEFAULT_SEED] + _FALLBACK_SEEDS
    for p in candidates:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent: guarantees schema + schema_version row

    seed = _resolve_seed()
    if seed is None:
        existing = db.execute(
            "SELECT count(*) FROM derived.sms_reply_is_positive_qwen"
        ).fetchone()[0]
        logger.warning(
            "sms qwen reply seed not found (looked: env/%s + fallbacks) — "
            "leaving existing table untouched (%d rows). No-op.",
            _DEFAULT_SEED, existing,
        )
        return PhaseResult(rows_in=0, rows_out=existing, notes={"skipped": "no_seed"})

    # Count rows in the seed first (attempted), so we can assert committed == attempted.
    rows_in = db.execute(
        "SELECT count(*) FROM read_json_auto(?, format='newline_delimited')", [str(seed)]
    ).fetchone()[0]

    # Rebuild from the seed. CREATE OR REPLACE preserves the canonical column order/types from the
    # DDL via explicit casts; classified_at = load watermark.
    db.execute(
        """
        CREATE OR REPLACE TABLE derived.sms_reply_is_positive_qwen AS
        SELECT
            CAST(reply_id    AS VARCHAR)      AS reply_id,
            CAST(is_positive AS BOOLEAN)      AS is_positive,
            CAST(is_human    AS BOOLEAN)      AS is_human,
            CAST(reason      AS VARCHAR)      AS reason,
            CAST(received_at AS TIMESTAMPTZ)  AS received_at,
            CAST(model       AS VARCHAR)      AS model,
            now()                             AS classified_at
        FROM read_json_auto(?, format='newline_delimited')
        """,
        [str(seed)],
    )

    rows_out = db.execute(
        "SELECT count(*) FROM derived.sms_reply_is_positive_qwen"
    ).fetchone()[0]

    if rows_out != rows_in:
        # Attempted-vs-committed gap = silent-failure surface. Fail the phase loudly so the nightly
        # records a failed ingest rather than a quietly-truncated table.
        raise RuntimeError(
            f"sms_reply_is_positive_qwen row mismatch: seed={rows_in} committed={rows_out} "
            f"(seed={seed})"
        )

    # CREATE OR REPLACE ... AS SELECT (CTAS) does NOT carry the DDL's PRIMARY KEY, so a duplicate
    # reply_id in the seed would load silently and then FAN OUT the LEFT JOIN in v_omni_sms_performance
    # (the row-count assert above can't catch a dup). Enforce one-row-per-reply_id loudly.
    distinct_ids = db.execute(
        "SELECT count(DISTINCT reply_id) FROM derived.sms_reply_is_positive_qwen"
    ).fetchone()[0]
    if distinct_ids != rows_out:
        raise RuntimeError(
            f"sms_reply_is_positive_qwen duplicate reply_id: rows={rows_out} distinct={distinct_ids} "
            f"(seed={seed}) — a dup would fan out the v_omni_sms_performance join"
        )

    positives = db.execute(
        "SELECT count(*) FROM derived.sms_reply_is_positive_qwen WHERE is_positive"
    ).fetchone()[0]
    logger.info(
        "sms_reply_is_positive_qwen: rebuilt %d rows from %s (%d positive)",
        rows_out, seed, positives,
    )

    # Coverage guard. The seed is a point-in-time backfill of the DISTINCT non-opt-out residual;
    # v_omni_sms_performance counts an unlabeled non-opt-out reply as NEITHER positive nor negative.
    # If the seed covers materially less than the current distinct non-opt-out inbound, positives are
    # under-reported — surface that loudly rather than silently. Net-new replies are filled by the
    # incremental classifier; modest lag is expected, a large gap is not.
    coverage = None
    try:
        distinct_nonopt = db.execute(
            "SELECT count(DISTINCT inbound_message_id) FROM raw_sendivo_inbound "
            "WHERE NOT coalesce(is_opt_out, false) AND length(trim(message)) > 0"
        ).fetchone()[0]
        coverage = (rows_out / distinct_nonopt) if distinct_nonopt else 1.0
        log = logger.warning if coverage < 0.90 else logger.info
        log("sms_reply_is_positive_qwen coverage: %d labels / %d distinct non-opt-out inbound (%.1f%%)%s",
            rows_out, distinct_nonopt, 100 * coverage,
            " — LOW: seed may be stale, run the incremental classifier" if coverage < 0.90 else "")
    except Exception as exc:  # never fail the nightly on the advisory check
        logger.warning("sms_reply_is_positive_qwen coverage check skipped: %s", exc)

    return PhaseResult(
        rows_in=rows_in,
        rows_out=rows_out,
        notes={"seed": str(seed), "positive": positives, "coverage": coverage},
    )


def register(registry: Registry) -> None:
    # Ride the existing 'derived' phase — no PHASE_ORDER edit needed.
    registry.add_phase("derived", "sms_reply_is_positive_qwen", run)
