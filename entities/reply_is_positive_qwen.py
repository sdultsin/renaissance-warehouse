"""qwen positive-reply labels — durable warehouse table (persistence layer).

Re-materializes derived.reply_is_positive_qwen from the out-of-band seed JSONL on EVERY
nightly rebuild, so the 741,785 offline-qwen reply classifications survive the nightly
DROP/rebuild and reach the gated serving snapshot.

WHY: the table was first loaded as a one-off `CREATE OR REPLACE TABLE` into the primary.
Nothing in the DDL/sources rebuilt it, so the next nightly would lose it, and it never
reached /opt/duckdb/warehouse_current.duckdb (the read-API's serving copy). This entity is
the fix — same out-of-band-seed pattern as entities/partner_feedback.py (loads a local seed
file the public repo never commits).

NOT a re-classification: the labels already exist in the seed file. This is pure persistence
— we never call the LLM here.

Pipeline (idempotent, every nightly):
  seed JSONL --> CREATE OR REPLACE TABLE derived.reply_is_positive_qwen (one row / reply_id)

Seed file (gitignored — *.jsonl + seed_data/ in .gitignore; local/box only):
  seed_data/reply-is-positive-qwen/full_qwen.partial.jsonl
Each line: {"reply_id", "is_positive", "confidence", "reason", "is_question",
            "is_referral", "is_later", "model"}.  classified_at is added at load (watermark).

Resilience: if the seed file is absent (a fresh clone without the operator's seed data) we
log + skip rather than DROP an existing table or abort the nightly — exactly like
partner_feedback. The DDL (sql/ddl/83_reply_is_positive_qwen.sql) guarantees the empty table
shape exists regardless.

Integrity: we assert rows-committed == rows-in-seed (attempted-vs-committed) and log both, so a
silently-truncated load can never pass as healthy.

Registers under the existing `derived` phase — core/config.py PHASE_ORDER is untouched.
Schema = sql/ddl/83_reply_is_positive_qwen.sql.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.reply_is_positive_qwen")

_DDL = REPO_ROOT / "sql" / "ddl" / "83_reply_is_positive_qwen.sql"

# Seed location. Default = the canonical box seed dir; overridable via env for local dev / a
# different drop path. Falls back to the original results path if the canonical seed is absent
# (covers the box state before the seed is moved into seed_data/).
_DEFAULT_SEED = REPO_ROOT / "seed_data" / "reply-is-positive-qwen" / "full_qwen.partial.jsonl"
_FALLBACK_SEEDS = [
    Path("/root/positive-reply-bi/results/full_qwen.partial.jsonl"),
    Path(os.path.expanduser("~/positive-reply-bi/results/full_qwen.partial.jsonl")),
]


def _resolve_seed() -> Path | None:
    env = os.environ.get("REPLY_IS_POSITIVE_QWEN_SEED")
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
            "SELECT count(*) FROM derived.reply_is_positive_qwen"
        ).fetchone()[0]
        logger.warning(
            "qwen positive-reply seed not found (looked: env/%s + fallbacks) — "
            "leaving existing table untouched (%d rows). No-op.",
            _DEFAULT_SEED, existing,
        )
        return PhaseResult(rows_in=0, rows_out=existing, notes={"skipped": "no_seed"})

    # Count rows in the seed first (attempted), so we can assert committed == attempted.
    rows_in = db.execute(
        "SELECT count(*) FROM read_json_auto(?, format='newline_delimited')", [str(seed)]
    ).fetchone()[0]

    # Rebuild the table from the seed. CREATE OR REPLACE preserves the canonical column
    # order/types from the DDL via explicit casts; classified_at = load watermark.
    db.execute(
        """
        CREATE OR REPLACE TABLE derived.reply_is_positive_qwen AS
        SELECT
            CAST(reply_id    AS UUID)      AS reply_id,
            CAST(is_positive AS BOOLEAN)   AS is_positive,
            CAST(confidence  AS JSON)      AS confidence,
            CAST(reason      AS VARCHAR)   AS reason,
            CAST(is_question AS BOOLEAN)   AS is_question,
            CAST(is_referral AS BOOLEAN)   AS is_referral,
            CAST(is_later    AS BOOLEAN)   AS is_later,
            CAST(model       AS VARCHAR)   AS model,
            now()                          AS classified_at
        FROM read_json_auto(?, format='newline_delimited')
        """,
        [str(seed)],
    )

    rows_out = db.execute(
        "SELECT count(*) FROM derived.reply_is_positive_qwen"
    ).fetchone()[0]

    if rows_out != rows_in:
        # Attempted-vs-committed gap = silent-failure surface. Fail the phase loudly so the
        # nightly records a failed ingest rather than a quietly-truncated table.
        raise RuntimeError(
            f"reply_is_positive_qwen row mismatch: seed={rows_in} committed={rows_out} "
            f"(seed={seed})"
        )

    positives = db.execute(
        "SELECT count(*) FROM derived.reply_is_positive_qwen WHERE is_positive"
    ).fetchone()[0]
    logger.info(
        "reply_is_positive_qwen: rebuilt %d rows from %s (%d positive)",
        rows_out, seed, positives,
    )
    return PhaseResult(
        rows_in=rows_in,
        rows_out=rows_out,
        notes={"seed": str(seed), "positive": positives},
    )


def register(registry: Registry) -> None:
    # Ride the existing 'derived' phase — no PHASE_ORDER edit needed.
    registry.add_phase("derived", "reply_is_positive_qwen", run)
