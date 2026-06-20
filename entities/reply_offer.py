"""per-reply offer attribution — durable warehouse table (persistence layer).

Re-materializes derived.reply_offer from the out-of-band seed JSONL on EVERY nightly
rebuild, so the 741,785 reply->offer attributions survive the nightly DROP/rebuild and reach
the gated serving snapshot.

WHY: the table was first loaded as a one-off DIRECT write into the primary. Nothing in the
DDL/sources rebuilt it, so the next nightly would lose it, and it never reached
/opt/duckdb/warehouse_current.duckdb (the read-API's serving copy). This entity is the fix —
same out-of-band-seed pattern as entities/reply_is_positive_qwen.py (DDL 83) and
entities/reply_is_positive_strict.py (DDL 93).

NOT a re-classification: the attributions already exist in the seed file. This is pure
persistence — we never call the LLM here.

Pipeline (idempotent, every nightly):
  seed JSONL --> CREATE OR REPLACE TABLE derived.reply_offer (one row / reply_id)

Seed file (gitignored — *.jsonl + seed_data/ in .gitignore; local/box only):
  seed_data/reply-offer/reply_offer.jsonl
Each line: {"reply_id", "campaign_id", "workspace_id", "offer", "offer_source"}.
campaign_id is nullable (a small number of replies have no campaign); classified_at is
added at load (watermark).

Resilience: if the seed file is absent (a fresh clone without the operator's seed data) we
log + skip rather than DROP an existing table or abort the nightly — exactly like
reply_is_positive_qwen. The DDL (sql/ddl/94_reply_offer.sql) guarantees the empty table
shape exists regardless.

Integrity: we assert rows-committed == rows-in-seed (attempted-vs-committed) and log both, so a
silently-truncated load can never pass as healthy.

Registers under the existing `derived` phase — core/config.py PHASE_ORDER is untouched.
Schema = sql/ddl/94_reply_offer.sql.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.reply_offer")

_DDL = REPO_ROOT / "sql" / "ddl" / "94_reply_offer.sql"

# Seed location. Default = the canonical box seed dir; overridable via env for local dev / a
# different drop path. Falls back to the original results path if the canonical seed is absent
# (covers the box state before the seed is moved into seed_data/).
_DEFAULT_SEED = REPO_ROOT / "seed_data" / "reply-offer" / "reply_offer.jsonl"
_FALLBACK_SEEDS = [
    Path("/root/positive-reply-bi-full/reply_offer.jsonl"),
    Path(os.path.expanduser("~/positive-reply-bi-full/reply_offer.jsonl")),
]


def _resolve_seed() -> Path | None:
    env = os.environ.get("REPLY_OFFER_SEED")
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
            "SELECT count(*) FROM derived.reply_offer"
        ).fetchone()[0]
        logger.warning(
            "reply_offer seed not found (looked: env/%s + fallbacks) — "
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
        CREATE OR REPLACE TABLE derived.reply_offer AS
        SELECT
            CAST(reply_id     AS UUID)    AS reply_id,
            CAST(campaign_id  AS UUID)    AS campaign_id,
            CAST(workspace_id AS VARCHAR) AS workspace_id,
            CAST(offer        AS VARCHAR) AS offer,
            CAST(offer_source AS VARCHAR) AS offer_source,
            now()                         AS classified_at
        FROM read_json_auto(?, format='newline_delimited')
        """,
        [str(seed)],
    )

    rows_out = db.execute(
        "SELECT count(*) FROM derived.reply_offer"
    ).fetchone()[0]

    if rows_out != rows_in:
        # Attempted-vs-committed gap = silent-failure surface. Fail the phase loudly so the
        # nightly records a failed ingest rather than a quietly-truncated table.
        raise RuntimeError(
            f"reply_offer row mismatch: seed={rows_in} committed={rows_out} "
            f"(seed={seed})"
        )

    distinct_offers = db.execute(
        "SELECT count(DISTINCT offer) FROM derived.reply_offer"
    ).fetchone()[0]
    logger.info(
        "reply_offer: rebuilt %d rows from %s (%d distinct offers)",
        rows_out, seed, distinct_offers,
    )
    return PhaseResult(
        rows_in=rows_in,
        rows_out=rows_out,
        notes={"seed": str(seed), "distinct_offers": distinct_offers},
    )


def register(registry: Registry) -> None:
    # Ride the existing 'derived' phase — no PHASE_ORDER edit needed.
    registry.add_phase("derived", "reply_offer", run)
