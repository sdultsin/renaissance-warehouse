"""enriched SMS-meeting email->mobile phones — durable warehouse table (persistence layer).

Re-materializes derived.sms_meeting_phone_enriched from the out-of-band seed JSONL on every
nightly rebuild, so the email->phone enrichment results (LeadMagic, pay-per-valid) survive the
DROP/rebuild and reach the gated serving snapshot. Same pattern as entities/reply_is_positive_strict.py.

NOT a re-enrichment: the rows already exist in the seed. Pure persistence — never calls a vendor.

Seed (gitignored — *.jsonl + seed_data/ in .gitignore; box/local only):
  seed_data/sms-meeting-phone/enriched.jsonl
Each line: {"lead_email","phone_e164","source"}.  loaded_at is added at load (watermark).

Resilience: if the seed is absent (fresh clone) we log + skip rather than DROP an existing table
or abort the nightly. Integrity: assert rows-committed == rows-in-seed, log both, so a truncated
load fails loud. Registers under the existing 'derived' phase — PHASE_ORDER untouched.
Schema = sql/ddl/115_sms_meeting_phone_enriched.sql.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.sms_meeting_phone_enriched")

_DDL = REPO_ROOT / "sql" / "ddl" / "115_sms_meeting_phone_enriched.sql"
_DEFAULT_SEED = REPO_ROOT / "seed_data" / "sms-meeting-phone" / "enriched.jsonl"


def _resolve_seed() -> Path | None:
    env = os.environ.get("SMS_MEETING_PHONE_SEED")
    for p in ([Path(env)] if env else []) + [_DEFAULT_SEED]:
        if p.exists() and p.stat().st_size > 0:
            return p
    return None


def run(ctx: RunContext) -> PhaseResult:
    db = ctx.db
    db.execute(_DDL.read_text())  # idempotent: guarantees schema

    seed = _resolve_seed()
    if seed is None:
        existing = db.execute(
            "SELECT count(*) FROM derived.sms_meeting_phone_enriched"
        ).fetchone()[0]
        logger.warning(
            "sms_meeting_phone seed not found (looked: env/%s) — leaving existing table "
            "untouched (%d rows). No-op.", _DEFAULT_SEED, existing,
        )
        return PhaseResult(rows_in=0, rows_out=existing, notes={"skipped": "no_seed"})

    rows_in = db.execute(
        "SELECT count(*) FROM read_json_auto(?, format='newline_delimited')", [str(seed)]
    ).fetchone()[0]

    db.execute(
        """
        CREATE OR REPLACE TABLE derived.sms_meeting_phone_enriched AS
        SELECT
            CAST(lead_email AS VARCHAR) AS lead_email,
            CAST(phone_e164 AS VARCHAR) AS phone_e164,
            CAST(source     AS VARCHAR) AS source,
            now()                       AS loaded_at
        FROM read_json_auto(?, format='newline_delimited')
        """,
        [str(seed)],
    )

    rows_out = db.execute(
        "SELECT count(*) FROM derived.sms_meeting_phone_enriched"
    ).fetchone()[0]
    if rows_out != rows_in:
        raise RuntimeError(
            f"sms_meeting_phone_enriched row mismatch: seed={rows_in} committed={rows_out} "
            f"(seed={seed})"
        )
    logger.info("sms_meeting_phone_enriched: rebuilt %d rows from %s", rows_out, seed)
    return PhaseResult(rows_in=rows_in, rows_out=rows_out, notes={"seed": str(seed)})


def register(registry: Registry) -> None:
    registry.add_phase("derived", "sms_meeting_phone_enriched", run)
