"""main.raw_reply_label_event — nightly incremental load of the reply-label escrow (DDL 1110).

Each nightly ('canonical' phase): anti-join load of NEW events from the labeler escrow
parquet into main.raw_reply_label_event. The escrow is append-only JSONL regenerated to
parquet at PAUSED/DONE and after each daily increment
(deliverables/2026-07-14-cold-email-bi/labeling/v02-backfill-runbook.md); keys are 1:1
with the table columns. Events are NEVER updated or deleted here (charter §4 append-only
label history); the anti-join key is the table's uniqueness grain
(message_ref_table, message_ref_id, labeler_version).

PATH / REHOME (⚠ migration lane): escrow default is
/root/mof/labeling/backfill/escrow/events.parquet — /root/mof DIES WITH THE DROPLET
(~2026-07-25). Override via env REPLY_LABEL_ESCROW_PARQUET. The rehome must carry the
escrow directory (or repoint the labeler's output) and set the env var; the warehouse
table itself rides the nightly publish + MotherDuck migration, so history already loaded
is safe regardless.

TIMESTAMPS: escrow message_ts/labeled_at are UTC-naive TIMESTAMP; the table columns are
TIMESTAMPTZ. The cast assumes a UTC session (true on the droplet); do not run this from
a non-UTC host without setting the DuckDB TimeZone.

FAIL-SOFT (DDL-92 nightly-killer class): missing table or missing/unreadable escrow file
degrades to a logged warning + no-op, never a raise. A shrunken escrow (fewer rows than
already loaded for the file's labeler versions) logs a warning — append-only escrow
should only grow.

ONE-TIME FIRST LOAD (ship step): run orchestrator-scoped under the write lock —
  scripts/with_warehouse_lock.sh .venv/bin/python -m core.orchestrator \
      --phase canonical --ingest reply_label_event
(NEVER `python -m entities.reply_label_event` — entities are orchestrator phases with no
__main__; that invocation silently no-ops. Verify by ROW COUNTS, not exit codes.)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.reply_label_event")

DEFAULT_ESCROW = "/root/mof/labeling/backfill/escrow/events.parquet"

# Escrow columns, 1:1 with main.raw_reply_label_event (DDL 1110); _loaded_at/_run_id are
# warehouse-side provenance appended here.
_COLS = [
    "event_id", "workspace_slug", "lead_email", "campaign_id",
    "message_ref_table", "message_ref_id", "message_ts", "label", "opt_out",
    "confidence", "refute_fired", "refute_agree", "refute_alt_label",
    "evidence", "rationale", "deterministic_gate", "flag_human", "n_inbound",
    "trick_class", "labeler_version", "prompt_hash", "model", "snapshot_id",
    "labeled_at",
]

_LOAD = f"""
INSERT INTO main.raw_reply_label_event ({', '.join(_COLS)}, _run_id)
SELECT {', '.join('e.' + c for c in _COLS)}, ?
FROM read_parquet(?) e
ANTI JOIN main.raw_reply_label_event t
  USING (message_ref_table, message_ref_id, labeler_version)
"""


def _table_exists(conn, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
    )


def run(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    if not _table_exists(conn, "main", "raw_reply_label_event"):
        logger.warning("reply_label_event SKIP: main.raw_reply_label_event missing (DDL 1110 not applied yet).")
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "table missing"})

    escrow = Path(os.environ.get("REPLY_LABEL_ESCROW_PARQUET", DEFAULT_ESCROW))
    if not escrow.exists():
        logger.warning(
            "reply_label_event SKIP: escrow parquet missing at %s "
            "(set REPLY_LABEL_ESCROW_PARQUET; /root/mof dies with the droplet ~2026-07-25 — "
            "the rehome must carry the escrow).", escrow)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": f"escrow missing: {escrow}"})

    try:
        src_rows = conn.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(escrow)]
        ).fetchone()[0]
    except Exception as exc:  # unreadable/corrupt parquet mid-regeneration — keep last-good, retry next nightly
        logger.warning("reply_label_event SKIP: escrow unreadable (%s) — retrying next run.", exc)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": f"escrow unreadable: {exc}"})

    before = conn.execute("SELECT count(*) FROM main.raw_reply_label_event").fetchone()[0]
    if src_rows < before:
        logger.warning(
            "reply_label_event: escrow has FEWER rows (%d) than already loaded (%d) — "
            "append-only escrow should only grow; loading the delta anyway (anti-join is safe).",
            src_rows, before)

    conn.execute(_LOAD, [ctx.run_id, str(escrow)])
    after = conn.execute("SELECT count(*) FROM main.raw_reply_label_event").fetchone()[0]
    loaded = after - before

    label_mix = dict(conn.execute(
        "SELECT label, count(*) FROM main.raw_reply_label_event GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall())
    logger.info("reply_label_event: +%d events (escrow %d, table now %d). label mix: %s",
                loaded, src_rows, after, label_mix)
    return PhaseResult(rows_in=src_rows, rows_out=loaded,
                       notes={"escrow_rows": src_rows, "loaded": loaded,
                              "table_rows": after, "label_mix": label_mix})


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "reply_label_event", run)
