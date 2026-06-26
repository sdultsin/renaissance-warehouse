"""inbox_hub: nightly materialization of the canonical inbox hub.

Re-materializes core.inbox_hub from the live view core.v_inbox_overview on EVERY nightly
rebuild, in the 'derived' phase (AFTER the Instantly feeders — account_census, account_tags,
account_campaign_live — have populated). Gives a fast plain-table form (ms reads) of the
hub for automations to query all day instead of hitting the Instantly API.

Pure copy: CREATE OR REPLACE TABLE core.inbox_hub AS SELECT * FROM core.v_inbox_overview,
then re-apply the self-describing comment (the CREATE OR REPLACE drops it). Auto-matches
the view's columns, so adding a column to the view needs no change here. Integrity: assert
the materialized row count matches the view (attempted-vs-committed) so a silent truncation
can't pass as healthy. Registers under the existing 'derived' phase; PHASE_ORDER untouched.
Schema/comment = sql/ddl/101_inbox_hub.sql.
"""

from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.inbox_hub")

# Kept BYTE-IDENTICAL to the COMMENT ON TABLE in sql/ddl/101_inbox_hub.sql so the nightly
# re-apply does not shrink the self-describing comment clients introspect.
_COMMENT = (
    "CANONICAL INBOX HUB — the one true list of every live sending inbox (~433k), one row "
    "each, with identity, workspace, provider, status, lifecycle stage, ALL lifecycle dates "
    "(created/connected/warmup-start/first+last cold send/paused/retired), tags, batch/RG, "
    "and campaign membership. QUERY THIS (or the live view core.v_inbox_overview it "
    "materializes) for ANY inbox lookup instead of the Instantly API. Refreshed nightly. "
    "Created 2026-06-26."
)


def register(registry: Registry) -> None:
    registry.add_phase("derived", "inbox_hub", run_inbox_hub_materialize)


def run_inbox_hub_materialize(ctx: RunContext) -> PhaseResult:
    # source-of-truth count from the view
    view_n = ctx.db.execute("SELECT count(*) FROM core.v_inbox_overview").fetchone()[0]

    ctx.db.execute(
        "CREATE OR REPLACE TABLE core.inbox_hub AS SELECT * FROM core.v_inbox_overview"
    )
    # CREATE OR REPLACE drops the table comment — re-apply it so the hub stays self-describing.
    ctx.db.execute("COMMENT ON TABLE core.inbox_hub IS ?", [_COMMENT])

    table_n = ctx.db.execute("SELECT count(*) FROM core.inbox_hub").fetchone()[0]
    if table_n != view_n:
        # never let a silent truncation pass as healthy
        raise RuntimeError(
            f"inbox_hub materialize mismatch: view={view_n} table={table_n}"
        )
    logger.info("inbox_hub materialized: %d inboxes", table_n)
    return PhaseResult(notes={"inboxes": table_n})
