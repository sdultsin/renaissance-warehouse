"""Append-only daily snapshot of campaign-grain cumulative analytics.

Runs in the `instantly` phase, AFTER campaign_analytics. Entity discovery is
alphabetical (core/orchestrator.discover_and_register sorts the glob), so
`campaign_analytics_snapshot` registers — and therefore runs — right after
`campaign_analytics`, when raw_instantly_campaign_analytics is already refreshed
for this run.

It copies the current campaign-grain cumulative counters into an append-only,
date-stamped table so accurate per-WINDOW opportunity/sent counts can be
reconstructed by differencing — and, crucially, survive a campaign being deleted
from Instantly (the analytics endpoint only returns live campaigns, so once a
campaign is gone we can never re-derive its windowed counts; capturing the
cumulative daily while it is alive is the only deletion-proof method).

See sql/ddl/1066_campaign_analytics_snapshot.sql for the full rationale.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.campaign_analytics_snapshot")

# Snapshot EVERY campaign currently in the analytics table (not just this run's
# rows): live campaigns get today's fresh cumulative; already-frozen campaigns
# get their last-known value re-stamped for today (harmless — the daily delta is
# zero). UPSERT on (snapshot_date, campaign_id) so a same-day re-run overwrites.
_SNAPSHOT_SQL = """
INSERT INTO raw_instantly_campaign_analytics_snapshot
  (snapshot_date, campaign_id, workspace_id, campaign_name, campaign_status,
   emails_sent_count, reply_count_unique, total_opportunities,
   total_opportunity_value, _loaded_at, _run_id)
SELECT
  CURRENT_DATE, campaign_id, workspace_id, campaign_name, campaign_status,
  emails_sent_count, reply_count_unique, total_opportunities,
  total_opportunity_value, ?, ?
FROM raw_instantly_campaign_analytics
ON CONFLICT (snapshot_date, campaign_id) DO UPDATE SET
  workspace_id            = excluded.workspace_id,
  campaign_name           = excluded.campaign_name,
  campaign_status         = excluded.campaign_status,
  emails_sent_count       = excluded.emails_sent_count,
  reply_count_unique      = excluded.reply_count_unique,
  total_opportunities     = excluded.total_opportunities,
  total_opportunity_value = excluded.total_opportunity_value,
  _loaded_at              = excluded._loaded_at,
  _run_id                 = excluded._run_id
"""


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "campaign_analytics_snapshot", run_campaign_analytics_snapshot)


def run_campaign_analytics_snapshot(ctx: RunContext) -> PhaseResult:
    # Guard: if the source table is missing/empty (e.g. analytics phase had no
    # keys), do nothing rather than write an empty snapshot day.
    src = ctx.db.execute(
        "SELECT count(*) FROM raw_instantly_campaign_analytics"
    ).fetchone()[0]
    if not src:
        logger.warning("raw_instantly_campaign_analytics is empty — skipping snapshot")
        return PhaseResult(notes={"reason": "source_empty"})

    now = datetime.now(timezone.utc)
    ctx.db.execute(_SNAPSHOT_SQL, [now, ctx.run_id])
    n = ctx.db.execute(
        "SELECT count(*) FROM raw_instantly_campaign_analytics_snapshot "
        "WHERE snapshot_date = CURRENT_DATE"
    ).fetchone()[0]
    logger.info("Snapshotted %d campaign rows for today", n)
    return PhaseResult(rows_out=n, notes={"snapshot_rows_today": n})
