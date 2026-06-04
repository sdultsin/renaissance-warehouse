"""core.opportunity canonical entity — lead-level opportunity records (spec 10, revised).

IMPORTANT (Sam, 2026-05-31): "opportunities" is NOT the same as Instantly lead-status
`lead_interested`. We track OPPORTUNITIES, not interested-status. The earlier build used
`lead_events WHERE event_type='lead_interested'` — that was the wrong signal and is removed.

What "opportunity" means in the warehouse:
  * LEAD-LEVEL opportunity records (who specifically is an opportunity, with contact +
    call disposition) exist only in the warm-call/AIM pipeline: `raw_comms_call_opportunity`
    (source-aware: 'sendivo' SMS opps + a few 'instantly' email opps routed to calling).
    That is what core.opportunity holds.
  * INSTANTLY EMAIL OPPORTUNITIES (the dominant dashboard KPI — opportunities → meetings)
    exist ONLY as the aggregate `raw_pipeline_campaign_daily_metrics.opportunities`
    (per campaign×day). There is NO populated lead-level Instantly opportunity table in the
    mirror (`opportunity_webhook_log` is empty; lead_events only carries lead-status events).
    For the Instantly opportunity KPI, query campaign_daily_metrics — see SCHEMA.md + GAPS B8.

Full rebuild each run from the latest snapshot of raw_comms_call_opportunity.
Registers under the 'canonical' phase.
"""
from __future__ import annotations

import logging

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.opportunity")

CALL_OPP = "raw_comms_call_opportunity"


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "opportunity", run_opportunity)


def run_opportunity(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    db.execute("DELETE FROM core.opportunity")

    have = db.execute(
        f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{CALL_OPP}'"
    ).fetchone()[0]
    if not have or db.execute(f"SELECT count(*) FROM {CALL_OPP}").fetchone()[0] == 0:
        logger.warning("%s empty/absent — core.opportunity left empty", CALL_OPP)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no call_opportunity"})

    db.execute(
        f"""
        INSERT INTO core.opportunity
          (opportunity_id, source, source_event_id, lead_email, campaign_id,
           workspace_id, opened_at, state, state_updated_at, is_duplicate_of,
           cost_per_opp_usd_estimated, raw, _resolved_at)
        SELECT
          source || ':' || CAST(id AS VARCHAR)          AS opportunity_id,
          source,                                       -- 'sendivo' | 'instantly' (warm-call routed)
          CAST(id AS VARCHAR)                           AS source_event_id,
          email                                         AS lead_email,
          NULL                                          AS campaign_id,   -- not carried on call_opportunity
          source_workspace_id                           AS workspace_id,
          COALESCE(opportunity_marked_at, created_at)   AS opened_at,
          status                                        AS state,
          updated_at                                    AS state_updated_at,
          CASE WHEN duplicate_of IS NOT NULL
               THEN source || ':' || CAST(duplicate_of AS VARCHAR) END AS is_duplicate_of,
          NULL                                          AS cost_per_opp_usd_estimated,
          NULL                                          AS raw,
          now()                                         AS _resolved_at
        FROM {CALL_OPP}
        WHERE _run_id = (SELECT _run_id FROM {CALL_OPP} ORDER BY _loaded_at DESC LIMIT 1)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at) = 1
        """
    )

    n = db.execute("SELECT count(*) FROM core.opportunity").fetchone()[0]
    by_src = dict(db.execute(
        "SELECT source, count(*) FROM core.opportunity GROUP BY source"
    ).fetchall())
    n_unique = db.execute(
        "SELECT count(*) FROM core.opportunity WHERE state <> 'duplicate'"
    ).fetchone()[0]
    logger.info("core.opportunity rebuilt: %d rows (%s); %d non-duplicate",
                n, by_src, n_unique)
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"by_source": by_src, "non_duplicate": n_unique})
