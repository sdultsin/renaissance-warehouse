"""Campaign-grain analytics ingest.

Pulls GET /campaigns/analytics per workspace (one call returns every campaign in
the workspace) and UPSERTs into raw_instantly_campaign_analytics — exactly one
row per campaign, so SUM()/COUNT() are safe with no _run_id filter.

WHY (2026-06-02): the daily fact table's `unique_replies` / `unique_opportunities`
are per-day-distinct counts and overcount when summed across days. The analytics
endpoint is the only source matching the Instantly UI (sent / replied / opps).
Full rationale in sql/ddl/32_campaign_analytics.sql.

Runs in the `instantly` phase, after `campaign` (so core.campaign exists for the
canonical view to join), serial across workspaces per the no-parallel rule
(feedback_instantly_list_accounts_serial_only).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.campaign_analytics")


# Column order for the upsert. Matches sql/ddl/32_campaign_analytics.sql.
_COLS = [
    "campaign_id", "workspace_id", "campaign_name", "campaign_status",
    "campaign_is_evergreen", "leads_count", "contacted_count", "emails_sent_count",
    "new_leads_contacted_count", "open_count", "open_count_unique", "reply_count",
    "reply_count_unique", "reply_count_automatic", "reply_count_automatic_unique",
    "link_click_count", "link_click_count_unique", "bounced_count",
    "unsubscribed_count", "completed_count", "total_opportunities",
    "total_opportunity_value", "api_response_raw", "_loaded_at", "_run_id",
]

# Subset that maps 1:1 to JSON keys (everything except api_response_raw/_loaded_at/_run_id).
_JSON_FIELDS = [
    "campaign_id", "workspace_id", "campaign_name", "campaign_status",
    "campaign_is_evergreen", "leads_count", "contacted_count", "emails_sent_count",
    "new_leads_contacted_count", "open_count", "open_count_unique", "reply_count",
    "reply_count_unique", "reply_count_automatic", "reply_count_automatic_unique",
    "link_click_count", "link_click_count_unique", "bounced_count",
    "unsubscribed_count", "completed_count", "total_opportunities",
    "total_opportunity_value",
]

_PLACEHOLDERS = ", ".join("?" for _ in _COLS)
_UPDATE_SET = ", ".join(
    f"{c} = excluded.{c}" for c in _COLS if c != "campaign_id"
)
_UPSERT_SQL = (
    f"INSERT INTO raw_instantly_campaign_analytics ({', '.join(_COLS)}) "
    f"VALUES ({_PLACEHOLDERS}) "
    f"ON CONFLICT (campaign_id) DO UPDATE SET {_UPDATE_SET}"
)


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "campaign_analytics", run_campaign_analytics_ingest)


def run_campaign_analytics_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping campaign analytics")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    rows_out = 0
    failures: list[dict] = []
    workspaces_done: list[str] = []
    seen_workspace_ids: set[str] = set()

    for slug in sorted(keys.keys()):
        api_key = keys[slug]
        try:
            with InstantlyClient(api_key) as client:
                ws = client.get_current_workspace()
                workspace_id = ws.get("id")
                if not workspace_id:
                    failures.append({"slug": slug, "error": "missing_workspace_id"})
                    continue
                if workspace_id in seen_workspace_ids:
                    logger.info("Skipping duplicate workspace slug=%s (id %s seen)", slug, workspace_id)
                    continue
                seen_workspace_ids.add(workspace_id)

                w_rows = 0
                for a in client.campaign_analytics():
                    campaign_id = a.get("campaign_id")
                    if not campaign_id:
                        continue
                    values = [a.get(f) for f in _JSON_FIELDS]
                    # workspace_id from the endpoint may be absent — trust the key's workspace.
                    values[1] = a.get("workspace_id") or workspace_id
                    values += [json.dumps(a), now, ctx.run_id]
                    ctx.db.execute(_UPSERT_SQL, values)
                    w_rows += 1
                    rows_out += 1

                workspaces_done.append(slug)
                logger.info("Workspace %s (id=%s): %d campaign-analytics rows", slug, workspace_id, w_rows)
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(
        rows_in=rows_out,
        rows_out=rows_out,
        notes={"workspaces_done": workspaces_done, "failures": failures},
    )
