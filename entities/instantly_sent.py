"""IAM manual sent email ingest — pulls ue_type=3 outbound replies from Instantly.

Stores one row per manual IAM reply into raw_instantly_sent_email.
Used by iam_response_time to find when an IAM responded to each prospect reply.

Incremental: uses the latest sent_timestamp already stored minus a 2-day overlap.
Full-backfill mode: set WAREHOUSE_PULL_IAM_SENT=full to force from=None.
Runs in the `instantly` phase (after instantly_replies).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.instantly_sent")

_OVERLAP = timedelta(days=2)

_DDL_PATH = None  # inline DDL below — table created in setup_db

_COLS = [
    "email_id", "campaign_id", "workspace_id", "lead_email", "from_address_email",
    "eaccount", "subject", "thread_id", "message_id", "sent_timestamp",
    "i_status", "api_response_raw", "_loaded_at", "_run_id",
]
_PLACEHOLDERS = ", ".join("?" for _ in _COLS)
_UPDATE_SET = ", ".join(f"{c} = excluded.{c}" for c in _COLS if c != "email_id")
_UPSERT_SQL = (
    f"INSERT INTO raw_instantly_sent_email ({', '.join(_COLS)}) "
    f"VALUES ({_PLACEHOLDERS}) "
    f"ON CONFLICT (email_id) DO UPDATE SET {_UPDATE_SET}"
)


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "instantly_sent", run_instantly_sent_ingest)


def run_instantly_sent_ingest(ctx: RunContext) -> PhaseResult:
    if os.environ.get("WAREHOUSE_PULL_IAM_SENT", "").lower() not in ("1", "full", "true"):
        logger.info("WAREHOUSE_PULL_IAM_SENT not set — skipping IAM sent email ingest")
        return PhaseResult(notes={"reason": "disabled"})

    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping IAM sent ingest")
        return PhaseResult(notes={"reason": "no_keys"})

    full_backfill = os.environ.get("WAREHOUSE_PULL_IAM_SENT", "").lower() == "full"

    if full_backfill:
        since = None
        logger.info("IAM sent ingest: FULL BACKFILL mode")
    else:
        row = ctx.db.execute(
            "SELECT max(sent_timestamp) FROM raw_instantly_sent_email"
        ).fetchone()
        since = None
        if row and row[0] is not None:
            since = (row[0] - _OVERLAP).astimezone(timezone.utc).isoformat()
        logger.info("IAM sent ingest watermark since=%s", since or "(full backfill)")

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
                    logger.info("Skipping duplicate workspace slug=%s", slug)
                    continue
                seen_workspace_ids.add(workspace_id)

                w_rows = 0
                for e in client.sent_emails(since=since, workspace_id=workspace_id):
                    email_id = e.get("id")
                    if not email_id:
                        continue
                    lead = e.get("lead") or e.get("to_address_email_list")
                    values = [
                        email_id,
                        e.get("campaign_id"),
                        e.get("organization_id") or workspace_id,
                        lead,
                        e.get("from_address_email"),
                        e.get("eaccount") or e.get("from_address_email"),
                        e.get("subject"),
                        e.get("thread_id"),
                        e.get("message_id"),
                        e.get("timestamp_email") or e.get("timestamp_created"),
                        e.get("i_status"),
                        json.dumps(e),
                        now,
                        ctx.run_id,
                    ]
                    ctx.db.execute(_UPSERT_SQL, values)
                    w_rows += 1
                    rows_out += 1

                workspaces_done.append(slug)
                logger.info("Workspace %s (id=%s): %d IAM sent emails", slug, workspace_id, w_rows)
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(
        rows_in=rows_out,
        rows_out=rows_out,
        notes={"workspaces_done": workspaces_done, "failures": failures, "since": since},
    )
