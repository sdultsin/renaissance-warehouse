"""Direct-Instantly inbound reply ingest (Pipeline-Supabase retirement, approach A).

Pulls GET /api/v2/emails?email_type=received per workspace and UPSERTs into
raw_instantly_email (one row per Instantly email id). This is the direct-Instantly
substitute for the slim mirror of pipeline-supabase.public.reply_data — the n8n
webhook collector that produces reply_data is not owned by us, and repointing the
warehouse to the Instantly /emails source removes that dependency (and the
INSERT-vs-UPSERT dup bug). See sql/ddl/36_instantly_replies.sql.

ADDITIVE + BEHIND CONFIG: this ingest only runs when WAREHOUSE_PULL_REPLIES=1
(env). Until parity is confirmed via v_reply_source_parity, the existing
raw_pipeline_reply_data mirror stays the source for v_campaign_metrics; this table
is populated in parallel for comparison only. Once parity holds, flip the canonical
view's `pipe_replies` CTE to raw_instantly_email and drop the mirror.

Runs in the `instantly` phase (after `campaign_analytics`), serial across
workspaces per feedback_instantly_list_accounts_serial_only.

INCREMENTAL: pulls replies newer than the latest reply_timestamp already in the
table minus a 2-day overlap (the /emails endpoint is newest-first, so the client
stops paginating once it crosses the cutoff). First run with an empty table does a
full backfill of the workspace's received emails.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.instantly_replies")

_OVERLAP = timedelta(days=2)

_COLS = [
    "email_id", "campaign_id", "workspace_id", "lead_email", "from_address_email",
    "eaccount", "subject", "reply_text", "step", "ue_type", "thread_id",
    "message_id", "reply_timestamp", "api_response_raw", "_loaded_at", "_run_id",
]
_PLACEHOLDERS = ", ".join("?" for _ in _COLS)
_UPDATE_SET = ", ".join(f"{c} = excluded.{c}" for c in _COLS if c != "email_id")
_UPSERT_SQL = (
    f"INSERT INTO raw_instantly_email ({', '.join(_COLS)}) "
    f"VALUES ({_PLACEHOLDERS}) "
    f"ON CONFLICT (email_id) DO UPDATE SET {_UPDATE_SET}"
)


def _body_text(item: dict) -> str | None:
    body = item.get("body")
    if isinstance(body, dict):
        return body.get("html") or body.get("text")
    if isinstance(body, str):
        return body
    return None


def _to_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "instantly_replies", run_instantly_replies_ingest)


def run_instantly_replies_ingest(ctx: RunContext) -> PhaseResult:
    if os.environ.get("WAREHOUSE_PULL_REPLIES") != "1":
        logger.info("WAREHOUSE_PULL_REPLIES != 1 — skipping direct-Instantly reply ingest")
        return PhaseResult(notes={"reason": "disabled"})

    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping reply ingest")
        return PhaseResult(notes={"reason": "no_keys"})

    # Incremental watermark: latest reply already stored, minus overlap.
    row = ctx.db.execute(
        "SELECT max(reply_timestamp) FROM raw_instantly_email"
    ).fetchone()
    since = None
    if row and row[0] is not None:
        since = (row[0] - _OVERLAP).astimezone(timezone.utc).isoformat()
    logger.info("Reply ingest watermark since=%s", since or "(full backfill)")

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
                for e in client.received_emails(since=since, workspace_id=workspace_id):
                    email_id = e.get("id")
                    if not email_id:
                        continue
                    values = [
                        email_id,
                        e.get("campaign_id"),
                        e.get("organization_id") or workspace_id,
                        e.get("lead"),
                        e.get("from_address_email"),
                        e.get("eaccount"),
                        e.get("subject"),
                        _body_text(e),
                        _to_int(e.get("step")),
                        _to_int(e.get("ue_type")),
                        e.get("thread_id"),
                        e.get("message_id"),
                        e.get("timestamp_email") or e.get("timestamp_created"),
                        json.dumps(e),
                        now,
                        ctx.run_id,
                    ]
                    ctx.db.execute(_UPSERT_SQL, values)
                    w_rows += 1
                    rows_out += 1

                workspaces_done.append(slug)
                logger.info("Workspace %s (id=%s): %d received emails", slug, workspace_id, w_rows)
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
