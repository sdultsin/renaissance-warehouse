"""account_tag: nightly account <-> infra-TAG membership sync.

Populates core.sending_account_tag (the account->tag edge table) from Instantly —
the GENERATOR that DDL 111 referenced ("the extended poll_live_accounts.py raw-API
tag sync; see gen_account_tag_sync.py") but that was never built, leaving the table
empty (0 rows, measured 2026-06-23 / HANDOFF-WAREHOUSE-B Gap 1).

Per workspace key (serial, mirrors entities/campaign.py):
  1. GET /custom-tags -> filter to the WORKFLOW tags Sam manages
     ("Reseller Active/Warmup", "Outreach Today Active/Warmup").
  2. For each workflow tag: GET /accounts?tag_ids=<id> (SERVER-SIDE filter; verified
     public-v2, 2026-06-23) -> the member account emails + live status/daily_limit.
  3. INSERT one raw row per (email, tag) into raw_instantly_account_tag.

After all workspaces ingest, resolve the run's raw rows into core.sending_account_tag:
  - UPSERT membership (keep first_seen_at; refresh last_seen_at/tag_id/workspace_slug),
    filling provider_code + canonical workspace_slug from the latest census.
  - PRUNE memberships that disappeared (an account removed from a tag), scoped to the
    (workspace, tag) pairs we SUCCESSFULLY scanned this run so a single failed
    workspace can never wipe its rows.

Registered under the 'instantly' phase. Account->tag is a live, daily-changing edge
(HANDOFF-B hard rule) — this refreshes it every nightly run.

Once this lands (with HANDOFF-A's census-based capacity), /cm-work Step 1-2.3 can move
per-tag sending capacity from Instantly back to the warehouse via the new
core.v_sending_capacity_by_tag view (DDL 1002).
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_tag")

# The infra/workflow tags Sam manages (memory: active tags = "Reseller Active" +
# "Outreach Today Active"; the "Warmup" variants feed core.v_otd_tag_membership).
# A tag is in-scope if its label matches this regex OR is listed in the env override
# WAREHOUSE_ACCOUNT_TAG_LABELS (comma-separated) — lets Sam add a workflow tag
# without a code change.
_WORKFLOW_TAG_RE = re.compile(r"^(Reseller|Outreach Today) (Active|Warmup)$")
_ENV_EXTRA = {
    s.strip()
    for s in os.environ.get("WAREHOUSE_ACCOUNT_TAG_LABELS", "").split(",")
    if s.strip()
}


def _is_workflow_tag(label: str | None) -> bool:
    if not label:
        return False
    return bool(_WORKFLOW_TAG_RE.match(label)) or label in _ENV_EXTRA


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "account_tag", run_account_tag_ingest)


def run_account_tag_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No INSTANTLY_KEY_* env vars found — skipping account_tag ingest")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    rows_in = 0   # accounts fetched
    rows_out = 0  # raw rows written
    failures: list[dict] = []
    workspaces_done: list[str] = []
    seen_workspace_ids: set[str] = set()
    # (canonical workspace_slug, tag_label) pairs SUCCESSFULLY scanned — the prune scope.
    scanned_pairs: set[tuple[str, str]] = set()

    # workspace_uuid -> canonical (census) workspace_slug, so the membership rows and
    # the prune scope use the warehouse-canonical slug, not the (possibly renamed)
    # live Instantly slug.
    uuid_to_census_slug: dict[str, str] = {}
    try:
        for wsid, slug in ctx.db.execute(
            "SELECT DISTINCT workspace_uuid, workspace_slug "
            "FROM core.v_account_census_latest WHERE workspace_uuid IS NOT NULL"
        ).fetchall():
            if wsid and slug:
                uuid_to_census_slug[wsid] = slug
    except Exception as exc:  # noqa: BLE001 — census may be absent on a fresh DB
        logger.warning("account_tag: could not read census slug map: %s", exc)

    # Clear any partial rows from a prior aborted run with this run_id (idempotent).
    ctx.db.execute("DELETE FROM raw_instantly_account_tag WHERE _run_id = ?", [ctx.run_id])

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
                    logger.info("Skipping duplicate workspace slug=%s (uuid %s already done)",
                                slug, workspace_id)
                    continue
                seen_workspace_ids.add(workspace_id)
                canon_slug = uuid_to_census_slug.get(workspace_id, ws.get("slug") or slug)

                # 1. Workflow tags in this workspace.
                workflow_tags = [
                    (t.get("id"), t.get("label"))
                    for t in client.list_tags(workspace_id)
                    if t.get("id") and _is_workflow_tag(t.get("label"))
                ]
                if not workflow_tags:
                    workspaces_done.append(slug)
                    continue

                w_accounts = 0
                for tag_id, label in workflow_tags:
                    # 2. Members of this tag (server-side tag_ids filter).
                    for acct in client.list_accounts(tag_ids=tag_id, workspace_id=workspace_id):
                        email = (acct.get("email") or "").strip().lower()
                        if not email:
                            continue
                        rows_in += 1
                        w_accounts += 1
                        ctx.db.execute(
                            """
                            INSERT INTO raw_instantly_account_tag
                              (_loaded_at, _run_id, workspace_uuid, workspace_slug,
                               email, tag_id, tag_label, provider_code, status, daily_limit)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT DO NOTHING
                            """,
                            [
                                now, ctx.run_id, workspace_id, canon_slug,
                                email, tag_id, label,
                                _as_int(acct.get("provider_code")),
                                _as_int(acct.get("status")),
                                _as_int(acct.get("daily_limit")),
                            ],
                        )
                        rows_out += 1
                    scanned_pairs.add((canon_slug, label))

                workspaces_done.append(slug)
                logger.info("Workspace %s (uuid=%s, slug=%s): %d workflow-tag memberships across %d tags",
                            slug, workspace_id, canon_slug, w_accounts, len(workflow_tags))
        except InstantlyError as exc:
            logger.error("Workspace %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("Workspace %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    # ---- canonical resolution ------------------------------------------
    resolved = _resolve_core_account_tag(ctx, now, scanned_pairs)

    notes = {
        "workspaces_done": workspaces_done,
        "failures": failures,
        "tags_scanned": len(scanned_pairs),
        "core_rows_after": resolved,
    }
    return PhaseResult(rows_in=rows_in, rows_out=rows_out, notes=notes)


def _as_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _resolve_core_account_tag(ctx, now: datetime, scanned_pairs: set[tuple[str, str]]) -> int:
    """Upsert this run's raw membership into core.sending_account_tag, then prune
    memberships that disappeared within the SUCCESSFULLY-scanned (workspace, tag) pairs.
    provider_code + canonical workspace_slug are taken from the latest census."""
    db = ctx.db

    # Deduped membership observed this run (one row per email x tag).
    db.execute("DROP TABLE IF EXISTS _run_account_tag")
    db.execute(
        """
        CREATE TEMP TABLE _run_account_tag AS
        SELECT DISTINCT
            lower(email)   AS email,
            tag_id,
            tag_label,
            workspace_slug,
            provider_code
        FROM raw_instantly_account_tag
        WHERE _run_id = ?
        """,
        [ctx.run_id],
    )

    # UPSERT. first_seen_at kept on conflict; everything else refreshed. census is the
    # authority for provider_code + canonical workspace_slug (raw is the fallback).
    db.execute(
        """
        INSERT INTO core.sending_account_tag
          (email, workspace_slug, tag_id, tag_label, first_seen_at, last_seen_at, provider_code)
        SELECT
            r.email,
            COALESCE(c.workspace_slug, r.workspace_slug),
            r.tag_id,
            r.tag_label,
            ?, ?,
            COALESCE(c.provider_code, r.provider_code)
        FROM _run_account_tag r
        LEFT JOIN core.v_account_census_latest c ON c.email = r.email
        ON CONFLICT (email, tag_label) DO UPDATE SET
            last_seen_at   = excluded.last_seen_at,
            tag_id         = excluded.tag_id,
            workspace_slug = excluded.workspace_slug,
            provider_code  = excluded.provider_code
        """,
        [now, now],
    )

    # PRUNE removed memberships — ONLY within (workspace_slug, tag_label) pairs we
    # successfully scanned this run, so a failed/skipped workspace keeps its rows.
    if scanned_pairs:
        db.execute("DROP TABLE IF EXISTS _scanned_pairs")
        db.execute("CREATE TEMP TABLE _scanned_pairs (workspace_slug VARCHAR, tag_label VARCHAR)")
        db.executemany(
            "INSERT INTO _scanned_pairs VALUES (?, ?)",
            [[ws, lbl] for (ws, lbl) in sorted(scanned_pairs)],
        )
        db.execute(
            """
            DELETE FROM core.sending_account_tag s
            WHERE EXISTS (
                SELECT 1 FROM _scanned_pairs p
                WHERE p.workspace_slug = s.workspace_slug AND p.tag_label = s.tag_label
            )
            AND NOT EXISTS (
                SELECT 1 FROM _run_account_tag r
                WHERE r.email = s.email AND r.tag_label = s.tag_label
            )
            """
        )
        db.execute("DROP TABLE IF EXISTS _scanned_pairs")

    n = db.execute("SELECT count(*) FROM core.sending_account_tag").fetchone()[0]
    db.execute("DROP TABLE IF EXISTS _run_account_tag")
    logger.info("core.sending_account_tag resolved -> %d rows (%d scanned pairs)",
                n, len(scanned_pairs))
    return n
