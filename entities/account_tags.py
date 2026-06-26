"""account_tags: nightly per-INBOX tag column from Instantly.

Populates core.account_tags (DDL 98): ONE row per inbox, with every tag that inbox
carries in Instantly rolled into a single `tags` column (verbatim, no curation). This
is the per-inbox tag field the Inbox Hub overview reads — distinct from the curated
core.sending_account_tag (4 workflow tags, feeds capacity math — left untouched).

Per workspace key:
  1. GET /custom-tags                          -> {tag_id: label}
  2. GET /custom-tag-mappings?resource_type=1  -> every (email, tag_id) edge, one pass
  3. group edges by email -> sorted-distinct labels -> ONE row per inbox
  4. FULL-REPLACE this workspace's rows (delete+insert after a clean pull) so a single
     failed workspace can never wipe its rows.

Registered under the 'instantly' phase (04:00); refreshes every nightly run.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_tags")


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "account_tags", run_account_tags_ingest)


def run_account_tags_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No Instantly workspace keys — skipping account_tags")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)

    uuid_to_slug: dict[str, str] = {}
    try:
        for wsid, slug in ctx.db.execute(
            "SELECT DISTINCT workspace_uuid, workspace_slug "
            "FROM core.v_account_census_latest WHERE workspace_uuid IS NOT NULL"
        ).fetchall():
            if wsid and slug:
                uuid_to_slug[wsid] = slug
    except Exception as exc:  # noqa: BLE001 — census may be absent on a fresh DB
        logger.warning("account_tags: could not read census slug map: %s", exc)

    total_inboxes = 0
    workspaces_done: list[str] = []
    failures: list[dict] = []
    seen_workspace_ids: set[str] = set()

    for slug in sorted(keys.keys()):
        try:
            with InstantlyClient(keys[slug]) as client:
                ws = client.get_current_workspace()
                workspace_id = ws.get("id")
                if not workspace_id:
                    failures.append({"slug": slug, "error": "missing_workspace_id"})
                    continue
                if workspace_id in seen_workspace_ids:
                    continue
                seen_workspace_ids.add(workspace_id)
                canon_slug = uuid_to_slug.get(workspace_id, ws.get("slug") or slug)

                labels = {
                    t.get("id"): t.get("label")
                    for t in client.list_tags(workspace_id)
                    if t.get("id")
                }

                # group every account<->tag edge by inbox email
                by_email: dict[str, set] = {}
                for m in client.list_custom_tag_mappings(
                    resource_type=1, workspace_id=workspace_id
                ):
                    if m.get("resource_type") not in (1, None):
                        continue
                    email = (m.get("resource_id") or "").strip().lower()
                    label = labels.get(m.get("tag_id"))
                    if not email or "@" not in email or not label:
                        continue
                    by_email.setdefault(email, set()).add(label)

                # one row per inbox
                rows = []
                for email, tagset in by_email.items():
                    arr = sorted(tagset)
                    rows.append((email, canon_slug, workspace_id,
                                 " | ".join(arr), arr, len(arr), now, ctx.run_id))

                # full-replace this workspace (only after a clean pull)
                ctx.db.execute(
                    "DELETE FROM core.account_tags WHERE workspace_uuid = ?",
                    [workspace_id],
                )
                for r in rows:
                    ctx.db.execute(
                        "INSERT INTO core.account_tags "
                        "(email, workspace_slug, workspace_uuid, tags, tags_arr, "
                        " n_tags, _loaded_at, _run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT DO NOTHING",
                        list(r),
                    )
                total_inboxes += len(rows)
                workspaces_done.append(slug)
                logger.info("account_tags %s (slug=%s): %d inboxes tagged", slug, canon_slug, len(rows))
        except InstantlyError as exc:
            logger.error("account_tags %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_tags %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(notes={
        "inboxes_tagged": total_inboxes,
        "workspaces_done": workspaces_done,
        "failures": failures,
    })
