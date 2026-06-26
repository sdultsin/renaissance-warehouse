"""account_campaign_live: nightly per-inbox campaign membership, straight from Instantly.

Populates core.account_campaign_live (DDL 100): one row per inbox that is attached to
>=1 campaign, with campaign count / active count / names. Independent of the box-side
core.account_campaign (owned by sync_account_campaign.py + read by the portal) — that
table is left untouched; this is a fresh, additive source for the inbox overview.

Per workspace key:
  1. GET /campaigns            -> {id, name, status, email_list, email_tag_list}
  2. resolve each campaign's accounts = explicit email_list UNION members of each tag in
     email_tag_list (GET /accounts?tag_ids=<tag>, cached per tag so shared tags resolve once)
  3. invert -> account -> {campaigns}, count distinct + active (status==1) + names
  4. FULL-REPLACE this workspace's rows (delete+insert after a clean pull).

Registered under the 'instantly' phase (04:00); refreshes every nightly run.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_campaign_live")

_ACTIVE_STATUS = 1  # Instantly campaign status: 1=active, 0=draft, 2=paused, 3=completed


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "account_campaign_live", run_account_campaign_live_ingest)


def run_account_campaign_live_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No Instantly workspace keys — skipping account_campaign_live")
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
    except Exception as exc:  # noqa: BLE001
        logger.warning("account_campaign_live: could not read census slug map: %s", exc)

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

                campaigns = list(client.list_campaigns(workspace_id))

                # resolve each attach-tag's members once (shared across campaigns)
                tag_members: dict[str, list] = {}
                def members_of_tag(tid: str) -> list:
                    if tid not in tag_members:
                        emails = []
                        for a in client.list_accounts(tag_ids=tid, workspace_id=workspace_id):
                            em = (a.get("email") or "").strip().lower()
                            if "@" in em:
                                emails.append(em)
                        tag_members[tid] = emails
                    return tag_members[tid]

                # account_email -> {(campaign_name, status)}
                by_email: dict[str, set] = {}
                for c in campaigns:
                    cid = c.get("id")
                    name = c.get("name")
                    status = c.get("status")
                    members = set()
                    for em in (c.get("email_list") or []):
                        em = (em or "").strip().lower()
                        if "@" in em:
                            members.add(em)
                    for tid in (c.get("email_tag_list") or []):
                        if tid:
                            members.update(members_of_tag(tid))
                    for em in members:
                        by_email.setdefault(em, set()).add((cid, name, status))

                rows = []
                for email, camps in by_email.items():
                    camp_ids = {cid for cid, _, _ in camps if cid}
                    active_ids = {cid for cid, _, s in camps if cid and s == _ACTIVE_STATUS}
                    names = sorted({n for _, n, _ in camps if n})
                    rows.append((email, canon_slug, workspace_id,
                                 len(camp_ids), len(active_ids), " | ".join(names), now, ctx.run_id))

                ctx.db.execute(
                    "DELETE FROM core.account_campaign_live WHERE workspace_uuid = ?",
                    [workspace_id],
                )
                for r in rows:
                    ctx.db.execute(
                        "INSERT INTO core.account_campaign_live "
                        "(account_email, workspace_slug, workspace_uuid, n_campaigns, "
                        " n_active_campaigns, campaigns, _loaded_at, _run_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        list(r),
                    )
                total_inboxes += len(rows)
                workspaces_done.append(slug)
                logger.info("account_campaign_live %s (slug=%s): %d inboxes in campaigns across %d campaigns",
                            slug, canon_slug, len(rows), len(campaigns))
        except InstantlyError as exc:
            logger.error("account_campaign_live %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_campaign_live %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(notes={
        "inboxes_in_campaigns": total_inboxes,
        "workspaces_done": workspaces_done,
        "failures": failures,
    })
