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


def uncovered_active_workspaces(db, covered_slugs, failures) -> list[str]:
    """Active workspaces that no key reached this run — i.e. never even attempted.

    Excludes workspaces that WERE attempted and failed: those are already reported in
    `failures`, and double-reporting them would bury the distinct "we have no key for this
    workspace at all" case, which is the one that stays broken forever if nobody notices.

    Fail-LOUD in the log, but NEVER raises: this runs after the pulls have already
    committed, so a roster read problem must not turn a successful ingest into a failed
    phase ([[feedback_no_breaking_guards]]).
    """
    # Only demand coverage for active workspaces that actually HOLD inboxes.
    #
    # [2026-07-21, same day, second pass] `is_active` here does NOT mean "one of David's
    # operational workspaces" — entities/workspace.py sets it TRUE for any workspace whose key
    # ANSWERED this run. So it is TRUE for `the-eagles`: we still hold a working key and Instantly
    # still returns it (5 campaigns), even though it holds 0 inboxes, has sent nothing in 90 days,
    # and is NOT among the 11 on the canonical roster. Flipping that row by hand would be undone by
    # the very next nightly, because the key keeps answering.
    #
    # A workspace with no inboxes has nothing whose in_campaign could be wrong, so demanding
    # campaign coverage for it is meaningless — and would have made this check cry wolf EVERY night
    # about `the-eagles` and `growth-1`. A guard that always fires is a guard nobody reads.
    try:
        active = {
            s for (s,) in db.execute(
                "SELECT w.slug FROM core.workspace w "
                "WHERE w.is_active AND EXISTS ("
                "  SELECT 1 FROM core.sending_account a WHERE a.workspace_slug = w.slug)"
            ).fetchall() if s
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("account_campaign_live: coverage check skipped (%s)", exc)
        return []

    attempted = set(covered_slugs) | {f.get("slug") for f in failures if f.get("slug")}
    uncovered = sorted(active - attempted)
    if uncovered:
        logger.error(
            "account_campaign_live COVERAGE GAP: %d active workspace(s) were never pulled "
            "(no INSTANTLY_KEY_<SLUG> reached them): %s — for every inbox in them "
            "in_campaign is UNKNOWN, not false.",
            len(uncovered), ", ".join(uncovered))
    else:
        logger.info("account_campaign_live: all %d active workspaces covered", len(active))
    return uncovered


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
    # [2026-07-21] Coverage bookkeeping. A workspace can be absent from
    # core.account_campaign_live for two VERY different reasons: (a) it genuinely has no
    # inbox attached to any campaign (correct — e.g. a workspace still in warm-up), or
    # (b) we never pulled it at all because no INSTANTLY_KEY_<SLUG> exists for it. The
    # table alone cannot tell those apart, so a brand-new workspace would stay silently
    # uncovered forever. rows_by_slug records an explicit 0 for (a); the roster check
    # below names (b) out loud.
    rows_by_slug: dict[str, int] = {}
    covered_slugs: set[str] = set()

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

                # Stage the workspace in a temp table, then swap it in with TWO set-based
                # statements. The old per-row INSERT loop (~100k+ autocommit transactions/
                # night) was a large chunk of this 90-minute ingest and a writer-DB bloat
                # source (tiny transactions churn row groups). Temp tables are not
                # WAL-logged, so the executemany staging costs no bloat.
                ctx.db.execute(
                    "CREATE OR REPLACE TEMP TABLE _acl_batch ("
                    "account_email VARCHAR, workspace_slug VARCHAR, workspace_uuid VARCHAR, "
                    "n_campaigns INTEGER, n_active_campaigns INTEGER, campaigns VARCHAR, "
                    "_loaded_at TIMESTAMPTZ, _run_id VARCHAR)"
                )
                if rows:
                    ctx.db.executemany(
                        "INSERT INTO _acl_batch VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [list(r) for r in rows],
                    )
                # One transaction: an INSERT failure can no longer leave the workspace
                # empty behind a committed DELETE (full-replace is now atomic).
                ctx.db.execute("BEGIN TRANSACTION")
                try:
                    ctx.db.execute(
                        "DELETE FROM core.account_campaign_live WHERE workspace_uuid = ?",
                        [workspace_id],
                    )
                    ctx.db.execute(
                        "INSERT INTO core.account_campaign_live "
                        "(account_email, workspace_slug, workspace_uuid, n_campaigns, "
                        " n_active_campaigns, campaigns, _loaded_at, _run_id) "
                        "SELECT * FROM _acl_batch ON CONFLICT DO NOTHING"
                    )
                    ctx.db.execute("COMMIT")
                except Exception:
                    ctx.db.execute("ROLLBACK")
                    raise
                ctx.db.execute("DROP TABLE IF EXISTS _acl_batch")
                total_inboxes += len(rows)
                workspaces_done.append(slug)
                rows_by_slug[canon_slug] = len(rows)
                covered_slugs.add(canon_slug)
                logger.info("account_campaign_live %s (slug=%s): %d inboxes in campaigns across %d campaigns",
                            slug, canon_slug, len(rows), len(campaigns))
        except InstantlyError as exc:
            logger.error("account_campaign_live %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_campaign_live %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    # Roster coverage: every ACTIVE workspace must have been reached by some key. This is
    # fail-LOUD-but-degrade-gracefully: it logs an error and reports the uncovered slugs in
    # the phase notes, and never raises — a roster read problem must not lose a pull that
    # already succeeded and committed.
    uncovered = uncovered_active_workspaces(ctx.db, covered_slugs, failures)

    return PhaseResult(notes={
        "inboxes_in_campaigns": total_inboxes,
        "workspaces_done": workspaces_done,
        "failures": failures,
        # explicit 0 = pulled and genuinely empty (NOT the same as absent/uncovered)
        "rows_by_workspace": rows_by_slug,
        "uncovered_active_workspaces": uncovered,
    })
