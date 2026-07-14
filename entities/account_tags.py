"""account_tags: nightly per-INBOX tag column from Instantly (ADDITIVE COMPLETE REFRESH).

Populates core.account_tags (DDL 1026/98): ONE row per inbox, with every tag that inbox
carries in Instantly rolled into a single `tags` column (verbatim, no curation). This is
the per-inbox tag field core.v_inbox_overview reads (prov_tag / batch_tag / verbatim tags)
— distinct from the curated core.sending_account_tag (4 workflow tags, capacity math, left
to entities/account_tag.py).

WHY A COMPLETE REBUILD (2026-07-06 rewrite — restores the DDL's intended full-replace)
-------------------------------------------------------------------------------------
Two prior shapes both failed:
  * The ORIGINAL full-replace pulled /custom-tag-mappings per workspace. That endpoint returns
    EVERY edge ever (incl. deleted accounts), newest-first, NO date filter — ~9M edges / ~91k
    pages / ~15h fleet-wide. It hung the nightly (2026-06-30).
  * The INCREMENTAL fix (walk /custom-tag-mappings newest-first, stop at a watermark, union-merge
    only touched inboxes) was fast but NEVER did a complete refresh: it only ADDS recent edges,
    so tag REMOVALS lagged until a manual reconcile, a workspace with no key (e.g. Tariffs) was
    simply absent, and coverage drifted — needing periodic `manual_full_reconcile` runs. The
    result was a warehouse whose tags were stale/incomplete on any given night.

This version gets BOTH complete AND fast by using the RIGHT endpoint — the same one
entities/account_tag.py already uses for workflow tags:
  1. Per workspace, list every custom tag, then GET /accounts?tag_ids=<id> per tag. This is a
     SERVER-SIDE filter that returns only CURRENT accounts (no historical-edge bloat), WITH the
     account object — so we build the inbox→{labels} map directly, completely, cheaply.
  2. ADDITIVE refresh per workspace: DELETE only the inboxes in THIS pull, then re-INSERT them with
     their current tags — so a LIVE inbox's tag changes (add or remove) are reflected. Rows for
     inboxes no longer present (ghosts of moved/deleted accounts) are KEPT: account_tags is the
     permanent per-inbox tag record; the domain/inbox archive lives in core.sending_account_batch.
     Deletes nothing historical.
  3. Because it never shrinks, a bad/partial pull can only fail to refresh some inboxes — never wipe
     a workspace, so no shrink guard is needed. A workspace whose whole pull errors is skipped
     (its rows stay last-good).

Workspaces are pulled with BOUNDED CONCURRENCY (each a distinct key). The "serial within a
workspace" discipline still holds — one cursor at a time per key — this only overlaps DIFFERENT
keys, capped so the aggregate IP rate stays gentle. All DB writes stay in the main thread
(single writer). Registered as the LAST phase ('account_tags_late', after 'derived') so even a
slow pull can never block the critical phases.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_tags")

# Concurrency tuning (env-overridable).
#   WORKERS — workspaces pulled CONCURRENTLY (each a distinct key). Bounded so the aggregate
#             IP rate stays gentle; per-workspace paging is still serial (one cursor at a time).
_WORKERS = max(1, int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_WORKERS", "3")))

# DEADLINE (env-overridable; 0 = off, the historic behaviour).
# This phase now runs inside PASS A of the nightly (before the morning serving promote), so an
# Instantly-side slowdown here would delay BOTH the portal's morning snapshot and every phase in
# PASS B. Cap the API pull at a wall clock: when it expires we keep whatever workspaces already
# finished and SKIP the rest. Safe by construction — the refresh is ADDITIVE per workspace, so a
# skipped workspace simply keeps its last-good tag rows (nothing is wiped, nothing half-written).
# Degrades gracefully: warn + carry on, never abort the run. [2026-07-14]
_DEADLINE_S = max(0, int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_DEADLINE_MIN", "0"))) * 60


def register(registry: Registry) -> None:
    registry.add_phase("account_tags_late", "account_tags", run_account_tags_ingest)


def _pull_workspace(slug: str, api_key: str) -> dict:
    """READ-ONLY, DB-free (safe in a worker thread): pull the COMPLETE current tag map for one
    workspace via /accounts?tag_ids= (server-side tag filter → CURRENT accounts only, no
    historical-edge bloat). Returns {slug, status, workspace_id, ws_slug, n_tags, by_email}."""
    try:
        with InstantlyClient(api_key) as client:
            ws = client.get_current_workspace()
            wid = ws.get("id")
            if not wid:
                return {"slug": slug, "status": "fail", "err": "missing_workspace_id"}
            tags = [
                (t.get("id"), t.get("label"))
                for t in client.list_tags(wid)
                if t.get("id") and t.get("label")
            ]
            by_email: dict[str, set] = defaultdict(set)
            for tag_id, label in tags:
                for acct in client.list_accounts(tag_ids=tag_id, workspace_id=wid):
                    email = (acct.get("email") or "").strip().lower()
                    if email and "@" in email:
                        by_email[email].add(label)
            return {
                "slug": slug, "status": "ok", "workspace_id": wid,
                "ws_slug": ws.get("slug"), "n_tags": len(tags), "by_email": by_email,
            }
    except InstantlyError as exc:
        return {"slug": slug, "status": "fail", "err": str(exc)[:300]}
    except Exception as exc:  # noqa: BLE001
        return {"slug": slug, "status": "fail", "err": f"{type(exc).__name__}: {exc}"[:300]}


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

    # --- pull every workspace's COMPLETE tag map concurrently (API only, no DB) -------------
    # Bounded by _DEADLINE_S when set: on expiry we keep the workspaces that finished and skip the
    # rest (they retain last-good rows — the refresh is additive). Never aborts the run.
    results: list[dict] = []
    skipped: list[str] = []
    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        futs = {ex.submit(_pull_workspace, slug, keys[slug]): slug for slug in sorted(keys)}
        try:
            for fut in as_completed(futs, timeout=_DEADLINE_S or None):
                results.append(fut.result())
        except FuturesTimeout:
            skipped = [slug for fut, slug in futs.items() if not fut.done()]
            for fut in futs:
                fut.cancel()
            logger.warning(
                "account_tags: hit the %d-min deadline — %d workspace(s) pulled, SKIPPING %s "
                "(they keep their last-good tag rows; nothing wiped). Raise "
                "WAREHOUSE_ACCOUNT_TAGS_DEADLINE_MIN if this recurs.",
                _DEADLINE_S // 60, len(results), ", ".join(skipped) or "-",
            )

    # --- serial full-replace per CLEAN workspace (single writer) ----------------------------
    inboxes_written = 0
    workspaces_done: list[str] = []
    failures: list[dict] = []
    seen_uuids: set[str] = set()

    for r in sorted(results, key=lambda x: x["slug"]):
        slug = r["slug"]
        if r["status"] != "ok":
            logger.error("account_tags %s: %s", slug, r.get("err"))
            failures.append({"slug": slug, "error": r.get("err")})
            continue
        wid = r["workspace_id"]
        if wid in seen_uuids:
            continue
        seen_uuids.add(wid)
        canon_slug = uuid_to_slug.get(wid, r.get("ws_slug") or slug)
        by_email = r["by_email"]
        new_n = len(by_email)
        rows = [(email, sorted(labels)) for email, labels in by_email.items()]
        try:
            ctx.db.execute("CREATE OR REPLACE TEMP TABLE _wt (email VARCHAR, tags_arr VARCHAR[])")
            if rows:
                ctx.db.executemany("INSERT INTO _wt VALUES (?, ?)", rows)
            # ADDITIVE refresh: DELETE only the inboxes present in THIS pull, then re-INSERT them
            # with their current tags. Rows for inboxes no longer in the workspace (ghosts) are KEPT
            # — account_tags is the permanent per-inbox tag record; the domain/inbox archive lives in
            # core.sending_account_batch. Deletes nothing historical, and never shrinks, so a bad or
            # partial pull can only fail to refresh some live inboxes — it can never wipe a workspace
            # (hence no shrink guard needed). A workspace whose whole pull errors is skipped upstream.
            # NOT `ON CONFLICT DO UPDATE`, and NOT a same-transaction delete+reinsert — BOTH trip a
            # DuckDB ART-index INTERNAL "duplicate key" abort on this table (proven 2026-07-01; see
            # scripts/backfill_account_tags_full.py). AUTO-COMMIT: the DELETE commits before the INSERT
            # so the re-INSERT sees fresh keys.
            ctx.db.execute(
                "DELETE FROM core.account_tags "
                "WHERE workspace_uuid = ? AND email IN (SELECT email FROM _wt)", [wid]
            )
            ctx.db.execute(
                """
                INSERT INTO core.account_tags
                  (email, workspace_slug, workspace_uuid, tags, tags_arr, n_tags, _loaded_at, _run_id)
                SELECT email, ?, ?, array_to_string(tags_arr, ' | '), tags_arr, len(tags_arr), ?, ?
                FROM _wt
                """,
                [canon_slug, wid, now, ctx.run_id],
            )
            ctx.db.execute("DROP TABLE IF EXISTS _wt")
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_tags %s: write failed — last-good kept", slug)
            failures.append({"slug": slug, "error": f"write_failed: {type(exc).__name__}"})
            continue

        inboxes_written += new_n
        workspaces_done.append(slug)
        logger.info("account_tags %s (slug=%s): full-replace %d inboxes across %d tags",
                    slug, canon_slug, new_n, r.get("n_tags"))

    return PhaseResult(rows_out=inboxes_written, notes={
        "inboxes_written": inboxes_written,
        "workspaces_done": workspaces_done,
        "failures": failures,
        "workers": _WORKERS,
        "deadline_min": _DEADLINE_S // 60,
        "skipped_on_deadline": skipped,
    })
