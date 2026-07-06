"""account_tags: nightly per-INBOX tag column from Instantly (COMPLETE REBUILD).

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
  2. FULL-REPLACE per workspace (the DDL's intended design): delete the inboxes that no longer
     carry any tag, then upsert the current set. Removals ARE reflected; nothing lags.
  3. A failed/degraded workspace keeps its LAST-GOOD rows — a per-workspace clean-pull gate plus
     a shrink guard (a fresh pull returning < MIN_KEEP of the prior count is treated as a partial
     pull and skipped), so one bad pull can never wipe a workspace.

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
from datetime import datetime, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_tags")

# Complete-rebuild tuning (env-overridable).
#   WORKERS   — workspaces pulled CONCURRENTLY (each a distinct key). Bounded so the aggregate
#               IP rate stays gentle; per-workspace paging is still serial (one cursor at a time).
#   MIN_KEEP  — guard ratio: skip the replace for a workspace whose fresh pull returns fewer than
#               this fraction of its prior row count (a degraded/partial pull) → keep last-good.
_WORKERS = max(1, int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_WORKERS", "3")))
_MIN_KEEP = float(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_MIN_KEEP_RATIO", "0.5"))


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
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        futs = {ex.submit(_pull_workspace, slug, keys[slug]): slug for slug in sorted(keys)}
        for fut in as_completed(futs):
            results.append(fut.result())

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

        prior = ctx.db.execute(
            "SELECT count(*) FROM core.account_tags WHERE workspace_uuid = ?", [wid]
        ).fetchone()[0]
        # Shrink guard: a fresh pull far smaller than last time = suspected partial/degraded pull
        # → keep last-good, do not replace. (new_n==0 with prior>0 is caught here too.)
        if prior > 0 and new_n < prior * _MIN_KEEP:
            logger.error("account_tags %s: fresh pull %d < %.0f%% of prior %d — keeping last-good "
                         "(suspected partial pull)", slug, new_n, _MIN_KEEP * 100, prior)
            failures.append({"slug": slug, "error": f"shrink_guard new={new_n} prior={prior}"})
            continue

        rows = [(email, sorted(labels)) for email, labels in by_email.items()]
        try:
            ctx.db.execute("CREATE OR REPLACE TEMP TABLE _wt (email VARCHAR, tags_arr VARCHAR[])")
            if rows:
                ctx.db.executemany("INSERT INTO _wt VALUES (?, ?)", rows)
            # Full-replace this workspace: DELETE all its rows, then bulk INSERT the fresh set.
            # NOT `ON CONFLICT DO UPDATE`, and NOT a same-transaction delete+reinsert — BOTH trip a
            # DuckDB ART-index INTERNAL "duplicate key" abort on core.account_tags (proven 2026-07-01;
            # see scripts/backfill_account_tags_full.py). Statements AUTO-COMMIT (no BEGIN/COMMIT): the
            # DELETE commits first so the INSERT sees fresh keys. Removals ARE reflected (full delete);
            # the shrink guard above already refuses to run this on a suspiciously-small pull, so the
            # brief post-DELETE window can't wipe a workspace from a bad pull.
            ctx.db.execute("DELETE FROM core.account_tags WHERE workspace_uuid = ?", [wid])
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
    })
