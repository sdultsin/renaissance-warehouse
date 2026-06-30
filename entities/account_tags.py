"""account_tags: nightly per-INBOX tag column from Instantly (INCREMENTAL).

Populates core.account_tags (DDL 1026/98): ONE row per inbox, with every tag that inbox
carries in Instantly rolled into a single `tags` column (verbatim, no curation). This is
the per-inbox tag field core.v_inbox_overview reads (prov_tag / batch_tag / verbatim tags)
— distinct from the curated core.sending_account_tag (4 workflow tags, capacity math, left
to entities/account_tag.py).

WHY INCREMENTAL (2026-06-30 rewrite — root fix for the 15h nightly hang)
----------------------------------------------------------------------
The old shape FULL-pulled /custom-tag-mappings every night and DELETE+INSERT-replaced each
workspace. That endpoint:
  * ignores `resource_type` server-side (returns EVERY edge, ~9M across workspaces — one per
    account-ever × its tags), newest-first, with NO server-side date filter;
  * so it walked ~91k pages (100/page) for hours, holding the single writer lock the whole
    time, then did ~1M single-row INSERTs — together ~15h. It hung the nightly (2026-06-30),
    bloated the writer DB, and after the PAGINATION_CEILING stopgap (PR #122) it just
    fail-loud-refused a partial set every night, so core.account_tags went stale.

This mirrors the SAME wall entities/email_thread_sync.py hit and solved (a `since=None` full
/emails walk is "catastrophic" on big workspaces → it watermarks instead). So account_tags is
now INCREMENTAL:
  1. WATERMARK per workspace = max(core.account_tags._loaded_at) for that workspace, minus an
     OVERLAP — like email_thread_sync, the watermark is DERIVED FROM THE COMMITTED DATA (no
     separate state table), but note the deliberate TWO-CLOCK design: the watermark is our own
     WRITE-time (_loaded_at) while the walk below stops on the SOURCE's timestamp_created.
     (email_thread_sync watermarks on the source clock, max(message_at); account_tags is a
     per-inbox ROLLUP with no per-edge timestamp to store, so it uses write-time.) This is safe
     because OVERLAP (default 12h) comfortably exceeds write-vs-source clock skew + run duration,
     so no edge created before the last refresh is ever skipped — DO NOT shrink OVERLAP below the
     clock gap. NULL (never pulled) → seed from now-SEED_WINDOW. Floored at now-MAX_LOOKBACK so a
     long-idle workspace can never widen the window unbounded.
  2. Walk /custom-tag-mappings NEWEST-FIRST and BREAK once an edge's timestamp_created crosses
     the watermark — pulling only edges created since the last refresh (tens-to-hundreds of
     pages, not 91k). A ceiling_flag hit (incremental window > PAGINATION_CEILING pages = a
     real anomaly) fails that workspace LOUD and does NOT merge a partial set.
  3. UNION-MERGE only the touched inboxes into core.account_tags (set-based, batched): a
     touched inbox's existing tags_arr is unioned with the new labels; UNtouched inboxes keep
     their last-good row (no full-replace, no wipe). New inboxes are inserted fresh.

REMOVAL LAG (documented, low-impact): because we only ADD edges since the watermark and never
re-walk all history, an UN-tag upstream is not seen until a full reconcile. This is acceptable
here: the only consumer (v_inbox_overview prov_tag / batch_tag) reads SET-ONCE tags (an inbox's
provider/batch is fixed at creation and never removed), and the operationally-removable workflow
tags (Reseller/OTD Active↔Warmup graduation) are maintained CORRECTLY + with pruning by the
separate bounded path (entities/account_tag.py via /accounts?tag_ids=). A wider reconcile can be
forced out-of-band by widening WAREHOUSE_ACCOUNT_TAGS_SEED_DAYS for one run.

Registered as the LAST phase ('account_tags_late', after 'derived') — kept there from PR #124
so even a pathological pull can never block the critical phases. With the incremental window it
now completes in minutes.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.account_tags")

# Incremental-window tuning (env-overridable; bounded by design).
#   OVERLAP        — re-pull this far behind the watermark so a boundary edge created during the
#                    prior pull can't be missed (idempotent: the union-merge dedups re-seen edges).
#   SEED_WINDOW    — first-ever pull for a workspace with no prior rows: how far back to seed.
#   MAX_LOOKBACK   — hard floor on the window so a long-idle workspace can never re-walk all history.
_OVERLAP = timedelta(hours=int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_OVERLAP_HOURS", "12")))
_SEED_WINDOW = timedelta(days=int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_SEED_DAYS", "3")))
_MAX_LOOKBACK = timedelta(days=int(os.environ.get("WAREHOUSE_ACCOUNT_TAGS_MAX_LOOKBACK_DAYS", "30")))


def register(registry: Registry) -> None:
    registry.add_phase("account_tags_late", "account_tags", run_account_tags_ingest)


def _parse_ts(v) -> datetime | None:
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def run_account_tags_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    if not keys:
        logger.warning("No Instantly workspace keys — skipping account_tags")
        return PhaseResult(notes={"reason": "no_keys"})

    now = datetime.now(timezone.utc)
    floor = now - _MAX_LOOKBACK

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

    inboxes_touched = 0
    edges_seen = 0
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

                # --- per-workspace watermark (max committed _loaded_at, minus overlap) ----
                row = ctx.db.execute(
                    "SELECT max(_loaded_at) FROM core.account_tags WHERE workspace_uuid = ?",
                    [workspace_id],
                ).fetchone()
                proxy = row[0] if row else None
                if proxy is not None:
                    if proxy.tzinfo is None:
                        proxy = proxy.replace(tzinfo=timezone.utc)
                    eff_wm = proxy - _OVERLAP
                else:
                    eff_wm = now - _SEED_WINDOW
                if eff_wm < floor:
                    eff_wm = floor

                labels = {
                    t.get("id"): t.get("label")
                    for t in client.list_tags(workspace_id)
                    if t.get("id")
                }

                # --- incremental newest-first walk, STOP at the watermark -----------------
                ceiling_flag: dict = {"hit": False}
                new_pairs: list[tuple[str, str]] = []  # (email, label) created since eff_wm
                for m in client.list_custom_tag_mappings(
                    resource_type=1, workspace_id=workspace_id, ceiling_flag=ceiling_flag
                ):
                    ts = _parse_ts(m.get("timestamp_created"))
                    if ts is not None and ts <= eff_wm:
                        break  # newest-first: everything beyond here is already captured
                    if m.get("resource_type") not in (1, None):
                        continue
                    email = (m.get("resource_id") or "").strip().lower()
                    label = labels.get(m.get("tag_id"))
                    if not email or "@" not in email or not label:
                        continue
                    new_pairs.append((email, label))

                if ceiling_flag.get("hit"):
                    # Incremental window itself exceeded the page backstop (>~500k edges since
                    # last run) — a real anomaly. Fail this ws LOUD; do NOT merge a partial set
                    # (we didn't write, so the watermark stays put and next run retries).
                    logger.error("account_tags %s: incremental window hit pagination backstop — "
                                 "refusing partial set (window since %s)", slug, eff_wm.isoformat())
                    failures.append({"slug": slug, "error": "pagination_ceiling_hit_incremental"})
                    continue

                edges_seen += len(new_pairs)
                if not new_pairs:
                    workspaces_done.append(slug)
                    logger.info("account_tags %s (slug=%s): no new edges since %s",
                                slug, canon_slug, eff_wm.isoformat())
                    continue

                # --- union-merge touched inboxes (set-based, batched) ---------------------
                ctx.db.execute("CREATE OR REPLACE TEMP TABLE _new_edges (email VARCHAR, tag_label VARCHAR)")
                ctx.db.executemany("INSERT INTO _new_edges VALUES (?, ?)", new_pairs)
                # union each touched inbox's NEW labels with its existing tags_arr (last-good)
                ctx.db.execute(
                    """
                    CREATE OR REPLACE TEMP TABLE _merged AS
                    WITH newr AS (
                        SELECT email, list_sort(list_distinct(array_agg(tag_label))) AS new_arr
                        FROM _new_edges GROUP BY email
                    )
                    SELECT n.email,
                           list_sort(list_distinct(
                               list_concat(COALESCE(a.tags_arr, CAST([] AS VARCHAR[])), n.new_arr)
                           )) AS arr
                    FROM newr n
                    LEFT JOIN core.account_tags a
                      ON a.workspace_uuid = ? AND a.email = n.email
                    """,
                    [workspace_id],
                )
                # Atomic per-row UPSERT of ONLY the touched inboxes (untouched inboxes keep their
                # last-good row). _merged.arr is already the FINAL merged array, so ON CONFLICT just
                # overwrites the touched row — a single statement, so there is NO delete-then-insert
                # window that could drop rows if the write fails mid-way (vs a separate DELETE+INSERT
                # under autocommit). PK is (workspace_uuid, email); _merged has one row per email.
                ctx.db.execute(
                    """
                    INSERT INTO core.account_tags
                      (email, workspace_slug, workspace_uuid, tags, tags_arr, n_tags, _loaded_at, _run_id)
                    SELECT email, ?, ?, array_to_string(arr, ' | '), arr, len(arr), ?, ?
                    FROM _merged
                    ON CONFLICT (workspace_uuid, email) DO UPDATE SET
                        workspace_slug = excluded.workspace_slug,
                        tags           = excluded.tags,
                        tags_arr       = excluded.tags_arr,
                        n_tags         = excluded.n_tags,
                        _loaded_at     = excluded._loaded_at,
                        _run_id        = excluded._run_id
                    """,
                    [canon_slug, workspace_id, now, ctx.run_id],
                )
                n_touched = ctx.db.execute("SELECT count(*) FROM _merged").fetchone()[0]
                ctx.db.execute("DROP TABLE IF EXISTS _new_edges")
                ctx.db.execute("DROP TABLE IF EXISTS _merged")

                inboxes_touched += n_touched
                workspaces_done.append(slug)
                logger.info("account_tags %s (slug=%s): %d new edges -> %d inboxes refreshed (since %s)",
                            slug, canon_slug, len(new_pairs), n_touched, eff_wm.isoformat())
        except InstantlyError as exc:
            logger.error("account_tags %s: API error: %s", slug, exc)
            failures.append({"slug": slug, "error": str(exc)[:300]})
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_tags %s: unexpected error", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:300]})

    return PhaseResult(notes={
        "inboxes_touched": inboxes_touched,
        "edges_seen": edges_seen,
        "workspaces_done": workspaces_done,
        "failures": failures,
    })
