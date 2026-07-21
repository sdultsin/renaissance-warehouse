"""account_tags: nightly per-INBOX tag sync from Instantly — ONE sweep, THREE surfaces.

Populates, from a single per-workspace /accounts?tag_ids= sweep:
  1. core.account_tags (DDL 1026/98): ONE row per inbox, every tag rolled into a single
     `tags` column (verbatim, no curation) — the per-inbox tag field core.v_inbox_overview
     reads (prov_tag / batch_tag / verbatim tags). ADDITIVE COMPLETE REFRESH (below).
  2. raw_instantly_account_tag (DDL 1002): one raw row per (email x tag x run) for EVERY
     tag — the dated membership record time-series queries read.
  3. core.sending_account_tag: the account->tag edge table (upsert + prune), feeding the
     capacity views (core.v_sending_capacity_by_tag, DDL 1002/1073), v_otd_tag_membership
     (DDL 111) and v_tag_coverage_gaps (DDL 1013).

WHY (2)+(3) LIVE HERE (2026-07-19, task #28 tag-gap fix): surfaces 2-3 were produced by
entities/account_tag.py ('instantly' phase, now RETIRED), which gated tags through a
hard-coded allowlist regex ^(Reseller|Outreach Today) (Active|Warmup)$ — so the warmup-pool
tags ("Warmy Warmup" ~137k, "Instantly Warmup" ~57k accounts) were never requested and the
canonical tag surface silently missed ~223k tagged accounts (domain-rehab was blocked on
this). This entity's sweep already walks EVERY tag in every workspace's catalog daily, so
the fix is one sweep feeding all three surfaces: no allowlist, no duplicate API pass, and a
new pool tag can never be silently dropped again. Freshness is unchanged-or-better: this
phase runs later in the SAME pass (PASS A) as the old 'instantly' slot, AFTER the census
promote, so the resolve step joins TONIGHT's census rather than yesterday's.

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

This version gets BOTH complete AND fast by using the RIGHT endpoint (the same one the
retired entities/account_tag.py used for its 4 workflow tags):
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
(single writer). Registered as 'account_tags_late' — the LAST phase of PASS A (see
core/config.py PHASE_ORDER, 2026-07-14 two-pass split) so even a slow pull can never block the
fleet-health phases in front of it.
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

try:  # bulk-load path (see _bulk_insert); executemany remains the fallback
    import pyarrow as _pa
except Exception:  # noqa: BLE001 — never let a missing optional dep fail the nightly
    _pa = None

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


# --- bulk temp-table load -----------------------------------------------------------------
# WHY: this phase writes ~3.1M (email x tag) edge rows + ~390k inbox rows every night, and it
# ran them through executemany — DuckDB's row-at-a-time prepared-statement path (~280 rows/sec
# measured on the box). That made the SERIAL write loop 3h16m of the phase's 3h47m on
# 2026-07-21, which by itself pushed the fleet-health snapshot from ~02:45 to 06:03 ET and the
# Data Hub from ~05:30 to 08:47 ET. Registering an Arrow table and letting DuckDB read it
# columnar is the same pattern entities/account_census.py already uses (399,279 rows in 23s).
#
# SEMANTICS ARE UNCHANGED: same target temp tables, same column order, same subsequent SQL —
# only the transport differs. Proven byte-identical against the executemany path at full
# production scale (391,871 inboxes / 3,096,450 edge rows) before shipping.
#
# DEGRADES GRACEFULLY (feedback_no_breaking_guards): no pyarrow, or any error building/loading
# the Arrow table, falls straight back to the original executemany. INSERT is a single atomic
# statement, so a failed bulk insert lands nothing and the fallback cannot double-write.
_WT_ARROW_COLS = ("email", "tags_arr")
_EDGE_ARROW_COLS = ("_loaded_at", "_run_id", "workspace_uuid", "workspace_slug", "email",
                    "tag_id", "tag_label", "provider_code", "status", "daily_limit")


def _arrow_types(kind: str):
    if kind == "wt":
        return [_pa.string(), _pa.list_(_pa.string())]
    return [_pa.timestamp("us", tz="UTC"), _pa.string(), _pa.string(), _pa.string(),
            _pa.string(), _pa.string(), _pa.string(), _pa.int32(), _pa.int32(), _pa.int32()]


def _bulk_insert(conn, table: str, rows: list, kind: str) -> None:
    """INSERT `rows` into the ALREADY-CREATED temp table `table` (columns in positional order).

    Arrow-registered bulk insert; falls back to executemany on any problem."""
    if not rows:
        return
    cols = _WT_ARROW_COLS if kind == "wt" else _EDGE_ARROW_COLS
    if _pa is not None:
        view = f"_arrow_{table.lstrip('_')}"
        try:
            columnar = list(zip(*rows))
            tbl = _pa.table({name: _pa.array(list(vals), typ)
                             for name, vals, typ in zip(cols, columnar, _arrow_types(kind))})
            conn.register(view, tbl)
            try:
                conn.execute(f"INSERT INTO {table} SELECT * FROM {view}")
            finally:
                conn.unregister(view)
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("account_tags: bulk load into %s failed (%s: %s) — falling back to "
                           "executemany (slower, same result)", table, type(exc).__name__,
                           str(exc)[:120])
            try:
                conn.unregister(view)
            except Exception:  # noqa: BLE001
                pass
    conn.executemany(f"INSERT INTO {table} VALUES ({', '.join(['?'] * len(cols))})", rows)


def register(registry: Registry) -> None:
    registry.add_phase("account_tags_late", "account_tags", run_account_tags_ingest)


def _as_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _pull_workspace(slug: str, api_key: str) -> dict:
    """READ-ONLY, DB-free (safe in a worker thread): pull the COMPLETE current tag map for one
    workspace via /accounts?tag_ids= (server-side tag filter → CURRENT accounts only, no
    historical-edge bloat). Keeps tag IDs and a light per-account meta snapshot
    (provider_code/status/daily_limit from the /accounts payload) so the ONE sweep can feed
    core.account_tags AND raw_instantly_account_tag / core.sending_account_tag (see module
    docstring). Returns {slug, status, workspace_id, ws_slug, n_tags, tag_labels, by_email,
    acct_meta} where by_email maps email -> {tag_label: tag_id}."""
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
            by_email: dict[str, dict[str, str]] = defaultdict(dict)
            acct_meta: dict[str, tuple] = {}
            for tag_id, label in tags:
                for acct in client.list_accounts(tag_ids=tag_id, workspace_id=wid):
                    email = (acct.get("email") or "").strip().lower()
                    if email and "@" in email:
                        by_email[email][label] = tag_id
                        if email not in acct_meta:
                            acct_meta[email] = (
                                _as_int(acct.get("provider_code")),
                                _as_int(acct.get("status")),
                                _as_int(acct.get("daily_limit")),
                            )
            return {
                "slug": slug, "status": "ok", "workspace_id": wid,
                "ws_slug": ws.get("slug"), "n_tags": len(tags),
                "tag_labels": [label for _tid, label in tags],
                "by_email": by_email, "acct_meta": acct_meta,
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
    raw_edge_rows = 0
    workspaces_done: list[str] = []
    failures: list[dict] = []
    seen_uuids: set[str] = set()
    # (canonical workspace_slug, tag_label) pairs SUCCESSFULLY landed — the prune scope for
    # core.sending_account_tag (a failed/skipped workspace keeps its last-good edge rows).
    scanned_pairs: set[tuple[str, str]] = set()

    # Clear any partial raw rows from a prior aborted run with this run_id (idempotent).
    ctx.db.execute("DELETE FROM raw_instantly_account_tag WHERE _run_id = ?", [ctx.run_id])

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
        acct_meta = r["acct_meta"]
        new_n = len(by_email)
        rows = [(email, sorted(labels)) for email, labels in by_email.items()]
        try:
            ctx.db.execute("CREATE OR REPLACE TEMP TABLE _wt (email VARCHAR, tags_arr VARCHAR[])")
            if rows:
                _bulk_insert(ctx.db, "_wt", rows, "wt")
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

            # --- raw_instantly_account_tag: one dated row per (email x tag) — ALL tags -----
            # Same single sweep, set-based write (temp table -> one INSERT; the per-row
            # autocommit INSERT was the retired account_tag.py's writer-bloat lesson).
            # ON CONFLICT DO NOTHING: PK (email, tag_id, _run_id) absorbs any payload dupes.
            edge_batch = [
                [now, ctx.run_id, wid, canon_slug, email, tag_id, label,
                 *(acct_meta.get(email) or (None, None, None))]
                for email, labels in by_email.items()
                for label, tag_id in labels.items()
            ]
            if edge_batch:
                ctx.db.execute(
                    "CREATE OR REPLACE TEMP TABLE _acct_tag_batch ("
                    "_loaded_at TIMESTAMPTZ, _run_id VARCHAR, workspace_uuid VARCHAR, "
                    "workspace_slug VARCHAR, email VARCHAR, tag_id VARCHAR, tag_label VARCHAR, "
                    "provider_code INTEGER, status INTEGER, daily_limit INTEGER)"
                )
                _bulk_insert(ctx.db, "_acct_tag_batch", edge_batch, "edge")
                ctx.db.execute(
                    """
                    INSERT INTO raw_instantly_account_tag
                      (_loaded_at, _run_id, workspace_uuid, workspace_slug,
                       email, tag_id, tag_label, provider_code, status, daily_limit)
                    SELECT * FROM _acct_tag_batch
                    ON CONFLICT DO NOTHING
                    """
                )
                ctx.db.execute("DROP TABLE IF EXISTS _acct_tag_batch")
                raw_edge_rows += len(edge_batch)

            # Mark pairs scanned ONLY after this workspace's writes landed: an exception
            # mid-workspace adds NOTHING to the prune scope, so a pair can never enter it
            # with zero raw rows (which would wipe its last-good membership in the prune).
            # EVERY catalog tag is a scanned pair — including tags with zero members, so an
            # emptied tag correctly prunes to zero.
            scanned_pairs.update((canon_slug, label) for label in r["tag_labels"])
        except Exception as exc:  # noqa: BLE001
            logger.exception("account_tags %s: write failed — last-good kept", slug)
            failures.append({"slug": slug, "error": f"write_failed: {type(exc).__name__}"})
            continue

        inboxes_written += new_n
        workspaces_done.append(slug)
        logger.info("account_tags %s (slug=%s): full-replace %d inboxes across %d tags",
                    slug, canon_slug, new_n, r.get("n_tags"))

    # ---- resolve core.sending_account_tag (upsert + prune) ----------------------------------
    # Unguarded on purpose (matches the retired account_tag.py): a resolve failure fails the
    # phase loud; the account_tags writes above are already committed and re-heal next night.
    core_tag_rows = _resolve_core_account_tag(ctx, now, scanned_pairs)

    # ---- daily stage-history snapshot (DDL 1115) --------------------------------------------
    # core.account_tags is CURRENT-STATE ONLY: each inbox's row is overwritten with its current
    # tags, so yesterday's tag state is unrecoverable. The lifecycle stage (Warmup/Rampup/Active/
    # Rehab) is carried ENTIRELY by tags, so without a dated copy the warehouse can never answer
    # "which stage was this inbox in on <date>?" or "when did it start ramping up?". Instantly
    # cannot report tag-apply dates, so observing daily is the only way to build that history.
    #
    # Snapshots ONLY the inboxes refreshed in THIS run (_run_id = ctx.run_id) — i.e. the ones
    # actually seen live tonight — so the ghost rows account_tags deliberately retains do NOT
    # pollute the history. tags_arr is stored verbatim alongside the derived stage so the stage
    # can always be recomputed if the derivation ever changes.
    #
    # STAGE PRECEDENCE — Rehab > Warmup > Rampup > Active. An inbox is supposed to carry exactly
    # ONE status tag, so precedence only decides TRANSIENT double-tagged rows (a flip caught
    # mid-flight: the pipeline adds the new tag and removes the old one, and a nightly landing
    # between those two writes sees both). The rule is "record the EARLIER stage": a half-applied
    # flip has not completed, so claiming the inbox already graduated would date its stage entry
    # too early — and stage-entry dates are the whole point of this table. Rehab is checked first
    # because it is the one stage that does NOT sit on the linear Warmup→Rampup→Active line (an
    # old inbox re-entering rehab), so it can legitimately co-occur with any of them and must win.
    # This is a tie-break for a rare, transient state, NOT a claim about normal lifecycle order —
    # and it is reversible: tags_arr is stored verbatim, so any row can be re-derived if wrong.
    #
    # DEGRADES GRACEFULLY: wrapped so a snapshot failure can NEVER fail the tags phase. Worst
    # case is a one-day hole in the history; the tags themselves are already committed above.
    snapshot_rows = 0
    try:
        snap_date = now.date()
        ctx.db.execute(
            """
            INSERT INTO core.account_tags_daily
              (snapshot_date, email, workspace_uuid, workspace_slug, tags_arr, n_tags, stage,
               _loaded_at, _run_id)
            SELECT ?::DATE, t.email, t.workspace_uuid, t.workspace_slug, t.tags_arr, t.n_tags,
                   CASE WHEN list_contains(t.tags_arr, 'Rehab')  THEN 'Rehab'
                        WHEN list_contains(t.tags_arr, 'Warmup') THEN 'Warmup'
                        WHEN list_contains(t.tags_arr, 'Rampup') THEN 'Rampup'
                        WHEN list_contains(t.tags_arr, 'Active') THEN 'Active'
                        ELSE NULL END,
                   ?, ?
            FROM core.account_tags t
            WHERE t._run_id = ?
              AND NOT EXISTS (
                    SELECT 1 FROM core.account_tags_daily d
                    WHERE d.snapshot_date = ?::DATE
                      AND d.email = t.email
                      AND d.workspace_uuid = t.workspace_uuid
              )
            """,
            [snap_date, now, ctx.run_id, ctx.run_id, snap_date],
        )
        snapshot_rows = ctx.db.execute(
            "SELECT COUNT(*) FROM core.account_tags_daily WHERE snapshot_date = ?::DATE",
            [snap_date],
        ).fetchone()[0]
        logger.info("account_tags_daily: snapshot for %s now holds %d rows", snap_date, snapshot_rows)
    except Exception:  # noqa: BLE001
        # Never re-raise: the tag write above is what the phase exists for and it already succeeded.
        logger.exception("account_tags_daily: snapshot failed — one-day history gap; tags unaffected")

    return PhaseResult(rows_out=inboxes_written, notes={
        "inboxes_written": inboxes_written,
        "raw_edge_rows": raw_edge_rows,
        "core_tag_rows_after": core_tag_rows,
        "tag_pairs_scanned": len(scanned_pairs),
        "workspaces_done": workspaces_done,
        "failures": failures,
        "workers": _WORKERS,
        "deadline_min": _DEADLINE_S // 60,
        "skipped_on_deadline": skipped,
        "stage_snapshot_rows": snapshot_rows,
    })


def _resolve_core_account_tag(ctx, now: datetime, scanned_pairs: set[tuple[str, str]]) -> int:
    """Upsert this run's raw membership into core.sending_account_tag, then prune memberships
    that disappeared within the SUCCESSFULLY-scanned (workspace, tag) pairs. provider_code +
    canonical workspace_slug are taken from the latest census (raw is the fallback). Moved
    verbatim from the retired entities/account_tag.py, minus its allowlist, plus a
    (email, tag_label) dedupe: with the full tag universe the same email+label can now
    legitimately appear under two workspaces in one run, and DuckDB's ON CONFLICT DO UPDATE
    raises if a single statement updates one target row twice."""
    db = ctx.db

    # Deduped membership observed this run: exactly ONE row per (email, tag_label).
    db.execute("DROP TABLE IF EXISTS _run_account_tag")
    db.execute(
        """
        CREATE TEMP TABLE _run_account_tag AS
        SELECT email, tag_id, tag_label, workspace_slug, provider_code
        FROM (
            SELECT lower(email) AS email,
                   tag_id,
                   tag_label,
                   workspace_slug,
                   provider_code,
                   row_number() OVER (
                       PARTITION BY lower(email), tag_label
                       ORDER BY workspace_slug, tag_id
                   ) AS _rn
            FROM raw_instantly_account_tag
            WHERE _run_id = ?
        ) WHERE _rn = 1
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
