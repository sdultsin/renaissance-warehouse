"""BULK workspace-window email drain (SYNC-7 turnaround, 2026-07-14).

WHY THIS EXISTS: the per-lead drain (entities/email_thread_sync.run_fetch) is N API
calls per replying lead (~25-34 staged emails/min/ws measured 2026-07-14), capped by
the per-WORKSPACE Instantly rate budget (~20 req/min on /emails). Weekday inflow of
1.5-3k repliers/ws/day therefore outruns the drain on dense days (the Jul-8 bulge:
big-4 lag 115-143h and rising). The SAME per-ws budget spent on workspace-scoped
LISTING moves ~100 emails per request:

    GET /api/v2/emails?sort_order=asc&min_timestamp_created=A&max_timestamp_created=B

VERIFIED 2026-07-14 (probe /root/core/_bulk_probe.py + parity /root/core/_bulk_parity.py
on renaissance-1 Jul-8 06:00-12:00Z): server-side window + asc ordering + cursor
pagination all work; item shape identical to the per-lead pull (transform_item applies
verbatim); parity vs committed per-lead rows = 0 payload mismatches, 0 missing either
direction on 20 live-compared leads; measured ~2,000 raw emails/min sustained (~20
pages/min, the documented per-ws limit) ≈ 60-70x the per-lead drain rate.

DESIGN (maximum reuse — the apply path is UNCHANGED):
  * Ascending walk of a bounded window [watermark - overlap, fetch_start], appending
    rows in the EXACT stage schema via email_thread_sync.transform_item. The apply's
    DISTINCT-latest + non-destructive upsert dedupe anything we re-list.
  * Contiguity: sort_order=asc on timestamp_created (server-assigned at creation ->
    the stream is append-only in this key; a 60-min overlap absorbs clock skew and
    mid-burst resume). After every fsync'd page-batch the .progress sidecar records
    drained_through = last item's timestamp_created (mode="watermark"), so the
    EXISTING apply (_merge_drain_watermarks) advances the SAME per-ws watermark file
    only after rows durably commit. A killed/starved fetch loses nothing.
  * Two lanes, one code path: the watermark FILE is parametrized by the existing
    WAREHOUSE_THREADS_WATERMARK env (read here for the window floor, honored by the
    apply for the advance). The BACKFILL lane uses the default main file (contiguous
    low-watermark stays honest); the FRESH lane points at threads_fresh_watermark.json
    and clamps its floor to now - --window-floor-hours (default 48h), so operational
    consumers get current data every pass regardless of backfill depth.
  * Atom grain preserved: keep ue_type 2 (prospect reply) + 3 (our/IM reply) always;
    keep ue_type 1 (cold send) only for leads in this workspace's replied set (local
    raw_instantly_email, zero API calls — same source as per-lead discovery) or leads
    with a ue2/ue3 seen earlier in the walk. KNOWN bounded gap: a send whose lead
    first replies only in a LATER window is dropped this window and not re-listed —
    detectable as inbound-only threads; see /root/core/README-THREADS.md.
  * Page cap per invocation (--max-pages) bounds a pass; rc=2 signals "window not
    finished" (healthy partial — progress persisted), rc=0 window complete, rc=3+
    hard error (driver falls back to the per-lead fetch for the workspace).

Workspace selection reuses enumerate_orgs + WAREHOUSE_THREADS_ORG_ALLOWLIST (the
driver exports one slug per worker, exactly like the per-lead lane).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from core import db as db_module
from core.credentials import load_credentials
from entities.email_thread_sync import (
    _advance_watermarks,
    _assert_no_writer_lock_held,
    _coerce_utc,
    _ensure_trailing_newline,
    _load_watermarks,
    _parse_iso_utc,
    _read_progress_sidecar,
    _write_progress_sidecar,
    enumerate_orgs,
    transform_item,
    workspace_watermark,
)
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.email_thread_bulk")

_ISO_MS = "%Y-%m-%dT%H:%M:%S.000Z"


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(_ISO_MS)


def _replied_set(ws_slug: str, org_id: str | None, ws_uuid: str | None) -> set[str]:
    """This workspace's replied-lead set from the LOCAL inbound atom + already-committed
    thread leads (zero API calls). Matches discovery's UUID-keyed workspace_id handling."""
    leads: set[str] = set()
    ids = [str(x) for x in {org_id, ws_uuid} if x]
    con = db_module.connect(read_only=True)
    try:
        if ids:
            ph = ", ".join(["?"] * len(ids))
            try:
                for (l,) in con.execute(
                    f"SELECT DISTINCT lower(trim(lead_email)) FROM raw_instantly_email "
                    f"WHERE workspace_id IN ({ph}) AND lead_email IS NOT NULL", ids).fetchall():
                    if l:
                        leads.add(l)
            except Exception as exc:  # noqa: BLE001 — table absent on a fresh warehouse
                logger.warning("replied-set: raw_instantly_email unavailable (%s)", exc)
        try:
            for (l,) in con.execute(
                "SELECT DISTINCT lead_email FROM raw_instantly_email_message "
                "WHERE workspace_id = ? AND direction = 'inbound'", [ws_slug]).fetchall():
                if l:
                    leads.add(l)
        except Exception as exc:  # noqa: BLE001
            logger.warning("replied-set: raw_instantly_email_message unavailable (%s)", exc)
    finally:
        con.close()
    return leads


def run_bulk_fetch(stage_path: str, window_floor_hours: float | None,
                   max_pages: int, overlap_minutes: float, fsync_pages: int = 5,
                   received_only: bool = False, max_span_hours: float | None = None) -> int:
    """One bounded ascending bulk-window pull for every allowlisted workspace (the
    driver allowlists exactly one per worker). Returns worst rc across workspaces:
    0 = window complete, 2 = page-cap partial (healthy, resume next pass), 3 = error."""
    _assert_no_writer_lock_held()
    creds = load_credentials()
    conn = db_module.connect(read_only=True)
    try:
        workspaces, _diag = enumerate_orgs(creds, conn)
    finally:
        conn.close()
    if not workspaces:
        logger.error("bulk-fetch: no workspaces enumerated (check allowlist/keys)")
        return 3

    worst_rc = 0
    for ws_uuid, (ws_slug, api_key, organization_id) in workspaces.items():
        rc = _bulk_fetch_one(ws_uuid, ws_slug, api_key, organization_id, stage_path,
                             window_floor_hours, max_pages, overlap_minutes, fsync_pages,
                             received_only, max_span_hours)
        worst_rc = max(worst_rc, rc)
    return worst_rc


def _bulk_fetch_one(ws_uuid: str, ws_slug: str, api_key: str, org_id: str | None,
                    stage_path: str, window_floor_hours: float | None,
                    max_pages: int, overlap_minutes: float, fsync_pages: int,
                    received_only: bool = False, max_span_hours: float | None = None) -> int:
    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    # ── window floor: prior drained-through for THIS lane's watermark file ──────
    # (WAREHOUSE_THREADS_WATERMARK env — main file for backfill, fresh file for the
    # fresh lane). An unapplied prior partial's progress sidecar may be FURTHER than
    # the watermark file (fetch ran, apply deferred by the nightly guard) — resume
    # from the sidecar (its rows are durably staged; apply dedupes) so a guard-heavy
    # morning never re-lists the same pages.
    wm = _parse_iso_utc(_load_watermarks().get(ws_slug) or "")
    prog = _read_progress_sidecar(stage_path).get(ws_slug) or {}
    sidecar_dt = _parse_iso_utc(prog.get("drained_through") or "") if isinstance(prog, dict) else None
    floor = max((d for d in (wm, sidecar_dt) if d is not None), default=None)
    if window_floor_hours is not None:
        clamp = now - timedelta(hours=window_floor_hours)
        floor = clamp if floor is None else max(floor, clamp)
    if floor is None:
        # backfill lane bootstrap: fall back to the legacy max(message_at) derivation
        con = db_module.connect(read_only=True)
        try:
            floor = _coerce_utc(workspace_watermark(con, ws_slug))
        finally:
            con.close()
    if floor is None:
        logger.error("bulk-fetch ws=%s: no watermark and no floor — refusing an unbounded "
                     "full-history walk (use the per-lead full backfill for first population)",
                     ws_slug)
        return 3
    start = floor - timedelta(minutes=overlap_minutes)
    if start >= now:
        logger.info("RUNLOG-BULK ws=%s window empty (floor %s >= now)", ws_slug, floor.isoformat())
        return 0

    # WINDOW-CEILING [2026-07-14]: cap the span per pass. A wide (multi-day) ascending window on a
    # DENSE workspace makes Instantly's server-side sort time out (koi 5d window: limit=1 ReadTimeout
    # after 30s), stalling the whole backfill. Bounded slices (default 36h) each return page-1 in
    # <1s; the watermark advances per completed slice and the next pass continues -> contiguous.
    ceiling = now
    capped_span = False
    if max_span_hours is not None:
        cap = start + timedelta(hours=max_span_hours + overlap_minutes / 60.0)
        if cap < now:
            ceiling = cap
            capped_span = True

    params = {
        "limit": 100, "sort_order": "asc",
        "min_timestamp_created": _iso_z(start),
        "max_timestamp_created": _iso_z(ceiling),
    }
    if received_only:
        # RECEIVED-ONLY reply-capture mode [2026-07-14]: email_type=received returns ONLY
        # inbound prospect replies (ue2). On the DENSE big-4 backlog the stream is >90% cold
        # sends (ue1) that all-emails must page through (koi Jul-8: all-emails page-1
        # ReadTimeout after 225s; received-only walked the same 12h in 39 pages / 2.4 min).
        # Advances the watermark + captures every reply ~5-10x faster and reliably; drops ue1
        # cold-send bodies + ue3 our-replies from THIS lane, enriched later by a full pass (the
        # upsert is non-destructive — a later ue1/ue3 pull only ADDS). See README-THREADS.md.
        params["email_type"] = "received"
    replied = set() if received_only else _replied_set(ws_slug, org_id, ws_uuid)
    logger.info("bulk-fetch ws=%s window [%s .. %s] capped_span=%s received_only=%s replied_set=%d max_pages=%d stage=%s",
                ws_slug, params["min_timestamp_created"], params["max_timestamp_created"],
                capped_span, received_only, len(replied), max_pages, stage_path)

    progress_all = _read_progress_sidecar(stage_path)
    pages = raw_n = kept_n = dropped_ue1 = 0
    last_created: str | None = None
    complete = False
    t0 = time.time()
    _ensure_trailing_newline(stage_path)

    def _flush_progress(stage_fh) -> None:
        stage_fh.flush()
        os.fsync(stage_fh.fileno())
        if last_created:
            dt = _parse_iso_utc(last_created)
            if dt is not None:
                # never claim past the window we actually walked
                progress_all[ws_slug] = {
                    "drained_through": dt.isoformat(),
                    "complete": False, "mode": "watermark",
                }
                _write_progress_sidecar(stage_path, progress_all)

    try:
        with InstantlyClient(api_key) as client, open(stage_path, "a") as stage:
            cursor: str | None = None
            while pages < max_pages:
                p = dict(params)
                if cursor:
                    p["starting_after"] = cursor
                payload = client._get("/emails", params=p)
                items = payload.get("items") or []
                pages += 1
                raw_n += len(items)
                for it in items:
                    lead = (it.get("lead") or "").lower().strip()
                    ue = it.get("ue_type")
                    try:
                        ue = int(ue) if ue is not None else None
                    except (TypeError, ValueError):
                        ue = None
                    if ue in (2, 3) and lead:
                        replied.add(lead)  # walk-seen: later sends to this lead are kept
                    if ue == 1 and lead and lead not in replied:
                        dropped_ue1 += 1
                        continue
                    row = transform_item(it, org_id, ws_slug, fetched_at)
                    if row is None:
                        continue
                    stage.write(json.dumps(row, default=str) + "\n")
                    kept_n += 1
                if items:
                    last_created = items[-1].get("timestamp_created") or last_created
                cursor = payload.get("next_starting_after")
                if pages % fsync_pages == 0:
                    _flush_progress(stage)
                if not cursor:
                    complete = True
                    break
            _flush_progress(stage)
            if complete:
                # cursor exhausted -> everything created <= the window ceiling is staged. On a
                # span-capped slice the ceiling is < now, so the watermark advances to the ceiling
                # and the NEXT pass continues from there (contiguous). complete=True here means
                # "this slice is done", not necessarily "caught up to now".
                progress_all[ws_slug] = {
                    "drained_through": ceiling.isoformat(),
                    "complete": not capped_span, "mode": "watermark",
                }
                _write_progress_sidecar(stage_path, progress_all)
            el = max(time.time() - t0, 0.001)
            logger.info(
                "RUNLOG-BULK ws=%s pages=%d raw=%d kept=%d dropped_ue1=%d 429s=%d "
                "elapsed_s=%.0f raw_per_min=%.0f drained_through=%s complete=%s",
                ws_slug, pages, raw_n, kept_n, dropped_ue1, client.rate_limit_hits,
                el, raw_n / el * 60,
                (progress_all.get(ws_slug) or {}).get("drained_through"), complete,
            )
    except InstantlyError as exc:
        logger.error("bulk-fetch ws=%s API failure after %d pages (%s) — progress through "
                     "the last fsync'd page is preserved in the sidecar", ws_slug, pages, exc)
        return 3
    except OSError as exc:
        logger.error("bulk-fetch ws=%s stage IO failure (%s)", ws_slug, exc)
        return 3
    return 0 if complete else 2


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="bounded ascending bulk-window pull (no DB writes)")
    f.add_argument("--stage", required=True)
    f.add_argument("--window-floor-hours", type=float, default=None,
                   help="clamp the window floor to now-N hours (FRESH lane); default: none (BACKFILL)")
    f.add_argument("--max-pages", type=int, default=450)
    f.add_argument("--overlap-minutes", type=float, default=60.0)
    f.add_argument("--received-only", action="store_true",
                   help="email_type=received: capture ONLY inbound replies (ue2), ~5-10x fewer "
                        "pages on dense workspaces; drops ue1/ue3 from this lane (enriched later).")
    f.add_argument("--max-span-hours", type=float, default=None,
                   help="cap the window span per pass (dense-ws robustness; a multi-day window "
                        "times out server-side). The watermark advances per completed slice.")
    args = ap.parse_args(argv)
    if os.environ.get("WAREHOUSE_PULL_THREADS") != "1":
        logger.info("WAREHOUSE_PULL_THREADS != 1 — bulk fetch disabled, no-op")
        return 0
    return run_bulk_fetch(args.stage, args.window_floor_hours, args.max_pages,
                          args.overlap_minutes, received_only=args.received_only,
                          max_span_hours=args.max_span_hours)


if __name__ == "__main__":
    sys.exit(main())
