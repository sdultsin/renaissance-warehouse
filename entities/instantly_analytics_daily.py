"""Daily-report §1/§1b centralization — Instantly daily analytics mirror.

Mirrors EXACTLY what scripts/render_daily.py live-pulls at render time
(PROVENANCE-MAP §2, 2026-07-01), day-scoped, into:

  raw_instantly_workspace_analytics_daily  <- GET /campaigns/analytics/daily
      (no campaign_id) per report-roster workspace: one row per (workspace, day).
      §1 "Sent"/"Opps" == sent/opportunities here (validated 2026-06-30:
      Σ sent over the roster == 2,433,955 == the report total).
  raw_instantly_campaign_analytics_daily   <- GET /campaigns/analytics/daily
      ?campaign_id=… fan-out: one row per (campaign, day), carrying the RAW
      campaign tag dimensions (email_tag_list ids + labels). NO tag->tier/infra
      mapping here — that taxonomy is a pending business definition; §1b's
      _infra_of() stays in the renderer until the flip decision.
  raw_instantly_tag_def                    <- per-id GET /custom-tags/{id} for
      ids actually referenced by campaigns (bounded; avoids paging the full
      multi-thousand-row RG tag catalogs — the #124/#126 runaway class).
  raw_instantly_analytics_sync_status      <- one row per (run, workspace):
      the 100%-or-flagged rule. A roster workspace that fails is VISIBLY
      recorded here + raises at the end (phase 'failed', run 'partial'),
      never a silent zero. Successful workspaces commit regardless.

ADDITIVE ONLY: the renderer keeps live-pulling; nothing is repointed.

Fault tolerance (Instantly is fragile — some workspace endpoints 500):
  * per-workspace isolation: one workspace failing cannot drop the others
    (each workspace is fetched, then written in its own transaction);
  * per-campaign isolation inside a workspace: a failed campaign fetch is
    counted (campaigns_failed) and its EXISTING rows are left untouched
    (upserts are keyed per (campaign_id, date) — no window-wide DELETE);
  * a workspace with NO key or a dead fetch -> status='failed' row + raise
    AFTER all other workspaces committed.

Scope: the report roster (config/daily_report_sources.json workspaces.roster —
registry = code = reality; a roster change flows through with no code change).
Extra slugs can be added via WAREHOUSE_INSTANTLY_ANALYTICS_EXTRA_SLUGS
(comma-separated; best-effort — failures logged but do NOT fail the phase).

Nightly cost is bounded: per workspace 1 ws-daily call + campaign-list pages
(~100/page) + 1 daily call per campaign (272 campaigns across the roster as of
2026-07-01) + 1 call per NEW referenced tag id — all day-scoped
(WAREHOUSE_INSTANTLY_ANALYTICS_DAYS back, default 3). Never a full-history pull.

Backfill (one-off, NOT on the nightly path; takes the writer flock itself):
    python -m entities.instantly_analytics_daily --start 2026-06-01 --end 2026-07-01
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

from core.daily_report_sources import workspace_roster
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.instantly_analytics_daily")

# How many days back the nightly re-pulls (overlap covers timezone edges +
# late-restated replies/opps on recent days). Day-scoped by design.
WINDOW_DAYS = int(os.environ.get("WAREHOUSE_INSTANTLY_ANALYTICS_DAYS", "3"))
# Fan-out concurrency WITHIN one workspace (fetch only; all DB writes are on the
# caller thread). 6 matches the proven render_daily (8) / build_campaign_daily (8)
# range while staying gentle — Instantly is fragile right now.
FANOUT_WORKERS = int(os.environ.get("WAREHOUSE_INSTANTLY_ANALYTICS_WORKERS", "6"))

_METRIC_FIELDS = [
    "sent", "contacted", "new_leads_contacted", "opened", "unique_opened",
    "clicks", "unique_clicks", "replies", "unique_replies",
    "replies_automatic", "unique_replies_automatic",
    "opportunities", "unique_opportunities",
]

_WS_COLS = ["workspace_slug", "date", *_METRIC_FIELDS, "api_response_raw", "_loaded_at", "_run_id"]
_WS_UPSERT = (
    f"INSERT INTO raw_instantly_workspace_analytics_daily ({', '.join(_WS_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _WS_COLS)}) "
    "ON CONFLICT (workspace_slug, date) DO UPDATE SET "
    + ", ".join(f"{c} = excluded.{c}" for c in _WS_COLS if c not in ("workspace_slug", "date"))
)

_CAMP_COLS = [
    "campaign_id", "date", "workspace_slug", "campaign_name", "campaign_status",
    *_METRIC_FIELDS, "tag_ids", "tag_labels", "_loaded_at", "_run_id",
]
_CAMP_UPSERT = (
    f"INSERT INTO raw_instantly_campaign_analytics_daily ({', '.join(_CAMP_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _CAMP_COLS)}) "
    "ON CONFLICT (campaign_id, date) DO UPDATE SET "
    + ", ".join(f"{c} = excluded.{c}" for c in _CAMP_COLS if c not in ("campaign_id", "date"))
)

_TAG_UPSERT = (
    "INSERT INTO raw_instantly_tag_def (tag_id, label, organization_id, "
    "timestamp_created, timestamp_updated, _loaded_at, _run_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT (tag_id) DO UPDATE SET label = excluded.label, "
    "organization_id = excluded.organization_id, "
    "timestamp_created = excluded.timestamp_created, "
    "timestamp_updated = excluded.timestamp_updated, "
    "_loaded_at = excluded._loaded_at, _run_id = excluded._run_id"
)

_STATUS_INSERT = (
    "INSERT INTO raw_instantly_analytics_sync_status "
    "(_run_id, workspace_slug, status, error, window_start, window_end, "
    "ws_day_rows, campaign_day_rows, campaigns_total, campaigns_failed, "
    "tags_unresolved, _loaded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def register(registry: Registry) -> None:
    registry.add_phase("instantly", "analytics_daily", run_analytics_daily)


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _daily_rows(client: InstantlyClient, start: str, end: str, campaign_id: str | None = None) -> list[dict]:
    """GET /campaigns/analytics/daily for [start, end]; bare-list or {items:[...]}."""
    params: dict = {"start_date": start, "end_date": end}
    if campaign_id:
        params["campaign_id"] = campaign_id
    payload = client._get("/campaigns/analytics/daily", params=params)  # noqa: SLF001 — house pattern (render_daily, email_thread_sync)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("items") or []
    return []


def _fetch_workspace(client: InstantlyClient, slug: str, start: str, end: str) -> dict:
    """Network-only fetch for one workspace (NO db access — thread/lock safe).

    Returns {ws_days: [day dict], campaigns: [campaign dict],
             camp_days: {campaign_id: [day dict]}, failed_campaigns: [(id, err)]}.
    Raises on a workspace-level failure (ws-daily call or campaign listing dead).
    """
    ws_days = _daily_rows(client, start, end)
    campaigns = [
        {
            "id": c.get("id"),
            "name": c.get("name"),
            "status": c.get("status"),
            "email_tag_list": c.get("email_tag_list") or [],
        }
        for c in client.list_campaigns()
        if c.get("id")
    ]
    camp_days: dict[str, list[dict]] = {}
    failed_campaigns: list[tuple[str, str]] = []
    if campaigns:
        with ThreadPoolExecutor(max_workers=FANOUT_WORKERS) as ex:
            futs = {
                ex.submit(_daily_rows, client, start, end, c["id"]): c["id"]
                for c in campaigns
            }
            for fut in as_completed(futs):
                cid = futs[fut]
                try:
                    camp_days[cid] = fut.result()
                except Exception as exc:  # noqa: BLE001 — isolate per campaign
                    failed_campaigns.append((cid, f"{type(exc).__name__}: {exc}"[:200]))
    logger.info(
        "%s: fetched %d ws-day rows, %d campaigns (%d fetch-failed)",
        slug, len(ws_days), len(campaigns), len(failed_campaigns),
    )
    return {
        "ws_days": ws_days,
        "campaigns": campaigns,
        "camp_days": camp_days,
        "failed_campaigns": failed_campaigns,
    }


def _resolve_tags(conn, client: InstantlyClient, tag_ids: set[str], now, run_id) -> tuple[dict[str, str], int]:
    """Resolve tag ids -> labels via per-id GET (bounded), falling back to the
    raw_instantly_tag_def cache for ids whose fetch fails. Upserts fresh defs.
    Returns ({tag_id: label}, unresolved_count). DB access on caller thread only.
    """
    labels: dict[str, str] = {}
    cached: dict[str, str] = dict(
        conn.execute(
            "SELECT tag_id, label FROM raw_instantly_tag_def WHERE tag_id IN "
            f"({', '.join('?' for _ in tag_ids)})",
            list(tag_ids),
        ).fetchall()
    ) if tag_ids else {}
    unresolved = 0
    for tid in sorted(tag_ids):
        try:
            t = client._get(f"/custom-tags/{tid}")  # noqa: SLF001
            label = t.get("label") or t.get("name")
            labels[tid] = label
            conn.execute(
                _TAG_UPSERT,
                [tid, label, t.get("organization_id"),
                 t.get("timestamp_created"), t.get("timestamp_updated"), now, run_id],
            )
        except Exception as exc:  # noqa: BLE001 — fall back to cache, never fail the ws
            if tid in cached and cached[tid]:
                labels[tid] = cached[tid]
                logger.warning("tag %s fetch failed (%s); using cached label %r", tid, exc, cached[tid])
            else:
                unresolved += 1
                logger.warning("tag %s fetch failed (%s); no cached label", tid, exc)
    return labels, unresolved


def _write_workspace(conn, slug: str, fetched: dict, labels: dict[str, str],
                     now, run_id) -> tuple[int, int]:
    """Upsert one workspace's fetched window inside a single transaction.
    Returns (ws_day_rows, campaign_day_rows)."""
    by_id = {c["id"]: c for c in fetched["campaigns"]}
    conn.execute("BEGIN")
    try:
        ws_rows = 0
        for day in fetched["ws_days"]:
            d = str(day.get("date", ""))[:10]
            if not d:
                continue
            conn.execute(
                _WS_UPSERT,
                [slug, d, *[_int_or_none(day.get(f)) for f in _METRIC_FIELDS],
                 json.dumps(day), now, run_id],
            )
            ws_rows += 1
        camp_rows = 0
        for cid, days in fetched["camp_days"].items():
            c = by_id.get(cid, {})
            tag_ids = c.get("email_tag_list") or []
            tag_ids_json = json.dumps(tag_ids)
            tag_labels_json = json.dumps([labels.get(t) for t in tag_ids])
            for day in days:
                d = str(day.get("date", ""))[:10]
                if not d:
                    continue
                conn.execute(
                    _CAMP_UPSERT,
                    [cid, d, slug, c.get("name"), _int_or_none(c.get("status")),
                     *[_int_or_none(day.get(f)) for f in _METRIC_FIELDS],
                     tag_ids_json, tag_labels_json, now, run_id],
                )
                camp_rows += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return ws_rows, camp_rows


def _ingest(conn, credentials, run_id: str, start: str, end: str,
            slugs: list[str] | None = None) -> PhaseResult:
    keys = credentials.instantly_workspace_keys()
    roster = [s for s, _ in workspace_roster()]
    required = slugs if slugs is not None else roster
    extra = [
        s.strip() for s in
        os.environ.get("WAREHOUSE_INSTANTLY_ANALYTICS_EXTRA_SLUGS", "").split(",")
        if s.strip() and s.strip() not in required
    ] if slugs is None else []

    now = datetime.now(timezone.utc)
    total_ws = total_camp = 0
    failures: list[dict] = []
    done: list[str] = []

    for slug in [*required, *extra]:
        best_effort = slug in extra
        key = keys.get(slug)
        if not key:
            err = "no INSTANTLY_KEY_* credential for this slug"
            logger.error("%s: %s", slug, err)
            conn.execute(_STATUS_INSERT, [run_id, slug, "failed", err, start, end,
                                          None, None, None, None, None, now])
            if not best_effort:
                failures.append({"slug": slug, "error": err})
            continue
        try:
            with InstantlyClient(key) as client:
                fetched = _fetch_workspace(client, slug, start, end)
                referenced = {t for c in fetched["campaigns"] for t in c["email_tag_list"]}
                labels, unresolved = _resolve_tags(conn, client, referenced, now, run_id)
            ws_rows, camp_rows = _write_workspace(conn, slug, fetched, labels, now, run_id)
            conn.execute(
                _STATUS_INSERT,
                [run_id, slug, "ok", None, start, end, ws_rows, camp_rows,
                 len(fetched["campaigns"]), len(fetched["failed_campaigns"]),
                 unresolved, now],
            )
            total_ws += ws_rows
            total_camp += camp_rows
            done.append(slug)
            if fetched["failed_campaigns"]:
                logger.error(
                    "%s: %d campaign fetches failed (rows for those campaigns kept "
                    "at last-good): %s", slug, len(fetched["failed_campaigns"]),
                    fetched["failed_campaigns"][:5],
                )
            if ws_rows == 0:
                logger.warning(
                    "%s: 0 workspace-day rows in %s..%s — NOT necessarily an error "
                    "(no activity), but this workspace normally sends daily", slug, start, end,
                )
        except Exception as exc:  # noqa: BLE001 — per-workspace isolation
            err = f"{type(exc).__name__}: {exc}"[:400]
            logger.exception("%s: workspace ingest failed", slug)
            try:
                conn.execute(_STATUS_INSERT, [run_id, slug, "failed", err, start, end,
                                              None, None, None, None, None, now])
            except Exception:  # noqa: BLE001 — status write must not mask the cause
                logger.exception("%s: could not write failed-status row", slug)
            if not best_effort:
                failures.append({"slug": slug, "error": err})

    notes = {
        "window": f"{start}..{end}",
        "workspaces_done": done,
        "failures": failures,
        "ws_day_rows": total_ws,
        "campaign_day_rows": total_camp,
    }
    if failures:
        # Fail LOUD after every healthy workspace committed: phase logs 'failed',
        # the run goes 'partial' — a dead roster workspace is never a silent zero.
        raise RuntimeError(
            f"instantly analytics_daily: {len(failures)} roster workspace(s) failed "
            f"({[f['slug'] for f in failures]}); healthy workspaces committed "
            f"(ws_rows={total_ws}, campaign_rows={total_camp}). "
            f"See raw_instantly_analytics_sync_status _run_id={run_id}."
        )
    return PhaseResult(rows_in=total_ws + total_camp, rows_out=total_ws + total_camp, notes=notes)


def run_analytics_daily(ctx: RunContext) -> PhaseResult:
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    return _ingest(ctx.db, ctx.credentials, ctx.run_id, start, today.isoformat())


def main(argv: list[str] | None = None) -> int:
    """One-off scoped backfill (NOT the nightly path). Opens its own writer
    connection — core.db.connect() acquires the box flock (acquire-or-wait)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--slugs", help="comma-separated slug subset (default: report roster)")
    args = parser.parse_args(argv)
    date.fromisoformat(args.start), date.fromisoformat(args.end)  # validate

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from core import db as db_module
    from core.credentials import load_credentials

    run_id = f"backfill_instantly_analytics_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    conn = db_module.connect()
    try:
        slugs = [s.strip() for s in args.slugs.split(",") if s.strip()] if args.slugs else None
        result = _ingest(conn, load_credentials(), run_id, args.start, args.end, slugs=slugs)
        print(json.dumps(result.notes, indent=2))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
