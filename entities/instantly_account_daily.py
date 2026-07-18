"""Warehouse-native per-account daily analytics mirror (sent + human/auto reply split).

Pulls Instantly GET /accounts/analytics/daily per workspace into
`main.raw_instantly_account_daily` (one row per account per day), mirroring the schema
of the dead DP-v2 `infra_account_daily_metrics` so downstream is drop-in. Feeds the
per-domain reply-rate rollup `core.v_domain_reply_daily` (DDL 1142).

THE 413 FIX (why this entity exists — the DP-v2 sync never got it): the whole-workspace
pull 413s ("Payload Too Large — add an emails filter or request a smaller date range")
once a workspace crosses ~a few hundred accounts. The break is account-COUNT driven, not
date-range driven — even a single day 413s at workspace scope. So we CHUNK BY THE `emails`
FILTER: list the workspace's accounts, request /accounts/analytics/daily?emails=<batch>
in batches of CHUNK_SIZE (<=100; verified live 2026-07-18: renaissance-1 @ 13,607 accounts
whole-workspace 413s, emails-filtered batches of 200 return 200 / 500 413s). A single
filtered request spans the whole rolling window (one row per account per active day), so a
137-batch workspace covers the full window in ~137 requests, not per-day.

`unique_replies` = HUMAN replies (per-lead dedup); `unique_replies_automatic` = AUTO —
Instantly's own authoritative split, the same columns core.campaign_daily.replies_human/
_auto come from at campaign grain. Reply truth is Instantly-native ONLY (the home-grown
auto/human classifier was dropped 2026-06-14, reference_warehouse_reply_and_tag_truth_20260614).

Scope: EVERY workspace from the credential enumerator (credentials.instantly_workspace_keys()
— all INSTANTLY_KEY_* except PERSONAL/SAM_TEST). A workspace on a dead/retired plan returns
402 Payment Required -> SKIPPED (not a failure). Any OTHER per-workspace failure is isolated
(healthy workspaces still commit) and the phase FAILS LOUD at the end so a break is never a
silent zero.

Instantly = direct REST, browser UA, never MCP (feedback_instantly_api_not_mcp_20260630);
429 retries adaptively; the 413 is handled by emails-chunking, not retry.

Nightly: registers under phase 'replies_late' (PASS B — NOT fleet-health-critical, so it
never blocks PASS A's ~03:30 ET promote), rolling WAREHOUSE_ACCOUNT_DAILY_DAYS window back
(default 3, overlap covers late-restated recent days).

Backfill (one-off, NOT the nightly path; takes the writer flock itself):
    python -m entities.instantly_account_daily --start 2026-07-10 --end 2026-07-18
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.instantly_account_daily")


def _env_int(name: str, default: int) -> int:
    """A malformed env var degrades to the default, never raises at import (an
    import-time raise makes discover_and_register silently skip registration)."""
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        logger.warning("invalid %s; using default %d", name, default)
        return default


# How many days back the nightly re-pulls (overlap covers timezone edges + late-restated
# replies on recent days). Day-scoped by design.
WINDOW_DAYS = _env_int("WAREHOUSE_ACCOUNT_DAILY_DAYS", 3)
# Accounts per /accounts/analytics/daily request. <=200 verified safe live; 100 = wide
# margin under the 413 payload cap (renaissance-1 @ 13,607 accounts).
CHUNK_SIZE = _env_int("WAREHOUSE_ACCOUNT_DAILY_CHUNK", 100)
# Fan-out concurrency WITHIN one workspace (network fetch ONLY — every DB write is on the
# caller thread; the httpx client is shared thread-safe, per email_thread_sync). Gentle by
# default — Instantly is fragile and rate-limits.
FANOUT_WORKERS = _env_int("WAREHOUSE_ACCOUNT_DAILY_WORKERS", 4)

# Per-account fields returned by /accounts/analytics/daily (verified live 2026-07-18).
_METRIC_FIELDS = [
    "sent", "bounced", "contacted", "new_leads_contacted",
    "opened", "unique_opened", "replies", "unique_replies",
    "replies_automatic", "unique_replies_automatic", "clicks", "unique_clicks",
]

_COLS = [
    "account_email", "metric_date", "workspace_slug", "domain", "provider_group",
    *_METRIC_FIELDS, "api_synced_at", "_loaded_at", "_run_id",
]
_UPSERT = (
    f"INSERT INTO main.raw_instantly_account_daily ({', '.join(_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _COLS)}) "
    "ON CONFLICT (account_email, metric_date) DO UPDATE SET "
    + ", ".join(
        f"{c} = excluded.{c}" for c in _COLS if c not in ("account_email", "metric_date")
    )
)

# Instantly provider_code -> best-effort provider family. NOT the dead pipeline's OTD-aware
# business taxonomy — consumers needing true vendor join core.sending_account_vendor.
_PROVIDER_GROUP = {1: "imap", 2: "google", 3: "outlook", 4: "outlook"}


def register(registry: Registry) -> None:
    registry.add_phase("replies_late", "account_daily", run_account_daily)


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _chunks(seq: list, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _list_accounts(client: InstantlyClient) -> tuple[list[str], dict[str, str | None]]:
    """Return (emails, {email: provider_group}) for a workspace. Network only."""
    emails: list[str] = []
    provider: dict[str, str | None] = {}
    seen: set[str] = set()
    for a in client.list_accounts():
        em = (a.get("email") or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        emails.append(em)
        provider[em] = _PROVIDER_GROUP.get(_int_or_none(a.get("provider_code")))
    return emails, provider


def _fetch_workspace(client: InstantlyClient, slug: str, start: str, end: str) -> list[dict]:
    """Network-only fetch for one workspace (NO db access — thread/lock safe).

    Lists accounts, then pulls /accounts/analytics/daily?emails=<batch> per CHUNK_SIZE
    batch (fanned out FANOUT_WORKERS-wide) over [start, end]. Returns the raw day rows
    annotated with the resolved provider_group. Raises on a workspace-level failure
    (account listing dead, or a batch 413/errors — which would mean the chunk fix regressed).
    """
    emails, provider = _list_accounts(client)
    if not emails:
        logger.warning("%s: 0 accounts listed", slug)
        return []
    batches = list(_chunks(emails, CHUNK_SIZE))
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=FANOUT_WORKERS) as ex:
        futs = {
            ex.submit(client.account_daily_analytics, b, start, end): i
            for i, b in enumerate(batches)
        }
        for fut in as_completed(futs):
            # A batch failure (incl. a 413 that should NOT happen after chunking) must
            # fail the whole workspace, not silently drop accounts — surface it.
            for item in fut.result():
                em = (item.get("email_account") or "").strip().lower()
                if not em:
                    continue
                item["_provider_group"] = provider.get(em)
                rows.append(item)
    logger.info("%s: %d accounts, %d batches -> %d day-rows", slug, len(emails), len(batches), len(rows))
    return rows


def _write_workspace(conn, slug: str, rows: list[dict], now, run_id: str) -> int:
    """Upsert one workspace's fetched rows in a single transaction. Returns row count."""
    conn.execute("BEGIN")
    try:
        written = 0
        for item in rows:
            em = (item.get("email_account") or "").strip().lower()
            d = str(item.get("date", ""))[:10]
            if not em or not d:
                continue
            domain = em.split("@", 1)[1] if "@" in em else None
            conn.execute(
                _UPSERT,
                [
                    em, d, slug, domain, item.get("_provider_group"),
                    *[_int_or_none(item.get(f)) for f in _METRIC_FIELDS],
                    now, now, run_id,
                ],
            )
            written += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return written


def _ingest(conn, credentials, run_id: str, start: str, end: str,
            slugs: list[str] | None = None) -> PhaseResult:
    if date.fromisoformat(end) < date.fromisoformat(start):
        raise ValueError(f"end {end} before start {start} — inverted window ingests nothing")
    keys = credentials.instantly_workspace_keys()
    targets = slugs if slugs is not None else sorted(keys)

    now = datetime.now(timezone.utc)
    total = 0
    done: list[str] = []
    skipped: list[str] = []
    failures: list[dict] = []

    for slug in targets:
        key = keys.get(slug)
        if not key:
            err = "no INSTANTLY_KEY_* credential for this slug"
            logger.error("%s: %s", slug, err)
            failures.append({"slug": slug, "error": err})
            continue
        try:
            with InstantlyClient(key) as client:
                rows = _fetch_workspace(client, slug, start, end)
            written = _write_workspace(conn, slug, rows, now, run_id)
            total += written
            done.append(slug)
            if written == 0:
                logger.warning("%s: 0 account-day rows in %s..%s (no activity?)", slug, start, end)
        except InstantlyError as exc:
            # A dead/retired workspace plan returns 402 -> SKIP (not a failure). Any other
            # Instantly error is a real failure (esp. a 413, which would mean the emails
            # chunk fix regressed) -> isolate + fail loud at the end.
            msg = str(exc)
            if " -> 402" in msg or "402:" in msg:
                logger.warning("%s: 402 Payment Required (dead/retired plan) -> skipped", slug)
                skipped.append(slug)
            else:
                logger.exception("%s: workspace ingest failed", slug)
                failures.append({"slug": slug, "error": f"{type(exc).__name__}: {msg}"[:400]})
        except Exception as exc:  # noqa: BLE001 — per-workspace isolation
            logger.exception("%s: workspace ingest failed", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:400]})

    notes = {
        "window": f"{start}..{end}",
        "workspaces_done": done,
        "workspaces_skipped_402": skipped,
        "failures": failures,
        "rows": total,
    }
    if failures:
        # Fail LOUD after every healthy workspace committed: the phase logs 'failed', the
        # run goes 'partial' — a dead workspace is never a silent zero.
        raise RuntimeError(
            f"instantly account_daily: {len(failures)} workspace(s) failed "
            f"({[f['slug'] for f in failures]}); healthy committed (rows={total}, "
            f"done={len(done)}, skipped_402={len(skipped)})."
        )
    return PhaseResult(rows_in=total, rows_out=total, notes=notes)


def run_account_daily(ctx: RunContext) -> PhaseResult:
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    return _ingest(ctx.db, ctx.credentials, ctx.run_id, start, today.isoformat())


def main(argv: list[str] | None = None) -> int:
    """One-off scoped backfill (NOT the nightly path). Opens its own writer connection —
    core.db.connect() acquires the box flock (acquire-or-wait)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--slugs", help="comma-separated slug subset (default: all credential keys)")
    args = parser.parse_args(argv)
    date.fromisoformat(args.start), date.fromisoformat(args.end)  # validate

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from core import db as db_module
    from core.credentials import load_credentials

    run_id = f"backfill_account_daily_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
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
