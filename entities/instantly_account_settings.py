"""Per-account SETTINGS + warmup-start daily snapshot (task #28 step 3).

One paginated `GET /accounts` sweep per workspace -> `main.raw_instantly_account_settings`,
one row per (workspace_slug, account_email, snapshot_date). Captures the CONFIG axis the
per-account analytics mirror (instantly_account_daily) does not: `daily_limit`, `status`,
`provider_code`, `warmup_status`, `enable_slow_ramp`, `sending_gap`,
`timestamp_warmup_start`, `timestamp_created`, `stat_warmup_score`.

WHY (three gaps this closes):
  * `timestamp_warmup_start` is 100% populated on /accounts (600/600 verified 2026-07-19,
    active+paused, Google+MS) -> this sweep is the WARMUP-START SOURCE OF TRUTH, superseding
    the stale 14-row manual-CSV `core.batch_warmup_schedule` (as_of 2026-06-25; deprecated
    by the same DDL that creates this table).
  * `enable_slow_ramp` / `sending_gap` had NO warehouse home (the census parquet omits them).
  * The existing per-date settings source (`core.account_census`) is fed by a BOX-ONLY
    hourly poller (`poll_live_accounts.py`, not in this repo) — the same droplet-death
    liability class as the retired account_truth CSVs. This entity is the repo-versioned,
    box-portable sweep of the same endpoint.

ENDPOINT SEMANTICS (verified live 2026-07-19, task #28 scout): pagination is
`next_starting_after` -> passed back as `starting_after` (sources/instantly.py._paginate
already does this — we simply reuse `client.list_accounts()`). The `emails=` param is a
NO-OP on /accounts (unlike /accounts/analytics/daily) — there is NO 413 risk at workspace
scope and NO chunking needed; one serial paginated walk covers the workspace. `search=`
targets one account (unused here). Client already sends the browser UA (Instantly
fingerprint-blocks python UAs) and adaptive 429 backoff. Direct REST, never MCP
(feedback_instantly_api_not_mcp_20260630). Serial across workspaces
(feedback_instantly_list_accounts_serial_only). A workspace above ~500k accounts would
trip the client's PAGINATION_CEILING and fail LOUD for that workspace (correct: never a
silent partial snapshot).

PHASE: 'replies_late' (PASS B), alongside sibling instantly_account_daily. Why not PASS A:
this is a full-fleet walk (~1.5M accounts ≈ ~15k pages ≈ hours), and PASS A exists to
promote the fleet-health snapshot at ~03:30 ET — a slow sweep must never sit in front of
that promote. Nothing in the nightly rebuild DEPENDS on this raw table (its consumers are
views, resolved at query time), so PASS B ordering is dependency-safe.

FAILURE CONTRACT (same as sibling): 401/402 = dead/retired workspace credential -> SKIP;
any other per-workspace failure is isolated (healthy workspaces still commit) and the phase
fails LOUD at the end. Upsert is idempotent per (workspace, email, snapshot_date) — a
re-run the same UTC day refreshes that day's snapshot.

Smoke (network-only, NO db, NO writer lock — safe anywhere):
    python -m entities.instantly_account_settings --smoke <workspace-slug> [--pages 1]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

import pyarrow as pa

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.instantly import InstantlyClient, InstantlyError

logger = logging.getLogger("entities.instantly_account_settings")

# Settings fields lifted verbatim from the /accounts item (top-level keys; the census
# parquet confirms warmup_status / stat_warmup_score / timestamp_warmup_start are
# top-level, and account_truth.py confirms sending_gap).
_INT_FIELDS = ["daily_limit", "status", "provider_code", "warmup_status",
               "sending_gap", "stat_warmup_score"]
_TS_FIELDS = ["timestamp_warmup_start", "timestamp_created"]

_COLS = [
    "workspace_slug", "account_email", "snapshot_date", "domain",
    *_INT_FIELDS, "enable_slow_ramp", *_TS_FIELDS,
    "api_synced_at", "_loaded_at", "_run_id",
]

# Vectorized upsert via a registered pyarrow staging table (same pattern as
# instantly_account_daily — per-row executemany was O(minutes) at this scale). The window
# de-dups per PK so a duplicate account in the API payload can never abort the statement.
_STG = "_stg_account_settings"
_UPSERT_SELECT = (
    f"INSERT INTO main.raw_instantly_account_settings ({', '.join(_COLS)}) "
    f"SELECT {', '.join(_COLS)} FROM ("
    f"  SELECT *, row_number() OVER "
    f"    (PARTITION BY workspace_slug, account_email, snapshot_date "
    f"     ORDER BY timestamp_created DESC NULLS LAST) AS _rn "
    f"  FROM {_STG}"
    f") WHERE _rn = 1 "
    "ON CONFLICT (workspace_slug, account_email, snapshot_date) DO UPDATE SET "
    + ", ".join(
        f"{c} = excluded.{c}" for c in _COLS
        if c not in ("workspace_slug", "account_email", "snapshot_date")
    )
)


def register(registry: Registry) -> None:
    registry.add_phase("replies_late", "account_settings", run_account_settings)


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _bool_or_none(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
    return None


def _str_or_none(v):
    """Timestamps stay ISO strings in staging; DuckDB casts to TIMESTAMPTZ on insert."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _fetch_workspace(client: InstantlyClient, slug: str) -> list[dict]:
    """Network-only: one paginated /accounts walk (all fields, no filter). No DB access."""
    items = list(client.list_accounts())
    logger.info("%s: %d accounts listed", slug, len(items))
    return items


def _write_workspace(conn, slug: str, items: list[dict], snapshot_date: str,
                     now, run_id: str) -> tuple[int, int]:
    """One vectorized upsert of a workspace's snapshot. Returns (rows, warmup_start_filled)."""
    cols: dict[str, list] = {c: [] for c in _COLS}
    warmup_filled = 0
    seen: set[str] = set()
    for a in items:
        em = (a.get("email") or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        cols["workspace_slug"].append(slug)
        cols["account_email"].append(em)
        cols["snapshot_date"].append(snapshot_date)
        cols["domain"].append(em.split("@", 1)[1] if "@" in em else None)
        for f in _INT_FIELDS:
            cols[f].append(_int_or_none(a.get(f)))
        cols["enable_slow_ramp"].append(_bool_or_none(a.get("enable_slow_ramp")))
        for f in _TS_FIELDS:
            cols[f].append(_str_or_none(a.get(f)))
        if cols["timestamp_warmup_start"][-1]:
            warmup_filled += 1
        cols["api_synced_at"].append(now)
        cols["_loaded_at"].append(now)
        cols["_run_id"].append(run_id)

    n = len(cols["account_email"])
    if n == 0:
        return 0, 0
    # Explicit arrow types so an all-NULL column can't infer to `null` and break the cast.
    # snapshot_date + timestamps stay VARCHAR and cast on insert (DATE / TIMESTAMPTZ).
    schema = pa.schema(
        [(c, pa.string()) for c in ("workspace_slug", "account_email", "snapshot_date",
                                    "domain", "_run_id", *_TS_FIELDS)]
        + [(c, pa.int64()) for c in _INT_FIELDS]
        + [("enable_slow_ramp", pa.bool_())]
        + [("api_synced_at", pa.timestamp("us", tz="UTC")),
           ("_loaded_at", pa.timestamp("us", tz="UTC"))]
    )
    tbl = pa.table({c: cols[c] for c in schema.names}, schema=schema)
    conn.register(_STG, tbl)
    try:
        conn.execute("BEGIN")
        conn.execute(_UPSERT_SELECT)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister(_STG)
    return n, warmup_filled


def run_account_settings(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    now = datetime.now(timezone.utc)
    snapshot_date = now.date().isoformat()

    total = 0
    total_warmup_filled = 0
    done: list[str] = []
    skipped: list[str] = []
    failures: list[dict] = []

    for slug in sorted(keys):
        try:
            with InstantlyClient(keys[slug]) as client:
                items = _fetch_workspace(client, slug)
            written, filled = _write_workspace(
                conn=ctx.db, slug=slug, items=items,
                snapshot_date=snapshot_date, now=now, run_id=ctx.run_id,
            )
            total += written
            total_warmup_filled += filled
            done.append(slug)
            if written == 0:
                logger.warning("%s: 0 accounts in /accounts sweep", slug)
        except InstantlyError as exc:
            # 402 Payment Required (dead/retired workspace plan — NOT a rate signal) or
            # 401 Unauthorized (rotated/invalid key) -> SKIP: nothing pullable, and failing
            # loud forever on a dead key would poison the phase. Anything else is real ->
            # isolate + fail loud at the end.
            msg = str(exc)
            if any(t in msg for t in (" -> 402", "402:", " -> 401", "401:")):
                code = "402 Payment Required" if "402" in msg else "401 Unauthorized"
                logger.warning("%s: %s (dead/unreachable workspace credential) -> skipped", slug, code)
                skipped.append(slug)
            else:
                logger.exception("%s: settings sweep failed", slug)
                failures.append({"slug": slug, "error": f"{type(exc).__name__}: {msg}"[:400]})
        except Exception as exc:  # noqa: BLE001 — per-workspace isolation
            logger.exception("%s: settings sweep failed", slug)
            failures.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"[:400]})

    notes = {
        "snapshot_date": snapshot_date,
        "workspaces_done": done,
        "workspaces_skipped_401_402": skipped,
        "failures": failures,
        "rows": total,
        # Acceptance metric (task #28 step 3): expected >=99%. Surfaced per-run so a fill
        # collapse (API shape change) is visible in sync_run notes, not just downstream.
        "warmup_start_fill_pct": round(100.0 * total_warmup_filled / total, 2) if total else None,
    }
    if failures:
        raise RuntimeError(
            f"instantly account_settings: {len(failures)} workspace(s) failed "
            f"({[f['slug'] for f in failures]}); healthy committed (rows={total}, "
            f"done={len(done)}, skipped_401_402={len(skipped)})."
        )
    return PhaseResult(rows_in=total, rows_out=total, notes=notes)


def main(argv: list[str] | None = None) -> int:
    """--smoke: read-only, network-only verification of the sweep against ONE workspace.
    Fetches up to --pages pages of /accounts, prints row count + field-fill summary.
    Touches NO database and takes NO lock — safe to run anywhere with the env keys."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", metavar="SLUG", required=True,
                        help="workspace slug (INSTANTLY_KEY_<SLUG> must resolve)")
    parser.add_argument("--pages", type=int, default=1, help="max pages of 100 (default 1)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from core.credentials import load_credentials

    keys = load_credentials().instantly_workspace_keys()
    key = keys.get(args.smoke)
    if not key:
        print(f"No INSTANTLY_KEY_* credential for slug '{args.smoke}'. Known: {sorted(keys)}")
        return 2

    cap = max(1, args.pages) * 100
    fill = {f: 0 for f in (*_INT_FIELDS, "enable_slow_ramp", *_TS_FIELDS)}
    n = 0
    with InstantlyClient(key) as client:
        for a in client.list_accounts():
            n += 1
            for f in fill:
                if a.get(f) is not None:
                    fill[f] += 1
            if n >= cap:
                break
    print(json.dumps({
        "workspace": args.smoke,
        "accounts_sampled": n,
        "field_fill": {f: (round(100.0 * c / n, 1) if n else None) for f, c in fill.items()},
    }, indent=2))
    return 0 if n > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
