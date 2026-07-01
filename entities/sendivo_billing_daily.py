"""Daily-report §2 centralization — Sendivo billing report at DAY grain.

Mirrors EXACTLY what scripts/render_daily.py live-pulls for §2 "SMS Sent"
(PROVENANCE-MAP §2, 2026-07-01): GET /billing/report with start=end=<day>,
one row per (day, sub_account) into raw_sendivo_billing_daily.

  * sms_fee_qty == billing_report.sms_fees.quantity — the calibrated §2 "sent"
    truth (NOT delivery_metrics, which is wrong per the provenance map).
    Validated 2026-06-30: sub 14603 (Ren3 webform) qty == 94,796 == the report.
  * The fee columns (sms/carrier/setup/renewal/brand $) double as the SMS
    cost/day feed (provenance map §5 cost-per-meeting: per-day actual $).

The existing raw_sendivo_billing (month-window grain, entities/sendivo.py) is
UNTOUCHED — this is a new additive day-grain table; the renderer keeps
live-pulling until a separate flip decision.

Nightly cost: one API call per day in the re-pull window
(SENDIVO_BILLING_DAILY_DAYS back, default 7 — matches SENDIVO_BACKFILL_DAYS'
delivery-metrics convention; billing for a closed day is stable, the overlap
just self-heals late corrections). Day-scoped upserts, never a history pull.

Fault tolerance: per-day isolation — one day failing cannot drop the others;
a failed day is loudly logged and re-raised at the end AFTER the healthy days
committed (phase 'failed', run 'partial' — never a silent gap).

Backfill (one-off, NOT on the nightly path; takes the writer flock itself):
    python -m entities.sendivo_billing_daily --start 2026-06-01 --end 2026-07-01
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.sendivo import SendivoClient

logger = logging.getLogger("entities.sendivo_billing_daily")

WINDOW_DAYS = int(os.environ.get("SENDIVO_BILLING_DAILY_DAYS", "7"))

_COLS = [
    "metric_date", "sub_account_id", "location_id", "total_spend",
    "sms_fee_qty", "sms_fee_usd", "carrier_fee_qty", "carrier_fee_usd",
    "campaign_setup_usd", "campaign_renewal_usd", "brand_fee_usd",
    "phone_setup_usd", "phone_renewal_usd", "raw_json", "_loaded_at", "_run_id",
]
_UPSERT = (
    f"INSERT INTO raw_sendivo_billing_daily ({', '.join(_COLS)}) "
    f"VALUES ({', '.join('?' for _ in _COLS)}) "
    "ON CONFLICT (metric_date, sub_account_id) DO UPDATE SET "
    + ", ".join(f"{c} = excluded.{c}" for c in _COLS if c not in ("metric_date", "sub_account_id"))
)


def register(registry: Registry) -> None:
    registry.add_phase("sendivo", "billing_daily", run_billing_daily)


def _fee(rec: dict, key: str) -> tuple:
    f = rec.get(key) or {}
    if not isinstance(f, dict):
        return None, None
    return f.get("quantity"), f.get("total_spend")


def _day_rows(cli: SendivoClient, day: str, now, run_id: str) -> list[list]:
    rows = []
    for r in cli.billing_report(day, day):
        sub = r.get("sub_account_id")
        if sub is None:
            logger.warning("billing %s: record without sub_account_id skipped: %s",
                           day, json.dumps(r)[:200])
            continue
        sms_q, sms_u = _fee(r, "sms_fees")
        car_q, car_u = _fee(r, "carrier_fees")
        rows.append([
            day, int(sub), r.get("locationID"), r.get("total_spend"),
            sms_q, sms_u, car_q, car_u,
            (r.get("campaign_setup_fees") or {}).get("total_spend"),
            (r.get("campaign_renewal_fees") or {}).get("total_spend"),
            (r.get("brand_fees") or {}).get("total_spend"),
            (r.get("phone_number_setup_fees") or {}).get("total_spend"),
            (r.get("phone_number_renewal_fees") or {}).get("total_spend"),
            json.dumps(r), now, run_id,
        ])
    return rows


def _ingest(conn, api_key: str, run_id: str, start: str, end: str) -> PhaseResult:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    if d1 < d0:
        raise ValueError(f"end {end} before start {start}")
    now = datetime.now(timezone.utc)
    total = 0
    days_ok: list[str] = []
    days_empty: list[str] = []
    failures: list[dict] = []
    with SendivoClient(api_key) as cli:
        d = d0
        while d <= d1:
            day = d.isoformat()
            d += timedelta(days=1)
            try:
                rows = _day_rows(cli, day, now, run_id)
            except Exception as exc:  # noqa: BLE001 — per-day isolation
                err = f"{type(exc).__name__}: {exc}"[:300]
                logger.error("billing_daily %s FAILED: %s", day, err)
                failures.append({"day": day, "error": err})
                continue
            if not rows:
                # An empty day is loudly visible but not fatal: the API returns []
                # for days with zero billable activity. Systemic staleness is caught
                # by the sync_registry biz-date SLA on metric_date.
                logger.warning("billing_daily %s: 0 billing records", day)
                days_empty.append(day)
                continue
            conn.execute("BEGIN")
            try:
                for row in rows:
                    conn.execute(_UPSERT, row)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            total += len(rows)
            days_ok.append(day)
            time.sleep(0.15)

    notes = {"window": f"{start}..{end}", "days_ok": len(days_ok),
             "days_empty": days_empty, "failures": failures, "rows": total}
    if failures:
        # Fail LOUD after the healthy days committed (phase 'failed', run 'partial').
        raise RuntimeError(
            f"sendivo billing_daily: {len(failures)} day(s) failed in {start}..{end}: "
            f"{[f['day'] for f in failures]}; {total} rows for the healthy days committed."
        )
    return PhaseResult(rows_in=total, rows_out=total, notes=notes)


def run_billing_daily(ctx: RunContext) -> PhaseResult:
    api_key = ctx.credentials.require("SENDIVO_API_KEY")
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=WINDOW_DAYS - 1)).isoformat()
    return _ingest(ctx.db, api_key, ctx.run_id, start, today.isoformat())


def main(argv: list[str] | None = None) -> int:
    """One-off scoped backfill (NOT the nightly path). Opens its own writer
    connection — core.db.connect() acquires the box flock (acquire-or-wait)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    from core import db as db_module
    from core.credentials import load_credentials

    run_id = f"backfill_sendivo_billing_daily_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    conn = db_module.connect()
    try:
        result = _ingest(conn, load_credentials().require("SENDIVO_API_KEY"),
                         run_id, args.start, args.end)
        print(json.dumps(result.notes, indent=2))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
