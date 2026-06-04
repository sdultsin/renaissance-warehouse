"""One-off historical backfill of Sendivo per-campaign daily rollups (raw_sendivo_campaign_daily).

Runs INDEPENDENT of the nightly/primary DB: pulls /sms/logs day-by-day, rolls up in memory, writes
to a SEPARATE DuckDB file (no lock contention with the orchestrator or Lens). Resumable — re-running
skips days already complete. Merge into primary afterwards with scripts/merge_sendivo_backfill (or the
ATTACH+INSERT one-liner in the module docstring), outside the 03:30-05:45 UTC nightly window.

Usage (on the worker):
    python -m scripts.backfill_sendivo_logs --start 2026-04-11 --end 2026-06-03 \
        --db /root/core/sendivo_backfill.duckdb

Merge into primary later:
    duckdb /root/core/warehouse.duckdb "ATTACH '/root/core/sendivo_backfill.duckdb' AS bf (READ_ONLY);
      DELETE FROM raw_sendivo_campaign_daily WHERE _run_id LIKE 'backfill-%';
      INSERT INTO raw_sendivo_campaign_daily SELECT * FROM bf.raw_sendivo_campaign_daily;"
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import duckdb

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.credentials import load_credentials  # noqa: E402
from sources.sendivo import SendivoClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("backfill_sendivo_logs")

PER_PAGE = int(os.environ.get("SENDIVO_LOGS_PER_PAGE", "1000"))
PACE = float(os.environ.get("SENDIVO_LOGS_PACE", "0.4"))
MAX_PAGES = int(os.environ.get("SENDIVO_LOGS_MAX_PAGES", "8000"))

_DDL = """
CREATE TABLE IF NOT EXISTS raw_sendivo_campaign_daily (
    metric_date DATE, campaign_id BIGINT, campaign_name VARCHAR,
    sub_account_id BIGINT, sub_account_name VARCHAR, status_group VARCHAR,
    n_messages BIGINT, segments BIGINT, cost_usd DOUBLE,
    _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR);
"""


def _days(start: str, end: str) -> list[str]:
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end)
    out = []
    d = d0
    while d <= d1:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _fold(logs, agg):
    for x in logs:
        c = x.get("campaign") or {}
        key = ((x.get("created_at") or "")[:10], c.get("id"), c.get("name"),
               x.get("sub_account_id"), x.get("sub_account_name"), x.get("status_group_name"))
        a = agg[key]
        a[0] += 1
        a[1] += int(x.get("segments") or 0)
        try:
            a[2] += float(x.get("price_per_message") or 0)
        except (TypeError, ValueError):
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--db", default="/root/core/sendivo_backfill.duckdb")
    ap.add_argument("--no-skip", action="store_true", help="re-pull days even if already present")
    args = ap.parse_args()

    conn = duckdb.connect(args.db)
    conn.execute(_DDL)
    done = set()
    if not args.no_skip:
        done = {r[0].isoformat() for r in conn.execute(
            "SELECT DISTINCT metric_date FROM raw_sendivo_campaign_daily").fetchall()}

    api_key = load_credentials().require("SENDIVO_API_KEY")
    days = _days(args.start, args.end)
    log.info("backfill %s..%s = %d days (%d already done, skipping)", args.start, args.end, len(days), len(done))

    with SendivoClient(api_key) as cli:
        for i, day in enumerate(days, 1):
            if day in done:
                continue
            t0 = time.time()
            agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0])
            d = cli.sms_logs_page(day, 1, PER_PAGE)
            pg = d.get("pagination") or {}
            total = int(pg.get("total") or 0)
            last_page = min(int(pg.get("last_page") or 1), MAX_PAGES)
            _fold(d.get("logs") or [], agg)
            for page in range(2, last_page + 1):
                time.sleep(PACE)
                _fold((cli.sms_logs_page(day, page, PER_PAGE)).get("logs") or [], agg)

            run_id = f"backfill-{day}"
            conn.execute("DELETE FROM raw_sendivo_campaign_daily WHERE metric_date = ?", [day])
            recs = [(md, cid, cn, sid, sn, sg, n, seg, round(cost, 6), run_id)
                    for (md, cid, cn, sid, sn, sg), (n, seg, cost) in agg.items()]
            if recs:
                conn.executemany(
                    "INSERT INTO raw_sendivo_campaign_daily (metric_date, campaign_id, campaign_name, "
                    "sub_account_id, sub_account_name, status_group, n_messages, segments, cost_usd, "
                    "_loaded_at, _run_id) VALUES (?,?,?,?,?,?,?,?,?,now(),?)", recs)
            log.info("[%d/%d] %s: total=%d pages=%d groups=%d msgs=%d (%.0fs)",
                     i, len(days), day, total, last_page, len(recs), sum(r[6] for r in recs), time.time() - t0)
    conn.close()
    log.info("backfill complete -> %s", args.db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
