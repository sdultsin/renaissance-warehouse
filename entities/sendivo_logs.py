"""Sendivo /sms/logs → per-campaign daily rollup (spec 14 granular addendum).

Phase 'sendivo', ingest 'sms_logs'. This is the per-campaign OUTBOUND funnel the agency-aggregate
`/delivery-metrics` (entities/sendivo.py) cannot give. `/sms/logs` is the only Sendivo source with
per-message `campaign{id,name}` + status_group + segments + price.

Mechanics (validated 2026-06-03 against the live key):
  - day-granular date filter (time-of-day ignored); ~500-630k rows/day; per_page=1000 → ~630 pages/day.
  - intermittent timeouts → SendivoClient.sms_logs_page has retry+backoff; we pace between pages.
We pull each target day fully and AGGREGATE in memory to (campaign, sub_account, day, status_group);
we never store the raw rows. Re-pulling a day appends a fresh run; v_sms_campaign_performance keeps
the latest run per metric_date.

Window: incremental nightly pulls yesterday (today is incomplete). SENDIVO_LOGS_BACKFILL_DAYS for history.
"""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources.sendivo import SendivoClient

logger = logging.getLogger("entities.sendivo_logs")

BACKFILL_DAYS = int(os.environ.get("SENDIVO_LOGS_BACKFILL_DAYS", "1"))
PER_PAGE = int(os.environ.get("SENDIVO_LOGS_PER_PAGE", "1000"))
PAGE_PACE_S = float(os.environ.get("SENDIVO_LOGS_PACE", "1.2"))
MAX_PAGES = int(os.environ.get("SENDIVO_LOGS_MAX_PAGES", "8000"))  # safety cap (~8M msgs/day)

_DDL = """
CREATE TABLE IF NOT EXISTS raw_sendivo_campaign_daily (
    metric_date DATE, campaign_id BIGINT, campaign_name VARCHAR,
    sub_account_id BIGINT, sub_account_name VARCHAR, status_group VARCHAR,
    n_messages BIGINT, segments BIGINT, cost_usd DOUBLE,
    _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR);
"""


def register(registry: Registry) -> None:
    registry.add_phase("sendivo", "sms_logs", run_sms_logs)


def _target_days(today) -> list[str]:
    return [(today - timedelta(days=i)).isoformat() for i in range(BACKFILL_DAYS, 0, -1)]


def run_sms_logs(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id
    api_key = ctx.credentials.require("SENDIVO_API_KEY")
    today = datetime.now(timezone.utc).date()
    conn.execute(_DDL)

    per: dict[str, dict] = {}
    rows_out = 0
    with SendivoClient(api_key) as cli:
        for day in _target_days(today):
            # key -> [n_messages, segments, cost]
            agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0])

            def fold(logs):
                for x in logs:
                    c = x.get("campaign") or {}
                    key = (
                        (x.get("created_at") or "")[:10],
                        c.get("id"), c.get("name"),
                        x.get("sub_account_id"), x.get("sub_account_name"),
                        x.get("status_group_name"),
                    )
                    a = agg[key]
                    a[0] += 1
                    a[1] += int(x.get("segments") or 0)
                    try:
                        a[2] += float(x.get("price_per_message") or 0)
                    except (TypeError, ValueError):
                        pass

            d = cli.sms_logs_page(day, 1, PER_PAGE)
            pg = d.get("pagination") or {}
            total = pg.get("total")
            last_page = min(int(pg.get("last_page") or 1), MAX_PAGES)
            fold(d.get("logs") or [])
            for page in range(2, last_page + 1):
                time.sleep(PAGE_PACE_S)
                fold((cli.sms_logs_page(day, page, PER_PAGE)).get("logs") or [])

            conn.execute(
                "DELETE FROM raw_sendivo_campaign_daily WHERE metric_date = ? AND _run_id = ?",
                [day, run_id],
            )
            recs = [
                (md, cid, cname, said, saname, sg, n, seg, round(cost, 6), run_id)
                for (md, cid, cname, said, saname, sg), (n, seg, cost) in agg.items()
            ]
            if recs:
                conn.executemany(
                    "INSERT INTO raw_sendivo_campaign_daily (metric_date, campaign_id, campaign_name, "
                    "sub_account_id, sub_account_name, status_group, n_messages, segments, cost_usd, "
                    "_loaded_at, _run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                    recs,
                )
            msgs = sum(r[6] for r in recs)
            per[day] = {"pages": last_page, "api_total": total, "groups": len(recs), "msgs": msgs}
            rows_out += len(recs)
            logger.info("sms_logs %s: %s", day, per[day])

    return PhaseResult(rows_in=rows_out, rows_out=rows_out, notes=per)
