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


class _BodyCapture:
    """Side-channel (2026-06-09 blast-body reconciliation): while this entity is
    paging every day's /sms/logs for the rollup anyway, also capture the raw
    blast BODIES for phones we track in comms (conversation.prospect_number) and
    upsert them into comms.sendivo_outbound_recovered (worker migration 010).
    That table is UNIONed into comms.v_sendivo_outbound_message, which the
    comms-orchestration worker resolves Sendivo original_message from — so this
    keeps fill at ~100% even when Sendivo's outbound-status webhook rots.

    HARD RULE: capture failures must NEVER fail the rollup — every public method
    swallows its own errors and just disables itself.
    Gated by SENDIVO_BODY_CAPTURE=1 — read through ctx.credentials (the
    orchestrator loads .env via dotenv_values WITHOUT exporting to os.environ,
    so a plain os.environ gate would silently never fire).
    """

    def __init__(self, ctx) -> None:
        flag = None
        try:
            flag = ctx.credentials.optional("SENDIVO_BODY_CAPTURE")
        except Exception:  # noqa: BLE001
            pass
        self.enabled = (flag or os.environ.get("SENDIVO_BODY_CAPTURE") or "0") == "1"
        self.was_enabled = self.enabled
        self.conn = None
        self.targets: set | None = None
        self.buf: list = []
        self.inserted = 0
        if not self.enabled:
            return
        try:
            import psycopg2  # droplet venv has it (comms mirror dependency)

            try:
                url = ctx.credentials.require("COMMS_SUPABASE_DB_URL")
            except Exception:  # noqa: BLE001
                url = os.environ["COMMS_SUPABASE_DB_URL"]
            self.conn = psycopg2.connect(url)
            with self.conn.cursor() as cur:
                cur.execute(
                    "select distinct right(regexp_replace(prospect_number, '[^0-9]', '', 'g'), 10) "
                    "from comms.conversation where prospect_number is not null"
                )
                self.targets = {r[0] for r in cur.fetchall() if r[0] and len(r[0]) == 10}
            logger.info("body-capture ON: %d target phone10s", len(self.targets))
        except Exception as exc:  # noqa: BLE001
            logger.warning("body-capture disabled (init failed, rollup unaffected): %s", exc)
            self.enabled = False

    def collect(self, logs) -> None:
        if not self.enabled:
            return
        try:
            for r in logs:
                body = (r.get("message_content") or "").strip()
                if not body or body.lower().startswith("you have successfully unsubscribed"):
                    continue  # empty / unsubscribe echo — never a blast body
                p10 = "".join(c for c in (r.get("to_number") or "") if c.isdigit())[-10:]
                if len(p10) != 10 or p10 not in self.targets:
                    continue
                self.buf.append((
                    str(r.get("id")), p10, r.get("to_number"), r.get("from_number"), body,
                    r.get("created_at"), (r.get("campaign") or {}).get("name"),
                    r.get("sub_account_name"),
                ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("body-capture collect failed (disabling, rollup unaffected): %s", exc)
            self.enabled = False

    def flush_day(self, day: str) -> None:
        if not self.enabled or not self.buf:
            self.buf = []
            return
        try:
            import psycopg2.extras

            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "insert into comms.sendivo_outbound_recovered "
                    "(sendivo_log_id, phone10, to_number, from_number, message_content, "
                    "sent_at, campaign_name, sub_account_name) "
                    "values %s on conflict (sendivo_log_id) do nothing",
                    self.buf,
                )
                n = cur.rowcount
            self.conn.commit()
            self.inserted += n
            logger.info("body-capture %s: matched %d, inserted %d", day, len(self.buf), n)
        except Exception as exc:  # noqa: BLE001
            logger.warning("body-capture flush %s failed (disabling, rollup unaffected): %s", day, exc)
            try:
                self.conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            self.enabled = False
        self.buf = []

    def close(self) -> None:
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:  # noqa: BLE001
            pass


def _target_days(today) -> list[str]:
    return [(today - timedelta(days=i)).isoformat() for i in range(BACKFILL_DAYS, 0, -1)]


def run_sms_logs(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id
    api_key = ctx.credentials.require("SENDIVO_API_KEY")
    today = datetime.now(timezone.utc).date()
    conn.execute(_DDL)

    per: dict[str, dict] = {}
    failures: dict[str, str] = {}
    rows_out = 0
    capture = _BodyCapture(ctx)
    with SendivoClient(api_key) as cli:
        for day in _target_days(today):
          # Per-day isolation: a flaky-endpoint failure on one day must NOT raise — that would
          # mark the whole ingest failed and block nightly.sh's dashboard/serving publish.
          try:
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
            capture.collect(d.get("logs") or [])
            for page in range(2, last_page + 1):
                time.sleep(PAGE_PACE_S)
                logs = (cli.sms_logs_page(day, page, PER_PAGE)).get("logs") or []
                fold(logs)
                capture.collect(logs)
            capture.flush_day(day)

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
          except Exception as exc:  # noqa: BLE001 — never let one flaky day fail the whole nightly
            failures[day] = str(exc)[:200]
            logger.warning("sms_logs %s FAILED (skipped, non-fatal): %s", day, exc)

    capture.close()
    notes = dict(per)
    if failures:
        notes["_failed_days"] = failures
    if capture.was_enabled:
        notes["_body_capture_inserted"] = capture.inserted
    return PhaseResult(rows_in=rows_out, rows_out=rows_out, notes=notes)
