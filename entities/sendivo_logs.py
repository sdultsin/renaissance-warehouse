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


def _blast(x):
    """(blast_id, blast_name) from a /sms/logs row, robust to Sendivo's shape.

    Larry added blast_id to the message-logs API [2026-06-26] so each outbound
    record carries which blast/script it came from. We don't pin the exact key
    name: accept a nested `blast{id,name}`, top-level `blast_id`/`blast_name`, or
    a scalar `blast`. Absent -> (None, None) so this is a clean no-op on any row
    (or any historical day) where Sendivo doesn't populate it.
    """
    bid = x.get("blast_id")
    bname = x.get("blast_name")
    b = x.get("blast")
    if isinstance(b, dict):
        if bid is None:
            bid = b.get("id")
        if bname is None:
            bname = b.get("name")
    elif bid is None and b is not None and not isinstance(b, (list, tuple)):
        bid = b  # scalar blast id
    try:
        bid = int(bid) if bid is not None and str(bid).strip() != "" else None
    except (TypeError, ValueError):
        bid = None
    if bname is not None:
        bname = str(bname) or None
    return bid, bname


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

-- G1 (2026-06-14): the granular status/error breakdown the day-rollup above collapses away.
-- One row per (day, sub, campaign, status, status_group, status_name, error_description). Kept
-- separate from raw_sendivo_campaign_daily so the dashboard rollup stays lean (this table only
-- fans out on the handful of distinct failure reasons). `error_description` is the actionable
-- account-health signal ("End user out of prepay credit", "Account not provisioned ...").
CREATE TABLE IF NOT EXISTS raw_sendivo_failure_daily (
    metric_date DATE, sub_account_id BIGINT, sub_account_name VARCHAR,
    campaign_id BIGINT, campaign_name VARCHAR,
    status VARCHAR, status_group VARCHAR, status_name VARCHAR, error_description VARCHAR,
    n_messages BIGINT, segments BIGINT, cost_usd DOUBLE,
    _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR);

-- G3 (2026-06-14): intraday OUTBOUND grain for the send-by-hour view / best-send-time. Lean
-- (<= 24 x campaigns x subs rows/day). Reply-by-hour comes free from raw_sendivo_inbound.received_at.
CREATE TABLE IF NOT EXISTS raw_sendivo_hourly (
    metric_date DATE, hour_utc INTEGER, sub_account_id BIGINT, sub_account_name VARCHAR,
    campaign_id BIGINT, campaign_name VARCHAR,
    n_messages BIGINT, delivered_messages BIGINT, segments BIGINT,
    _loaded_at TIMESTAMPTZ NOT NULL, _run_id VARCHAR);

-- G4 (2026-06-26): per-BLAST send breakdown — the finer-than-campaign granularity Larry
-- added to /sms/logs (blast_id/blast_name). One row per (day, sub, campaign, blast, status).
-- Kept SEPARATE from raw_sendivo_campaign_daily (same lean-rollup discipline as G1/G3) so the
-- main campaign rollup + v_sms_campaign_performance grain are UNTOUCHED — the blast view layers
-- on top. This is what answers "which scripts/blasts are landing" on the SEND side; reply-side
-- blast attribution comes from comms.sendivo_outbound_recovered.blast_id joined to the reply.
CREATE TABLE IF NOT EXISTS raw_sendivo_blast_daily (
    metric_date DATE, sub_account_id BIGINT, sub_account_name VARCHAR,
    campaign_id BIGINT, campaign_name VARCHAR,
    blast_id BIGINT, blast_name VARCHAR, status_group VARCHAR,
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
        self.blast_seen = 0  # captured rows carrying a non-null blast_id (field-live signal)
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
                bid, bname = _blast(r)
                if bid is not None:
                    self.blast_seen += 1
                self.buf.append((
                    str(r.get("id")), p10, r.get("to_number"), r.get("from_number"), body,
                    r.get("created_at"), (r.get("campaign") or {}).get("name"),
                    r.get("sub_account_name"), bid, bname,
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
                    "sent_at, campaign_name, sub_account_name, blast_id, blast_name) "
                    "values %s on conflict (sendivo_log_id) do update set "
                    "  blast_id = coalesce(comms.sendivo_outbound_recovered.blast_id, excluded.blast_id), "
                    "  blast_name = coalesce(comms.sendivo_outbound_recovered.blast_name, excluded.blast_name) "
                    "where comms.sendivo_outbound_recovered.blast_id is null "
                    "  and excluded.blast_id is not null",
                    self.buf,
                )
                n = cur.rowcount
            self.conn.commit()
            self.inserted += n
            day_blast = sum(1 for row in self.buf if row[8] is not None)  # row[8] = blast_id
            logger.info("body-capture %s: matched %d, inserted %d, blast=%d", day, len(self.buf), n, day_blast)
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
    # Explicit-day override (process env, set by the E1/E2 watchdog's --heal re-pull or a manual
    # backfill): pull exactly these days instead of the trailing window. e.g. "2026-06-11,2026-06-13".
    explicit = os.environ.get("SENDIVO_LOGS_TARGET_DAYS")
    if explicit:
        return [d.strip() for d in explicit.split(",") if d.strip()]
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
            # All three aggregators are folded in the SAME single pass over the day's pages.
            agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0])       # main daily rollup
            fail_agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0])  # G1 status/error detail
            hour_agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0])    # G3 [n, delivered, seg]
            blast_agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0]) # G4 per-blast [n, seg, cost]

            def fold(logs):
                for x in logs:
                    c = x.get("campaign") or {}
                    cid, cname = c.get("id"), c.get("name")
                    said, saname = x.get("sub_account_id"), x.get("sub_account_name")
                    created = x.get("created_at") or ""
                    day_s = created[:10]
                    sg = x.get("status_group_name")
                    seg = int(x.get("segments") or 0)
                    try:
                        price = float(x.get("price_per_message") or 0)
                    except (TypeError, ValueError):
                        price = 0.0
                    # 1) main daily rollup (grain UNCHANGED — keeps v_sms_campaign_performance lean)
                    a = agg[(day_s, cid, cname, said, saname, sg)]
                    a[0] += 1; a[1] += seg; a[2] += price
                    # 2) G1 status/error detail
                    fa = fail_agg[(day_s, said, saname, cid, cname,
                                   x.get("status"), sg, x.get("status_name"), x.get("error_description"))]
                    fa[0] += 1; fa[1] += seg; fa[2] += price
                    # 3) G3 hourly outbound (UTC hour from created_at)
                    try:
                        hour = int(created[11:13]) if len(created) >= 13 else None
                    except ValueError:
                        hour = None
                    h = hour_agg[(day_s, hour, said, saname, cid, cname)]
                    h[0] += 1
                    if sg == "DELIVERED":
                        h[1] += 1
                    h[2] += seg
                    # 4) G4 per-blast send breakdown (blast_id added to /sms/logs 2026-06-26).
                    #    Folds in the SAME pass; absent blast -> (None, None) bucket = a harmless
                    #    no-op until Sendivo populates it (then it self-fills going forward).
                    bid, bname = _blast(x)
                    ba = blast_agg[(day_s, said, saname, cid, cname, bid, bname, sg)]
                    ba[0] += 1; ba[1] += seg; ba[2] += price

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

            # G1 — status/error detail
            conn.execute(
                "DELETE FROM raw_sendivo_failure_daily WHERE metric_date = ? AND _run_id = ?",
                [day, run_id],
            )
            frecs = [
                (md, said, saname, cid, cname, st, sg, sn, err, n, seg, round(cost, 6), run_id)
                for (md, said, saname, cid, cname, st, sg, sn, err), (n, seg, cost) in fail_agg.items()
            ]
            if frecs:
                conn.executemany(
                    "INSERT INTO raw_sendivo_failure_daily (metric_date, sub_account_id, sub_account_name, "
                    "campaign_id, campaign_name, status, status_group, status_name, error_description, "
                    "n_messages, segments, cost_usd, _loaded_at, _run_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                    frecs,
                )

            # G3 — hourly outbound
            conn.execute(
                "DELETE FROM raw_sendivo_hourly WHERE metric_date = ? AND _run_id = ?",
                [day, run_id],
            )
            hrecs = [
                (md, hr, said, saname, cid, cname, n, deliv, seg, run_id)
                for (md, hr, said, saname, cid, cname), (n, deliv, seg) in hour_agg.items()
            ]
            if hrecs:
                conn.executemany(
                    "INSERT INTO raw_sendivo_hourly (metric_date, hour_utc, sub_account_id, sub_account_name, "
                    "campaign_id, campaign_name, n_messages, delivered_messages, segments, _loaded_at, _run_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                    hrecs,
                )

            # G4 — per-blast send breakdown
            conn.execute(
                "DELETE FROM raw_sendivo_blast_daily WHERE metric_date = ? AND _run_id = ?",
                [day, run_id],
            )
            brecs = [
                (md, said, saname, cid, cname, bid, bname, sg, n, seg, round(cost, 6), run_id)
                for (md, said, saname, cid, cname, bid, bname, sg), (n, seg, cost) in blast_agg.items()
            ]
            if brecs:
                conn.executemany(
                    "INSERT INTO raw_sendivo_blast_daily (metric_date, sub_account_id, sub_account_name, "
                    "campaign_id, campaign_name, blast_id, blast_name, status_group, "
                    "n_messages, segments, cost_usd, _loaded_at, _run_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)",
                    brecs,
                )

            msgs = sum(r[6] for r in recs)
            # E1/E2 reconciliation: committed vs the API's full-table count. delta!=0 => silent
            # drop (a mid-run page failure under-counted). The watchdog reads these notes + the
            # committed sum and re-pulls/alerts; surfacing here also lands it in the nightly log.
            delta = (total - msgs) if isinstance(total, int) else None
            per[day] = {"pages": last_page, "api_total": total, "groups": len(recs), "msgs": msgs,
                        "delta": delta, "reconciled": (delta == 0)}
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
        notes["_blast_id_seen"] = capture.blast_seen  # >0 confirms Larry's blast_id is live
    return PhaseResult(rows_in=rows_out, rows_out=rows_out, notes=notes)
