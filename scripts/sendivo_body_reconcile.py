#!/usr/bin/env python3
"""Sendivo blast-body reconciliation — recover "original message" bodies from the
/sms/logs API, independent of the flaky sendivo_outbound_status webhook.

WHY: comms captures Sendivo outbound bodies only from the webhook
(comms.v_sendivo_outbound_message). Sendivo only began populating the body
~2026-06-07, the webhook sometimes never arrives or arrives bodyless, and fill
decays (June-9: 86.5%). 493 in-Close sendivo opps had original_message NULL.

WHAT: pages GET /sms/logs day by day (the only server-side filter that works is
the day-granular date range — everything else is ignored), client-filters to
rows whose right-10(to_number) is a tracked comms.conversation.prospect_number
(~80k phones; keeps the table lean vs ~500-630k fanout sends/day), and upserts
into comms.sendivo_outbound_recovered (migration 010; UNIQUE sendivo_log_id →
idempotent). Migration 010 UNIONs that table into v_sendivo_outbound_message,
so the deployed worker's Stage-C re-attempt loop resolves the recovered bodies
into Close with ZERO worker changes.

MODES
  --backfill            historical run: --days "D1,D2" explicit, or --auto
                        (today → back-walk; stops at the retention boundary =
                        first empty day at/under --auto-stop-after empty days)
  --requeue             un-tombstone sendivo opps that NOW have a recovered
                        blast, then drive the worker /scheduled-call-pipeline
                        (wide reattemptWindowHours) to convergence.

OPS NOTES (proven API quirks)
  * per_page=1000; ~500-630 pages on big days; intermittent timeouts; 403 =
    rate-block → hard backoff (45s * attempt). Pace between pages.
  * API pulls PAUSE during 03:20-05:50 UTC: the nightly sendivo_logs warehouse
    entity pages the same endpoint then — double-hammering triggers 403s.
  * Checkpoint (day+page) → kill-safe resume. Writes go to comms Supabase
    (NOT the warehouse) so there is no DuckDB single-writer constraint.

ENV (from --env-file, default ./.env): SENDIVO_API_KEY, COMMS_SUPABASE_DB_URL,
WORKER_SHARED_SECRET (requeue only), CC_SLACK_BOT_TOKEN (optional, completion ping).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

API = "https://app.sendivo.io/api/v1/sms/logs"
WORKER_PIPELINE = "https://comms-orchestration.renaissance-84e.workers.dev/scheduled-call-pipeline"
SLACK_CHANNEL = "C0AR0EA21C1"  # cc-sam
PER_PAGE = 1000
PAGE_PACE_S = 1.5
PAUSE_WINDOW = (dt.time(3, 20), dt.time(5, 50))  # UTC — nightly sendivo_logs entity owns the API


def log(msg: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M:%S}Z] {msg}", flush=True)


def load_env(path: str) -> None:
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def pause_if_nightly_window() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    if PAUSE_WINDOW[0] <= now.time() < PAUSE_WINDOW[1]:
        resume = now.replace(hour=PAUSE_WINDOW[1].hour, minute=PAUSE_WINDOW[1].minute, second=0)
        wait = (resume - now).total_seconds()
        log(f"in nightly sendivo_logs window — pausing API pulls {wait/60:.0f} min until 05:50 UTC")
        time.sleep(max(wait, 0) + 5)


def get_page(day: str, page: int, tries: int = 7):
    """One /sms/logs page. Returns the `data` dict {logs, pagination} or None on hard failure."""
    q = urllib.parse.urlencode({"start_date": day, "end_date": day, "per_page": PER_PAGE, "page": page})
    req = urllib.request.Request(f"{API}?{q}", headers={"Authorization": "Bearer " + os.environ["SENDIVO_API_KEY"]})
    for a in range(tries):
        pause_if_nightly_window()
        try:
            return json.load(urllib.request.urlopen(req, timeout=90)).get("data") or {}
        except urllib.error.HTTPError as e:
            if e.code == 403:  # rate/abuse block — back off HARD
                wait = 45 * (a + 1)
                log(f"  {day} p{page}: 403 rate-block, backing off {wait}s")
                time.sleep(wait)
                continue
            if a < tries - 1:
                time.sleep(4 * (a + 1))
                continue
            log(f"  {day} p{page}: FAILED {e}")
            return None
        except Exception as e:  # noqa: BLE001 — timeouts, resets, bad JSON
            if a < tries - 1:
                time.sleep(4 * (a + 1))
                continue
            log(f"  {day} p{page}: FAILED {e}")
            return None
    return None


def pg():
    return psycopg2.connect(os.environ["COMMS_SUPABASE_DB_URL"])


def load_targets(conn) -> set[str]:
    """phone10s of every tracked conversation — the only sends worth keeping."""
    with conn.cursor() as cur:
        cur.execute(
            "select distinct right(regexp_replace(prospect_number, '[^0-9]', '', 'g'), 10) "
            "from comms.conversation where prospect_number is not null"
        )
        targets = {r[0] for r in cur.fetchall() if r[0] and len(r[0]) == 10}
    log(f"target phone set: {len(targets)} phone10s from comms.conversation")
    return targets


def upsert(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "insert into comms.sendivo_outbound_recovered "
            "(sendivo_log_id, phone10, to_number, from_number, message_content, sent_at, campaign_name, sub_account_name) "
            "values %s on conflict (sendivo_log_id) do nothing",
            rows,
        )
        n = cur.rowcount
    conn.commit()
    return n


def match_rows(logs: list[dict], targets: set[str]) -> list[tuple]:
    out = []
    for r in logs:
        body = (r.get("message_content") or "").strip()
        if not body:
            continue
        p10 = "".join(c for c in (r.get("to_number") or "") if c.isdigit())[-10:]
        if len(p10) != 10 or p10 not in targets:
            continue
        out.append((
            str(r.get("id")), p10, r.get("to_number"), r.get("from_number"), body,
            r.get("created_at"), (r.get("campaign") or {}).get("name"), r.get("sub_account_name"),
        ))
    return out


# ---------------------------------------------------------------- checkpoint

def ck_load(path: str) -> dict:
    try:
        return json.load(open(path))
    except Exception:
        return {"done_days": [], "empty_days": [], "current": None, "inserted_total": 0}


def ck_save(path: str, ck: dict) -> None:
    tmp = path + ".tmp"
    json.dump(ck, open(tmp, "w"))
    os.replace(tmp, path)


# ------------------------------------------------------------------ backfill

def scrape_day(conn, targets: set[str], day: str, ck: dict, ckpath: str) -> str:
    """Returns 'done' | 'empty' | 'failed'. Resumes from checkpointed page."""
    start_page = 1
    cur = ck.get("current")
    if cur and cur.get("day") == day:
        start_page = int(cur.get("page", 1))

    first = get_page(day, start_page)
    if first is None:
        return "failed"
    pgn = first.get("pagination") or {}
    last_page = int(pgn.get("last_page") or 1)
    total = pgn.get("total")
    if not (first.get("logs") or []):
        log(f"{day}: EMPTY (total={total}) — retention boundary candidate")
        return "empty"
    log(f"{day}: total={total} pages={last_page} (resuming at p{start_page})")

    t0 = time.time()
    matched = inserted = 0
    page = start_page
    d = first
    while True:
        rows = match_rows(d.get("logs") or [], targets)
        matched += len(rows)
        inserted += upsert(conn, rows)
        ck["current"] = {"day": day, "page": page + 1}
        ck["inserted_total"] = ck.get("inserted_total", 0) + len(rows)
        ck_save(ckpath, ck)
        if page % 25 == 0 or page == last_page:
            rate = (page - start_page + 1) / max(time.time() - t0, 1)
            eta = (last_page - page) / max(rate, 0.001) / 60
            log(f"  {day} p{page}/{last_page} matched={matched} inserted={inserted} "
                f"({rate*60:.1f} pages/min, day-ETA {eta:.0f}m)")
        if page >= last_page:
            break
        page += 1
        time.sleep(PAGE_PACE_S)
        d = get_page(day, page)
        if d is None:
            log(f"  {day}: hard page failure at p{page} — will retry day on next run")
            return "failed"
    log(f"{day}: DONE matched={matched} inserted={inserted}")
    return "done"


def run_backfill(args) -> None:
    ckpath = args.checkpoint
    ck = ck_load(ckpath)
    conn = pg()
    targets = load_targets(conn)

    if args.days:
        days = [d.strip() for d in args.days.split(",") if d.strip()]
    else:  # --auto: today back to --min-day; stop after consecutive empty days
        today = dt.datetime.now(dt.timezone.utc).date()
        lo = dt.date.fromisoformat(args.min_day)
        days = [(today - dt.timedelta(days=i)).isoformat() for i in range((today - lo).days + 1)]

    consec_empty = consec_failed = 0
    had_failures = False
    for day in days:
        if day in ck["done_days"] or day in ck["empty_days"]:
            continue
        status = scrape_day(conn, targets, day, ck, ckpath)
        if status == "done":
            ck["done_days"].append(day)
            ck["current"] = None
            consec_empty = consec_failed = 0
        elif status == "empty":
            ck["empty_days"].append(day)
            ck["current"] = None
            consec_failed = 0
            consec_empty += 1
            if not args.days and consec_empty >= args.auto_stop_after:
                log(f"{consec_empty} consecutive empty days — retention boundary reached, stopping back-walk")
                ck_save(ckpath, ck)
                break
        else:  # failed — leave checkpoint, retry on next invocation
            had_failures = True
            consec_failed += 1
            log(f"{day}: leaving incomplete (resume point saved)")
            if consec_failed >= 2:
                # Two day-level hard failures in a row = the API is globally
                # rate-blocking us. Burning the rest of the range is pointless —
                # exit and let the watchdog relaunch after the block clears.
                log("2 consecutive day-level failures — likely a global rate-block; aborting this pass")
                ck_save(ckpath, ck)
                break
        ck_save(ckpath, ck)

    log(f"backfill pass complete: done={len(ck['done_days'])} empty={len(ck['empty_days'])} "
        f"inserted_total={ck.get('inserted_total', 0)} had_failures={had_failures}")
    if not had_failures:
        # Clean pass: every requested day is done/empty (boundary or list
        # exhausted) → completion marker. The watchdog stops relaunching on this.
        open(ckpath + ".done", "w").write(dt.datetime.now(dt.timezone.utc).isoformat())
        log("wrote completion marker " + ckpath + ".done")
    conn.close()


# ------------------------------------------------------------------- requeue

REMAINING_SQL = """
select count(*) from comms.call_opportunity o
 where o.source = 'sendivo' and o.close_lead_id is not null
   and o.original_message is null and o.enrich_exhausted_at is null
   and exists (select 1 from comms.sendivo_outbound_recovered r
                where r.phone10 = right(regexp_replace(coalesce(o.phone_e164,''), '[^0-9]', '', 'g'), 10))
"""


def run_requeue(args) -> None:
    conn = pg()
    with conn.cursor() as cur:
        # Un-tombstone: rows retired as "provably unrecoverable" that NOW have a
        # recovered blast. Pure un-retirement — fabricates nothing; the worker
        # re-derives original/response from real rows.
        cur.execute("""
            update comms.call_opportunity o
               set enrich_exhausted_at = null
             where o.source = 'sendivo' and o.close_lead_id is not null
               and o.original_message is null and o.enrich_exhausted_at is not null
               and exists (select 1 from comms.sendivo_outbound_recovered r
                            where r.phone10 = right(regexp_replace(coalesce(o.phone_e164,''), '[^0-9]', '', 'g'), 10))
        """)
        untombed = cur.rowcount
    conn.commit()
    log(f"un-tombstoned {untombed} opps that now have a recovered blast")

    with conn.cursor() as cur:
        cur.execute(REMAINING_SQL)
        remaining = cur.fetchone()[0]
    log(f"eligible (recovered-blast, original NULL, not exhausted): {remaining}")

    secret = os.environ.get("WORKER_SHARED_SECRET")
    if not secret:
        log("WORKER_SHARED_SECRET not set — skipping worker kicks (pg_cron will drain within its window)")
        conn.close()
        return

    url = f"{WORKER_PIPELINE}?reattemptWindowHours={args.window_hours}"
    stall = 0
    for i in range(args.max_kicks):
        if remaining == 0:
            break
        req = urllib.request.Request(url, data=b"{}", method="POST", headers={
            "Authorization": "Bearer " + secret, "Content-Type": "application/json"})
        try:
            resp = urllib.request.urlopen(req, timeout=120)
            body = resp.read()[:200].decode(errors="replace")
            log(f"kick {i+1}: HTTP {resp.status} {body}")
        except Exception as e:  # noqa: BLE001
            log(f"kick {i+1}: ERROR {e}")
        time.sleep(args.kick_pause)
        with conn.cursor() as cur:
            cur.execute(REMAINING_SQL)
            now_remaining = cur.fetchone()[0]
        if now_remaining >= remaining:
            stall += 1
            if stall >= 3:
                log(f"no progress over 3 kicks (remaining={now_remaining}) — stopping; "
                    "leftovers are inside the normal cron's reach or genuinely unresolvable")
                break
        else:
            stall = 0
        remaining = now_remaining
        log(f"  remaining eligible: {remaining}")
    log(f"requeue finished: remaining={remaining}")
    conn.close()


# --------------------------------------------------------------------- slack

def slack_ping(text: str) -> None:
    tok = os.environ.get("CC_SLACK_BOT_TOKEN")
    if not tok:
        return
    body = urllib.parse.urlencode({"channel": SLACK_CHANNEL, "text": text}).encode()
    req = urllib.request.Request("https://slack.com/api/chat.postMessage", data=body, headers={
        "Authorization": "Bearer " + tok, "Content-Type": "application/x-www-form-urlencoded"})
    try:
        r = json.load(urllib.request.urlopen(req, timeout=30))
        if not r.get("ok"):
            log(f"slack error: {r.get('error')}")
    except Exception as e:  # noqa: BLE001
        log(f"slack ping failed: {e}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--env-file", default=os.path.join(os.path.dirname(__file__), "..", ".env"))
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--requeue", action="store_true")
    ap.add_argument("--days", help="explicit comma-separated YYYY-MM-DD list (newest first)")
    ap.add_argument("--auto", action="store_true", help="today → back-walk to --min-day / retention boundary")
    ap.add_argument("--min-day", default="2026-05-01")
    ap.add_argument("--auto-stop-after", type=int, default=2, help="consecutive empty days = retention boundary")
    ap.add_argument("--checkpoint", default="/root/reconcile/checkpoint.json")
    ap.add_argument("--window-hours", type=int, default=400, help="reattemptWindowHours for the worker sweep")
    ap.add_argument("--max-kicks", type=int, default=40)
    ap.add_argument("--kick-pause", type=float, default=20.0)
    ap.add_argument("--slack-done", action="store_true", help="ping #cc-sam when this invocation finishes")
    args = ap.parse_args()
    load_env(args.env_file)

    if args.backfill:
        run_backfill(args)
    if args.requeue:
        run_requeue(args)
    if not args.backfill and not args.requeue:
        ap.error("pass --backfill and/or --requeue")
    if args.slack_done:
        ck = ck_load(args.checkpoint)
        slack_ping(
            ":card_index_dividers: Sendivo blast-body reconcile pass finished — "
            f"days done {len(ck.get('done_days', []))}, matched rows so far {ck.get('inserted_total', 0)}."
        )


if __name__ == "__main__":
    main()
