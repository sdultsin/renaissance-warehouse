#!/usr/bin/env python3
"""Nightly signature->phone self-enrichment sync (sig-phone Phase 2).

Extracts US phone numbers from the signatures of NEW inbound cold-email replies
landed in the warehouse since the last run, and writes them to the lead DB
sidecar columns (public.leads enriched_phone / enriched_phone_source /
enriched_phone_at). Free self-enrichment from data we already own — replaces a
paid Prospeo->LeadMagic->Findymail lookup wherever a replier signed their email.
Spec: Renaissance handoffs/2026-06-12-signature-phone-warehouse-native.md.

SOURCES (warehouse, read-only; new rows by _loaded_at watermark):
  raw_pipeline_conversation_messages  lead-authored rows (sender_email = lead_email)
  raw_instantly_email                 received replies (ue_type = 2)

POLICY (orchestrator-confirmed 2026-06-12):
  Overwrite enriched_phone, latest reply wins — even over a prior enriched value.
  NEVER touch the canonical leads.phone column.
  Source tag: 'reply_signature_v2'.

WRITE DISCIPLINE (leads-DB ~100-130 rows/s non-HOT ceiling):
  Batched commits (5k), value-diff guard (IS DISTINCT FROM) so an unchanged
  phone costs nothing, and an other-writers preflight that defers the run
  (watermark NOT advanced -> retried next night) when the delta is large and
  another bulk writer is active on public.leads.

STATE: /root/sig-phone/sync_state.json (watermarks + shared-phone frequency map).
  Shared-number guard: a phone seen on >= 10 distinct lead emails (cumulative)
  is junk (shared office line / brand number) — blocklisted, never written.

FAIL-LOUD: any exception posts to the Slack alert channel (SLACK_ALERT_CHANNEL)
and exits non-zero. Designed to be invoked from scripts/nightly.sh AFTER
compaction (warehouse lock free; coexists with read-only publish steps).

USAGE
    python3 scripts/signature_phone_sync.py             # nightly real run
    python3 scripts/signature_phone_sync.py --dry-run   # extract + report, no writes
    python3 scripts/signature_phone_sync.py --init-watermark '2026-06-12T00:00:00'
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parent))
from phone_parser import best_signature_phone, html_to_text  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_CANDIDATES = [REPO_ROOT / ".env", REPO_ROOT.parent / ".env", Path("/root/core/.env")]

WAREHOUSE_DB = os.environ.get("WAREHOUSE_DB", "/root/core/warehouse.duckdb")
STATE_PATH = Path(os.environ.get("SIG_PHONE_STATE", "/root/sig-phone/sync_state.json"))
SOURCE_TAG = "reply_signature_v2"
FREQ_CAP = 10
BATCH = 5000
BIG_DELTA = 20_000  # defer (not force through) if another bulk writer is active
EPOCH = "1970-01-01T00:00:00"


def load_env() -> dict[str, str]:
    env: dict[str, str] = dict(os.environ)
    for path in ENV_CANDIDATES:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            if k and k not in env:
                env[k] = v.strip().strip('"').strip("'")
    return env


def slack_post(env: dict[str, str], text: str) -> None:
    token = env.get("SLACK_TOKEN") or env.get("SLACK_BROWSER_TOKEN") or env.get("CC_SLACK_BOT_TOKEN")
    cookie = env.get("SLACK_COOKIE") or env.get("SLACK_BROWSER_COOKIE")
    channel = env.get("SLACK_ALERT_CHANNEL", "")
    if not token or not channel:
        print("signature_phone_sync: no Slack token/channel, skipping alert", flush=True)
        return
    body = json.dumps({"channel": channel, "text": text}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage", data=body, method="POST",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json; charset=utf-8",
                 **({"Cookie": f"d={cookie}"} if cookie else {})},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            out = json.loads(resp.read())
            if not out.get("ok"):
                print(f"signature_phone_sync: slack error {out.get('error')}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"signature_phone_sync: slack post failed: {exc}", flush=True)


def leads_conn(env: dict[str, str]):
    url = env.get("LEADS_DB_URL")
    if url:
        return psycopg2.connect(url)
    pw = env.get("LEADS_DB_PASSWORD")
    if not pw:
        raise RuntimeError("no LEADS_DB_URL / LEADS_DB_PASSWORD in env")
    return psycopg2.connect(
        host="aws-1-ap-southeast-1.pooler.supabase.com", port=5432,
        dbname="postgres", user="postgres.edpyqbiqzduabtjhwfaa",
        password=pw, connect_timeout=15,
    )


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"wm_conversation_messages": EPOCH, "wm_instantly_email": EPOCH,
            "phone_email_hashes": {}, "blocklist": []}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_PATH)


def is_htmlish(body: str) -> bool:
    low = body.lower()
    return ("</" in low) or ("<br" in low) or ("<div" in low) or ("<p" in low)


def extract_new(con, state: dict, stats: dict) -> dict[str, tuple[str, str]]:
    """Pull rows newer than the watermarks, extract phones, update freq map.
    Returns winners: email -> (reply_ts, phone). Advances watermarks in `state`."""
    winners: dict[str, tuple[str, str]] = {}
    hashes: dict[str, list[str]] = state.setdefault("phone_email_hashes", {})
    blocklist: set[str] = set(state.setdefault("blocklist", []))

    def consider(email, body, ts, loaded_at, wm_key):
        if loaded_at is not None:
            la = str(loaded_at)
            if la > state[wm_key]:
                state[wm_key] = la
        if not email or not body:
            return
        email = email.strip().lower()
        if "@" not in email:
            return
        stats["rows_considered"] += 1
        if is_htmlish(body):
            body = html_to_text(body)
        phone = best_signature_phone(body)
        if not phone:
            return
        stats["phones_found"] += 1
        h = hashlib.sha1(email.encode()).hexdigest()[:12]
        lst = hashes.setdefault(phone, [])
        if h not in lst:
            if len(lst) >= FREQ_CAP:
                blocklist.add(phone)
            else:
                lst.append(h)
        if phone in blocklist:
            stats["freq_guard_rejected"] += 1
            return
        tss = str(ts) if ts is not None else ""
        cur = winners.get(email)
        if cur is None or tss > cur[0]:
            winners[email] = (tss, phone)

    # Inbound filter VERIFIED empirically 2026-06-12: direction='inbound' <->
    # ue_type=2 (834,590 rows at verification); sender==lead covers only 85% of
    # them, so filter on direction/ue_type. Timestamps cast to VARCHAR: the
    # droplet venv lacks pytz, which duckdb needs to materialize TIMESTAMPTZ.
    q1 = """
        SELECT lead_email,
               CASE WHEN body_text IS NOT NULL AND length(body_text) > 0
                    THEN body_text ELSE body_html END AS body,
               CAST(CASE WHEN message_timestamp >= TIMESTAMP '2024-01-01'
                              AND message_timestamp <= now() + INTERVAL 1 DAY
                         THEN message_timestamp ELSE synced_at END AS VARCHAR) AS ts,
               CAST(_loaded_at AS VARCHAR) AS _loaded_at
        FROM raw_pipeline_conversation_messages
        WHERE _loaded_at > CAST(? AS TIMESTAMPTZ)
          AND lead_email IS NOT NULL
          AND (direction = 'inbound' OR ue_type = 2)
          AND (body_text IS NOT NULL OR body_html IS NOT NULL)
    """
    cur = con.execute(q1, [state["wm_conversation_messages"]])
    while True:
        batch = cur.fetchmany(10_000)
        if not batch:
            break
        stats["cm_rows"] += len(batch)
        for email, body, ts, la in batch:
            consider(email, body, ts, la, "wm_conversation_messages")

    q2 = """
        SELECT lead_email, reply_text,
               CAST(CASE WHEN reply_timestamp >= TIMESTAMP '2024-01-01'
                              AND reply_timestamp <= now() + INTERVAL 1 DAY
                         THEN reply_timestamp ELSE _loaded_at END AS VARCHAR) AS ts,
               CAST(_loaded_at AS VARCHAR) AS _loaded_at
        FROM raw_instantly_email
        WHERE _loaded_at > CAST(? AS TIMESTAMPTZ)
          AND coalesce(ue_type, 2) = 2
          AND lead_email IS NOT NULL AND reply_text IS NOT NULL
    """
    cur = con.execute(q2, [state["wm_instantly_email"]])
    while True:
        batch = cur.fetchmany(10_000)
        if not batch:
            break
        stats["ie_rows"] += len(batch)
        for email, body, ts, la in batch:
            consider(email, body, ts, la, "wm_instantly_email")

    state["blocklist"] = sorted(blocklist)
    # drop winners that got blocklisted mid-run
    winners = {e: (t, p) for e, (t, p) in winners.items() if p not in blocklist}
    return winners


def other_bulk_writers(cur) -> list[str]:
    cur.execute("""
        SELECT pid || ' ' || left(coalesce(query,''), 80)
        FROM pg_stat_activity
        WHERE state <> 'idle' AND pid <> pg_backend_pid()
          AND query ILIKE '%public.leads%'
          AND (query ILIKE '%update%' OR query ILIKE '%insert%' OR query ILIKE '%delete%')
          AND query NOT ILIKE '%pg_stat_activity%'
    """)
    return [r[0] for r in cur.fetchall()]


def write_winners(env, winners, stats, dry_run: bool) -> bool:
    """Returns True if the write happened (or nothing to write); False = deferred."""
    if not winners:
        print("signature_phone_sync: no new phones tonight", flush=True)
        return True
    rows = sorted((e, p, SOURCE_TAG) for e, (t, p) in winners.items())
    conn = leads_conn(env)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        others = other_bulk_writers(cur)
        if others and len(rows) > BIG_DELTA and not dry_run:
            print(f"DEFER: {len(rows)} rows but other writers active: {others[:3]}", flush=True)
            return False
        written = 0
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            if dry_run:
                cur.execute("SELECT count(*) FROM public.leads WHERE email = ANY(%s)",
                            ([c[0] for c in chunk],))
                stats["would_match"] += cur.fetchone()[0]
                continue
            psycopg2.extras.execute_values(cur, """
                UPDATE public.leads l
                SET enriched_phone = v.phone,
                    enriched_phone_source = v.source,
                    enriched_phone_at = now()
                FROM (VALUES %s) AS v(email, phone, source)
                WHERE l.email = v.email
                  AND (l.enriched_phone IS DISTINCT FROM v.phone
                       OR l.enriched_phone_source IS DISTINCT FROM v.source)
                """, chunk, page_size=len(chunk))
            written += cur.rowcount
            time.sleep(0.5)
        stats["written"] = written
        stats["staged"] = len(rows)
        return True
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--init-watermark", help="set both watermarks and exit (ISO ts)")
    args = ap.parse_args()
    env = load_env()

    if args.init_watermark:
        state = load_state()
        state["wm_conversation_messages"] = args.init_watermark
        state["wm_instantly_email"] = args.init_watermark
        save_state(state)
        print(f"watermarks set to {args.init_watermark}")
        return 0

    t0 = time.time()
    stats = {k: 0 for k in ("cm_rows", "ie_rows", "rows_considered", "phones_found",
                            "freq_guard_rejected", "staged", "written", "would_match")}
    state = load_state()
    wm_before = (state["wm_conversation_messages"], state["wm_instantly_email"])
    try:
        con = duckdb.connect(WAREHOUSE_DB, read_only=True)
        winners = extract_new(con, state, stats)
        con.close()
        ok = write_winners(env, winners, stats, args.dry_run)
        if ok and not args.dry_run:
            save_state(state)  # advance watermarks only after a successful write
        elif not ok:
            # restore watermarks; rows retried next night
            state["wm_conversation_messages"], state["wm_instantly_email"] = wm_before
            save_state(state)
        summary = (f"signature_phone_sync: ok scanned cm={stats['cm_rows']} "
                   f"ie={stats['ie_rows']} found={stats['phones_found']} "
                   f"staged={stats['staged']} written={stats['written']} "
                   f"deferred={'yes' if not ok else 'no'} "
                   f"({time.time() - t0:.0f}s)")
        print(summary, flush=True)
        if not ok:
            slack_post(env, f":warning: {summary} — leads-DB busy, retrying tomorrow")
        return 0
    except Exception:
        err = traceback.format_exc()
        print(err, flush=True)
        slack_post(env, f":rotating_light: signature_phone_sync FAILED "
                        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%MZ}\n```{err[-1500:]}```")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
