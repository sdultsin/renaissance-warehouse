#!/usr/bin/env python3
"""Nightly signature->phone self-enrichment sync (sig-phone Phase 2).

Extracts US phone numbers from the signatures of NEW inbound cold-email replies
landed in the warehouse since the last run, and writes them to the LEAD MIRROR
DuckDB sidecar columns (mirror.leads_current enriched_phone /
enriched_phone_source / enriched_phone_at). Free self-enrichment from data we
already own — replaces a paid Prospeo->LeadMagic->Findymail lookup wherever a
replier signed their email.
Spec: Renaissance handoffs/2026-06-12-signature-phone-warehouse-native.md.

REPOINTED 2026-07-01 ([[project_phone_truth_lead_mirror_20260701]]): the write
target was the retired Supabase public.leads — its Supabase->DuckDB delta-sync
was disabled at the 2026-06-24 cutover, so 6+ nights of signature phones (~8.3k)
landed in a dead-end DB, and the ~4.9M post-cutover mirror-only leads could
never receive one at all. Writes now go DIRECT to the lead mirror via
scripts/apply_phone_mirror_updates.sh (.writer.lock held, single transaction,
freshness-guarded). The stranded 06-23->07-01 gap is replayed by resetting the
watermarks (--init-watermark '2026-06-23T00:00:00') — the extraction sources
are append-only in the warehouse, so a replay is loss-free.

SOURCES (warehouse, read-only; new rows by _loaded_at watermark):
  raw_pipeline_conversation_messages  lead-authored rows (sender_email = lead_email)
  raw_instantly_email                 received replies (ue_type = 2)

POLICY (orchestrator-confirmed 2026-06-12; freshness-guarded 2026-07-01):
  Latest observation wins: enriched_phone_at carries the REPLY timestamp (not
  the write time), and the apply script writes only when the incoming
  observation is FRESHER than the enrichment already on the lead — a newer paid
  enrichment is never clobbered by an older signature replay, and vice versa.
  NEVER touches the bought leads phone / phone10 columns.
  Source tag: 'reply_signature_v2'.

STATE: /root/sig-phone/sync_state.json (watermarks + shared-phone frequency map).
  Shared-number guard: a phone seen on >= 10 distinct lead emails (cumulative)
  is junk (shared office line / brand number) — blocklisted, never written.
  Watermarks advance ONLY after a successful mirror apply.

FAIL-LOUD: any exception posts to the Slack alert channel (SLACK_ALERT_CHANNEL)
and exits non-zero. Designed to be invoked from scripts/nightly.sh AFTER
compaction (warehouse lock free; the mirror write serializes on the mirror's
own .writer.lock inside the apply script).

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
import re
import subprocess
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from phone_parser import best_signature_phone, html_to_text  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_CANDIDATES = [REPO_ROOT / ".env", REPO_ROOT.parent / ".env", Path("/root/core/.env")]

WAREHOUSE_DB = os.environ.get("WAREHOUSE_DB", "/root/core/warehouse.duckdb")
STATE_PATH = Path(os.environ.get("SIG_PHONE_STATE", "/root/sig-phone/sync_state.json"))
APPLY_SH = REPO_ROOT / "scripts" / "apply_phone_mirror_updates.sh"
STAGE_TSV = Path(os.environ.get("SIG_PHONE_STAGE_TSV", "/root/sig-phone/staged_mirror.tsv"))
SOURCE_TAG = "reply_signature_v2"
FREQ_CAP = 10
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
        # Body cap: signatures sit at the end of the author's own portion, which
        # is at the TOP of the body (quoted history below). Multi-MB bodies
        # (base64 inline images) cost ~25ms+/row unbounded — truncate.
        if len(body) > 200_000:
            body = body[:200_000]
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
               left(CASE WHEN body_text IS NOT NULL AND length(body_text) > 0
                         THEN body_text ELSE body_html END, 200000) AS body,
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
        SELECT lead_email, left(reply_text, 200000) AS reply_text,
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


_FIELD_JUNK = re.compile(r"[\t\r\n]")


def write_winners(env, winners, stats, dry_run: bool) -> bool:
    """Stage winners to TSV and apply to the LEAD MIRROR via the shared writer-lock
    apply script (enriched_phone/_source/_at only; freshness-guarded; never touches
    the bought phone/phone10 columns). Returns True on success (or nothing to
    write); raises on apply failure so the fail-loud path fires and the watermark
    is NOT advanced (rows retried next night)."""
    if not winners:
        print("signature_phone_sync: no new phones tonight", flush=True)
        return True
    # email \t phone \t source \t event_ts (reply timestamp — freshness comparator)
    rows = sorted(
        (
            _FIELD_JUNK.sub(" ", e).strip(),
            _FIELD_JUNK.sub(" ", p).strip(),
            SOURCE_TAG,
            _FIELD_JUNK.sub(" ", t).strip(),
        )
        for e, (t, p) in winners.items()
    )
    stats["staged"] = len(rows)
    STAGE_TSV.parent.mkdir(parents=True, exist_ok=True)
    with STAGE_TSV.open("w") as f:
        for r in rows:
            f.write("\t".join(r) + "\n")
    if dry_run:
        print(f"signature_phone_sync: dry-run staged {len(rows)} rows -> {STAGE_TSV} (no write)",
              flush=True)
        return True
    proc = subprocess.run(
        ["bash", str(APPLY_SH), str(STAGE_TSV)],
        capture_output=True, text=True, timeout=3600,
    )
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise RuntimeError(f"apply_phone_mirror_updates.sh exited {proc.returncode}")
    metrics = dict(
        m.split("=", 1) for line in proc.stdout.splitlines()
        for m in line.split() if "=" in m and m.split("=", 1)[0] in
        ("staged", "matched", "will_update", "committed")
    )
    try:
        stats["written"] = int(metrics.get("will_update", 0))
        stats["would_match"] = int(metrics.get("matched", 0))
    except ValueError:
        pass
    return True


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
    try:
        con = duckdb.connect(WAREHOUSE_DB, read_only=True)
        winners = extract_new(con, state, stats)
        con.close()
        write_winners(env, winners, stats, args.dry_run)  # raises on apply failure
        if not args.dry_run:
            save_state(state)  # advance watermarks only after a successful mirror apply
        summary = (f"signature_phone_sync: ok scanned cm={stats['cm_rows']} "
                   f"ie={stats['ie_rows']} found={stats['phones_found']} "
                   f"staged={stats['staged']} matched={stats['would_match']} "
                   f"written={stats['written']} target=lead_mirror "
                   f"({time.time() - t0:.0f}s)")
        print(summary, flush=True)
        return 0
    except Exception:
        err = traceback.format_exc()
        print(err, flush=True)
        slack_post(env, f":rotating_light: signature_phone_sync FAILED "
                        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%MZ}\n```{err[-1500:]}```")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
