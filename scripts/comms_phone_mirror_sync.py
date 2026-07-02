#!/usr/bin/env python3
"""Nightly comms paid-enrichment -> lead-mirror phone sync (phone-truth item 1).

The lead-mirror DuckDB is THE master store for phones
([[project_phone_truth_lead_mirror_20260701]]). Paid enrichment results
(Prospeo / LeadMagic / Findymail) historically died in the comms Supabase
(comms.phone_enrichment) and never flowed back to the lead — so the next system
that needed the same person's phone re-bought it (~38% of Prospeo / ~42% of
LeadMagic credits re-bought known phones, COST-1). This job closes the loop:

  1. READ comms Supabase (COMMS_SUPABASE_DB_URL): every phone_enrichment HIT
     (mobile_e164 NOT NULL) from a PAID vendor, joined to call_opportunity for
     the lead email; latest attempt per lower(email) wins (dedup by pe.id via
     DISTINCT ON). reply_signature rows are EXCLUDED (they flow via
     signature_phone_sync, with the true reply timestamp); reuse_* rows are
     EXCLUDED (provenance noise — the underlying phone came from an original
     vendor/signature row that is already synced).
  2. STAGE a TSV (email, phone, source_tag='comms_<provider>', event_ts=attempted_at).
  3. APPLY via scripts/apply_phone_mirror_updates.sh — the one writer path:
     .writer.lock held, enriched_phone/_source/_at only (NEVER the bought
     phone/phone10 columns), never clobbers a fresher enriched_phone_at.

STATELESS + IDEMPOTENT: full pull every night (the hit set is small, ~14k rows
as of 2026-07-01); re-running writes 0 changed rows thanks to the value-diff +
freshness guards in the apply script. No watermark to strand — the failure mode
that cut the mirror off from signature phones for 7 nights can't recur here.

SCHEDULING: cron 23:05 UTC — clear of the warehouse writer window (03:30-05:45Z)
and the mirror's own nightly writers (03:25-04:00Z), and BEFORE the 03:50Z
serving-snapshot refresh so tonight's phones are servable by the lead-query API
(and the comms worker's mirror tier) the next morning.

FAIL-LOUD: any exception posts to Slack (SLACK_ALERT_CHANNEL) and exits non-zero.

USAGE
    python3 scripts/comms_phone_mirror_sync.py            # nightly real run
    python3 scripts/comms_phone_mirror_sync.py --dry-run  # stage + report, no write
"""
from __future__ import annotations

import argparse
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

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_CANDIDATES = [REPO_ROOT / ".env", REPO_ROOT.parent / ".env", Path("/root/core/.env")]

APPLY_SH = REPO_ROOT / "scripts" / "apply_phone_mirror_updates.sh"
STAGE_DIR = Path(os.environ.get("COMMS_PHONE_STAGE_DIR", "/root/comms-phone-sync"))

# Paid vendors only. reply_signature is owned by signature_phone_sync (true reply
# ts); reuse_* rows re-serve an already-synced phone under a misleading tag.
PAID_PROVIDERS = ("prospeo", "leadmagic", "findymail", "aleads")

PULL_SQL = """
    SELECT DISTINCT ON (lower(co.email))
           lower(co.email)  AS email,
           pe.mobile_e164   AS phone,
           pe.provider      AS provider,
           pe.attempted_at  AS attempted_at
    FROM comms.phone_enrichment pe
    JOIN comms.call_opportunity co ON co.id = pe.opportunity_id
    WHERE pe.mobile_e164 IS NOT NULL
      AND pe.provider = ANY(%s)
      AND co.email IS NOT NULL
      AND position('@' in co.email) > 1
    ORDER BY lower(co.email), pe.attempted_at DESC NULLS LAST, pe.id DESC
"""


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
        print("comms_phone_mirror_sync: no Slack token/channel, skipping alert", flush=True)
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
                print(f"comms_phone_mirror_sync: slack error {out.get('error')}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"comms_phone_mirror_sync: slack post failed: {exc}", flush=True)


_FIELD_JUNK = re.compile(r"[\t\r\n]")


def _clean(v: str) -> str:
    return _FIELD_JUNK.sub(" ", v).strip()


def pull_hits(env: dict[str, str]) -> list[tuple[str, str, str, str]]:
    url = env.get("COMMS_SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("COMMS_SUPABASE_DB_URL not in env")
    rows: list[tuple[str, str, str, str]] = []
    conn = psycopg2.connect(url, connect_timeout=20)
    try:
        cur = conn.cursor()
        cur.execute(PULL_SQL, (list(PAID_PROVIDERS),))
        for email, phone, provider, attempted_at in cur.fetchall():
            email = _clean(email or "")
            phone = _clean(phone or "")
            if not email or "@" not in email or not phone:
                continue
            source = f"comms_{_clean(provider or 'unknown')}"
            ts = attempted_at.isoformat() if attempted_at is not None else ""
            rows.append((email, phone, source, ts))
    finally:
        conn.close()
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    env = load_env()
    t0 = time.time()
    try:
        rows = pull_hits(env)
        STAGE_DIR.mkdir(parents=True, exist_ok=True)
        tsv = STAGE_DIR / "staged_paid_hits.tsv"
        with tsv.open("w") as f:
            for r in rows:
                f.write("\t".join(r) + "\n")
        print(f"comms_phone_mirror_sync: pulled {len(rows)} paid hits -> {tsv}", flush=True)

        if args.dry_run:
            print("comms_phone_mirror_sync: dry-run, skipping apply", flush=True)
            return 0

        proc = subprocess.run(
            ["bash", str(APPLY_SH), str(tsv)],
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
        summary = (f"comms_phone_mirror_sync: ok pulled={len(rows)} "
                   f"staged={metrics.get('staged', '?')} matched={metrics.get('matched', '?')} "
                   f"updated={metrics.get('will_update', '?')} committed={metrics.get('committed', '?')} "
                   f"({time.time() - t0:.0f}s)")
        print(summary, flush=True)
        return 0
    except Exception:
        err = traceback.format_exc()
        print(err, flush=True)
        slack_post(env, f":rotating_light: comms_phone_mirror_sync FAILED "
                        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%MZ}\n```{err[-1500:]}```")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
