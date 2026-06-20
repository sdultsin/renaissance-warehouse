#!/usr/bin/env python3
"""ONE-SHOT apply of DDL 71 — ALL-TIME Advisor + Inbox-Manager leaderboard views.

Run ONCE on the droplet, UNDER flock (/root/core/warehouse.write.lock), in an idle writer
window (outside 03:30-05:45 UTC; no nightly/meetings_refresh/compaction running). Pure view
creation — NO table writes, NO backfill, NO re-ingestion. Idempotent: apply_ddl_file is
version-guarded and the views are CREATE OR REPLACE. Safe to re-run.

Steps (each verified, fail-loud), then a #cc-sam Slack post on done/fail:
  0. preflight: schema_version (expect 70), no competing writer, sources present
  1. apply DDL 71 (derived.v_advisor_alltime[_summary] + v_inbox_manager_alltime[_summary])
  2. verify: views exist; all-time advisor total is in the FULL-HISTORY range (>> 2.5k);
     per-advisor and per-IM totals; the ~12% legacy IM-null coverage caveat.

DDL is applied via core.db.apply_ddl_file (the warehouse's own mechanism).
"""
from __future__ import annotations

import json
import logging
import os
import sys
import urllib.request
from pathlib import Path

from core import db as db_module
from core.config import REPO_ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("apply_advisor_im_alltime")

DDL = REPO_ROOT / "sql" / "ddl"
DDL_FILE = DDL / "71_advisor_im_alltime_leaderboards.sql"
DDL_VERSION = 71
ENV_PATH = REPO_ROOT / ".env"

FROZEN_SNAP = "2026-05-31"
FROZEN_SOURCE = "darcy_portal_im_bookings"


# ── Slack (#cc-sam) — identical mechanism to scripts/warehouse_qa.py ───────────────
def _load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def slack_post(text: str) -> dict:
    env = _load_env(ENV_PATH)
    token = env.get("SLACK_TOKEN")
    cookie = env.get("SLACK_COOKIE")
    channel = os.environ.get("SLACK_ALERT_CHANNEL") or env.get("SLACK_ALERT_CHANNEL", "")
    if not token or not channel:
        log.warning("no SLACK_TOKEN/channel, skipping Slack post")
        return {"ok": False, "error": "no_token_or_channel"}
    body = json.dumps({"channel": channel, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            **({"Cookie": f"d={cookie}"} if cookie else {}),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            out = json.loads(resp.read().decode("utf-8"))
            if not out.get("ok"):
                log.error("slack error %s", out.get("error"))
            return out
    except Exception as exc:  # noqa: BLE001
        log.error("slack post failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def schema_version(conn) -> int:
    return conn.execute("SELECT COALESCE(max(version), -1) FROM core.schema_version").fetchone()[0]


def main() -> int:
    if not DDL_FILE.exists():
        log.error("MISSING DDL FILE: %s", DDL_FILE)
        return 2

    conn = db_module.connect()  # read-write; flock held externally
    log.info("connected to warehouse")

    # STEP 0 — preflight.
    sv0 = schema_version(conn)
    log.info("STEP 0 preflight: schema_version=%d", sv0)
    if sv0 != 70:
        log.warning("schema_version is %d (expected 70). Continuing — apply_ddl_file is "
                    "version-guarded and will no-op if 71 already exists.", sv0)
    src_rows = conn.execute(
        "SELECT count(*) FROM raw_im_bookings WHERE _snapshot_date = CAST(? AS DATE) AND _source = ? "
        "AND TRY_CAST(date AS DATE) < DATE '2026-06-01'",
        [FROZEN_SNAP, FROZEN_SOURCE],
    ).fetchone()[0]
    sheet_rows = conn.execute(
        "SELECT count(*) FROM core.meeting WHERE source='sheet' AND advisor_name IS NOT NULL"
    ).fetchone()[0]
    log.info("STEP 0 sources: frozen legacy pre-Jun1=%d rows; sheet advisor rows=%d", src_rows, sheet_rows)
    if src_rows == 0:
        log.error("frozen legacy snapshot slice is empty — refusing to build empty views")
        conn.close()
        return 3

    # STEP 1 — apply DDL 71.
    before = schema_version(conn)
    applied = db_module.apply_ddl_file(conn, DDL_FILE, version=DDL_VERSION)
    after = schema_version(conn)
    if applied:
        assert after >= DDL_VERSION, f"version did not advance to {DDL_VERSION} (now {after})"
        log.info("STEP 1 applied %s -> schema_version %d->%d", DDL_FILE.name, before, after)
    else:
        log.info("STEP 1 already-applied %s (schema_version stays %d) — re-running CREATE OR "
                 "REPLACE manually to pick up any edits", DDL_FILE.name, after)
        # If the version row already exists, apply_ddl_file no-ops. Re-run the body so the
        # CREATE OR REPLACE views still reflect this file (safe, idempotent).
        conn.execute(DDL_FILE.read_text())
    conn.close()

    # STEP 2 — verify (read-only).
    conn = db_module.connect(read_only=True)

    def scalar(sql: str):
        return conn.execute(sql).fetchone()[0]

    adv_total = scalar("SELECT count(*) FROM derived.v_advisor_alltime")
    adv_distinct = scalar("SELECT count(DISTINCT advisor_name) FROM derived.v_advisor_alltime")
    adv_pre = scalar("SELECT count(*) FROM derived.v_advisor_alltime WHERE source='im_bookings_legacy'")
    adv_post = scalar("SELECT count(*) FROM derived.v_advisor_alltime WHERE source='sheet'")
    im_total = scalar("SELECT count(*) FROM derived.v_inbox_manager_alltime")
    im_distinct = scalar("SELECT count(DISTINCT inbox_manager) FROM derived.v_inbox_manager_alltime")
    im_pre = scalar("SELECT count(*) FROM derived.v_inbox_manager_alltime WHERE source='im_bookings_legacy'")
    im_post = scalar("SELECT count(*) FROM derived.v_inbox_manager_alltime WHERE source='sheet'")

    # IM-null coverage caveat (~12%): legacy bookings (any advisor/IM era) vs IM-populated.
    legacy_all = scalar(
        f"SELECT count(*) FROM raw_im_bookings WHERE _snapshot_date=DATE '{FROZEN_SNAP}' "
        f"AND _source='{FROZEN_SOURCE}' AND TRY_CAST(date AS DATE) < DATE '2026-06-01'"
    )
    im_null_pct = round(100.0 * (legacy_all - im_pre) / legacy_all, 1) if legacy_all else None

    top_adv = conn.execute(
        "SELECT advisor_name, bookings_all_time FROM derived.v_advisor_alltime_summary LIMIT 10"
    ).fetchall()
    top_im = conn.execute(
        "SELECT inbox_manager, bookings_all_time FROM derived.v_inbox_manager_alltime_summary LIMIT 10"
    ).fetchall()
    final_sv = scalar("SELECT max(version) FROM core.schema_version")
    conn.close()

    log.info("STEP 2 verified: advisor view rows=%d (distinct=%d) [legacy=%d, sheet=%d]",
             adv_total, adv_distinct, adv_pre, adv_post)
    log.info("STEP 2 verified: IM view rows=%d (distinct=%d) [legacy=%d, sheet=%d]; legacy IM-null=%s%%",
             im_total, im_distinct, im_pre, im_post, im_null_pct)
    log.info("STEP 2 top advisors: %s", top_adv)
    log.info("STEP 2 top IMs: %s", top_im)

    full_history_ok = adv_total > 10_000  # was ~2.5k (sheet only); must now be full-history
    if not full_history_ok:
        log.error("ANOMALY: advisor all-time total=%d still in the sheet-only range — legacy "
                  "seed did not land", adv_total)

    status = "✅ DONE" if full_history_ok else "⚠️ CHECK"
    msg = (
        f"{status} — Warehouse DDL 71: ALL-TIME Advisor + Inbox-Manager leaderboard views "
        f"(derived.v_advisor_alltime / v_inbox_manager_alltime + *_summary). schema_version={final_sv}.\n"
        f"• Advisor all-time bookings: {adv_total:,} across {adv_distinct} advisors "
        f"(pre-Jun-1 legacy={adv_pre:,} + Jun-1+ sheet={adv_post:,}) — now FULL-history, not ~2.5k.\n"
        f"• Inbox-Manager all-time bookings: {im_total:,} across {im_distinct} IMs "
        f"(legacy={im_pre:,} + sheet={im_post:,}).\n"
        f"• Legacy IM-null coverage caveat: ~{im_null_pct}% of pre-Jun-1 bookings carry no inbox_manager "
        f"(expected — present as advisor facts, absent from IM leaderboard).\n"
        f"• Seam dedup: legacy date<2026-06-01 ∪ sheet>=2026-06-01 (disjoint, no double-count). "
        f"Idempotent; nothing pushed."
    )
    slack_post(msg)

    log.info("ALL DONE. final schema_version=%s", final_sv)
    return 0 if full_history_ok else 7


if __name__ == "__main__":
    sys.exit(main())
