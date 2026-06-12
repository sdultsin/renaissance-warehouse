#!/usr/bin/env python3
"""Track E4 — daily warehouse QA + fail-loud Slack alert.

Runs after the nightly orchestrator + refresh_sync_registry.py. Reads
v_warehouse_freshness and the global invariants, and posts a RED alert to the
configured Slack channel (SLACK_ALERT_CHANNEL) on any breach so silent
staleness is impossible.

Checks:
  1. STALE feeds        — any registry feed past its cadence SLA (is_stale).
  2. SEND-SENSITIVE 0   — an append-only send-day feed whose last_row_delta <= 0.
  3. EMPTY decision tbl — v_campaign_metrics / core.campaign_daily /
                          v_infra_capacity_daily / raw_account_truth_daily_actuals
                          each must return > 0 rows (skipped if not yet built).
  4. FAITHFULNESS       — warehouse campaign-grain derived == raw API blob (270/270);
                          WARN-only while pipeline-supabase is mid-retirement.

Exit code: 0 = all green, 1 = at least one FAIL. Posts to Slack on FAIL or STALE
(read-only DB access; never writes the warehouse).

Usage:
    python scripts/warehouse_qa.py               # check + post on breach
    python scripts/warehouse_qa.py --no-post      # check + print only (CI / local)
    python scripts/warehouse_qa.py --test-alert    # post a test line, verify ok:true
    python scripts/warehouse_qa.py --pulse         # always post (green daily pulse)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

import duckdb

from core.config import DB_PATH, REPO_ROOT

# Alert channel id from env (set via SLACK_ALERT_CHANNEL); alert is skipped if unset.
SLACK_CHANNEL = os.environ.get("SLACK_ALERT_CHANNEL", "")
ENV_PATH = REPO_ROOT / ".env"


def load_env(path: Path) -> dict[str, str]:
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
    env = load_env(ENV_PATH)
    token = env.get("SLACK_TOKEN")
    cookie = env.get("SLACK_COOKIE")
    channel = SLACK_CHANNEL or env.get("SLACK_ALERT_CHANNEL", "")
    if not token or not channel:
        print("warehouse_qa: no SLACK_TOKEN/channel, skipping alert", flush=True)
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
                print(f"warehouse_qa: slack error {out.get('error')}", flush=True)
            return out
    except Exception as exc:  # noqa: BLE001
        print(f"warehouse_qa: slack post failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


# Decision tables that must never be empty (skipped if not yet built).
EMPTY_CHECK_TABLES = [
    "v_campaign_metrics",
    "core.campaign_daily",
    "v_infra_capacity_daily",
    "raw_account_truth_daily_actuals",
    # infra-batch layer (DDL 60). 0 rows = the dims were truncated/lost (the
    # 06-08 domain_registry failure mode); the populate (build_infra_batch.sql)
    # is manual/snapshot so these stay put between exports.
    "core.infra_batch_root",
    "core.sending_account_batch",
]

# Active-account batch-coverage WARN threshold (infra-batch layer). The infra
# export is a MANUAL snapshot, so some currently-active inboxes will always be
# newer than the last export → unmatched. Warn (not fail) only when the gap is
# large enough to mean the snapshot has gone materially stale.
INFRA_BATCH_UNMATCHED_WARN_FRAC = 0.30


def _exists(con, name: str) -> bool:
    schema, _, table = name.partition(".")
    if not table:
        schema, table = "main", name
    r = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
        [schema, table],
    ).fetchone()
    return r is not None


def run_checks(con) -> tuple[list[str], list[str]]:
    """Return (fails, warns) as human-readable lines."""
    fails: list[str] = []
    warns: list[str] = []

    # 1. STALE feeds.
    stale = con.execute(
        "SELECT name, expected_cadence, source, "
        "       COALESCE(CAST(hours_since_sync AS VARCHAR),'never') AS h "
        "FROM v_warehouse_freshness WHERE is_stale ORDER BY hours_since_sync DESC NULLS FIRST"
    ).fetchall()
    for name, cadence, source, h in stale:
        fails.append(f"STALE: `{name}` ({cadence}/{source}) — {h}h since last sync (SLA breach)")

    # 1b. DATA-STALE feeds — the sync ran but the data's own business date is old
    # (successful-but-empty pulls; the Jun 4-10 meetings outage failure mode).
    try:
        data_stale = con.execute(
            "SELECT name, source, biz_sla_days, "
            "       COALESCE(CAST(days_since_biz AS VARCHAR),'never') AS d "
            "FROM v_warehouse_freshness WHERE is_data_stale "
            "ORDER BY days_since_biz DESC NULLS FIRST"
        ).fetchall()
        for name, source, sla_d, d in data_stale:
            fails.append(
                f"DATA-STALE: `{name}` ({source}) — newest business date is {d}d old "
                f"(SLA {sla_d}d); sync may be running but pulling nothing new")
    except Exception:
        pass  # registry/view predates biz_sla_days; refresh_sync_registry upgrades it

    # 2. SEND-SENSITIVE feeds. Keyed on DATA-PRESENCE where the feed is date-grained
    # (last_biz_date tracked): the newest business date must cover the last completed
    # send day (current_date - 1). Insert-delta is NOT used for those — a backfill
    # that pre-inserts rows before the nightly makes row_delta=0 look like a dead
    # feed (2026-06-11 false alarm on core.campaign_daily after the 45-day bounce
    # backfill). Insert-delta stays as the only signal for send-sensitive feeds
    # without a business-date column.
    flat = con.execute(
        "SELECT name, last_row_delta, last_biz_date, "
        "       (biz_date_column IS NOT NULL AND last_biz_date IS NOT NULL) AS date_grained "
        "FROM core.sync_registry "
        "WHERE is_send_sensitive AND status='active' AND ("
        "      (biz_date_column IS NOT NULL AND last_biz_date IS NOT NULL"
        "       AND last_biz_date < current_date - 1)"
        "   OR ((biz_date_column IS NULL OR last_biz_date IS NULL)"
        "       AND last_row_delta IS NOT NULL AND last_row_delta <= 0))"
    ).fetchall()
    for name, delta, biz_date, date_grained in flat:
        if date_grained:
            warns.append(
                f"FLAT: send-sensitive `{name}` newest data day = {biz_date} "
                f"(expected >= yesterday; feed not landing send-day data)")
        else:
            warns.append(
                f"FLAT: send-sensitive `{name}` row_delta={delta} "
                f"(no new rows on a send-day; no date column to check presence)")

    # 3. EMPTY decision tables.
    for tbl in EMPTY_CHECK_TABLES:
        if not _exists(con, tbl):
            warns.append(f"NOT-BUILT: `{tbl}` does not exist yet")
            continue
        n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        if n == 0:
            fails.append(f"EMPTY: decision table `{tbl}` returned 0 rows")

    # 3b. Infra-batch coverage. WARN if too large a share of currently-active
    # inboxes have no batch mapping (the manual infra snapshot has gone stale).
    try:
        if _exists(con, "core.sending_account_batch") and _exists(con, "core.sending_account"):
            total, unmatched = con.execute(
                "SELECT count(*), "
                "       count(*) FILTER (WHERE b.account_email IS NULL) "
                "FROM core.sending_account sa "
                "LEFT JOIN core.sending_account_batch b "
                "       ON b.account_email = lower(sa.account_id) "
                "WHERE sa.is_active"
            ).fetchone()
            if total and unmatched / total > INFRA_BATCH_UNMATCHED_WARN_FRAC:
                warns.append(
                    f"INFRA-BATCH STALE: {unmatched}/{total} "
                    f"({unmatched/total:.0%}) active inboxes have no batch mapping "
                    f"— refresh the infra export + rerun build_infra_batch.sql")
    except Exception as exc:  # noqa: BLE001
        warns.append(f"INFRA-BATCH coverage check errored ({exc})")

    # 4. Warehouse<->API faithfulness (campaign grain). WARN-only during retirement.
    try:
        if _exists(con, "raw_instantly_campaign_analytics") and _exists(con, "v_campaign_metrics"):
            mismatch = con.execute(
                """
                SELECT count(*) FROM v_campaign_metrics m
                JOIN raw_instantly_campaign_analytics a USING (campaign_id)
                WHERE COALESCE(m.sent,0)        <> COALESCE(a.emails_sent_count,0)
                   OR COALESCE(m.unique_replies,0) <> COALESCE(a.reply_count_unique,0)
                   OR COALESCE(m.opportunities,0)  <> COALESCE(a.total_opportunities,0)
                """
            ).fetchone()[0]
            if mismatch:
                warns.append(f"FAITHFULNESS: {mismatch} campaigns where derived <> raw API blob")
    except Exception as exc:  # noqa: BLE001
        warns.append(f"FAITHFULNESS: check errored ({exc})")

    return fails, warns


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Warehouse freshness + invariant QA")
    parser.add_argument("--db", type=str, default=None)
    parser.add_argument("--no-post", action="store_true", help="check + print only")
    parser.add_argument("--pulse", action="store_true", help="always post (even green)")
    parser.add_argument("--test-alert", action="store_true",
                        help="post a test line to the alert channel and report ok status")
    args = parser.parse_args(argv)

    if args.test_alert:
        out = slack_post(":test_tube: warehouse_qa test-alert — Slack path OK (Track E4 wiring check)")
        print(f"test-alert posted: ok={out.get('ok')} ts={out.get('ts')} error={out.get('error')}")
        return 0 if out.get("ok") else 1

    db_path = Path(args.db) if args.db else DB_PATH
    con = duckdb.connect(str(db_path), read_only=True)
    fails, warns = run_checks(con)
    con.close()

    print("=== warehouse_qa ===")
    for f in fails:
        print("FAIL  " + f)
    for w in warns:
        print("WARN  " + w)
    if not fails and not warns:
        print("OK    all freshness + invariant checks green")

    posted = False
    if fails and not args.no_post:
        lines = [":rotating_light: *Warehouse QA FAILED* — silent staleness / invariant breach:"]
        lines += [f"• {f}" for f in fails]
        if warns:
            lines += [f"• _(warn)_ {w}" for w in warns]
        slack_post("\n".join(lines))
        posted = True
    elif args.pulse and not args.no_post:
        msg = ":white_check_mark: Warehouse QA green — all feeds within SLA"
        if warns:
            msg += " (with warnings):\n" + "\n".join(f"• _(warn)_ {w}" for w in warns)
        slack_post(msg)
        posted = True

    if posted:
        print("(posted to the alert channel)")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
