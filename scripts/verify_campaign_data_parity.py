#!/usr/bin/env python3
"""Point-in-time parity check: D1 campaign_data snapshot vs live Pipeline Supabase.

This is the second half of Campaign Control's read-model hardening (the first is
CC's own self-audit freshness guard). After the nightly publishes the
campaign_data snapshot to Cloudflare D1
(publish_campaign_data_d1.py), this script asserts that the D1 snapshot agrees
with LIVE Pipeline Supabase (public.campaign_data) on three invariants, within a
tight tolerance, and posts a fail-loud Slack alert on divergence so a corrupt /
partial / stale publish can never be silent:

  1. ACTIVE-CAMPAIGN SET — the set of campaign_ids whose __ALL__/__ALL__ rollup
     row has status in ('1','Active'). This is exactly the set CC evaluates
     (pipeline-data.getActiveCampaigns). Compared as a symmetric difference;
     a small, additive D1>SUPA delta is tolerated (freeze-on-delete in the
     warehouse mirror keeps a few dead campaigns — see PARITY NOTE in
     publish_campaign_data_d1.py), but SUPA>D1 (live campaigns MISSING from D1)
     is always a hard fail (that is the blindness the 2026-06-17 incident hit).
  2. Σ emails_sent — over all non-rollup rows. Relative tolerance.
  3. Σ opportunities — over all non-rollup rows. Relative tolerance.

Why live Pipeline Supabase (not the warehouse mirror): the publisher reads the
warehouse mirror (raw_pipeline_campaign_data), so comparing D1 to the mirror only
tests one hop. Comparing D1 to LIVE Pipeline Supabase tests the whole chain
(live -> mirror -> D1) end to end, which is what actually matters for CC. The
mirror is a point-in-time snapshot refreshed by the 03:30Z pipeline_mirror phase,
so a modest tolerance absorbs the in-flight delta between the mirror refresh and
the live read; the alert fires only on a divergence too large to be normal lag.

Connections (reuse the established warehouse patterns):
  - Live Pipeline Supabase: DuckDB ATTACH via PIPELINE_SUPABASE_DB_URL
    (core.credentials), READ_ONLY -- same as meetings_late_arrival_sweep.py.
  - D1: Cloudflare D1 HTTP API (CC_D1_API_TOKEN / CLOUDFLARE_RG_ACCOUNT_ID /
    CC_D1_DATABASE_ID) -- same env the publisher uses.

Exit code: 0 = within tolerance, 1 = divergence (alert posted unless --no-post),
2 = could not run (missing creds / unreachable). Slack uses the same
SLACK_TOKEN / SLACK_ALERT_CHANNEL path as warehouse_qa.py.

Usage:
    python scripts/verify_campaign_data_parity.py            # check + post on divergence
    python scripts/verify_campaign_data_parity.py --no-post  # check + print only
    python scripts/verify_campaign_data_parity.py --json     # machine-readable result
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

# core.* import shim (mirrors meetings_late_arrival_sweep.py / warehouse_qa.py).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core.config import REPO_ROOT  # noqa: E402

try:
    from core.credentials import load_credentials  # noqa: E402
except Exception:  # pragma: no cover - credentials module optional in some envs
    load_credentials = None  # type: ignore

ENV_PATH = REPO_ROOT / ".env"

# Relative tolerance on the summed metrics (Σemails_sent / Σopportunities).
#
# The publisher snapshots the warehouse MIRROR (raw_pipeline_campaign_data,
# refreshed by the 03:30Z pipeline_mirror phase), while this check reads LIVE
# Pipeline Supabase later in the nightly. So even a perfectly-correct fresh
# publish carries the mirror-vs-live drift accumulated across that gap — high
# send-volume days add a meaningful number of sends in 1-2h. 3% absorbs that
# normal in-flight lag while still catching a dropped INSERT chunk / partial /
# truncated publish, which moves the sum by FAR more than 3%. The tight, lag-free
# signal is the ACTIVE-SET missing-in-D1 check below (a live campaign either made
# it into the snapshot or it didn't — no accumulation noise), so the Σ tolerance
# is deliberately the looser of the two. (Measured 2026-06-17 against a 28.8h
# STALE snapshot: Σ drift was ~1.46% — i.e. even a full day stale stayed under
# 3%, confirming 3% won't false-fire on ordinary same-night lag, while the
# active-set check still hard-failed on the 17 genuinely-missing campaigns.)
SUM_TOLERANCE_FRAC = 0.03

# CC state D1 database id. NOT a secret (it is in campaign-control/wrangler.toml
# and hardcoded as the publisher's DEFAULT_D1_DATABASE_ID on the box). Used as a
# fallback so this check runs even when CC_D1_DATABASE_ID is absent from .env —
# exactly the publisher's behaviour, so parity covers whatever the publisher wrote.
DEFAULT_D1_DATABASE_ID = "25a32aa3-9d95-42a3-9e9e-8cd3a9e3f3eb"

# Active-set tolerance. D1 may carry a few EXTRA dead campaigns (freeze-on-delete
# in the warehouse mirror), so D1\SUPA up to this many is tolerated. SUPA\D1
# (live campaigns missing from D1) is NEVER tolerated -> any count fails.
ACTIVE_EXTRA_IN_D1_TOLERANCE = 15


# ---------------------------------------------------------------------------
# Slack (same path as warehouse_qa.py)
# ---------------------------------------------------------------------------
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
    token = env.get("SLACK_TOKEN") or os.environ.get("SLACK_TOKEN", "")
    cookie = env.get("SLACK_COOKIE") or os.environ.get("SLACK_COOKIE", "")
    channel = (
        os.environ.get("SLACK_ALERT_CHANNEL", "")
        or env.get("SLACK_ALERT_CHANNEL", "")
    )
    if not token or not channel:
        print("parity: no SLACK_TOKEN/channel, skipping alert", flush=True)
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
                print(f"parity: slack error {out.get('error')}", flush=True)
            return out
    except Exception as exc:  # noqa: BLE001
        print(f"parity: slack post failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Live Pipeline Supabase side (DuckDB ATTACH, READ_ONLY)
# ---------------------------------------------------------------------------
def _pipeline_url() -> str | None:
    if load_credentials is not None:
        try:
            return load_credentials().require("PIPELINE_SUPABASE_DB_URL")
        except Exception:
            pass
    env = load_env(ENV_PATH)
    return (
        env.get("PIPELINE_SUPABASE_DB_URL")
        or os.environ.get("PIPELINE_SUPABASE_DB_URL")
    )


def fetch_supabase() -> dict:
    """Active-campaign id set + Σ emails_sent + Σ opportunities from LIVE Pipeline Supabase.

    Uses an IN-MEMORY DuckDB purely as a Postgres-ATTACH host — it never opens the
    warehouse .duckdb file, so it cannot conflict with the nightly's writer lock
    (the parity check reads live Pipeline Supabase + D1 only; the warehouse is not
    involved). This makes it safe to run during or right after the nightly.
    """
    pg_url = _pipeline_url()
    if not pg_url:
        raise RuntimeError("no PIPELINE_SUPABASE_DB_URL in credentials/.env")

    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        try:
            con.execute("DETACH pg")
        except Exception:
            pass
        con.execute(f"ATTACH '{pg_url}' AS pg (TYPE postgres, READ_ONLY)")

        active = con.execute(
            "SELECT campaign_id FROM pg.public.campaign_data "
            "WHERE step = '__ALL__' AND variant = '__ALL__' "
            "AND status IN ('1','Active')"
        ).fetchall()

        sums = con.execute(
            "SELECT COALESCE(SUM(emails_sent),0), COALESCE(SUM(opportunities),0) "
            "FROM pg.public.campaign_data "
            "WHERE step <> '__ALL__' AND variant <> '__ALL__'"
        ).fetchone()

        con.execute("DETACH pg")
    finally:
        con.close()

    return {
        "active_ids": {r[0] for r in active},
        "sum_sent": int(sums[0] or 0),
        "sum_opps": int(sums[1] or 0),
    }


# ---------------------------------------------------------------------------
# D1 side (Cloudflare D1 HTTP API — same env as the publisher)
# ---------------------------------------------------------------------------
def d1_query(account_id: str, database_id: str, token: str, sql: str) -> list[dict]:
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/d1/database/{database_id}/query"
    )
    body = json.dumps({"sql": sql}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        out = json.loads(resp.read().decode("utf-8"))
    if not out.get("success"):
        raise RuntimeError(f"D1 query failed: {out.get('errors')}")
    result = out.get("result") or []
    if not result:
        return []
    return result[0].get("results", []) or []


def fetch_d1() -> dict:
    env = load_env(ENV_PATH)
    account_id = os.environ.get("CLOUDFLARE_RG_ACCOUNT_ID", "") or env.get("CLOUDFLARE_RG_ACCOUNT_ID", "")
    database_id = (
        os.environ.get("CC_D1_DATABASE_ID", "")
        or env.get("CC_D1_DATABASE_ID", "")
        or DEFAULT_D1_DATABASE_ID
    )
    token = os.environ.get("CC_D1_API_TOKEN", "") or env.get("CC_D1_API_TOKEN", "")
    missing = [
        n
        for n, v in (
            ("CLOUDFLARE_RG_ACCOUNT_ID", account_id),
            ("CC_D1_DATABASE_ID", database_id),
            ("CC_D1_API_TOKEN", token),
        )
        if not v
    ]
    if missing:
        raise RuntimeError(f"missing D1 env: {', '.join(missing)}")

    active = d1_query(
        account_id, database_id, token,
        "SELECT campaign_id FROM campaign_data "
        "WHERE step = '__ALL__' AND variant = '__ALL__' "
        "AND status IN ('1','Active')",
    )
    sums = d1_query(
        account_id, database_id, token,
        "SELECT COALESCE(SUM(emails_sent),0) AS s, COALESCE(SUM(opportunities),0) AS o "
        "FROM campaign_data WHERE step <> '__ALL__' AND variant <> '__ALL__'",
    )
    s_row = sums[0] if sums else {"s": 0, "o": 0}
    return {
        "active_ids": {r["campaign_id"] for r in active},
        "sum_sent": int(s_row.get("s") or 0),
        "sum_opps": int(s_row.get("o") or 0),
    }


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def _rel_diff(a: int, b: int) -> float:
    denom = max(abs(a), abs(b), 1)
    return abs(a - b) / denom


def compare(supa: dict, d1: dict) -> tuple[list[str], list[str], dict]:
    fails: list[str] = []
    warns: list[str] = []

    supa_ids: set = supa["active_ids"]
    d1_ids: set = d1["active_ids"]
    missing_in_d1 = supa_ids - d1_ids          # live campaigns CC would be BLIND to
    extra_in_d1 = d1_ids - supa_ids            # dead campaigns (freeze-on-delete) - tolerated

    if missing_in_d1:
        sample = ", ".join(sorted(missing_in_d1)[:10])
        fails.append(
            f"ACTIVE-SET: {len(missing_in_d1)} active campaigns in Pipeline Supabase "
            f"MISSING from D1 (CC would be blind to them): {sample}"
        )
    if len(extra_in_d1) > ACTIVE_EXTRA_IN_D1_TOLERANCE:
        sample = ", ".join(sorted(extra_in_d1)[:10])
        warns.append(
            f"ACTIVE-SET: {len(extra_in_d1)} campaigns active in D1 but not in live "
            f"Pipeline Supabase (>{ACTIVE_EXTRA_IN_D1_TOLERANCE} tolerated; likely "
            f"frozen-deleted): {sample}"
        )

    sent_diff = _rel_diff(supa["sum_sent"], d1["sum_sent"])
    if sent_diff > SUM_TOLERANCE_FRAC:
        fails.append(
            f"Σemails_sent: D1={d1['sum_sent']} vs Supabase={supa['sum_sent']} "
            f"({sent_diff:.2%} > {SUM_TOLERANCE_FRAC:.0%} tolerance)"
        )

    opps_diff = _rel_diff(supa["sum_opps"], d1["sum_opps"])
    if opps_diff > SUM_TOLERANCE_FRAC:
        fails.append(
            f"Σopportunities: D1={d1['sum_opps']} vs Supabase={supa['sum_opps']} "
            f"({opps_diff:.2%} > {SUM_TOLERANCE_FRAC:.0%} tolerance)"
        )

    metrics = {
        "supa_active": len(supa_ids),
        "d1_active": len(d1_ids),
        "missing_in_d1": len(missing_in_d1),
        "extra_in_d1": len(extra_in_d1),
        "supa_sum_sent": supa["sum_sent"],
        "d1_sum_sent": d1["sum_sent"],
        "sent_rel_diff": round(sent_diff, 5),
        "supa_sum_opps": supa["sum_opps"],
        "d1_sum_opps": d1["sum_opps"],
        "opps_rel_diff": round(opps_diff, 5),
    }
    return fails, warns, metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="D1 campaign_data vs live Pipeline Supabase parity")
    # --db accepted for backward-compat / CLI symmetry but unused: the check uses
    # an in-memory DuckDB ATTACH host and never opens the warehouse file.
    parser.add_argument("--db", type=str, default=None, help="(ignored) kept for CLI compatibility")
    parser.add_argument("--no-post", action="store_true", help="check + print only")
    parser.add_argument("--json", action="store_true", help="emit machine-readable result")
    args = parser.parse_args(argv)

    try:
        supa = fetch_supabase()
    except Exception as exc:  # noqa: BLE001
        msg = f":warning: campaign_data parity could NOT run (Pipeline Supabase side): {exc}"
        print(msg, flush=True)
        if not args.no_post:
            slack_post(msg)
        return 2

    try:
        d1 = fetch_d1()
    except Exception as exc:  # noqa: BLE001
        msg = f":warning: campaign_data parity could NOT run (D1 side): {exc}"
        print(msg, flush=True)
        if not args.no_post:
            slack_post(msg)
        return 2

    fails, warns, metrics = compare(supa, d1)

    if args.json:
        print(json.dumps({"fails": fails, "warns": warns, "metrics": metrics}, indent=2))
    else:
        print("=== campaign_data parity (D1 vs live Pipeline Supabase) ===")
        print(json.dumps(metrics, indent=2))
        for f in fails:
            print("FAIL  " + f)
        for w in warns:
            print("WARN  " + w)
        if not fails and not warns:
            print("OK    D1 snapshot matches live Pipeline Supabase within tolerance")

    if fails and not args.no_post:
        lines = [":rotating_light: *campaign_data parity FAILED* — D1 read-model diverged from live Pipeline Supabase:"]
        lines += [f"• {f}" for f in fails]
        if warns:
            lines += [f"• _(warn)_ {w}" for w in warns]
        lines.append(
            f"_(D1 active={metrics['d1_active']}, Supabase active={metrics['supa_active']}; "
            f"Σsent D1={metrics['d1_sum_sent']}/SUPA={metrics['supa_sum_sent']}; "
            f"Σopps D1={metrics['d1_sum_opps']}/SUPA={metrics['supa_sum_opps']})_"
        )
        slack_post("\n".join(lines))

    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
