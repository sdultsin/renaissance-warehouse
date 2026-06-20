#!/usr/bin/env python3
"""WS-D — Phone backfill: enriched mobiles (comms Supabase) -> lead DB sidecar.

Today, enriched mobile numbers for Instantly/Sendivo opportunities live ONLY in the
comms Supabase (``comms.phone_enrichment.mobile_e164``, pushed to Close) and are NEVER
written back to the lead database. They die there. This job lands them in the lead DB
(``edpyqbiqzduabtjhwfaa``, ``public.leads``) as an ADDITIVE SIDECAR so the nightly
lead-mirror picks them up — single source, no double-sync, canonical lead fields untouched.

WHAT IT DOES
  1. READ comms Supabase (COMMS_SUPABASE_DB_URL, psycopg2 over pooler:6543):
       latest ``comms.phone_enrichment`` row per opportunity WHERE mobile_e164 IS NOT NULL
       (mobile_status = found: 'leadmagic_verified' / 'VERIFIED' both carry a number;
        UNAVAILABLE / NULL never do — the NOT-NULL filter is the robust criterion),
       JOINed to its ``comms.call_opportunity`` (carries email, source, source_lead_id,
       phone_e164).
  2. RESOLVE each enriched phone to a lead in the lead DB (public.leads) by the §1
     identity FALLBACK CHAIN:
        email match (UNIQUE leads_email_key) FIRST
          -> on miss, phone match on DIGITS (hits idx_leads_phone_digits =
             regexp_replace(phone,'[^0-9]','','g'); E.164 won't hit that index)
          -> if NEITHER matches: FLAG (collected to a flag log, NEVER silently dropped).
     If a phone matches MULTIPLE lead rows -> ALERT #cc-sam and SKIP (no auto-pick).
     (Email is UNIQUE so an email match is always 0 or 1; only phone can be multi.)
  3. WRITE the enriched phone into NEW additive sidecar columns on public.leads:
        enriched_phone, enriched_phone_source, enriched_phone_at
     (ADD COLUMN IF NOT EXISTS, additive — fits the table's existing enrich_* sidecar
      convention; never overwrites the canonical ``phone`` column).
     Idempotent: re-run writes 0 changed rows (UPDATE only sets rows where the value
     actually differs).

SAFETY — this touches a 27M-row PRODUCTION table:
  --dry-run (DEFAULT): verifies the sidecar columns' existence (information_schema),
    reports how many enrichments WOULD match by email vs phone vs flag vs multi-match,
    prints sample rows. Writes NOTHING. Does NOT run ADD COLUMN.
  --apply (explicit opt-in): runs ADD COLUMN IF NOT EXISTS, then the upserts. Multi-match
    rows are alerted to #cc-sam and skipped. No-match rows go to the flag log only.

USAGE
    # from repo root (env auto-loaded from the Renaissance parent .env):
    python scripts/backfill_enriched_phones.py             # dry-run (default, safe)
    python scripts/backfill_enriched_phones.py --apply     # real backfill (after sign-off)
    python scripts/backfill_enriched_phones.py --flag-log /tmp/enriched_phone_flags.csv

CREDENTIALS (repo-root .env = /Users/sam/Documents/Claude Code/Renaissance/.env):
    COMMS_SUPABASE_DB_URL              comms Supabase pooler URL (read)
    LEADS_DB_URL                       lead DB (edpyqbiqzduabtjhwfaa) connection (read/write)
    SLACK_* (token + optional cookie)  for the #cc-sam multi-match alert
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

# --- env / config ----------------------------------------------------------------------
# The repo-root .env is the Renaissance parent dir (matches core/config.py ENV search).
REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_CANDIDATES = [
    REPO_ROOT / ".env",
    Path("/Users/sam/Documents/Claude Code/Renaissance/.env"),
]
SLACK_CHANNEL = "C0AR0EA21C1"  # #cc-sam (same as scripts/warehouse_qa.py)
SOURCE_TAG = "comms_phone_enrichment"
DEFAULT_FLAG_LOG = Path("/tmp/enriched_phone_flags.csv")

SIDECAR_COLUMNS = ["enriched_phone", "enriched_phone_source", "enriched_phone_at"]
ADD_COLUMN_DDL = [
    "ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS enriched_phone text",
    "ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS enriched_phone_source text",
    "ALTER TABLE public.leads ADD COLUMN IF NOT EXISTS enriched_phone_at timestamptz",
]


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for path in ENV_CANDIDATES:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            if k and k not in env:  # first match wins (mirrors config.py)
                env[k] = v.strip().strip('"').strip("'")
    return env


def slack_post(env: dict[str, str], text: str) -> dict:
    """Post to #cc-sam. Mirrors scripts/warehouse_qa.py; tolerant of token-key naming.

    Droplet .env uses SLACK_TOKEN/SLACK_COOKIE; the local repo-root .env uses
    SLACK_BROWSER_TOKEN/SLACK_BROWSER_COOKIE (and CC_SLACK_BOT_TOKEN). Try in order.
    """
    token = (
        env.get("SLACK_TOKEN")
        or env.get("SLACK_BROWSER_TOKEN")
        or env.get("CC_SLACK_BOT_TOKEN")
    )
    cookie = env.get("SLACK_COOKIE") or env.get("SLACK_BROWSER_COOKIE")
    if not token:
        print("backfill_enriched_phones: no Slack token, skipping alert", flush=True)
        return {"ok": False, "error": "no_token"}
    body = json.dumps({"channel": SLACK_CHANNEL, "text": text}).encode("utf-8")
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
                print(f"backfill_enriched_phones: slack error {out.get('error')}", flush=True)
            return out
    except Exception as exc:  # noqa: BLE001
        print(f"backfill_enriched_phones: slack post failed: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


def digits_only(s: str | None) -> str:
    return re.sub(r"[^0-9]", "", s or "")


# --- source read -----------------------------------------------------------------------
def fetch_enrichments(comms_url: str) -> list[dict]:
    """Latest found mobile per opportunity, joined to its call_opportunity."""
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (pe.opportunity_id)
                   pe.opportunity_id,
                   pe.mobile_e164,
                   pe.mobile_status,
                   pe.attempted_at
            FROM comms.phone_enrichment pe
            WHERE pe.mobile_e164 IS NOT NULL
            ORDER BY pe.opportunity_id, pe.attempted_at DESC NULLS LAST
        )
        SELECT l.opportunity_id,
               l.mobile_e164,
               l.mobile_status,
               o.email,
               o.source,
               o.source_lead_id,
               o.phone_e164
        FROM latest l
        JOIN comms.call_opportunity o ON o.id = l.opportunity_id
    """
    conn = psycopg2.connect(comms_url)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# --- resolution against lead DB --------------------------------------------------------
def sidecar_columns_present(cur) -> dict[str, bool]:
    cur.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema='public' AND table_name='leads'
             AND column_name = ANY(%s)""",
        [SIDECAR_COLUMNS],
    )
    have = {r[0] for r in cur.fetchall()}
    return {c: (c in have) for c in SIDECAR_COLUMNS}


def resolve_lead(cur, email: str | None, mobile_e164: str | None):
    """Return (lead_id_or_None, how) where how in
    {email, phone, phone_multi, flag}. email match first, then phone (digits)."""
    if email:
        cur.execute("SELECT id FROM public.leads WHERE email = %s", [email])
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], "email"
        # email is UNIQUE -> never >1; len 0 falls through to phone.
    d = digits_only(mobile_e164)
    if len(d) >= 10:
        cur.execute(
            "SELECT id FROM public.leads "
            "WHERE regexp_replace(phone,'[^0-9]','','g') = %s LIMIT 6",
            [d],
        )
        rows = cur.fetchall()
        if len(rows) == 1:
            return rows[0][0], "phone"
        if len(rows) > 1:
            return None, "phone_multi"
    return None, "flag"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="ACTUALLY add the sidecar columns + write. Default is --dry-run.")
    ap.add_argument("--flag-log", type=Path, default=DEFAULT_FLAG_LOG,
                    help="CSV path for no-match flag rows (default /tmp/enriched_phone_flags.csv).")
    ap.add_argument("--limit", type=int, default=None,
                    help="For smoke tests: only process the first N enrichments.")
    args = ap.parse_args(argv)
    dry_run = not args.apply

    env = load_env()
    for key in ("COMMS_SUPABASE_DB_URL", "LEADS_DB_URL"):
        if not env.get(key):
            print(f"FATAL: {key} missing from .env", file=sys.stderr)
            return 2

    print(f"=== backfill_enriched_phones ({'DRY-RUN' if dry_run else 'APPLY'}) ===")
    print("Reading enriched mobiles from comms Supabase ...", file=sys.stderr)
    rows = fetch_enrichments(env["COMMS_SUPABASE_DB_URL"])
    if args.limit:
        rows = rows[: args.limit]
    print(f"Enriched opportunities with a found mobile: {len(rows)}")

    lead = psycopg2.connect(env["LEADS_DB_URL"])
    lead.autocommit = False
    cur = lead.cursor()

    # 1) sidecar column existence (information_schema).
    present = sidecar_columns_present(cur)
    print("Sidecar columns present BEFORE this run:")
    for c in SIDECAR_COLUMNS:
        print(f"  {c}: {'EXISTS' if present[c] else 'absent (will be added on --apply)'}")
    all_present = all(present.values())

    # 2) resolve every enrichment.
    by_email: list[tuple] = []   # (lead_id, mobile, source)
    by_phone: list[tuple] = []
    multi: list[dict] = []
    flags: list[dict] = []
    samples: list[tuple] = []
    for r in rows:
        lead_id, how = resolve_lead(cur, r["email"], r["mobile_e164"])
        if how == "email":
            by_email.append((lead_id, r["mobile_e164"], r["source"]))
        elif how == "phone":
            by_phone.append((lead_id, r["mobile_e164"], r["source"]))
        elif how == "phone_multi":
            multi.append(r)
        else:
            flags.append(r)
        if len(samples) < 10:
            samples.append((r["opportunity_id"], r["email"], r["source"],
                            r["mobile_e164"], how, str(lead_id)[:12] if lead_id else "-"))

    n_email, n_phone, n_multi, n_flag = len(by_email), len(by_phone), len(multi), len(flags)
    print("\n--- resolution breakdown ---")
    print(f"  matched by EMAIL : {n_email}")
    print(f"  matched by PHONE : {n_phone}")
    print(f"  MULTI-match phone: {n_multi}  (ALERT #cc-sam + SKIP)")
    print(f"  FLAGGED (no match): {n_flag}  (-> flag log, never dropped)")
    print(f"  total            : {n_email + n_phone + n_multi + n_flag} / {len(rows)}")

    print("\n--- sample rows (opp, email, source, mobile, how, lead_id) ---")
    for s in samples:
        print("  ", s)

    # 3) flag log — always write (it's local/temp, no PII into git).
    if flags:
        with args.flag_log.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["opportunity_id", "email", "source", "source_lead_id",
                        "mobile_e164", "mobile_status"])
            for r in flags:
                w.writerow([r["opportunity_id"], r["email"], r["source"],
                            r["source_lead_id"], r["mobile_e164"], r["mobile_status"]])
        print(f"\nWrote {len(flags)} no-match flag rows -> {args.flag_log}")

    # 4) multi-match -> #cc-sam alert (skip writes for these).
    if multi:
        lines = [
            f":rotating_light: *Enriched-phone backfill: {len(multi)} phone multi-match* "
            f"(NOT auto-written — Sam decide):"
        ]
        for r in multi[:15]:
            lines.append(
                f"• opp {r['opportunity_id']} src={r['source']} "
                f"email={r['email'] or '-'} mobile={r['mobile_e164']}"
            )
        if dry_run:
            print("\n[dry-run] WOULD alert #cc-sam about "
                  f"{len(multi)} multi-match phone(s).")
        else:
            out = slack_post(env, "\n".join(lines))
            print(f"\n#cc-sam multi-match alert posted: ok={out.get('ok')} "
                  f"error={out.get('error')}")

    if dry_run:
        print("\n[dry-run] No ADD COLUMN, no writes performed. "
              "Re-run with --apply after sign-off.")
        if not all_present:
            print("[dry-run] Sidecar columns do NOT all exist yet -> the ADD is "
                  "genuinely additive.")
        cur.close()
        lead.close()
        return 0

    # ---- APPLY path ----------------------------------------------------------------
    # 5) additive DDL.
    print("\n[apply] ADD COLUMN IF NOT EXISTS (additive sidecar) ...")
    for ddl in ADD_COLUMN_DDL:
        cur.execute(ddl)
    lead.commit()

    # 6) idempotent writes. Only updates rows whose sidecar value actually differs,
    #    so a re-run reports 0 changed. Canonical `phone` is never touched.
    now = datetime.now(timezone.utc)
    changed = 0
    for lead_id, mobile, source in (by_email + by_phone):
        cur.execute(
            """UPDATE public.leads
                   SET enriched_phone = %s,
                       enriched_phone_source = %s,
                       enriched_phone_at = %s
                 WHERE id = %s
                   AND (enriched_phone IS DISTINCT FROM %s
                        OR enriched_phone_source IS DISTINCT FROM %s)""",
            [mobile, SOURCE_TAG, now, lead_id, mobile, SOURCE_TAG],
        )
        changed += cur.rowcount
    lead.commit()
    print(f"[apply] sidecar rows changed this run: {changed} "
          f"(email={n_email} + phone={n_phone} candidates; "
          f"re-run should report 0).")

    cur.execute("SELECT count(*) FROM public.leads WHERE enriched_phone IS NOT NULL")
    print(f"[apply] public.leads WHERE enriched_phone IS NOT NULL: {cur.fetchone()[0]}")

    cur.close()
    lead.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
