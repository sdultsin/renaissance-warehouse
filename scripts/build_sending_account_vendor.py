#!/usr/bin/env python3
"""
Populate core.sending_account_vendor (DDL 72) — account-level VENDOR CATEGORY
(the portal's "Email Type": Reseller / MailIn / Cheap Inboxes / Outreach Today /
Inboxing / Panel / Microsoft Panel / Maildoso) — from LIVE Instantly tags.

WHY live tags: vendor category is a tag-keyed concept (decision 2026-05-20).
provider_code only gives MX provider; only the account's infra TAG distinguishes
OTD vs Reseller, MailIn vs Outlook. core.sending_account_tag is empty and the API
doesn't return account tags inline, so we pull each infra tag's membership.

MECHANISM (proven by tools/blocklist-surveillance/tag_domain_map.py):
  GET /custom-tags             — list every tag per workspace
  GET /accounts?tag_ids=<tid>  — members of each vendor tag
  User-Agent: curl/8.4.0       — dodges Cloudflare 1010 on the default UA
  cursor pagination via next_starting_after; per-workspace checkpoint to JSON.

RESOLUTION: an email may carry >1 vendor tag → resolved by CATEGORY_PRECEDENCE
(most specific / most expensive vendor wins). ESP home tags (plain Outlook/Google)
are 'esp_weak' — recorded only if no real vendor tag was found.

WRITE PATH (single-writer warehouse rules):
  - hold flock /root/core/warehouse.write.lock (exclusive) for the whole write
  - refuse to run inside the nightly window 03:30-05:45 UTC or if a writer is live
  - DELETE + INSERT (idempotent full re-pull)
  - the API pull is read-only and runs OUTSIDE the lock; only the final write holds it

USAGE (on the droplet):
  python3 scripts/build_sending_account_vendor.py            # pull + write
  python3 scripts/build_sending_account_vendor.py --pull-only --out /tmp/vendor.json
  python3 scripts/build_sending_account_vendor.py --load /tmp/vendor.json  # write a prior pull
"""
from __future__ import annotations
import os, sys, re, json, time, fcntl, argparse, urllib.parse, signal
from datetime import datetime, timezone

import requests

BASE = "https://api.instantly.ai/api/v2"
UA = "curl/8.4.0"
WRITE_LOCK = "/root/core/warehouse.write.lock"
DB_PATH = os.environ.get("WAREHOUSE_DB", "/root/core/warehouse.duckdb")

# env key  ->  warehouse workspace_slug (account_truth side).
# (env keys use OLD names; the API returns NEW names — UUID stable.)
KEYMAP = {
    "RENAISSANCE_1": "renaissance-1", "RENAISSANCE_2": "renaissance-2",
    "RENAISSANCE_3": "renaissance-3", "RENAISSANCE_4": "renaissance-4",
    "RENAISSANCE_5": "renaissance-5", "THE_GATEKEEPERS": "the-gatekeepers",
    "KOI_AND_DESTROY": "koi-and-destroy", "EQUINOX": "equinox",
    "PROSPECTS_POWER": "prospects-power", "OUTLOOK_1": "outlook-1",
    "OUTLOOK_3": "outlook-3", "AUTOMATED_APPLICATIONS": "automated-applications",
    "WARM_LEADS": "warm-leads", "SECTION_125_1": "section-125-1",
    "SECTION_125_2": "section-125-2", "ERC_1": "tariffs",
    # NB: FUNDING_4 == KOI_AND_DESTROY (same workspace) — skip the dup.
}

# Most-specific / most-expensive vendor wins when an account has multiple vendor tags.
CATEGORY_PRECEDENCE = [
    "Maildoso", "Cheap Inboxes", "Inboxing", "Outreach Today",
    "Reseller", "MailIn", "Microsoft Panel", "Panel",
]


def vendor_of(label: str):
    """tag label -> one of the 8 portal categories, ('ESP_GOOGLE'|'ESP_OUTLOOK',)
    for a weak ESP home tag, or None if not a vendor tag at all."""
    if not label:
        return None
    l = label.strip().lower()
    if "google map" in l:                       # industry/lead-source, NOT infra
        return None
    if re.match(r"^rg\d", l):
        return None
    if any(w in l for w in ("chief", "officer", "director", "campaign")):
        return None
    if "maildoso" in l:
        return "Maildoso"
    if "cheapinbox" in l or "cheap inbox" in l:
        return "Cheap Inboxes"
    if "inboxing" in l:
        return "Inboxing"
    if "outreach today" in l or re.search(r"\botd\b", l):
        return "Outreach Today"
    if "reseller" in l:
        return "Reseller"
    if "mailin" in l or "mail-in" in l or "mail in" in l:
        return "MailIn"
    if "microsoft panel" in l:
        return "Microsoft Panel"
    if re.search(r"\bpanel\b", l):
        return "Panel"
    if l in ("gmail", "i-google"):
        return "Reseller"                       # reseller-google sub-pool
    if l == "google":
        return ("ESP_GOOGLE",)
    if l in ("outlook", "outlook 3", "outlook pp"):
        return ("ESP_OUTLOOK",)
    return None


def log(m):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", file=sys.stderr, flush=True)


def session(key):
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {key}", "User-Agent": UA})
    return s


class _HardTimeout(Exception):
    pass


def _raise_hard_timeout(signum, frame):
    raise _HardTimeout()


def page(s, path, params):
    out, cur = [], None
    while True:
        p = dict(params); p["limit"] = 100
        if cur:
            p["starting_after"] = cur
        r = None
        for attempt in range(8):
            try:
                # HARD wall-clock deadline: requests' read-timeout resets on every
                # received byte, so Cloudflare keepalive-trickle can hang a request
                # indefinitely (observed on large tag-member pages). SIGALRM forces a
                # real abort -> retry with a fresh connection.
                signal.signal(signal.SIGALRM, _raise_hard_timeout)
                signal.alarm(35)
                try:
                    r = s.get(BASE + path, params=p, timeout=(10, 30))
                finally:
                    signal.alarm(0)
            except (requests.ConnectionError, requests.Timeout, _HardTimeout) as e:
                r = None
                log(f"  retry {attempt+1}/8 {path} ({type(e).__name__})")
                time.sleep(1.5 * (attempt + 1)); continue
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 * (attempt + 1)); continue
            break
        if r is None or r.status_code != 200:
            log(f"  WARN {path} -> {r.status_code if r is not None else 'NO-RESPONSE-AFTER-8-RETRIES'} "
                f"{(r.text[:80] if r is not None else '(hard-timeout/conn-fail)')}")
            break
        j = r.json()
        items = j.get("items", []) if isinstance(j, dict) else j
        out += items
        if len(out) > 0 and len(out) % 2000 < 100:
            log(f"  ...{path} +{len(out)} rows")
        cur = j.get("next_starting_after") if isinstance(j, dict) else None
        if not cur or not items:
            break
        time.sleep(0.12)
    return out


def pull_all(keys, checkpoint=None):
    """Return {email_lower: {workspace_slug, esp, cats:set, weak:set, tags:set}}."""
    acc_vendor = {}
    done_ws = set()
    if checkpoint and os.path.exists(checkpoint):
        prev = json.load(open(checkpoint))
        acc_vendor = {e: {**v, "cats": set(v["cats"]), "weak": set(v["weak"]),
                          "tags": set(v["tags"])} for e, v in prev["accts"].items()}
        done_ws = set(prev.get("done_ws", []))
        log(f"resumed checkpoint: {len(acc_vendor)} accts, {len(done_ws)} ws done")

    for envk, slug in KEYMAP.items():
        if slug in done_ws:
            log(f"{slug}: SKIP (checkpointed)"); continue
        key = keys.get("INSTANTLY_KEY_" + envk)
        if not key:
            log(f"{slug}: NO KEY — skip"); continue
        s = session(key)
        tags = page(s, "/custom-tags", {})
        vtags = []
        for t in tags:
            lbl = (t.get("label") or t.get("name") or "")
            v = vendor_of(lbl)
            if v is not None:
                vtags.append((t["id"], lbl, v))
        log(f"{slug}: {len(tags)} tags, {len(vtags)} vendor/ESP tags")
        # First, capture every account's ESP from a single /accounts pull (so
        # 'Unmapped' rows still get esp + workspace and are counted as live).
        allacc = page(s, "/accounts", {})
        for a in allacc:
            e = (a.get("email") or "").strip().lower()
            if not e:
                continue
            pc = a.get("provider_code")
            esp = {1: "otd", 2: "google", 3: "outlook"}.get(pc)
            d = acc_vendor.setdefault(e, {"workspace_slug": slug, "esp": esp,
                                          "cats": set(), "weak": set(), "tags": set()})
            d["workspace_slug"] = slug
            if esp:
                d["esp"] = esp
        # Then membership of each REAL vendor tag. ESP-home tags (plain Outlook /
        # Google — the (tuple,) classifications) are SKIPPED: they are huge (66k+
        # members, the run's dominant cost) AND fully redundant with the `esp` we
        # already captured from /accounts above. ESP-weak is derived from esp at
        # resolve time, not from a redundant membership pull.
        for tid, lbl, v in vtags:
            if isinstance(v, tuple):      # ESP-home tag — skip (derive from esp)
                continue
            members = page(s, "/accounts", {"tag_ids": tid})
            for a in members:
                e = (a.get("email") or "").strip().lower()
                if not e:
                    continue
                d = acc_vendor.setdefault(e, {"workspace_slug": slug, "esp": None,
                                             "cats": set(), "weak": set(), "tags": set()})
                d["cats"].add(v)
                d["tags"].add(lbl)
            log(f"  {lbl!r} -> {len(members)} members")
        done_ws.add(slug)
        if checkpoint:
            json.dump({"accts": {e: {**v, "cats": list(v["cats"]),
                                     "weak": list(v["weak"]), "tags": list(v["tags"])}
                                 for e, v in acc_vendor.items()},
                       "done_ws": list(done_ws)}, open(checkpoint, "w"))
            log(f"{slug}: checkpoint saved ({len(acc_vendor)} accts total)")
    return acc_vendor


def resolve(acc_vendor):
    """Collapse each account's tag set to ONE category by precedence."""
    rows = []
    for e, d in acc_vendor.items():
        cats = d["cats"]
        esp = d.get("esp")
        if cats:
            cat = next((c for c in CATEGORY_PRECEDENCE if c in cats), sorted(cats)[0])
            src, matched = "instantly_tag", sorted(d["tags"])[0] if d["tags"] else None
        elif esp in ("outlook", "google"):
            # No specific vendor tag — only the ESP is known. Weak inference:
            # Outlook ESP without a vendor tag ~ MailIn-class; Google ESP ~ Reseller-class.
            # (OTD ESP is itself the vendor, handled above via the OTD tag; bare OTD is rare.)
            cat = "MailIn" if esp == "outlook" else "Reseller"
            src, matched = "esp_weak", "esp:" + esp
        else:
            cat, src, matched = "Unmapped", "none", None
        rows.append({
            "account_email": e, "workspace_slug": d.get("workspace_slug"),
            "vendor_category": cat, "vendor_source": src, "matched_tag": matched,
            "n_vendor_tags": len(cats), "esp": d.get("esp"),
        })
    return rows


def write_db(rows, run_id):
    import duckdb
    lock_fh = open(WRITE_LOCK, "w")
    log("acquiring warehouse write flock (blocking)...")
    fcntl.flock(lock_fh, fcntl.LOCK_EX)
    try:
        con = duckdb.connect(DB_PATH)
        sv = con.execute("SELECT COALESCE(max(version),-1) FROM core.schema_version").fetchone()[0]
        log(f"connected; schema_version={sv}")
        assert con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='core' AND table_name='sending_account_vendor'"
        ).fetchone()[0] == 1, "DDL 72 not applied — apply 72_sending_account_vendor.sql first"
        con.execute("BEGIN")
        con.execute("DELETE FROM core.sending_account_vendor")
        con.executemany(
            "INSERT INTO core.sending_account_vendor "
            "(account_email,workspace_slug,vendor_category,vendor_source,matched_tag,"
            " n_vendor_tags,esp,_loaded_at,_run_id) VALUES (?,?,?,?,?,?,?,now(),?)",
            [(r["account_email"], r["workspace_slug"], r["vendor_category"],
              r["vendor_source"], r["matched_tag"], r["n_vendor_tags"], r["esp"], run_id)
             for r in rows])
        con.execute("COMMIT")
        n = con.execute("SELECT count(*) FROM core.sending_account_vendor").fetchone()[0]
        log(f"wrote {n} rows to core.sending_account_vendor")
        con.close()
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN); lock_fh.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pull-only", action="store_true")
    ap.add_argument("--out", default="/tmp/sending_account_vendor.json")
    ap.add_argument("--load", help="load a prior pull JSON and write to DB")
    ap.add_argument("--checkpoint", default="/tmp/sav_checkpoint.json")
    args = ap.parse_args()
    run_id = "sav_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.load:
        rows = json.load(open(args.load))
        write_db(rows, run_id); return

    keys = {k: v for k, v in os.environ.items() if k.startswith("INSTANTLY_KEY_")}
    acc = pull_all(keys, checkpoint=args.checkpoint)
    rows = resolve(acc)
    json.dump(rows, open(args.out, "w"))
    log(f"resolved {len(rows)} accounts -> {args.out}")
    from collections import Counter
    c = Counter(r["vendor_category"] for r in rows)
    for cat, n in c.most_common():
        log(f"  {cat}: {n}")
    if not args.pull_only:
        write_db(rows, run_id)


if __name__ == "__main__":
    main()
