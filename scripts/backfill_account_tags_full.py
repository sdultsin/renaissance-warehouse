#!/usr/bin/env python3
"""One-time FULL reconcile of core.account_tags from live Instantly.

WHY: the nightly account_tags pull is INCREMENTAL (entities/account_tags.py) and,
per its own design note, "never re-walks all history" — so inboxes tagged before the
short seed window were never back-filled. Big workspaces (Funding 2 / renaissance-5)
are missing ~35k inboxes' tags ENTIRELY, which makes v_inbox_overview.tags (and the
Inbox-Hub portal / QA) show empty RG tags. Proven 2026-07-01 by cross-checking live
Instantly (tag RG1082 lists 20 F2 inboxes; all 20 = NO tag row in the warehouse).

WHAT: for each workspace, invert EVERY custom-tag via /accounts?tag_ids= (the bounded,
server-side-filtered primitive — NOT the 91k-page /custom-tag-mappings full walk), build
the COMPLETE current per-inbox tag set, and UPSERT into core.account_tags. Additive/
corrective: ON CONFLICT overwrites the touched inbox's row with its complete tag set;
untouched rows are left alone. Deletes nothing.

SAFETY: read-only against Instantly during the long walk (no DB lock held). The DB write
is a short batched upsert at the end, through core.db.connect() which takes the box
warehouse-writer flock (serialized against the 5:30 AM nightly). DRY by default.

USAGE (on the box):
  python3 scripts/backfill_account_tags_full.py --dry [slug ...]     # walk + report, NO write
  python3 scripts/backfill_account_tags_full.py --write [slug ...]   # walk + upsert
  (no slug args = all workspaces)
"""
from __future__ import annotations
import sys, os, re, time, logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

sys.path.insert(0, "/root/renaissance-warehouse")
from core.credentials import load_credentials
from core import db as core_db
from sources.instantly import InstantlyClient, InstantlyError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill_tags")

RG_RE    = re.compile(r"RG[0-9]{3,4}$")                 # granular RG#### (NOT the RGxxxx-yyyy range)
BATCH_RE = re.compile(r"(^|[^A-Za-z])B[0-9]{1,3}")      # B### incl -R / date-prefixed variants
PROV_RE  = re.compile(r"(mailin|otd|outreach today|milkbox|milk box|reseller|cheap inbox|panel|pair [0-9]|inboxing|plusvibe)", re.I)

RUN_ID = "manual_full_reconcile_20260701"
STAGE_DIR = "/root/tagstage_20260701"


def stage_parquet(canon: str, wsid: str, rows: dict, now):
    """Write a workspace's complete tag rows to a staging parquet via an IN-MEMORY duckdb
    (fast, holds NO prod lock). The final upsert reads all staged parquets set-based."""
    import duckdb, os
    os.makedirs(STAGE_DIR, exist_ok=True)
    path = os.path.join(STAGE_DIR, f"{canon}.parquet")
    m = duckdb.connect()
    m.execute("CREATE TABLE s (email VARCHAR, workspace_slug VARCHAR, workspace_uuid VARCHAR, "
              "tags VARCHAR, tags_arr VARCHAR[], n_tags INTEGER, _loaded_at TIMESTAMPTZ, _run_id VARCHAR)")
    m.executemany("INSERT INTO s VALUES (?,?,?,?,?,?,?,?)",
                  [(e, canon, wsid, " | ".join(labs), labs, len(labs), now, RUN_ID)
                   for e, labs in rows.items()])
    m.execute(f"COPY s TO '{path}' (FORMAT PARQUET)")
    m.close()
    return path


def canon_slug_map(con) -> dict:
    m = {}
    try:
        for wsid, slug in con.execute(
            "SELECT DISTINCT workspace_uuid, workspace_slug FROM core.v_account_census_latest "
            "WHERE workspace_uuid IS NOT NULL").fetchall():
            if wsid and slug:
                m[wsid] = slug
    except Exception as e:
        log.warning("census slug map unavailable: %s", e)
    return m


def pull_workspace(key: str):
    """Return (workspace_id, {email: sorted[label,...]}) — COMPLETE current tags."""
    with InstantlyClient(key) as client:
        ws = client.get_current_workspace()
        wsid = ws.get("id")
        if not wsid:
            raise RuntimeError("missing workspace id")
        tags = {t["id"]: (t.get("label") or "").strip()
                for t in client.list_tags(wsid) if t.get("id") and (t.get("label") or "").strip()}
        emap = defaultdict(set)
        lock = __import__("threading").Lock()

        def one(item):
            tid, label = item
            local = []
            for a in client.list_accounts(tag_ids=tid):
                e = (a.get("email") or "").strip().lower()
                if e and "@" in e:
                    local.append(e)
            if local:
                with lock:
                    for e in local:
                        emap[e].add(label)
            return len(local)

        with ThreadPoolExecutor(max_workers=5) as ex:
            done = 0
            for _ in ex.map(one, tags.items()):
                done += 1
                if done % 200 == 0:
                    log.info("    ...%d/%d tags inverted, %d inboxes so far", done, len(tags), len(emap))
        return wsid, {e: sorted(v) for e, v in emap.items()}


def stats(rows):
    n = len(rows)
    rg = sum(1 for _, labs in rows.items() if any(RG_RE.search(l) for l in labs))
    ba = sum(1 for _, labs in rows.items() if any(BATCH_RE.search(l) for l in labs))
    pr = sum(1 for _, labs in rows.items() if any(PROV_RE.search(l) for l in labs))
    return n, rg, ba, pr


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    write = "--write" in sys.argv
    creds = load_credentials()
    keys = creds.instantly_workspace_keys()          # {slug: key}
    if args:
        keys = {s: k for s, k in keys.items() if s in args}
    log.info("workspaces: %s   MODE=%s", list(keys), "WRITE" if write else "DRY")

    now = datetime.now(timezone.utc)
    slugmap = {}
    # read census slug map with a short read-only connection
    ro = core_db.connect(read_only=True) if hasattr(core_db, "connect") else None
    if ro is not None:
        slugmap = canon_slug_map(ro); ro.close()

    staged_files = []
    for slug in sorted(keys):
        t0 = time.time()
        try:
            wsid, rows = pull_workspace(keys[slug])
        except Exception as e:
            log.error("  %s FAILED: %s", slug, e); continue
        canon = slugmap.get(wsid, slug)
        n, rg, ba, pr = stats(rows)
        log.info("  %-16s (%s) inboxes=%d  with_RG=%d  with_batch=%d  with_provider=%d  [%.0fs]",
                 canon, slug, n, rg, ba, pr, time.time() - t0)
        # stage to parquet immediately (survives a kill; no prod lock held)
        p = stage_parquet(canon, wsid, rows, now)
        staged_files.append(p)
        log.info("    staged -> %s", p)

    if not write:
        log.info("DRY complete — %d parquet(s) staged, NO warehouse write. Re-run with --write.", len(staged_files))
        return
    if not staged_files:
        log.error("nothing staged — refusing to write"); return

    # ---- WRITE: ONE set-based bulk upsert from the staged parquets (fast; short lock) ----
    con = core_db.connect()            # takes /root/core/warehouse.write.lock
    try:
        pre = con.execute("SELECT count(*) FROM core.account_tags").fetchone()[0]
        con.execute(f"""
            INSERT INTO core.account_tags
              (email, workspace_slug, workspace_uuid, tags, tags_arr, n_tags, _loaded_at, _run_id)
            SELECT email, workspace_slug, workspace_uuid, tags, tags_arr, n_tags, _loaded_at, _run_id
            FROM read_parquet('{STAGE_DIR}/*.parquet')
            ON CONFLICT (workspace_uuid, email) DO UPDATE SET
              workspace_slug=excluded.workspace_slug, tags=excluded.tags,
              tags_arr=excluded.tags_arr, n_tags=excluded.n_tags,
              _loaded_at=excluded._loaded_at, _run_id=excluded._run_id""")
        post = con.execute("SELECT count(*) FROM core.account_tags").fetchone()[0]
        got = con.execute("SELECT count(*) FROM core.account_tags WHERE _run_id=?", [RUN_ID]).fetchone()[0]
        log.info("WRITE complete — table %d -> %d rows; %d rows carry this run_id", pre, post, got)
    finally:
        con.close()


if __name__ == "__main__":
    main()
