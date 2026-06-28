#!/usr/bin/env python3
"""Backfill IM outbound-reply bodies for renaissance-4 (Funding 1 / Samuel) and
renaissance-5 (Funding 2 / Ido).

WHY: in `raw_pipeline_conversation_messages`, the ue_type=3 rows (IM manual replies
sent from the Instantly unibox) for these two workspaces have `body_text` 100% NULL —
only a 60-char `content_preview` was ever mirrored from pipeline-supabase. The inbound
(ue_type=2) prospect bodies are already ~98% present, so this completes the IM SIDE of
each thread for the two biggest funding desks. It does NOT change the IM reply-coverage
buckets (those read inbound prospect text only) — it makes the "full email thread"
view readable for F1/F2.

SAFETY / SHAPE:
  * Read-only on Instantly — only GET /api/v2/emails/{id}.
  * Idempotent + non-destructive — the apply phase fills body_text ONLY where it is
    currently NULL/empty (never overwrites real text), and writes every touched id to a
    manifest file for trivial rollback.
  * Two phases so the slow network fetch never holds the single-writer flock:
      fetch  — GET each target email, store the VERBATIM body.html to a parquet (no DB write).
      apply  — read parquet, extract the clean top message, UPDATE under the writer flock.
    Re-running apply with cleaning tweaks needs no re-fetch (raw html is persisted).

USAGE (on the droplet):
  export INSTANTLY_BACKFILL_KEY_RENAISSANCE_4=...   # Funding 1 api_key_new_2026_04_13
  export INSTANTLY_BACKFILL_KEY_RENAISSANCE_5=...   # Funding 2 api_key_new_2026_04_13
  .venv/bin/python scripts/backfill_im_outbound_bodies.py fetch \
      --out /root/core/backfill_im_bodies.parquet
  WAREHOUSE_LOCK_WAIT_S=3600 scripts/with_warehouse_lock.sh \
      .venv/bin/python scripts/backfill_im_outbound_bodies.py apply \
      --in /root/core/backfill_im_bodies.parquet
"""
from __future__ import annotations

import argparse
import datetime as dt
import html as htmllib
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import httpx

WAREHOUSE = os.environ.get("WAREHOUSE_DB", "/root/core/warehouse.duckdb")
BASE_URL = "https://api.instantly.ai/api/v2"
# Instantly fingerprint-blocks the default python-httpx UA; mimic curl (matches sources/instantly.py).
_UA = "curl/8.4.0"
_TIMEOUT = 60.0

# warehouse slug -> env var holding that workspace's Instantly api key
KEY_ENV = {
    "renaissance-4": "INSTANTLY_BACKFILL_KEY_RENAISSANCE_4",
    "renaissance-5": "INSTANTLY_BACKFILL_KEY_RENAISSANCE_5",
}
TARGET_SLUGS = tuple(KEY_ENV)

# --- HTML -> clean top message ------------------------------------------------
# Instantly composes outbound replies as: <top message> then the quoted history wrapped
# in a reply-timestamp-box / reply-body-conatiner block (Gmail forwards use gmail_quote /
# <blockquote>). Cut at the first such marker, then strip tags. If no marker, keep all.
_QUOTE_MARKER = re.compile(r'reply-timestamp-box|reply-body-conatiner|gmail_quote|<blockquote', re.I)
_BODY_TAG = re.compile(r'<body[^>]*>(.*)</body>', re.I | re.S)
_BR = re.compile(r'(?i)<br\s*/?>')
_BLOCK_CLOSE = re.compile(r'(?i)</(div|p|tr|li|h[1-6]|table)>')
_TAGS = re.compile(r'<[^>]+>')
_WS_EOL = re.compile(r'[ \t]+\n')
_MULTI_NL = re.compile(r'\n{3,}')


def clean_html(raw_html: str | None, fallback_text: str | None = None) -> str:
    """Extract the clean top message (sans quoted history) from an Instantly email body."""
    if not raw_html or not raw_html.strip():
        return (fallback_text or "").strip()
    m = _BODY_TAG.search(raw_html)
    s = m.group(1) if m else raw_html
    qm = _QUOTE_MARKER.search(s)
    if qm:
        # cut at the START of the enclosing tag (e.g. <div class="reply-timestamp-box">),
        # not at the marker text, so no partial opening tag leaks past the tag-strip.
        cut = s.rfind("<", 0, qm.start())
        s = s[: cut if cut != -1 else qm.start()]
    s = _BR.sub("\n", s)
    s = _BLOCK_CLOSE.sub("\n", s)
    s = _TAGS.sub("", s)
    s = htmllib.unescape(s)
    s = _WS_EOL.sub("\n", s)
    s = _MULTI_NL.sub("\n\n", s)
    cleaned = s.strip()
    # Defensive: if cutting left nothing (rare layout), fall back to the full strip.
    if not cleaned and (m or raw_html):
        s2 = _TAGS.sub("", _BLOCK_CLOSE.sub("\n", _BR.sub("\n", m.group(1) if m else raw_html)))
        cleaned = _MULTI_NL.sub("\n\n", htmllib.unescape(s2)).strip()
    return cleaned or (fallback_text or "").strip()


# --- fetch phase --------------------------------------------------------------
def _targets(con) -> list[tuple[str, str]]:
    rows = con.execute(
        """
        SELECT id, workspace_id
        FROM raw_pipeline_conversation_messages
        WHERE workspace_id IN ('renaissance-4','renaissance-5')
          AND ue_type = 3
          AND (body_text IS NULL OR length(trim(body_text)) = 0)
        """
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _get_email(client: httpx.Client, email_id: str) -> tuple[int, dict]:
    """GET one email, retrying transient failures + 429 indefinitely (per the standing
    rate-limit rule: retry until it works). Returns (http_status, json|{})."""
    backoff = 1.0
    while True:
        try:
            r = client.get(f"/emails/{email_id}")
        except (httpx.TransportError, httpx.TimeoutException):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code == 404:
            return 404, {}
        if r.status_code != 200:
            # Unexpected client error — record it, don't spin forever.
            return r.status_code, {}
        try:
            return 200, r.json()
        except ValueError:
            return 200, {}


def run_fetch(out_path: str, workers: int) -> int:
    con = duckdb.connect(WAREHOUSE, read_only=True)
    targets = _targets(con)
    con.close()
    print(f"[fetch] {len(targets):,} target ue_type=3 rows (body NULL) across {len(TARGET_SLUGS)} workspaces", flush=True)

    # restartable: skip ids already in the out parquet
    done: set[str] = set()
    if os.path.exists(out_path):
        d = duckdb.connect()
        try:
            done = {r[0] for r in d.execute(f"SELECT id FROM read_parquet('{out_path}')").fetchall()}
        except Exception:
            done = set()
        d.close()
        print(f"[fetch] resume: {len(done):,} already fetched", flush=True)
    todo = [(i, w) for (i, w) in targets if i not in done]
    if not todo:
        print("[fetch] nothing to do", flush=True)
        return 0

    keys = {}
    for slug, env in KEY_ENV.items():
        k = os.environ.get(env)
        if not k:
            sys.exit(f"missing required env {env} (Instantly key for {slug})")
        keys[slug] = k
    clients = {
        slug: httpx.Client(base_url=BASE_URL, timeout=_TIMEOUT,
                           headers={"Authorization": f"Bearer {keys[slug]}", "User-Agent": _UA})
        for slug in TARGET_SLUGS
    }

    results: list[dict] = []
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    n_ok = n_miss = 0

    def work(item):
        email_id, slug = item
        status, js = _get_email(clients[slug], email_id)
        body = (js.get("body") or {}) if isinstance(js, dict) else {}
        return {
            "id": email_id,
            "workspace_id": slug,
            "html": body.get("html") or "",
            "text": body.get("text") or "",
            "content_preview": js.get("content_preview") if isinstance(js, dict) else None,
            "timestamp_email": str(js.get("timestamp_email")) if isinstance(js, dict) else None,
            "http_status": status,
            "fetched_at": now,
        }

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, it) for it in todo]
        for n, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            results.append(rec)
            if rec["http_status"] == 200 and (rec["html"] or rec["text"]):
                n_ok += 1
            else:
                n_miss += 1
            if n % 500 == 0:
                rate = n / max(time.time() - t0, 1e-9)
                print(f"[fetch] {n:,}/{len(todo):,}  ok={n_ok:,} miss={n_miss:,}  {rate:.1f}/s", flush=True)
    for c in clients.values():
        c.close()

    # merge with any prior results and write the full parquet (atomic-ish via temp rename)
    d = duckdb.connect()
    d.register("new_rows", _as_arrow(results))
    if done:
        d.execute(
            f"COPY (SELECT * FROM read_parquet('{out_path}') UNION ALL BY NAME SELECT * FROM new_rows) "
            f"TO '{out_path}.tmp' (FORMAT parquet)"
        )
    else:
        d.execute(f"COPY (SELECT * FROM new_rows) TO '{out_path}.tmp' (FORMAT parquet)")
    d.close()
    os.replace(f"{out_path}.tmp", out_path)
    print(f"[fetch] wrote {out_path}: +{len(results):,} rows (ok={n_ok:,} miss={n_miss:,})", flush=True)
    return n_miss


def _as_arrow(records: list[dict]):
    import pyarrow as pa
    if not records:
        cols = ["id", "workspace_id", "html", "text", "content_preview", "timestamp_email", "http_status", "fetched_at"]
        return pa.table({c: [] for c in cols})
    keys = list(records[0].keys())
    return pa.table({k: [r.get(k) for r in records] for k in keys})


# --- apply phase --------------------------------------------------------------
def run_apply(in_path: str) -> None:
    if not os.path.exists(in_path):
        sys.exit(f"input parquet not found: {in_path}")
    stage = duckdb.connect()
    rows = stage.execute(
        f"SELECT id, workspace_id, html, text FROM read_parquet('{in_path}') WHERE http_status = 200"
    ).fetchall()
    stage.close()
    cleaned = []
    for email_id, slug, html, text in rows:
        body = clean_html(html, text)
        if body:
            cleaned.append((email_id, body))
    print(f"[apply] {len(rows):,} fetched rows -> {len(cleaned):,} with non-empty cleaned body", flush=True)
    if not cleaned:
        print("[apply] nothing to write", flush=True)
        return

    con = duckdb.connect(WAREHOUSE)  # read-write; the with_warehouse_lock.sh wrapper holds the flock
    con.execute("CREATE TEMP TABLE _bf(id VARCHAR, body VARCHAR)")
    con.executemany("INSERT INTO _bf VALUES (?, ?)", cleaned)
    # idempotent + non-destructive: only fill rows still NULL/empty
    before = con.execute(
        """SELECT count(*) FROM raw_pipeline_conversation_messages m JOIN _bf b ON m.id=b.id
           WHERE m.ue_type=3 AND (m.body_text IS NULL OR length(trim(m.body_text))=0)"""
    ).fetchone()[0]
    con.execute("BEGIN")
    con.execute(
        """
        UPDATE raw_pipeline_conversation_messages AS m
        SET body_text = b.body
        FROM _bf AS b
        WHERE m.id = b.id
          AND m.ue_type = 3
          AND (m.body_text IS NULL OR length(trim(m.body_text)) = 0)
        """
    )
    con.execute("COMMIT")
    after_null = con.execute(
        """SELECT count(*) FROM raw_pipeline_conversation_messages
           WHERE workspace_id IN ('renaissance-4','renaissance-5') AND ue_type=3
             AND (body_text IS NULL OR length(trim(body_text))=0)"""
    ).fetchone()[0]

    # manifest for rollback
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest = os.path.join(os.path.dirname(os.path.abspath(WAREHOUSE)), f"backfill_im_bodies_manifest_{ts}.txt")
    ids = con.execute(
        """SELECT m.id FROM raw_pipeline_conversation_messages m JOIN _bf b ON m.id=b.id
           WHERE m.ue_type=3 AND m.body_text = b.body"""
    ).fetchall()
    with open(manifest, "w") as f:
        f.write("\n".join(r[0] for r in ids) + "\n")
    con.execute("CHECKPOINT")
    con.close()
    print(f"[apply] filled {before:,} rows; F1/F2 ue_type=3 still-NULL now {after_null:,}", flush=True)
    print(f"[apply] manifest: {manifest} ({len(ids):,} ids). Rollback: UPDATE ... SET body_text=NULL WHERE id IN (manifest).", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--out", default="/root/core/backfill_im_bodies.parquet")
    f.add_argument("--workers", type=int, default=6)
    a = sub.add_parser("apply")
    a.add_argument("--in", dest="in_path", default="/root/core/backfill_im_bodies.parquet")
    args = ap.parse_args()
    if args.cmd == "fetch":
        run_fetch(args.out, args.workers)
    elif args.cmd == "apply":
        run_apply(args.in_path)


if __name__ == "__main__":
    main()
