"""moderator_apply.py — the ON-DEMAND substrate apply + re-promote ("apply-now").

The pre-existing apply queue (moderator_engine.process_apply_queue) re-reviews + records to the
approval ledger, but the PHYSICAL warehouse apply was still the nightly (it returned
"substrate-pending"). This module is that missing substrate tooth, made runnable on demand so a
writer can take a ledger-approved change LIVE in minutes instead of waiting for ~03:30 UTC:

  acquire the warehouse-writer flock  (serialize vs the nightly / other writers — queue, never clobber)
    └─ for each queued apply_queue row that has a content-hash-bound approval_ledger pass:
         write its EXACT recorded content to a temp NN_*.sql and apply it with the SAME tooth the
         nightly uses (core.db.apply_ddl_file under the flock) → mark committed/failed/skipped.
  release the flock (close the writer connection)
    └─ re-promote the serving snapshot (reuse /opt/duckdb/bin/publisher.py — the ONE promote
       mechanism) so the change is visible to READERS, not just present in the live DB.

Safety properties (all required by the build):
  * FLOCK-SAFE: the apply binds the box-local warehouse-writer flock via core.db.connect(), so it
    SELF-SEQUENCES behind the nightly / a hand-launched writer (acquire-or-wait, never two writers
    on one DuckDB file). The promote binds the publisher's OWN publish.lock (LOCK_NB) — if a promote
    is already mid-run we DO NOT double-run it; we report apply-succeeded + promote-busy + retry.
  * CONTENT-HASH-BOUND: a queue row is applied ONLY if moderator.approval_ledger has a row for its
    (ddl_version, sha256-of-content) with verdict pass/pass-with-warn. An unledgered/edited queue
    row is refused (status='blocked'), never applied. This is the same authority the apply tooth and
    CI trust — the client is never trusted.
  * FAIL-CLOSED + REVERSIBLE: any apply error rolls back that DDL (apply_ddl_file BEGIN/ROLLBACK)
    and marks the row 'failed' with the error; other rows are unaffected; nothing is half-applied.
    apply_ddl_file is idempotent by version (core.schema_version) so re-running apply-now is safe.

This module is import-light at module scope; heavy deps (duckdb via core.db) load inside run().
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

import moderator_common as mc

# Process-level serialization of apply-now. The service runs route handlers in a threadpool, so two
# concurrent /apply-now calls (two editors, or the standing retry-every-minute rule) would share the
# process-global WAREHOUSE_WRITE_LOCK_HELD env and could TOCTOU-race on it — the second call would
# then re-enter core.db's own flock acquire on a 2nd fd of the same file and SELF-DEADLOCK for the
# full lock-wait window, re-creating the very nightly-starvation the explicit flock fixed. apply-now
# is a single serialized writer by intent, so we just serialize it here: a 2nd concurrent call waits
# (or fails fast) instead of racing. Held for the whole apply (incl. the ~10-min promote) — correct:
# you never want two concurrent applies/promotes anyway.
_APPLY_NOW_LOCK = threading.Lock()
# Max seconds a 2nd concurrent apply-now waits for the first to finish before giving up cleanly.
_APPLY_NOW_LOCK_WAIT_S = int(os.environ.get("MODERATOR_APPLY_NOW_LOCK_WAIT_S", "1500"))

# The serving-snapshot publisher (the ONE promote mechanism, serving-mcp SP-1). Overridable for
# the test profile / a relocation. Default = the prod droplet layout.
PUBLISHER_PY = os.environ.get("MODERATOR_PUBLISHER_PY", "/opt/duckdb/bin/publisher.py")
PUBLISHER_PYTHON = os.environ.get("MODERATOR_PUBLISHER_PYTHON", "/opt/duckdb/venv/bin/python")
# Max seconds to let the promote run before we stop waiting on it (a full ~50GB copy is ~10 min;
# give generous headroom). The apply has already committed by then — promote can be retried.
PROMOTE_TIMEOUT_S = int(os.environ.get("MODERATOR_PROMOTE_TIMEOUT_S", "1200"))
# The warehouse repo checkout co-located on the box — its core/ package owns the apply tooth.
WAREHOUSE_ROOT = mc.WAREHOUSE_ROOT

# The box-local warehouse-writer flock (same file with_warehouse_lock.sh / core.db use). We acquire
# it OURSELVES here (own fd, released in a finally) — NOT via core.db's atexit-only release, which in
# a long-lived service would hold the lock for the whole process lifetime and starve the nightly.
# We mirror with_warehouse_lock.sh: take the flock, export WAREHOUSE_WRITE_LOCK_HELD=1 so core.db's
# in-process net SKIPS (no double-lock/deadlock), run the apply, then release + unset the env.
WRITE_LOCK_PATH = os.environ.get("WAREHOUSE_WRITE_LOCK_PATH", "/root/core/warehouse.write.lock")
WRITE_LOCK_WAIT_S = int(os.environ.get("MODERATOR_WRITE_LOCK_WAIT_S", "1800"))
# Requeue an apply_queue row stuck in 'applying' longer than this (an apply-now that died mid-run /
# lost its PG conn before _finish). Reaped on the next apply-now entry so rows are never stranded.
STALE_APPLYING_MIN = int(os.environ.get("MODERATOR_STALE_APPLYING_MIN", "30"))


# ── the apply tooth (reuses core.db — the SAME path the nightly uses, under the writer flock) ─────
def _import_core_db():
    """Import the canonical core.db (apply_ddl_file + the writer-flock connect) from the box repo
    checkout. Done lazily so the moderator service boots even if the checkout is briefly absent."""
    if WAREHOUSE_ROOT and WAREHOUSE_ROOT not in sys.path:
        sys.path.insert(0, WAREHOUSE_ROOT)
    from core import db as core_db  # noqa: WPS433  (intentional lazy import)
    from core.config import DB_PATH  # noqa: WPS433
    return core_db, DB_PATH


class _WriterFlock:
    """Acquire-or-wait the box-local warehouse-writer flock for the duration of the apply, then
    RELEASE it (own fd, paired acquire/release we fully control). Sets WAREHOUSE_WRITE_LOCK_HELD=1
    inside the critical section so core.db.connect() does NOT re-lock (it would deadlock on a 2nd fd
    / leak the lock via its atexit-only release in this long-lived service), and UNSETS it on exit
    so the next apply-now re-acquires cleanly. Mirrors scripts/with_warehouse_lock.sh exactly.

    If the lock dir is unwritable (local dev / test profile) it degrades to a no-op so the apply
    still runs (the on-disk DuckDB lock is the final backstop). Raises TimeoutError on wait-exceeded
    so we never silently apply un-serialized vs the nightly."""

    def __init__(self):
        self.fd = None
        self._set_env = False

    def __enter__(self):
        try:
            os.makedirs(os.path.dirname(WRITE_LOCK_PATH), exist_ok=True)
            self.fd = os.open(WRITE_LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
        except OSError:
            self.fd = None  # no writable lock location — degrade to no-op (dev/test)
            return self
        deadline = None if WRITE_LOCK_WAIT_S <= 0 else (time.monotonic() + WRITE_LOCK_WAIT_S)
        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if deadline is not None and time.monotonic() >= deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise TimeoutError(
                        f"warehouse-writer flock held by another writer (nightly?) for >"
                        f"{WRITE_LOCK_WAIT_S}s — apply-now queued behind it, gave up; retry later")
                time.sleep(2)
        try:
            os.ftruncate(self.fd, 0)
            os.write(self.fd, f"pid={os.getpid()} acquired_by=moderator.apply-now\n".encode())
        except OSError:
            pass
        if os.environ.get("WAREHOUSE_WRITE_LOCK_HELD") != "1":
            os.environ["WAREHOUSE_WRITE_LOCK_HELD"] = "1"
            self._set_env = True  # only WE may unset it (don't clobber an outer wrapper's flag)
        return self

    def __exit__(self, *exc):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
                os.close(self.fd)
            except OSError:
                pass
            self.fd = None
        if self._set_env:
            os.environ.pop("WAREHOUSE_WRITE_LOCK_HELD", None)
            self._set_env = False
        return False


def _reap_stale_applying() -> int:
    """Requeue apply_queue rows wedged in 'applying' past STALE_APPLYING_MIN (an apply-now that died
    before _finish). Idempotent; safe to call on every apply-now entry. Returns count requeued."""
    try:
        with mc.pg_conn() as c, c.cursor() as cur:
            cur.execute(
                "UPDATE moderator.apply_queue SET status='queued', started_at=NULL "
                "WHERE status='applying' AND started_at < now() - make_interval(mins => %s)",
                (STALE_APPLYING_MIN,))
            return cur.rowcount or 0
    except Exception:
        return 0  # a reaper must never break the apply path


def _ledger_pass_exists(cur, ddl_version, content_sha256) -> bool:
    """Content-hash-bound authority check: does moderator.approval_ledger record a PASS for exactly
    this (ddl_version, content)? Only such rows are ever applied."""
    cur.execute(
        "SELECT 1 FROM moderator.approval_ledger "
        "WHERE ddl_version=%s AND content_sha256=%s "
        "AND verdict IN ('pass','pass-with-warn') LIMIT 1",
        (ddl_version, content_sha256))
    return cur.fetchone() is not None


def _claim_queued(max_items: int) -> list[tuple]:
    """Atomically claim up to max_items queued rows (status queued -> applying) so a concurrent
    apply-now / apply-process can't take the same row. Returns the claimed rows."""
    claimed = []
    for _ in range(max_items):
        with mc.pg_conn() as c:
            with c.transaction():
                with c.cursor() as cur:
                    cur.execute(
                        "SELECT queue_id, ddl_version, sql_file, content_sha256, content, actor "
                        "FROM moderator.apply_queue WHERE status='queued' "
                        "ORDER BY enqueued_at FOR UPDATE SKIP LOCKED LIMIT 1")
                    row = cur.fetchone()
                    if not row:
                        return claimed
                    cur.execute("UPDATE moderator.apply_queue SET status='applying', "
                                "started_at=now() WHERE queue_id=%s", (row[0],))
                    claimed.append(row)
    return claimed


def _finish(queue_id: int, status: str, result: dict) -> bool:
    """Mark a row terminal. Retried a few times so a transient Supavisor blip doesn't strand the
    recorded outcome (the stale-applying reaper is the final backstop if every retry fails)."""
    for attempt in range(3):
        try:
            with mc.pg_conn() as c, c.cursor() as cur:
                cur.execute("UPDATE moderator.apply_queue SET status=%s, finished_at=now(), "
                            "result=%s WHERE queue_id=%s",
                            (status, json.dumps(result, default=str), queue_id))
            return True
        except Exception:
            if attempt == 2:
                mc.log_event("apply_now_finish_failed", queue_id=queue_id, status=status)
                return False
            time.sleep(1)
    return False


def _apply_one(core_db, db_path, conn, row) -> dict:
    """Apply ONE claimed queue row against the open writer connection (flock already held).
    Re-verifies the ledger pass (content-hash) before touching the DB. Never raises — every failure
    is captured as a per-row status so one bad row can't abort the batch."""
    queue_id, ddl_version, sql_file, content_sha256, content, actor = row
    out = {"queue_id": queue_id, "ddl_version": ddl_version, "sql_file": sql_file, "actor": actor}

    # 1) integrity: the recorded content must hash to the recorded sha (tamper / corruption guard).
    actual_sha = hashlib.sha256((content or "").encode("utf-8")).hexdigest()
    if actual_sha != content_sha256:
        out.update(applied=False, status="blocked",
                   detail=f"content sha mismatch (row says {content_sha256[:12]}…, "
                          f"actual {actual_sha[:12]}…) — refusing")
        return out

    # 2) content-hash-bound authority: a ledger PASS must exist for exactly this content. A PG error
    # here fails THIS row only (never the batch), so a transient blip can't strand other rows.
    try:
        with mc.pg_conn() as c, c.cursor() as cur:
            has_pass = _ledger_pass_exists(cur, ddl_version, content_sha256)
    except Exception as e:  # noqa: BLE001
        out.update(applied=False, status="failed",
                   detail=f"authority check unavailable ({type(e).__name__}) — not applied; retry")
        return out
    if not has_pass:
        out.update(applied=False, status="blocked",
                   detail="no content-hash-bound approval_ledger pass for this DDL — "
                          "run `moderator_client.py loop --files <path>` first; not applied")
        return out

    if ddl_version is None:
        out.update(applied=False, status="failed",
                   detail="row has no ddl_version (cannot key core.schema_version) — refusing")
        return out

    # 3) apply with the SAME tooth the nightly uses. apply_ddl_file is idempotent by version
    # (core.schema_version) — a version already applied returns False (already-applied), never
    # re-runs / errors. We write the EXACT recorded content to a temp file named NN_<file> so the
    # apply tooth's own sha256(read_bytes) matches the ledger (CRLF/LF-safe via utf-8 round-trip).
    safe_name = sql_file or f"{ddl_version}_apply_now.sql"
    if not safe_name.startswith(f"{ddl_version}_"):
        safe_name = f"{ddl_version}_{safe_name}"
    with tempfile.TemporaryDirectory(prefix="moderator_apply_") as td:
        from pathlib import Path
        fpath = Path(td) / safe_name
        fpath.write_bytes((content or "").encode("utf-8"))
        try:
            newly = core_db.apply_ddl_file(conn, fpath, version=ddl_version)
            out.update(applied=bool(newly), status="committed",
                       detail=("applied" if newly else
                               "already-applied (version present in core.schema_version) — no-op"))
        except Exception as e:  # noqa: BLE001 — record the failure, keep other rows going.
            out.update(applied=False, status="failed", detail=f"{type(e).__name__}: {e}")
    return out


def _parse_publisher_json(stdout: str) -> dict:
    """The publisher prints a JSON object (pretty-printed) describing the promote. Don't assume it's
    the last line — scan from the end for the last line/blob that parses as a dict carrying a known
    publisher key. Falls back to whole-text parse (it pretty-prints multi-line JSON), then to raw."""
    text = (stdout or "").strip()
    if not text:
        return {}
    # publisher.py emits json.dumps(result, indent=2) — a multi-line object. Try the whole blob first.
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # else scan single lines from the end for a one-line JSON object (the abort/window paths do this).
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and ("promoted" in obj or "error" in obj):
                return obj
        except Exception:
            continue
    return {"raw": text[-500:]}


def _run_publisher(reason: str) -> dict:
    """Re-promote the serving snapshot via the ONE promote mechanism (publisher.py). The publisher
    holds its own LOCK_NB publish.lock: if a promote is already running it returns promoted=False
    with 'another publish holds the lock' — surfaced as promote_busy (apply already landed; the
    change is in the live DB and will be served by the in-flight or next promote). It also REFUSES
    inside the 03:30-05:45 UTC nightly window (default source) — surfaced as promote_refused_window."""
    if not os.path.exists(PUBLISHER_PY):
        return {"promoted": False, "promote_busy": False,
                "error": f"publisher not found at {PUBLISHER_PY}"}
    try:
        proc = subprocess.run(
            [PUBLISHER_PYTHON, PUBLISHER_PY, "--reason", reason],
            capture_output=True, text=True, timeout=PROMOTE_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"promoted": False, "promote_busy": False,
                "error": f"promote exceeded {PROMOTE_TIMEOUT_S}s — still running or stuck; "
                         f"check publish.jsonl. The apply DID land in the live DB."}
    result = _parse_publisher_json(proc.stdout)
    combined = (proc.stdout or "") + (proc.stderr or "") + str(result.get("error", ""))
    result["promote_busy"] = bool(not result.get("promoted")
                                  and "another publish holds the lock" in combined)
    result["promote_refused_window"] = bool(not result.get("promoted")
                                            and "nightly window" in combined.lower())
    if (proc.returncode != 0 and "promoted" not in result
            and not result["promote_busy"] and not result["promote_refused_window"]):
        result.setdefault("error", (proc.stderr or proc.stdout or "publisher failed").strip()[-500:])
    return result


def _snapshot_freshness() -> dict:
    """Best-effort current serving-snapshot id (from the symlink target) + the live-DB max DDL
    version, so the caller sees exactly what readers will now get."""
    fresh = {"snapshot_id": None, "live_schema_version_max": None}
    try:
        fresh["snapshot_id"] = os.path.basename(os.path.realpath(mc.DUCKDB_CURRENT))
    except Exception:
        pass
    try:
        with mc.duckdb_ro() as con:
            fresh["live_schema_version_max"] = con.execute(
                "SELECT max(version) FROM core.schema_version").fetchone()[0]
    except Exception:
        pass
    return fresh


# ── the public entry point ────────────────────────────────────────────────────────────────────
def _promote_acceptable(p: dict | None, promote_requested: bool) -> bool:
    """A promote outcome is 'acceptable for ok' if it succeeded, or it's a benign deferral (busy /
    nightly-window — the apply landed and a promote will serve it), or no promote was requested."""
    if not promote_requested or p is None:
        return True
    return bool(p.get("promoted") or p.get("promote_busy") or p.get("promote_refused_window"))


def apply_now(actor: str, max_items: int = 25, promote: bool = True,
              reason: str | None = None, force_promote: bool = False) -> dict:
    """Public entry. Serializes apply-now in-process (a 2nd concurrent call waits, then fails fast if
    the first is still running) so two callers can't race the writer-flock env flag and self-deadlock.
    Then runs the real apply under the warehouse-writer flock + re-promote."""
    if not _APPLY_NOW_LOCK.acquire(timeout=_APPLY_NOW_LOCK_WAIT_S):
        return {"applied": [], "promote": None, "freshness": _snapshot_freshness(), "actor": actor,
                "ok": False, "error": "another apply-now is already running on the server "
                f"(waited {_APPLY_NOW_LOCK_WAIT_S}s) — retry shortly"}
    try:
        return _apply_now_impl(actor, max_items=max_items, promote=promote, reason=reason,
                               force_promote=force_promote)
    finally:
        _APPLY_NOW_LOCK.release()


def _apply_now_impl(actor: str, max_items: int = 25, promote: bool = True,
                    reason: str | None = None, force_promote: bool = False) -> dict:
    """Drain ledger-approved queued DDLs to the LIVE warehouse under the warehouse-writer flock, then
    re-promote the serving snapshot. Returns a clear, structured result. (Always called with
    _APPLY_NOW_LOCK held — see apply_now.)

    The apply runs inside ONE explicitly-held writer flock (_WriterFlock — acquire-or-wait, then
    RELEASED in a finally; record+apply = one critical section vs the nightly). The flock + the DB
    connection are released BEFORE the promote so the publisher copies a writer-idle, fully-committed
    file. Promote runs only when something was NEWLY applied (a pure no-op/blocked batch skips the
    ~10-min copy). With an EMPTY queue we do NOT auto-fire a ~10-min promote on an accidental re-run;
    force_promote=True is the explicit "promote-only" path (surface an already-applied-but-unserved
    change). promote=False skips the promote entirely (advanced).
    """
    t0 = time.time()
    result = {"applied": [], "promote": None, "freshness": None, "actor": actor, "ok": True}

    reaped = _reap_stale_applying()
    if reaped:
        result["requeued_stale"] = reaped

    claimed = _claim_queued(max_items)
    if not claimed:
        result["detail"] = "no queued ledger-approved DDLs to apply"
        if promote and force_promote:
            result["promote"] = _run_publisher(reason or f"apply-now ({actor}): promote-only")
        elif promote:
            result["detail"] += " (nothing to promote; use --promote-only to force a re-promote)"
        result["freshness"] = _snapshot_freshness()
        result["ok"] = _promote_acceptable(result["promote"], promote and force_promote)
        result["elapsed_s"] = round(time.time() - t0, 1)
        return result

    # Apply under the EXPLICIT warehouse-writer flock (own fd, released in the finally). _WriterFlock
    # acquire-or-waits so we SELF-SEQUENCE behind the nightly / another writer instead of clobbering,
    # and sets WAREHOUSE_WRITE_LOCK_HELD=1 so core.db.connect() does NOT re-lock (deadlock guard).
    core_db, db_path = _import_core_db()
    conn = None
    try:
        with _WriterFlock():
            conn = core_db.connect(db_path)  # read-write; flock already held by us -> no re-lock
            for row in claimed:
                one = _apply_one(core_db, db_path, conn, row)
                result["applied"].append(one)   # record BEFORE _finish so the re-queue guard is correct
                _finish(one["queue_id"], one["status"], one)
            try:
                conn.close()  # close the DB handle INSIDE the flock so the file is quiescent at release
            except Exception:
                pass
            conn = None
    except Exception as e:  # noqa: BLE001 — flock wait-exceeded, import error, connect failure, etc.
        for row in claimed:                     # un-strand: any claimed-but-unprocessed row -> queued
            qid = row[0]
            if not any(a["queue_id"] == qid for a in result["applied"]):
                _finish(qid, "queued", {"detail": "released back to queue (writer unavailable)"})
        result["ok"] = False
        result["error"] = f"{type(e).__name__}: {e}"
        result["freshness"] = _snapshot_freshness()
        result["elapsed_s"] = round(time.time() - t0, 1)
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    applied_n = sum(1 for a in result["applied"] if a.get("applied"))          # NEWLY applied
    committed_n = sum(1 for a in result["applied"] if a.get("status") == "committed")
    failed_n = sum(1 for a in result["applied"] if a.get("status") == "failed")
    # Promote only when something NEWLY applied — an all-already-applied (no-op) or all-blocked batch
    # didn't change the live DB, so it must not trigger a ~10-min ~50GB copy.
    if promote and applied_n:
        result["promote"] = _run_publisher(
            reason or f"apply-now ({actor}): {applied_n} DDL(s) applied live")
    elif promote:
        result["promote"] = {"promoted": False, "promote_busy": False,
                             "promote_refused_window": False,
                             "detail": ("no NEWLY-applied DDLs (already-applied/blocked) — promote "
                                        "skipped; live DB unchanged")}

    result["freshness"] = _snapshot_freshness()
    # ok = no hard failures AND the promote (if requested) succeeded-or-benignly-deferred. A 'blocked'
    # row (e.g. enqueued-but-not-yet-ledgered) is an EXPECTED per-row outcome, not a batch failure;
    # it's reported per-row and surfaces as a 207, not an error.
    result["ok"] = (failed_n == 0) and _promote_acceptable(result["promote"], promote)
    result["committed"] = committed_n
    result["newly_applied"] = applied_n
    result["elapsed_s"] = round(time.time() - t0, 1)
    return result
