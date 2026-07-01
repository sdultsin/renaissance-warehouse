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
  * APPLY==COMMIT (no DB-ahead-of-repo drift): a row applies ONLY if its EXACT content is committed
    at origin/main as sql/ddl/<file> (MODERATOR_REQUIRE_COMMITTED=enforce, default), so apply-now
    can never take the live DB ahead of the repo (the v96 / 2026-06-20 drift class). It also REFUSES
    a version-number collision (a version already applied under a different file) instead of silently
    no-op'ing it — forcing a fresh number (next-version) rather than swallowing the writer's DDL.
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
import re
import subprocess
import sys
import tempfile
import threading
import time

import moderator_common as mc
import moderator_integrity_gate as _integrity_gate  # W1 post-apply integrity gate (dup unique _key)

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
# Max seconds to let the promote run before we stop waiting on it. subprocess.run(timeout=) KILLS
# the publisher child at this ceiling, so it must comfortably exceed the REAL copy time: the serving
# snapshot is a full byte-copy of the warehouse — ~183GB ≈ 22 min as of 2026-07-01, and it grows —
# so the old 1200s default killed every apply-now promote. The apply has already committed by then —
# a killed promote can be retried.
PROMOTE_TIMEOUT_S = int(os.environ.get("MODERATOR_PROMOTE_TIMEOUT_S", "3600"))
# The warehouse repo checkout co-located on the box — its core/ package owns the apply tooth.
WAREHOUSE_ROOT = mc.WAREHOUSE_ROOT

# ── apply==commit (refuse-if-uncommitted): bind the live apply to the COMMITTED repo ────────────
# The 2026-06-20 box-reconcile + the v96 incident (David applied a DDL via apply-now that was never
# in the repo, leaving the live DB AHEAD of origin/main — a drift `git status` can't see) are the
# exact failure this prevents. The content we apply comes from moderator.apply_queue (PG); nothing
# tied it to a COMMITTED repo file. Here we bind them: a queued DDL applies ONLY if its EXACT content
# (by sha256) is committed at origin/main as sql/ddl/<file>. So apply-now can take live ONLY what is
# already merged + pushed — enforcing GIT-SYNC-DISCIPLINE.md (edit -> PR -> merge to origin/main ->
# box pulls) at the schema chokepoint, for EVERY writer (incl. humans who bypass the bus ddl-number
# lock). Modes (MODERATOR_REQUIRE_COMMITTED): "enforce"/"1" (default) = REFUSE uncommitted
# (status='blocked'); "warn" = apply but flag it (the drift guard then catches the lapse); "off"/"0"
# = skip entirely (local dev / test). Fail-CLOSED: if git/the repo can't be consulted, "enforce"
# refuses (never apply on an unverifiable ref).
_REQUIRE_COMMITTED = os.environ.get("MODERATOR_REQUIRE_COMMITTED", "enforce").strip().lower()
_GIT_REF = os.environ.get("MODERATOR_APPLY_GIT_REF", "origin/main")  # the authoritative code ref
# Best-effort `git fetch` of the ref's branch before the check so a just-merged PR is visible. Tolerates
# fetch failure (uses the last-fetched ref; we fail toward REFUSE below, never apply on a stale ref we
# couldn't confirm matches). Disable with MODERATOR_APPLY_FETCH=0.
_APPLY_FETCH = os.environ.get("MODERATOR_APPLY_FETCH", "1") not in ("0", "false", "False", "")
_GIT_TIMEOUT_S = int(os.environ.get("MODERATOR_APPLY_GIT_TIMEOUT_S", "30"))

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

# ── box-sync drift guard (the silent-swallow fix) ────────────────────────────────────────────────
# pull_first runs _git_pull_ff() to sync the box checkout to origin/main BEFORE the nightly rebuilds
# from it. That helper is best-effort + NEVER raises — so when the box sits on a feature branch / dirty
# / diverged tree (the 2026-06-27 fleet-wide divergence), the ff-only pull silently FAILS, apply-now
# proceeds anyway, and the nightly keeps rebuilding from STALE code, silently dropping merged DDLs.
# Nobody notices for days (it hid this exact failure for ~a week). This guard turns that silent swallow
# into a LOUD, BLOCKING failure: after pull_first, ASSERT the box is on clean origin/main; if not,
# auto-preserve the box state, alert #cc-sam, and BLOCK the apply (fail-closed) — never apply live while
# the nightly stays stale. Override (advanced / local dev): MODERATOR_BOX_DRIFT_GUARD=off.
_BOX_DRIFT_GUARD = os.environ.get("MODERATOR_BOX_DRIFT_GUARD", "enforce").strip().lower() \
    not in ("off", "0", "false", "no", "")
# Where to drop a human-readable snapshot of a drifted tree (best-effort recovery convenience).
_BOX_DRIFT_RESCUE_DIR = os.environ.get("MODERATOR_BOX_DRIFT_RESCUE_DIR", "/root/box-drift-rescue")
# Slack alert helper — the SAME durable channel + script the hourly divergence guard uses.
_ALERT_SLACK_PY = os.environ.get("MODERATOR_ALERT_SLACK_PY",
                                 os.path.join(WAREHOUSE_ROOT or "", "scripts", "alert_slack.py"))
# Runtime-generated paths that are NOT real writer drift (mirrors warehouse_git_divergence_guard.sh's
# noise filter) so the guard never false-blocks on logs / snapshots / backups / env / parquet / wt dir.
_BOX_NOISE_RE = re.compile(
    r'(^|/)(logs/|data/|\.worktrees/)|\.duckdb(\.wal)?$|\.bak[-.]|(^|/)\.env($|\.)|\.log$|\.parquet$|\.tmp$|\.beat$')


# ── repo-commit binding helpers (apply==commit) ──────────────────────────────────────────────────
def _git(args: list, timeout: int | None = None) -> "subprocess.CompletedProcess":
    """Run `git -C <repo> <args>` capturing raw bytes (no text decode — blob bytes must hash exactly)."""
    return subprocess.run(["git", "-C", WAREHOUSE_ROOT, *args],
                          capture_output=True, timeout=(timeout or _GIT_TIMEOUT_S))


def _origin_ref_sha() -> str | None:
    try:
        p = _git(["rev-parse", "--short", _GIT_REF])
        return p.stdout.decode("utf-8", "replace").strip() if p.returncode == 0 else None
    except Exception:
        return None


def _eol_norm(b: bytes) -> bytes:
    return b.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def _committed_in_repo(sql_file: str | None, content_sha256: str, content: str | None = None) -> dict:
    """Is the content of this DDL committed at _GIT_REF (origin/main) as sql/ddl/<sql_file>? Returns
    {committed, verifiable, ref_sha, detail}. `verifiable=False` means git/the repo couldn't be
    consulted at all — under 'enforce' the caller then REFUSES (fail-closed), so we never apply against
    a ref we couldn't confirm. The blob is hashed with sha256 of its raw BYTES (NOT git's sha1 object
    id) to match moderator's content_sha256 convention. We first try an EXACT byte match; if `content`
    is given we then fall back to an EOL-NORMALIZED match, so a writer whose git normalized line
    endings (core.autocrlf -> an LF blob vs a CRLF working tree) isn't false-blocked on the same SQL."""
    if not sql_file:
        return {"committed": False, "verifiable": True, "ref_sha": None,
                "detail": "queue row has no sql_file — cannot locate a committed repo file"}
    if not WAREHOUSE_ROOT or not os.path.isdir(os.path.join(WAREHOUSE_ROOT, ".git")):
        return {"committed": False, "verifiable": False, "ref_sha": None,
                "detail": f"no git repo at WAREHOUSE_REPO_ROOT={WAREHOUSE_ROOT!r} — cannot verify commit"}
    if _APPLY_FETCH:
        try:
            branch = _GIT_REF.split("/", 1)[1] if "/" in _GIT_REF else _GIT_REF
            _git(["fetch", "origin", branch])  # best-effort; a failure just leaves the last-fetched ref
        except Exception:
            pass  # fail toward REFUSE below if the (stale) ref doesn't match — never apply unverified
    repo_path = f"sql/ddl/{sql_file}"
    try:
        p = _git(["cat-file", "blob", f"{_GIT_REF}:{repo_path}"])
    except Exception as e:  # noqa: BLE001 — git missing / timeout / repo error
        return {"committed": False, "verifiable": False, "ref_sha": _origin_ref_sha(),
                "detail": f"git cat-file failed ({type(e).__name__}) — cannot verify commit"}
    ref_sha = _origin_ref_sha()
    if p.returncode != 0:
        return {"committed": False, "verifiable": True, "ref_sha": ref_sha,
                "detail": f"{repo_path} is NOT committed at {_GIT_REF} — commit + PR + box-pull first "
                          f"(GIT-SYNC-DISCIPLINE.md), then re-run apply-now"}
    blob_sha = hashlib.sha256(p.stdout).hexdigest()
    if blob_sha == content_sha256:
        return {"committed": True, "verifiable": True, "ref_sha": ref_sha,
                "detail": f"content committed at {_GIT_REF} ({ref_sha})"}
    # EOL-normalized fallback: same SQL modulo CRLF/LF still counts as committed (re-applying the LF
    # repo copy produces the identical schema). Only collapses line endings — different SQL still fails.
    if content is not None and hashlib.sha256(_eol_norm(p.stdout)).hexdigest() == \
            hashlib.sha256(_eol_norm(content.encode("utf-8"))).hexdigest():
        return {"committed": True, "verifiable": True, "ref_sha": ref_sha,
                "detail": f"content committed at {_GIT_REF} ({ref_sha}) (matched modulo line-endings)"}
    return {"committed": False, "verifiable": True, "ref_sha": ref_sha,
            "detail": f"{repo_path} at {_GIT_REF} has DIFFERENT content (committed sha "
                      f"{blob_sha[:12]}…, applying {content_sha256[:12]}…) — the content you're "
                      f"applying isn't what's merged; commit the EXACT content + box-pull, re-run"}


def _git_pull_ff() -> dict:
    """Box-side `git pull --ff-only origin <ref-branch>` so the co-located checkout matches origin/main
    BEFORE we apply. Essential for the one-command 'ship' flow: it makes the working-tree sql/ddl carry
    the just-merged DDL so the NIGHTLY rebuild keeps it (else the next from-repo rebuild silently drops
    a change applied only transiently — the v96 nightly-drop risk). FF-only + box-never-commits = no
    conflicts; on the box this is the sanctioned deploy path (pull, never push). Best-effort + reported;
    never raises (apply-now still runs — Fix A's commit check is the backstop)."""
    out = {"pulled": False, "detail": None, "head_before": _origin_ref_sha()}
    if not WAREHOUSE_ROOT or not os.path.isdir(os.path.join(WAREHOUSE_ROOT, ".git")):
        out["detail"] = "no git repo at WAREHOUSE_REPO_ROOT — skipped"
        return out
    branch = _GIT_REF.split("/", 1)[1] if "/" in _GIT_REF else _GIT_REF
    try:
        p = _git(["pull", "--ff-only", "origin", branch], timeout=120)
        out["pulled"] = p.returncode == 0
        out["detail"] = (p.stdout.decode("utf-8", "replace") + p.stderr.decode("utf-8", "replace")).strip()[-300:]
    except Exception as e:  # noqa: BLE001
        out["detail"] = f"git pull failed ({type(e).__name__}: {e})"
    try:
        out["head_after"] = _git(["rev-parse", "--short", "HEAD"]).stdout.decode().strip()
    except Exception:
        out["head_after"] = None
    return out


def _box_is_noise(path: str) -> bool:
    """True for a runtime-generated box path that is NOT real writer drift (logs/snapshots/backups/env)."""
    return bool(_BOX_NOISE_RE.search(path.strip().strip('"')))


def _box_sync_status() -> dict:
    """Read-only: is the box checkout on CLEAN origin/main — the ONLY sanctioned runtime state? Returns
    {clean, branch, head, ahead, behind, dirty, dirty_files, detail}. origin/main was just fetched by
    _git_pull_ff(); we compare against it. FAIL-CLOSED: if git can't be consulted we return clean=False
    (never declare a box we couldn't verify 'clean'). Never raises."""
    st = {"clean": False, "branch": None, "head": None, "ahead": None, "behind": None,
          "dirty": None, "dirty_files": [], "detail": None}
    want_branch = _GIT_REF.split("/", 1)[1] if "/" in _GIT_REF else _GIT_REF  # origin/main -> main
    try:
        st["branch"] = _git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.decode("utf-8", "replace").strip()
        st["head"] = _git(["rev-parse", "--short", "HEAD"]).stdout.decode("utf-8", "replace").strip()
        st["ahead"] = int(_git(["rev-list", "--count", f"{_GIT_REF}..HEAD"]).stdout.decode().strip() or "0")
        st["behind"] = int(_git(["rev-list", "--count", f"HEAD..{_GIT_REF}"]).stdout.decode().strip() or "0")
        porc = _git(["status", "--porcelain"]).stdout.decode("utf-8", "replace").splitlines()
        dirty = [ln for ln in porc if ln.strip() and not _box_is_noise(ln[3:] if len(ln) > 3 else ln)]
        st["dirty"] = len(dirty)
        st["dirty_files"] = [(ln[3:] if len(ln) > 3 else ln) for ln in dirty][:20]
        st["clean"] = (st["branch"] == want_branch and st["ahead"] == 0
                       and st["behind"] == 0 and st["dirty"] == 0)
        st["detail"] = (f"branch={st['branch']}(want {want_branch}) head={st['head']} "
                        f"ahead={st['ahead']} behind={st['behind']} dirty={st['dirty']}")
    except Exception as e:  # noqa: BLE001 — git missing / timeout / repo error -> fail-closed
        st["detail"] = f"box-sync check failed ({type(e).__name__}: {e}) — treating as NOT clean"
        st["clean"] = False
    return st


def _auto_preserve_box_drift(drift: dict) -> dict:
    """Best-effort, NON-DESTRUCTIVE preservation of a drifted box. The guard BLOCKS (it never resets),
    so the working tree is left fully intact on disk — this only captures a recoverable snapshot so a
    later human reconcile loses nothing and has an easy restore point: status + diff written to a
    timestamped dir, and the dirty tracked state saved as a git stash-commit under a named ref. Never
    raises, never mutates the working tree."""
    out = {"snapshot_dir": None, "stash_ref": None, "detail": None}
    try:
        head = drift.get("head") or "unknown"
        base = os.path.join(_BOX_DRIFT_RESCUE_DIR, head)
        snap, n = base, 1
        while os.path.exists(snap):
            n += 1
            snap = f"{base}-{n}"
        os.makedirs(snap, exist_ok=True)
        for name, args in (("status.txt", ["status", "-sb"]),
                           ("branches.txt", ["branch", "-vv"]),
                           ("working_tree.diff", ["diff", "HEAD"])):
            try:
                with open(os.path.join(snap, name), "wb") as fh:
                    fh.write(_git(args, timeout=60).stdout)
            except Exception:
                pass
        out["snapshot_dir"] = snap
        try:  # capture tracked dirty changes into a stash commit object (no working-tree mutation) + ref it
            sha = _git(["stash", "create", "moderator box-drift auto-preserve"],
                       timeout=60).stdout.decode("utf-8", "replace").strip()
            if sha:
                ref = f"refs/box-drift-rescue/{head}-{n}"
                _git(["update-ref", ref, sha], timeout=30)
                out["stash_ref"] = f"{ref} ({sha[:12]})"
        except Exception:
            pass
        out["detail"] = "box state preserved (working tree left intact; recover via snapshot_dir / stash_ref)"
    except Exception as e:  # noqa: BLE001
        out["detail"] = f"auto-preserve best-effort failed ({type(e).__name__}: {e}) — tree still intact on box"
    return out


def _alert_cc_sam(text: str) -> bool:
    """Fire a Slack alert to the warehouse alert channel (#cc-sam) via the SAME scripts/alert_slack.py
    the hourly divergence guard uses. alert_slack reads SLACK_TOKEN/SLACK_ALERT_CHANNEL from the env,
    then falls back to the repo .env — so it works from the service even though the unit env omits them.
    Never raises."""
    try:
        if not _ALERT_SLACK_PY or not os.path.exists(_ALERT_SLACK_PY):
            return False
        return subprocess.run([sys.executable, _ALERT_SLACK_PY, text],
                              capture_output=True, timeout=30).returncode == 0
    except Exception:
        return False


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

    # 2.5) apply==commit (refuse-if-uncommitted): the EXACT content must be committed at origin/main
    # as sql/ddl/<file>, so apply-now can NEVER take the live DB ahead of the repo (the v96 /
    # 2026-06-20 drift class). 'enforce' (default) blocks; 'warn' applies but flags; 'off' skips.
    if _REQUIRE_COMMITTED not in ("off", "0"):
        commit = _committed_in_repo(sql_file, content_sha256, content)
        out["committed"] = commit["committed"]
        out["repo_ref"] = commit.get("ref_sha")
        if not commit["committed"]:
            if _REQUIRE_COMMITTED == "warn":
                out["commit_warning"] = commit["detail"]  # apply proceeds; the drift guard flags it
            else:
                out.update(applied=False, status="blocked",
                           detail="apply==commit: " + commit["detail"])
                return out

    # 2.7) version-collision guard: REFUSE (don't silently no-op) if this version is already applied
    # in core.schema_version under a DIFFERENT sql_file. apply_ddl_file is idempotent BY VERSION, so a
    # stale-local number (David's v96 was picked while the repo was already at 113) would otherwise be
    # swallowed as "already-applied" and the writer's real DDL would never land. A loud block forces a
    # fresh number (moderator_client.py next-version). A genuine same-file re-run still no-ops below.
    # NOTE: this intentionally also blocks a same-number FILE-RENAME (numbers are immutable + never
    # reused here, so "this number is taken — get a fresh one" is the right answer). We can't tell a
    # rename from a clobber without a content hash on core.schema_version (the filed David follow-up),
    # so we fail safe toward refusing; the apply would be a no-op anyway, so nothing is lost.
    try:
        existing = conn.execute(
            "SELECT sql_file FROM core.schema_version WHERE version = ?", [ddl_version]).fetchone()
    except Exception:
        existing = None  # table not created yet (fresh DB) — apply_ddl_file creates it
    if existing and existing[0] and os.path.basename(str(existing[0])) != (sql_file or ""):
        out.update(applied=False, status="blocked",
                   detail=f"version {ddl_version} already applied as '{existing[0]}' (you are applying "
                          f"'{sql_file}') — version COLLISION; allocate a fresh number with "
                          f"`moderator_client.py next-version` and re-gate. Refusing to silently no-op.")
        return out

    # 3) apply with the SAME tooth the nightly uses. apply_ddl_file is idempotent by version
    # (core.schema_version) — a version already applied returns False (already-applied), never
    # re-runs / errors. We write the EXACT recorded content to a temp file named NN_<file> so the
    # apply tooth's own sha256(read_bytes) matches the ledger (CRLF/LF-safe via utf-8 round-trip).
    safe_name = sql_file or f"{ddl_version}_apply_now.sql"
    if not safe_name.startswith(f"{ddl_version}_"):
        safe_name = f"{ddl_version}_{safe_name}"
    # PRE-APPLY catalog snapshot for the W1 CATALOG-DELTA gate (FIX 1). apply_ddl_file COMMITs
    # internally, so we MUST capture the per-table unique-on-_key index counts on THIS open writer conn
    # BEFORE the apply call; the post-check then fails only on a table whose count INCREASED (a NEWLY
    # introduced duplicate), regardless of how the index DDL was written (USING art, comments — the
    # QA-D bypass), because the snapshot reads the live catalog (duckdb_indexes()), not the SQL text.
    # Best-effort: a snapshot failure degrades to {} which the post-check treats as all-zero BEFORE,
    # i.e. it falls back to the absolute "any dup is newly-introduced" stance (fail-closed, never a
    # false-pass). Captured only when something will actually be applied is not knowable yet (apply is
    # idempotent-by-version), so we always snapshot here — it is a cheap catalog read.
    try:
        pre_key_index_snapshot = _integrity_gate.snapshot_key_index_counts(conn)
    except Exception:
        pre_key_index_snapshot = {}
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

    # 4) POST-APPLY INTEGRITY GATE (W1): apply_ddl_file already COMMITted (db.py), so a failed
    # post-check cannot auto-rollback the same BEGIN/ROLLBACK — instead we BLOCK the row (status
    # 'failed', which gates the batch -> 207) and record+alert. We only run this when something was
    # NEWLY applied (an already-applied no-op didn't change the schema). The check is SCOPED to the
    # tables this DDL touched (raw_pipeline_* expands to the whole upsert blast-radius family) so the
    # 9 PRE-EXISTING duplicate-index tables can't false-block an unrelated apply. conn is still the
    # open writer (flock held), so the gate's core.schema_issue / review_learnings writes land in the
    # SAME live DB + critical section. The gate never raises (returns a structured dict).
    if out.get("status") == "committed" and out.get("applied"):
        try:
            scope = _integrity_gate.tables_touched_by_sql(content or "")
            # CATALOG-DELTA mode: pass the pre-apply snapshot so the gate fails ONLY on a NEWLY
            # introduced duplicate unique _key index (regex-independent — kills the QA-D USING/comment
            # bypass on the apply path) and a pre-existing dup can't false-block. `tables=scope` is kept
            # for the smoke-test/back-compat, but in delta mode the snapshot watches the whole upsert
            # blast radius and the BEFORE/AFTER delta is what isolates this apply's effect.
            gate = _integrity_gate.run_post_apply_integrity(
                conn, tables=scope or None, ddl_file=safe_name, record=True, smoke_test=True,
                pre_snapshot=pre_key_index_snapshot)
            out["integrity_gate"] = {"ok": gate["ok"], "detail": gate["detail"],
                                     "offenders": gate.get("offenders"),
                                     "recorded_issues": gate.get("recorded_issues"),
                                     "learning_appended": gate.get("learning_appended"),
                                     "alerted": gate.get("alerted")}
            if not gate["ok"]:
                # The DDL is committed but it left the warehouse in the dup-unique-_key landmine state.
                # Mark the row failed (blocks the batch -> 207) and surface the offending tables so a
                # writer fixes it with a follow-up DROP INDEX (expand/contract) immediately.
                out.update(applied=False, status="failed",
                           detail="POST-APPLY INTEGRITY GATE BLOCKED this apply — " + gate["detail"]
                                  + " — ship a follow-up DROP INDEX (expand/contract) so exactly one "
                                    "unique index on (_key) remains, then re-gate.")
        except Exception as e:  # noqa: BLE001 — a gate fault must not strand the row; fail CLOSED.
            out.update(applied=False, status="failed",
                       detail=f"post-apply integrity gate fault ({type(e).__name__}: {e}) — failing "
                              f"closed; the DDL committed but could not be integrity-verified. Inspect "
                              f"core.schema_issue / duckdb_indexes() for duplicate unique _key indexes.")
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
              reason: str | None = None, force_promote: bool = False, pull_first: bool = False) -> dict:
    """Public entry. Serializes apply-now in-process (a 2nd concurrent call waits, then fails fast if
    the first is still running) so two callers can't race the writer-flock env flag and self-deadlock.
    Then runs the real apply under the warehouse-writer flock + re-promote. pull_first=True does a
    box-side ff-only pull of origin/main first (the 'ship' flow: keep the nightly in sync)."""
    if not _APPLY_NOW_LOCK.acquire(timeout=_APPLY_NOW_LOCK_WAIT_S):
        return {"applied": [], "promote": None, "freshness": _snapshot_freshness(), "actor": actor,
                "ok": False, "error": "another apply-now is already running on the server "
                f"(waited {_APPLY_NOW_LOCK_WAIT_S}s) — retry shortly"}
    try:
        return _apply_now_impl(actor, max_items=max_items, promote=promote, reason=reason,
                               force_promote=force_promote, pull_first=pull_first)
    finally:
        _APPLY_NOW_LOCK.release()


def _apply_now_impl(actor: str, max_items: int = 25, promote: bool = True,
                    reason: str | None = None, force_promote: bool = False,
                    pull_first: bool = False) -> dict:
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

    if pull_first:
        result["pull"] = _git_pull_ff()  # sync the box checkout to origin/main so the nightly keeps it
        # GUARD (silent-swallow fix): _git_pull_ff never raises, so a diverged / dirty / feature-branch
        # box would silently leave the nightly rebuilding from STALE code (the 2026-06-27 fleet-wide
        # divergence that broke every ship). After the pull, ASSERT the box is on clean origin/main; if
        # not, preserve the box state, alert #cc-sam, and BLOCK this apply (fail-closed) — never apply
        # live while the nightly stays stale. Override (advanced): MODERATOR_BOX_DRIFT_GUARD=off.
        if _BOX_DRIFT_GUARD:
            drift = _box_sync_status()
            result["box_sync"] = drift
            if not drift.get("clean"):
                result["box_sync"]["preserved"] = _auto_preserve_box_drift(drift)
                snap_dir = (result["box_sync"]["preserved"] or {}).get("snapshot_dir")
                msg = (":rotating_light: Warehouse SHIP BLOCKED — box NOT on clean origin/main after "
                       f"pull-first ({drift.get('detail')}). The nightly would rebuild from STALE code "
                       "(silent merged-DDL drop). apply-now HALTED (nothing applied this run); the box "
                       f"working tree is left intact + snapshotted ({snap_dir}). FIX: reconcile the box "
                       "to origin/main (handoffs/2026-06-28-warehouse-box-git-reconcile.md / "
                       "GIT-SYNC-DISCIPLINE.md), then re-run. Override: MODERATOR_BOX_DRIFT_GUARD=off.")
                result["box_sync"]["alerted_cc_sam"] = _alert_cc_sam(msg)
                mc.log_event("apply_now_box_drift_blocked", actor=actor,
                             drift=drift.get("detail"),
                             preserved=result["box_sync"]["preserved"],
                             alerted=result["box_sync"]["alerted_cc_sam"])
                result["ok"] = False
                result["error"] = ("BOX NOT ON CLEAN origin/main after pull-first — apply BLOCKED so the "
                                   "nightly can't silently rebuild from stale code "
                                   f"({drift.get('detail')}). Box state preserved + #cc-sam alerted; "
                                   "reconcile the box to origin/main, then re-run.")
                result["freshness"] = _snapshot_freshness()
                result["elapsed_s"] = round(time.time() - t0, 1)
                return result

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
