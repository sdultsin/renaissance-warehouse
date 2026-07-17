"""DuckDB connection helpers. One connection per orchestrator run."""

from __future__ import annotations

import atexit
import fcntl
import logging
import os
import time
from pathlib import Path

import duckdb

from core.config import DB_PATH

log = logging.getLogger(__name__)

# Bound DuckDB memory well below RAM and let DuckDB spill intermediates to disk
# (temp_directory) instead of triggering the kernel oom-killer (which reaped the 2026-06-12
# compaction IMPORT at ~12.9GB RSS back when this was a 16GB box). The box is 32GB now and the
# old 8GB cap became the 07-11/07-12 nightly killer: entities/f_reply_canonical's dedup window
# over raw_instantly_email (~12GB of reply_text/api_response_raw VARCHARs, growing daily)
# OOM-thrashed at 8GB — a 15h one-core livelock on 07-11, then 'Invalid argument'/OutOfMemory
# on 07-12 (both nightlies lost; ART-index collateral damage repaired by
# scripts/repair_20260712_art_rebuild.py). 16GB completes that rebuild in ~83s and leaves half
# the box for the OS page cache + the ~03:30Z cron herd. Override via
# WAREHOUSE_DUCKDB_MEMORY_LIMIT for one-offs (either direction).
_MEMORY_LIMIT = os.environ.get("WAREHOUSE_DUCKDB_MEMORY_LIMIT", "16GB")

# ---------------------------------------------------------------------------
# Single-writer safety net (warehouse-writer wlock — box-local realization).
#
# DuckDB is a single-writer store: two concurrent read-write opens of
# warehouse.duckdb collide ("Conflicting lock held by PID ..."). The nightly
# kept failing when an AD-HOC heal (e.g. a hand-launched `core.orchestrator
# --phase sendivo` re-pull) opened the writer at the same time as the 03:30Z
# nightly. The established box convention serializes writers with an OS flock on
# /root/core/warehouse.write.lock at the cron ENTRYPOINT (`flock -w N "$L" -c`).
#
# This is the in-process belt-and-suspenders: ANY read-write connect() that is
# NOT already inside a flock'd wrapper takes the SAME OS lock here, acquire-or-
# wait, so a writer launched by hand without the wrapper QUEUES instead of
# clobbering the nightly. Gated by WAREHOUSE_WRITE_LOCK_HELD so it never
# double-locks (deadlocks) under a wrapper that already holds the file:
#   * entrypoint wrappers export WAREHOUSE_WRITE_LOCK_HELD=1 before exec → we skip;
#   * a forgotten ad-hoc writer has it unset → we acquire here.
# Re-entrant within a process (a 2nd connect() sees the env we set). Opt out with
# WAREHOUSE_DISABLE_INPROC_LOCK=1 (for tooling that manages the lock itself).
# Reversible: deleting this block + the connect() call restores prior behaviour.
# ---------------------------------------------------------------------------
_WRITE_LOCK_PATH = os.environ.get("WAREHOUSE_WRITE_LOCK_PATH", "/root/core/warehouse.write.lock")
# Max seconds to wait for the lock before giving up loudly (default 60 min [2026-07-16, was 30] —
# a full nightly/heal can hold it long, AND the publisher now HOLDS this flock for its entire
# ~25-30min copy+validate+swap (torn-snapshot fix), so writers must out-wait a promote). 0 = forever.
_WRITE_LOCK_WAIT_S = int(os.environ.get("WAREHOUSE_WRITE_LOCK_WAIT_S", "3600"))
_held_lock_fd: int | None = None  # module-level so the fd (and the lock) outlive connect()


def _pid_alive(pid: int) -> bool:
    """True if `pid` is a live process. kill -0 semantics (signal 0 = existence probe)."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Exists but owned by another user we can't signal — treat as ALIVE (do not clear).
        return True
    except OSError:
        return True


def _read_lockfile_pid(lock_path: Path) -> int | None:
    """Parse `pid=N` out of the lockfile marker, or None if absent/unparseable."""
    try:
        txt = lock_path.read_text(errors="replace")
    except OSError:
        return None
    for tok in txt.split():
        if tok.startswith("pid="):
            try:
                return int(tok.split("=", 1)[1])
            except ValueError:
                return None
    return None


def _clear_stale_lock_marker(lock_path: Path) -> bool:
    """STALE-LOCK-ON-START guard (warehouse-writer wlock hardening, 2026-06-17).

    The warehouse-writer lock is a kernel flock on `lock_path` (auto-released by the
    kernel when the holder dies), but the file ALSO carries a `pid=N` marker. A run
    that was kill -9'd mid-write leaves the flock released (kernel) yet the marker
    lying about a now-DEAD pid, which (a) misleads operators/the success-watchdog
    and (b) on some failure modes blocks the next run. This guard, on acquire:
    if the marker's recorded pid is NOT alive (kill -0/ /proc check), backs the
    stale marker up (`<lock>.stale-bak-<UTC>`) and clears the file content, then
    lets the normal flock acquire proceed. Idempotent & race-safe: we only ever
    REWRITE marker content (never unlink the inode another writer may hold an flock
    on), so two racing starters either both find a dead pid (one backup wins; the
    flock still serializes the actual write) or one finds a LIVE pid and skips.
    Returns True if it cleared a stale marker. Never raises.
    """
    try:
        pid = _read_lockfile_pid(lock_path)
        if pid is None or _pid_alive(pid):
            return False  # no marker, or a live holder — do NOT touch
        # Dead pid in the marker -> stale. Preserve evidence, then clear the content.
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        bak = lock_path.with_name(lock_path.name + f".stale-bak-{ts}")
        try:
            data = lock_path.read_bytes()
            bak.write_bytes(data)
        except OSError:
            pass
        try:
            # Truncate in place (keep the inode so a concurrent flock holder is unaffected).
            with open(lock_path, "r+b") as fh:
                fh.truncate(0)
        except OSError:
            return False
        log.warning(
            "cleared STALE warehouse-writer lock marker (dead pid=%s) on %s; backup=%s",
            pid, lock_path, bak.name,
        )
        return True
    except Exception:
        # A stale-lock guard must never be the thing that breaks the run.
        return False


def _acquire_write_lock() -> None:
    """Acquire the box-local warehouse-writer flock unless already held upstream.

    No-op if WAREHOUSE_WRITE_LOCK_HELD=1 (a flock'd wrapper owns it), if the
    in-process lock is disabled, if we already hold it in this process, or if the
    lock dir is unwritable (local dev). Blocks up to _WRITE_LOCK_WAIT_S, polling,
    so concurrent writers SELF-SEQUENCE rather than collide.
    """
    global _held_lock_fd
    if os.environ.get("WAREHOUSE_DISABLE_INPROC_LOCK") == "1":
        return
    if os.environ.get("WAREHOUSE_WRITE_LOCK_HELD") == "1":
        return  # an outer flock wrapper already serialized us
    if _held_lock_fd is not None:
        return  # re-entrant within this process

    lock_path = Path(_WRITE_LOCK_PATH)
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        # No writable lock location (e.g. local dev without /root/core) — skip the
        # safety net rather than block a developer. The on-disk DB lock still applies.
        return

    # STALE-LOCK-ON-START: if the marker names a DEAD pid, back it up + clear it before
    # we try to flock. Safe even when nobody holds the flock (no-op if pid is alive/absent).
    _clear_stale_lock_marker(lock_path)

    deadline = None if _WRITE_LOCK_WAIT_S <= 0 else (time.monotonic() + _WRITE_LOCK_WAIT_S)
    waited = False
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if not waited:
                log.warning(
                    "warehouse writer lock held by another process; waiting (max %ss) on %s",
                    _WRITE_LOCK_WAIT_S or "inf",
                    lock_path,
                )
                waited = True
            if deadline is not None and time.monotonic() >= deadline:
                os.close(fd)
                raise RuntimeError(
                    f"could not acquire warehouse writer lock within {_WRITE_LOCK_WAIT_S}s "
                    f"({lock_path}) — another writer is holding it"
                )
            time.sleep(2)

    try:
        os.ftruncate(fd, 0)
        os.write(fd, f"pid={os.getpid()} acquired_by=core.db\n".encode())
    except OSError:
        pass
    _held_lock_fd = fd
    os.environ["WAREHOUSE_WRITE_LOCK_HELD"] = "1"  # children + re-entrant connects skip
    atexit.register(_release_write_lock)
    if waited:
        log.warning("warehouse writer lock acquired after wait")


def _release_write_lock() -> None:
    global _held_lock_fd
    if _held_lock_fd is None:
        return
    try:
        fcntl.flock(_held_lock_fd, fcntl.LOCK_UN)
        os.close(_held_lock_fd)
    except OSError:
        pass
    _held_lock_fd = None
    # Leave WAREHOUSE_WRITE_LOCK_HELD set: the process is exiting; clearing it has no benefit.


def connect(db_path: Path | None = None, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Caller is responsible for closing.

    Sets a memory_limit + on-disk temp_directory on every connection so a single heavy query
    (compaction IMPORT, large mirror, mv refresh) cannot OOM the box. Session-level settings,
    safe on read-only connections too (temp_directory only holds spill files, not the DB).

    For read-write connections, first acquires the box-local warehouse-writer flock
    (acquire-or-wait) UNLESS an outer flock wrapper already holds it — so a writer launched
    without the wrapper queues behind the nightly instead of clobbering it. See _acquire_write_lock.
    """
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if not read_only:
        _acquire_write_lock()
    conn = duckdb.connect(str(path), read_only=read_only)
    tmp_dir = path.parent / "duckdb_tmp"
    try:
        os.makedirs(tmp_dir, exist_ok=True)
        conn.execute(f"SET temp_directory='{tmp_dir}'")
    except OSError:
        pass  # fall back to DuckDB's default temp location rather than fail the connection
    conn.execute(f"SET memory_limit='{_MEMORY_LIMIT}'")
    return conn


def ensure_schema(conn: duckdb.DuckDBPyConnection, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _moderator_ledger_dsn() -> str | None:
    """Resolve the pipeline-Supabase DSN for the moderator approval ledger. Env first, then the
    repo .env (the nightly may not export it into os.environ). None if unresolvable."""
    dsn = os.environ.get("MODERATOR_PG_DSN") or os.environ.get("PIPELINE_SUPABASE_DB_URL")
    if dsn:
        return dsn
    try:
        from core.config import REPO_ROOT
        envf = REPO_ROOT / ".env"
        if envf.exists():
            for line in envf.read_text().splitlines():
                line = line.strip()
                for key in ("MODERATOR_PG_DSN=", "PIPELINE_SUPABASE_DB_URL="):
                    if line.startswith(key):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return None


_PG_LEDGER_DOWN = False  # process-level latch: one failed attempt -> skip PG for the rest of this
                         # run (avoids paying connect_timeout once per DDL inside the writer flock).


def _ledger_pass_in_postgres(version: int, sha: str) -> bool | None:
    """Authoritative check against moderator.approval_ledger (pipeline project, Supavisor 6543).
    Returns True/False if Postgres is reachable and answered, or None (psycopg missing / DSN
    missing / network error) so the caller falls back to the DuckDB mirror (degraded-but-safe)."""
    global _PG_LEDGER_DOWN
    if _PG_LEDGER_DOWN:
        return None
    dsn = _moderator_ledger_dsn()
    if not dsn:
        return None
    try:
        import psycopg  # absent in the nightly venv until P7 — then this path goes live
    except Exception:
        _PG_LEDGER_DOWN = True  # psycopg not installed here — don't re-probe every DDL
        return None
    try:
        with psycopg.connect(dsn, connect_timeout=5, prepare_threshold=None) as c, c.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM moderator.approval_ledger "
                "WHERE ddl_version=%s AND content_sha256=%s "
                "AND verdict IN ('pass','pass-with-warn') LIMIT 1", (version, sha))
            return cur.fetchone() is not None
    except Exception as exc:  # network blip / auth — never hard-fail; fall back to the mirror.
        _PG_LEDGER_DOWN = True  # latch: subsequent DDLs this run skip straight to the mirror
        log.warning("schema-gate: moderator.approval_ledger unreachable (%s) — using DuckDB mirror "
                    "for the rest of this run", exc)
        return None


def _schema_gate_apply_tooth(conn: duckdb.DuckDBPyConnection, sql_file: Path, version: int) -> None:
    """Schema Moderator apply tooth (BUILD-SPEC-v2 §7.2) — WARN-ONLY until the held flip.

    Authority: a DDL is "gate-passed" iff moderator.approval_ledger (Postgres) has a row for this
    (ddl_version, sha256-of-file-content) with verdict pass/pass-with-warn. If Postgres is
    unreachable (or psycopg/DSN absent — e.g. pre-P7), fall back to the DuckDB mirror
    core.schema_gate_pass (kept fresh by entities/moderator_ledger_mirror.py). Degraded-but-safe:
    the fallback is never bypass-open.

    On a miss we ONLY LOG (WARN-only) — we NEVER refuse — until SCHEMA_GATE_ENFORCE_APPLY=1 (the
    Sam-gated flip, after a clean WARN/calibration week). Then a miss RAISES (refuses the apply).
    Runs inside the writer flock with the apply, so record+apply are one critical section vs live.

    Fail-safe: any unexpected error is swallowed so the tooth can never break an apply in WARN mode;
    an enforce-mode refusal propagates.
    """
    try:
        import hashlib
        sha = hashlib.sha256(sql_file.read_bytes()).hexdigest()
        enforce = os.environ.get("SCHEMA_GATE_ENFORCE_APPLY", "0") == "1"

        # 1) authoritative: the Postgres approval ledger.
        passed = _ledger_pass_in_postgres(version, sha)
        source = "postgres-ledger"
        if passed is None:
            # 2) fallback: the DuckDB mirror (degraded-but-safe).
            source = "duckdb-mirror"
            have = conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='core' AND table_name='schema_gate_pass'").fetchone()
            if not have:
                # neither store available — nothing to check against.
                if enforce:
                    raise RuntimeError(
                        f"schema-gate: no approval ledger reachable (Postgres + DuckDB mirror both "
                        f"absent) for {sql_file.name} v{version} [SCHEMA_GATE_ENFORCE_APPLY=1 — REFUSING]")
                return
            row = conn.execute(
                "SELECT 1 FROM core.schema_gate_pass WHERE version = ? AND content_sha256 = ? "
                "AND verdict IN ('pass','pass-with-warn')", [version, sha]).fetchone()
            passed = row is not None  # verdict-filtered to match the authoritative Postgres path

        if passed:
            return  # gate-passed — apply proceeds

        msg = (f"schema-gate: DDL {sql_file.name} (v{version}) has no recorded approval-ledger pass "
               f"for its current content (sha256={sha[:12]}…, checked {source}). Run "
               f"`python scripts/moderator_client.py loop --files <path>` to review + record.")
        if enforce:
            raise RuntimeError(msg + " [SCHEMA_GATE_ENFORCE_APPLY=1 — REFUSING un-gated DDL]")
        log.warning("%s [WARN-ONLY — applying anyway]", msg)
    except RuntimeError:
        raise  # enforce-mode refusal must propagate
    except Exception as exc:  # noqa: BLE001 — never let the tooth break an apply in WARN mode.
        log.warning("schema-gate apply tooth skipped (non-fatal): %s", exc)


def apply_ddl_file(conn: duckdb.DuckDBPyConnection, sql_file: Path, version: int) -> bool:
    """Apply a DDL file if not already applied. Returns True if newly applied.

    Tracks application in core.schema_version. Idempotent across runs.
    """
    ensure_schema(conn, "core")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS core.schema_version (
          version INTEGER PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          sql_file VARCHAR NOT NULL
        )
        """
    )
    existing = conn.execute(
        "SELECT 1 FROM core.schema_version WHERE version = ?", [version]
    ).fetchone()
    if existing:
        return False
    # Schema-gate apply tooth (Phase 1: WARN-ONLY — logs but never refuses). Runs inside
    # the writer flock with the apply, so review+apply are one critical section vs live.
    _schema_gate_apply_tooth(conn, sql_file, version)
    sql = sql_file.read_text()
    conn.execute("BEGIN")
    try:
        conn.execute(sql)
        conn.execute(
            "INSERT INTO core.schema_version (version, sql_file) VALUES (?, ?)",
            [version, sql_file.name],
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise
