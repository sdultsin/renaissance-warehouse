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

# Bound DuckDB memory on the 16GB droplet. DuckDB defaults memory_limit to ~80% of RAM
# (~12.8GB here) — that is exactly the ~12.9GB RSS the kernel oom-killer reaped on 2026-06-12
# (the nightly compaction's IMPORT), and the memory pressure that SIGTERM'd the 2026-06-13
# nightly's reply_data mirror while ~10 cron jobs fired together at 03:30. Cap well below RAM and
# let DuckDB spill intermediates to disk (temp_directory) instead of triggering OOM. Override via
# WAREHOUSE_DUCKDB_MEMORY_LIMIT for memory-heavy one-offs on a quiet box.
_MEMORY_LIMIT = os.environ.get("WAREHOUSE_DUCKDB_MEMORY_LIMIT", "8GB")

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
# Max seconds to wait for the lock before giving up loudly (default 30 min — a full
# nightly/heal can legitimately hold it that long). 0 = wait forever.
_WRITE_LOCK_WAIT_S = int(os.environ.get("WAREHOUSE_WRITE_LOCK_WAIT_S", "1800"))
_held_lock_fd: int | None = None  # module-level so the fd (and the lock) outlive connect()


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
