"""DuckDB connection helpers. One connection per orchestrator run."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from core.config import DB_PATH

# Bound DuckDB memory on the 16GB droplet. DuckDB defaults memory_limit to ~80% of RAM
# (~12.8GB here) — that is exactly the ~12.9GB RSS the kernel oom-killer reaped on 2026-06-12
# (the nightly compaction's IMPORT), and the memory pressure that SIGTERM'd the 2026-06-13
# nightly's reply_data mirror while ~10 cron jobs fired together at 03:30. Cap well below RAM and
# let DuckDB spill intermediates to disk (temp_directory) instead of triggering OOM. Override via
# WAREHOUSE_DUCKDB_MEMORY_LIMIT for memory-heavy one-offs on a quiet box.
_MEMORY_LIMIT = os.environ.get("WAREHOUSE_DUCKDB_MEMORY_LIMIT", "8GB")


def connect(db_path: Path | None = None, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Caller is responsible for closing.

    Sets a memory_limit + on-disk temp_directory on every connection so a single heavy query
    (compaction IMPORT, large mirror, mv refresh) cannot OOM the box. Session-level settings,
    safe on read-only connections too (temp_directory only holds spill files, not the DB).
    """
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
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
