"""DuckDB connection helpers. One connection per orchestrator run."""

from __future__ import annotations

from pathlib import Path

import duckdb

from core.config import DB_PATH


def connect(db_path: Path | None = None, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Caller is responsible for closing."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


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
