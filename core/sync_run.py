"""Sync run audit log helpers.

Writes one row to core.sync_run per orchestrator invocation, and one row to
core.sync_run_phase per (run, phase, ingest) tuple.

Failures in individual phases do not abort the run — they record `status='failed'`
on the phase row and the orchestrator marks the run as 'partial' at the end.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb

logger = logging.getLogger("core.sync_run")


@dataclass
class PhaseResult:
    rows_in: int = 0
    rows_out: int = 0
    notes: dict[str, Any] = field(default_factory=dict)


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:6]


def start_run(conn: duckdb.DuckDBPyConnection, run_id: str) -> None:
    conn.execute(
        "INSERT INTO core.sync_run (run_id, started_at, status, phase_count, phase_failed_count) "
        "VALUES (?, now(), 'running', 0, 0)",
        [run_id],
    )


def end_run(conn: duckdb.DuckDBPyConnection, run_id: str, status: str, notes: dict | None = None) -> None:
    conn.execute(
        "UPDATE core.sync_run SET ended_at = now(), status = ?, notes = ? WHERE run_id = ?",
        [status, json.dumps(notes or {}), run_id],
    )


def log_phase(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    phase_name: str,
    ingest_name: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    result: PhaseResult | None = None,
    error: str | None = None,
) -> None:
    result = result or PhaseResult()
    conn.execute(
        """
        INSERT INTO core.sync_run_phase
        (run_id, phase_name, ingest_name, started_at, ended_at, status,
         rows_in, rows_out, error, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            phase_name,
            ingest_name,
            started_at,
            ended_at,
            status,
            result.rows_in,
            result.rows_out,
            error,
            json.dumps(result.notes),
        ],
    )
    # Bump aggregate counters on the run row.
    if status == "failed":
        conn.execute(
            "UPDATE core.sync_run SET phase_count = phase_count + 1, "
            "phase_failed_count = phase_failed_count + 1 WHERE run_id = ?",
            [run_id],
        )
    else:
        conn.execute(
            "UPDATE core.sync_run SET phase_count = phase_count + 1 WHERE run_id = ?",
            [run_id],
        )
