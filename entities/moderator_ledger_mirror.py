"""moderator_ledger_mirror — nightly PG->DuckDB mirror of the approval ledger (BUILD-SPEC-v2 §4/§7.2).

The Schema Moderator's authoritative approval ledger lives in Postgres (moderator.approval_ledger,
pipeline project). The apply tooth (core.db._schema_gate_apply_tooth) reads Postgres first, but
falls back to the DuckDB mirror core.schema_gate_pass when Postgres is unreachable (network blip,
or pre-P7 when psycopg isn't in the nightly venv). This entity keeps that mirror fresh: each nightly
it pulls the ledger from Postgres and UPSERTs it into core.schema_gate_pass.

Auto-discovered by core.orchestrator's entities/*.py glob via register(). Fail-safe / WARN-only:
any error returns a notes-only PhaseResult so the nightly is never broken. Degrades to a no-op
(logged) when psycopg or the pipeline DSN is unavailable — the mirror simply isn't refreshed.
"""
from __future__ import annotations

import logging

from core.config import REPO_ROOT
from core.db import _moderator_ledger_dsn
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.moderator_ledger_mirror")


def register(registry: Registry) -> None:
    registry.add_phase("derived", "moderator_ledger_mirror", run_moderator_ledger_mirror)


def _ensure_table(db) -> None:
    have = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='core' AND table_name='schema_gate_pass'").fetchone()
    if not have:
        ddl = REPO_ROOT / "sql" / "ddl" / "84_schema_gate.sql"
        if ddl.exists():
            db.execute(ddl.read_text())


def run_moderator_ledger_mirror(ctx: RunContext) -> PhaseResult:
    """Never raises (Phase 1 must never break the nightly)."""
    db = ctx.db
    try:
        _ensure_table(db)
        dsn = _moderator_ledger_dsn()
        if not dsn:
            return PhaseResult(notes={"skipped": "no pipeline DSN", "phase_mode": "warn-only"})
        try:
            import psycopg
        except Exception:
            return PhaseResult(notes={"skipped": "psycopg not installed in nightly venv (pre-P7)",
                                      "phase_mode": "warn-only"})
        rows = []
        with psycopg.connect(dsn, connect_timeout=10, prepare_threshold=None) as c, c.cursor() as cur:
            cur.execute(
                "SELECT ddl_version, sql_file, content_sha256, verdict, gate_version "
                "FROM moderator.approval_ledger ORDER BY ddl_version")
            rows = cur.fetchall()
        n = 0
        for ver, sql_file, sha, verdict, gate_version in rows:
            db.execute(
                """
                INSERT INTO core.schema_gate_pass
                  (version, sql_file, content_sha256, verdict, gated_by, gate_version, issue_count)
                VALUES (?, ?, ?, ?, 'moderator-svc', ?, 0)
                ON CONFLICT (version, content_sha256) DO UPDATE SET
                  verdict = excluded.verdict, gate_version = excluded.gate_version,
                  gated_by = 'moderator-svc', passed_at = now()
                """,
                [ver, sql_file, sha, verdict, gate_version])
            n += 1
        logger.info("moderator_ledger_mirror: mirrored %d approval-ledger row(s) -> core.schema_gate_pass", n)
        return PhaseResult(rows_in=len(rows), rows_out=n,
                           notes={"mirrored": n, "phase_mode": "warn-only"})
    except Exception as exc:  # noqa: BLE001 — Phase 1 must never break the nightly.
        logger.warning("moderator_ledger_mirror soft-failed (non-fatal): %s", exc)
        return PhaseResult(notes={"soft_error": str(exc)[:300], "phase_mode": "warn-only"})
