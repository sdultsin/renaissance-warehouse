"""Main orchestrator. Sequences registered ingests through the sync window phases.

Usage:
    python -m core.orchestrator                 # full nightly run
    python -m core.orchestrator --dry-run       # walk phases but skip ingest execution
    python -m core.orchestrator --phase instantly  # run only one phase
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from core import db as db_module
from core.config import PHASE_ORDER, REPO_ROOT
from core.credentials import load_credentials
from core.registry import Registration, Registry, RunContext
from core.sync_run import PhaseResult, end_run, log_phase, new_run_id, start_run

logger = logging.getLogger("core.orchestrator")


def discover_and_register(registry: Registry) -> None:
    """Import every entities/* and sources/* module. Each is expected to expose
    `register(registry: Registry) -> None`. Modules without that function are skipped.
    """
    for pkg_name in ("entities", "sources"):
        pkg_dir = REPO_ROOT / pkg_name
        if not pkg_dir.exists():
            continue
        for module_file in sorted(pkg_dir.glob("*.py")):
            if module_file.name.startswith("_"):
                continue
            mod_name = f"{pkg_name}.{module_file.stem}"
            try:
                module = importlib.import_module(mod_name)
            except Exception as exc:
                logger.warning("Failed to import %s: %s", mod_name, exc)
                continue
            register_fn = getattr(module, "register", None)
            if callable(register_fn):
                register_fn(registry)
                logger.info("Registered: %s", mod_name)


def run_phase(
    registrations: list[Registration],
    ctx: RunContext,
    phase_name: str,
    dry_run: bool,
) -> int:
    """Returns count of failed ingests in this phase."""
    if not registrations:
        return 0
    logger.info("=== Phase: %s (%d ingests) ===", phase_name, len(registrations))
    failed = 0
    for reg in registrations:
        started_at = datetime.now(timezone.utc)
        child_logger = logging.getLogger(f"core.{reg.phase_name}.{reg.ingest_name}")
        if dry_run:
            child_logger.info("[DRY-RUN] would execute")
            log_phase(
                ctx.db,
                ctx.run_id,
                reg.phase_name,
                reg.ingest_name,
                started_at,
                started_at,
                "skipped",
                PhaseResult(notes={"reason": "dry-run"}),
            )
            continue
        try:
            result = reg.fn(ctx)
            ended_at = datetime.now(timezone.utc)
            log_phase(
                ctx.db, ctx.run_id, reg.phase_name, reg.ingest_name,
                started_at, ended_at, "success", result,
            )
            child_logger.info(
                "ok rows_in=%d rows_out=%d", result.rows_in, result.rows_out
            )
        except Exception as exc:
            ended_at = datetime.now(timezone.utc)
            tb = traceback.format_exc()
            log_phase(
                ctx.db, ctx.run_id, reg.phase_name, reg.ingest_name,
                started_at, ended_at, "failed",
                error=f"{exc}\n{tb}",
            )
            child_logger.error("FAILED: %s", exc)
            failed += 1
    return failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Renaissance warehouse orchestrator")
    parser.add_argument("--dry-run", action="store_true", help="Log phases but skip execution")
    parser.add_argument(
        "--phase",
        type=str,
        default=None,
        help="Run only this phase (default: all phases)",
    )
    parser.add_argument(
        "--ingest",
        type=str,
        default=None,
        help="Within the selected phase(s), run only this named ingest (e.g. recipient_domain)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Override DB path (for local dev). Default: from config.DB_PATH",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path = Path(args.db) if args.db else None
    conn = db_module.connect(db_path)

    # Foundation DDL must exist before we can log anything.
    foundation_ddl = REPO_ROOT / "sql" / "ddl" / "00_sync_run.sql"
    if foundation_ddl.exists():
        db_module.apply_ddl_file(conn, foundation_ddl, version=0)

    creds = load_credentials()
    registry = Registry()
    discover_and_register(registry)

    run_id = new_run_id()
    start_run(conn, run_id)
    logger.info("Run %s starting (dry_run=%s, phase_filter=%s)", run_id, args.dry_run, args.phase)

    ctx = RunContext(run_id=run_id, db=conn, credentials=creds)

    phases_to_run = [args.phase] if args.phase else PHASE_ORDER
    if args.phase and args.phase not in PHASE_ORDER:
        logger.error("Unknown phase: %s. Valid: %s", args.phase, PHASE_ORDER)
        end_run(conn, run_id, "failed", {"error": "unknown_phase"})
        return 2

    total_failed = 0
    for phase_name in phases_to_run:
        regs = registry.by_phase(phase_name)
        if args.ingest:
            regs = [r for r in regs if r.ingest_name == args.ingest]
        total_failed += run_phase(regs, ctx, phase_name, args.dry_run)

    final_status = "success" if total_failed == 0 else "partial"
    end_run(conn, run_id, final_status)
    logger.info("Run %s ended (status=%s, failed_ingests=%d)", run_id, final_status, total_failed)

    conn.close()
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
