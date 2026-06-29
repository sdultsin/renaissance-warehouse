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


class FatalDBError(RuntimeError):
    """Raised when a phase poisons/invalidates the shared DuckDB connection.

    A NORMAL per-ingest failure (dead workspace 401, lock contention, a bad row)
    raises through run_phase's own try/except, gets logged as status='failed', and
    the run continues as 'partial' — that resilience is intentional and preserved.

    But a FATAL DuckDB error — the corrupt-index "Failed to delete all rows from
    index" / "database has been invalidated" class — does NOT just fail one phase:
    it POISONS the connection. After it, every subsequent `conn.execute()` (the next
    phase's work AND its own log_phase audit write AND end_run) silently raises/no-ops
    on the dead connection, so the orchestrator marches on marking each phase 'failed'
    and exits 1 ('partial'). The nightly wrapper then treats exit 1 as "tables rebuilt,
    only peripheral ingests failed" and PUBLISHES — masking that ~every phase after the
    poison point (e.g. account_truth actuals) silently did nothing. That is exactly how
    the account-truth ingest stalled at 06-25 for ~3 days with no hard alarm (2026-06-28).

    Non-recurrence guard: after each phase we probe the connection with `SELECT 1`. If
    the probe itself raises, the connection is invalidated → we raise FatalDBError, which
    main() catches to FAIL LOUD: a #cc-sam alert + a best-effort 'failed' run row on a
    FRESH connection + exit code 2 (hard abort). nightly.sh already treats exit 2 as a
    hard abort — it KEEPS the last-good dashboards/serving instead of publishing a
    half-built DB, and the nightly-success watchdog sees a failed run. Aborting (rather
    than reconnect-and-retry) is deliberate: the corrupt-index case needs a human table
    rebuild (already done for schema_consumers); silently re-running the poisoning phase
    on a fresh connection could loop or half-rebuild. Fail-loud + abort is the simplest
    option that makes a silent downstream skip impossible.
    """


def _connection_is_alive(conn) -> bool:
    """True iff the connection can still execute. A poisoned/invalidated DuckDB
    connection raises on any query (the FATAL state cannot be cleared in-process)."""
    try:
        conn.execute("SELECT 1").fetchone()
        return True
    except Exception:  # noqa: BLE001 — ANY raise here means the connection is dead.
        return False


def _alert_cc_sam(text: str) -> None:
    """Best-effort loud alert to the warehouse #cc-sam channel. Reuses the canonical
    scripts/alert_slack.py credential + post path (SLACK_TOKEN + SLACK_ALERT_CHANNEL,
    process env then repo .env) so there is one Slack primitive, not a re-implementation.
    Never raises — a Slack failure must not change the exit path."""
    try:
        from scripts.alert_slack import main as _alert_main

        _alert_main(["alert_slack.py", text])
    except Exception as exc:  # noqa: BLE001
        logger.warning("could not post #cc-sam alert: %s", exc)


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
        phase_exc: Exception | None = None
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
            phase_exc = exc
            ended_at = datetime.now(timezone.utc)
            tb = traceback.format_exc()
            # log_phase writes via the SAME connection; if the connection is already
            # poisoned this will itself raise — guard it so we still reach the liveness
            # check below (and FAIL LOUD) instead of dying on the audit write.
            try:
                log_phase(
                    ctx.db, ctx.run_id, reg.phase_name, reg.ingest_name,
                    started_at, ended_at, "failed",
                    error=f"{exc}\n{tb}",
                )
            except Exception as log_exc:  # noqa: BLE001
                child_logger.error("could not record phase failure (connection may be poisoned): %s", log_exc)
            child_logger.error("FAILED: %s", exc)
            failed += 1

        # NON-RECURRENCE GUARD: a fatal DuckDB error (corrupt-index FATAL / "database
        # has been invalidated") does not merely fail this phase — it poisons the shared
        # connection so EVERY downstream phase then silently no-ops/fails on a dead conn
        # while the run still exits 'partial' and the nightly publishes (the 2026-06-28
        # account-truth silent-stall root cause). Probe the connection after each ingest;
        # if it's dead, stop marching and raise FatalDBError so main() fails LOUD (abort
        # + #cc-sam alert + exit 2) instead of silently skipping the rest of the run.
        if not _connection_is_alive(ctx.db):
            raise FatalDBError(
                f"DuckDB connection invalidated during phase '{reg.phase_name}' "
                f"ingest '{reg.ingest_name}'"
                + (f": {phase_exc}" if phase_exc is not None else "")
            )
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
    try:
        for phase_name in phases_to_run:
            regs = registry.by_phase(phase_name)
            if args.ingest:
                regs = [r for r in regs if r.ingest_name == args.ingest]
            total_failed += run_phase(regs, ctx, phase_name, args.dry_run)
    except FatalDBError as exc:
        # A phase poisoned/invalidated the shared connection. We must NOT continue —
        # downstream phases would silently no-op on the dead connection and the run
        # would still exit 'partial' (the silent-skip the nightly publishes over). Fail
        # LOUD and abort: post a #cc-sam alert, record a 'failed' run row on a FRESH
        # connection (the poisoned one can't write it), and exit 2 so nightly.sh treats
        # it as a hard abort (keeps last-good dashboards/serving; the success-watchdog
        # sees a failed run) rather than publishing a half-built warehouse.
        logger.error("FATAL DB error — aborting run %s (no further phases will run): %s", run_id, exc)
        _alert_cc_sam(
            ":rotating_light: *Warehouse nightly ABORTED — fatal DuckDB error* "
            f"(run `{run_id}`). A phase invalidated the DB connection: {exc}. "
            "The orchestrator hard-aborted (exit 2) instead of silently skipping the "
            "remaining phases — dashboards/serving keep their last-good copy and do NOT "
            "publish a half-built warehouse. This is the 2026-06-28 corrupt-index "
            "silent-skip class; check core.sync_run / the nightly log and rebuild any "
            "corrupt table/index before the next run."
        )
        # The poisoned connection cannot write the audit row — use a fresh one so the
        # run is recorded 'failed' (so the success-watchdog/QA see it) and is never
        # left stuck on 'running'. Best-effort: never let bookkeeping mask the abort.
        try:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            fresh = db_module.connect(db_path)
            end_run(fresh, run_id, "failed", {"error": "fatal_db_error", "detail": str(exc)[:500]})
            fresh.close()
        except Exception as bookkeeping_exc:  # noqa: BLE001
            logger.error("could not record failed run row after fatal DB error: %s", bookkeeping_exc)
        return 2

    final_status = "success" if total_failed == 0 else "partial"
    end_run(conn, run_id, final_status)
    logger.info("Run %s ended (status=%s, failed_ingests=%d)", run_id, final_status, total_failed)

    conn.close()
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
