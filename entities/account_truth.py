"""account_truth raw mirror: account_inventory -> raw_account_truth_accounts.

Absorbs the existing account_truth_<date>.duckdb snapshot (the same file the
Sending Truth Vercel app reads) rather than re-deriving inbox state from Instantly.
Registers under the 'account_truth' phase. Canonical resolution into
core.sending_account lives in entities/sending_account.py ('canonical' phase).

Pattern (matches entities/pipeline_mirror.py): ATTACH the source DuckDB read-only,
then INSERT...SELECT into the raw table. Idempotent within a run (DELETE by _run_id
first); prior snapshots preserved.

NOTE: the source `raw_json` column (1.18 GB across ~1.5M rows) is intentionally NOT
copied — every field we need is already extracted into typed columns, and the source
.duckdb file remains the immutable archive if the raw blob is ever needed.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.account_truth")

# Where the periodic account_truth_<date>.duckdb snapshots live on the droplet.
SNAPSHOT_DIR = Path(os.environ.get("ACCOUNT_TRUTH_DIR", "/root/archive/mac-offload"))
SNAPSHOT_GLOB = "account_truth_*.duckdb"
# Explicit override wins over glob resolution.
SNAPSHOT_OVERRIDE = os.environ.get("ACCOUNT_TRUTH_DB")

_DATE_RE = re.compile(r"account_truth_(\d{4}-\d{2}-\d{2})\.duckdb$")


def _resolve_snapshot() -> Path:
    """Pick the newest account_truth_<date>.duckdb (filename dates sort correctly)."""
    if SNAPSHOT_OVERRIDE:
        p = Path(SNAPSHOT_OVERRIDE)
        if not p.exists():
            raise FileNotFoundError(f"ACCOUNT_TRUTH_DB override not found: {p}")
        return p
    candidates = sorted(SNAPSHOT_DIR.glob(SNAPSHOT_GLOB))
    if not candidates:
        raise FileNotFoundError(
            f"No {SNAPSHOT_GLOB} found in {SNAPSHOT_DIR}. Set ACCOUNT_TRUTH_DB."
        )
    return candidates[-1]


# Source columns copied through (raw_json deliberately excluded — see module docstring).
_SRC_COLS = [
    "workspace_slug", "workspace_name", "email", "domain", "status", "status_label",
    "daily_limit", "provider_code", "infra_type", "setup_pending", "warmup_status",
    "warmup_score", "sending_gap", "created_at", "updated_at",
]


def run_account_truth_mirror(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    snapshot = _resolve_snapshot()
    logger.info("account_truth snapshot: %s", snapshot)

    try:
        conn.execute("DETACH acct_src")
    except Exception:
        pass
    conn.execute(f"ATTACH '{snapshot}' AS acct_src (READ_ONLY)")

    select_list = ", ".join(_SRC_COLS)
    target_cols = ", ".join(_SRC_COLS + ["has_errors", "raw_json", "_snapshot_file", "_loaded_at", "_run_id"])
    try:
        conn.execute("BEGIN")
        conn.execute(
            "DELETE FROM raw_account_truth_accounts WHERE _run_id = ?", [ctx.run_id]
        )
        conn.execute(
            f"""
            INSERT INTO raw_account_truth_accounts ({target_cols})
            SELECT {select_list}, (status < 0) AS has_errors, NULL AS raw_json, ?, now(), ?
            FROM acct_src.account_inventory
            """,
            [snapshot.name, ctx.run_id],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        try:
            conn.execute("DETACH acct_src")
        except Exception:
            pass

    n = conn.execute(
        "SELECT count(*) FROM raw_account_truth_accounts WHERE _run_id = ?", [ctx.run_id]
    ).fetchone()[0]
    logger.info("raw_account_truth_accounts <- %d rows from %s", n, snapshot.name)
    return PhaseResult(rows_in=n, rows_out=n, notes={"snapshot": snapshot.name})


def register(registry: Registry) -> None:
    registry.add_phase("account_truth", "mirror", run_account_truth_mirror)
