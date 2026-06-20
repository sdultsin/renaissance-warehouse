# 01 — Foundation Scaffold Spec

**Phase:** 1 (sequential, must land before Phase 2 parallel tracks)
**Status:** in progress 2026-05-30

## What Phase 1 ships

A runnable skeleton: orchestrator can execute end-to-end with zero registered ingests, writes a `sync_run` row, logs phase outcomes, exits cleanly. Future ingests register themselves and the orchestrator picks them up.

## Components

| File | Responsibility |
|---|---|
| `core/config.py` | Paths, sync window phase order, env constants. Single source of truth for "where things live." |
| `core/credentials.py` | `.env` loader with allowlist of expected keys. Errors loudly when a required key is missing. Never logs values. |
| `core/db.py` | DuckDB connection helpers. Single connection per run. Read-only and writable variants. |
| `core/sync_run.py` | Start / end / phase logging helpers. Writes to `core.sync_run` and `core.sync_run_phase`. |
| `core/orchestrator.py` | Main entry. Discovers registered ingests, sequences them per the sync window phases, logs each. |
| `core/registry.py` | Plugin registry. Each ingest module exposes a `register(orchestrator)` function. |
| `sql/ddl/00_sync_run.sql` | `core.sync_run`, `core.sync_run_phase`, `core.schema_version` tables. |
| `scripts/setup_db.py` | Initializes a fresh DuckDB file with the foundation DDL applied. Idempotent. |
| `scripts/nightly.sh` | Cron entry — wraps `python -m core.orchestrator` with logging. |

## Orchestrator contract

Every ingest module exposes:

```python
def register(orchestrator: Orchestrator) -> None:
    orchestrator.add_phase("instantly", run_workspace_ingest)
    orchestrator.add_phase("instantly", run_campaign_ingest)
    ...
```

Each phase function signature:

```python
def run_workspace_ingest(ctx: RunContext) -> PhaseResult:
    """
    ctx.run_id          : str, the unique run identifier for this nightly invocation
    ctx.db              : duckdb.DuckDBPyConnection
    ctx.credentials     : Credentials (allowlisted env access)
    ctx.logger          : logging.Logger
    returns PhaseResult(rows_in, rows_out, notes)
    """
```

PhaseResult.notes is freeform dict — sources can record source-side metadata (rate limit headroom, pagination state, etc.) without schema changes.

## sync_run schema (preview, DDL in 00_sync_run.sql)

```sql
CREATE TABLE IF NOT EXISTS core.sync_run (
  run_id text PRIMARY KEY,
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  status text NOT NULL,           -- 'running' | 'success' | 'partial' | 'failed'
  phase_count int NOT NULL DEFAULT 0,
  phase_failed_count int NOT NULL DEFAULT 0,
  notes text
);

CREATE TABLE IF NOT EXISTS core.sync_run_phase (
  run_id text NOT NULL,
  phase_name text NOT NULL,       -- e.g. 'instantly', 'dns_sweep'
  ingest_name text NOT NULL,      -- e.g. 'workspace', 'campaign'
  started_at timestamptz NOT NULL,
  ended_at timestamptz,
  status text NOT NULL,
  rows_in bigint,
  rows_out bigint,
  error text,
  notes text,
  PRIMARY KEY (run_id, phase_name, ingest_name)
);

CREATE TABLE IF NOT EXISTS core.schema_version (
  version int PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now(),
  sql_file text NOT NULL
);
```

## Credentials pattern

`.env` files used:
- `/Users/sam/Documents/Claude Code/Renaissance/.env` (Mac dev)
- `/root/renaissance-warehouse/.env` (droplet runtime, symlinked or copied from Mac)
- `/Users/sam/Documents/Claude Code/Renaissance/.env.instantly` (per-workspace Instantly keys)

`core/credentials.py` exposes:
```python
class Credentials:
    def require(self, key: str) -> str: ...      # raises if missing
    def optional(self, key: str) -> str | None: ...
    def instantly_workspace_keys(self) -> dict[str, str]: ...  # workspace_slug → api_key
```

Never logs values. Loads via python-dotenv. No 1Password CLI in routine path (per `feedback_no_1password_for_routine_api_access`).

## What Phase 1 does NOT include

- Any actual entity ingest (workspace, campaign, etc.) — that's Phase 2
- Slim pipeline-supabase mirror — Phase 2 Track C
- SCHEMA.md — Phase 2 Track D
- DNS sweep — Phase 2 or later
- Slack notifications on failure — added when needed

## Verification

Phase 1 is done when:
1. Repo exists locally + on GitHub (public)
2. `python -m core.orchestrator` runs end-to-end and writes a `sync_run` row with `status='success'` and `phase_count=0`
3. `scripts/setup_db.py` is idempotent — re-running doesn't corrupt schema_version
4. `.env` loading errors loudly when expected keys are missing
5. Droplet has the repo cloned at `/root/renaissance-warehouse/` and `/root/core/core.duckdb` exists with foundation DDL applied
