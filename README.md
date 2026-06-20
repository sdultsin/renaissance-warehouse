# renaissance-warehouse

Internal data consolidation layer for Renaissance. Single-operator DuckDB warehouse running on a private droplet.

This repo holds the schema, sync scripts, and orchestration logic. No data, no secrets. Both live elsewhere.

## Layout

```
specs/        working specs (architecture, foundation, per-entity)
core/         orchestrator, DB helpers, credentials, sync_run
entities/     one module per canonical entity
sources/      one module per upstream data source
sql/ddl/      schema files, numbered for sequence
scripts/      one-shot scripts, setup, cron entry
SCHEMA.md     LLM-readable schema documentation
```

## Architecture

See `specs/00-architecture.md` for the load-bearing decisions.

Three layers: `raw_*` (append-only snapshots), `core.*` (one row per real-world entity, resolved across sources), derived views (materialized analytics). DuckDB at `/root/core/core.duckdb` on the droplet. Single nightly sync window 03:30-05:45 UTC.

## Running

```bash
# Local dev — never writes to production droplet
python -m core.orchestrator --db ./core.duckdb --dry-run

# Droplet — wired to /root/core/core.duckdb via .env
./scripts/nightly.sh
```

## Adding a new entity

1. Write `specs/NN-<entity>.md` with the resolution rules
2. Add DDL to `sql/ddl/`
3. Add source connector to `sources/<source>.py` if new
4. Add canonical builder to `entities/<entity>.py`
5. Register the phase in the entity module's `register()`
6. Run `scripts/setup_db.py` to apply DDL
7. Run orchestrator with the new phase

## Secrets

All API credentials live in `.env` files outside the repo. The orchestrator loads:
- `/Users/sam/Documents/Claude Code/Renaissance/.env` (Mac dev)
- `/root/renaissance-warehouse/.env` (droplet runtime)
- `/Users/sam/Documents/Claude Code/Renaissance/.env.instantly` (per-workspace Instantly keys)

Never `op read` in routine code paths.
