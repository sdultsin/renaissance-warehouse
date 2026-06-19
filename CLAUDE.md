# Renaissance Warehouse — Claude Code

Real running code: the DuckDB data-consolidation warehouse on the droplet. This repo
DOES push (`origin = github.com/sdultsin/renaissance-warehouse`, worktree → merge → main).

## Schema-gate — MANDATORY before ANY schema change (READ THIS)

There is exactly one shared warehouse and ~4 editors (Thomas/Sam/Darcy/David) + ~100
syncs reading it. An uncoordinated column rename/move/drop silently breaks a sync; a
careless `ADD COLUMN` drifts the schema into semantic dupes (`email` vs `email_address`).
The **schema-gate** is the deterministic review agent that prevents both.

**Whenever you propose a schema change — a new/edited `sql/ddl/NN_*.sql`, or an
`entities|sources|scripts/*.py` that changes an INSERT column list — you MUST:**

1. Claim the next DDL number atomically (the `ddl-number` wlock convention), write the
   change in `sql/ddl/NN_*.sql` and/or the entity `.py`.
2. Tag intent at the top of the DDL: `-- @gate: add | rename A->B | drop | alter-type`
   and `-- Depends on NN`. (So a rename reads as a rename and apply-order is checkable.)
3. Run the gate and read the checklist:
   ```
   python scripts/schema_gate.py review --files sql/ddl/NN_*.sql
   python scripts/schema_gate.py record sql/ddl/NN_*.sql   # checksum-bound pass
   ```
   Or just `git commit` — the **pre-commit hook runs the gate automatically** (wire it
   once with `scripts/install_schema_gate_hook.sh`).
4. Fix what the gate flags:
   - A **RENAME/DROP** of a column with consumers → switch to **expand/contract**
     (ADD the new name → dual-populate in the nightly → migrate each consumer one DDL at
     a time → DROP the old only when no consumer remains). NEVER a bare rename/drop.
   - A new column that's a **synonym** of an existing canonical column → use the canonical
     name (see `core.column_aliases`) or declare a new alias.
   - An **unparseable / dynamic-SQL consumer** the gate flagged `assumed` → annotate it
     once with `# @consumes: schema.table.col file:line` or `# @gate-resilient: <cols>`
     (if the file is column-name-agnostic by design).

**PHASE 1 IS WARN-ONLY (current state, since 2026-06-18).** The gate LOGS findings to
`core.schema_issue` but blocks NOTHING — no commit, no DDL apply, no nightly is refused.
This is deliberate: a Phase-1 that refused un-gated DDL would break the other editors'
nightly. Treat the checklist as the review you should heed; the hard block (R1–R4 → BLOCK,
apply-tooth refusal via `SCHEMA_GATE_ENFORCE_APPLY=1`) is a separate, later Sam decision.

### The queryable contract (the "manager with vision over the whole DB")
Everything dynamic lives in DuckDB tables, rebuilt nightly from live `information_schema`
by `entities/schema_manifest.py` (NOT a `*brain.md` — the runtime reads nothing from prose):
- `core.schema_catalog` — every (schema, table, column) + type + canonical_name + status.
- `core.schema_consumers` — who reads each column (file:line), tagged static/declared/assumed.
- `core.column_aliases` — canonical_name ↔ synonym (the deterministic dupe authority).
- `core.schema_issue` — the issue ledger (`SELECT * FROM core.schema_issue WHERE status='open'`).
- `core.schema_gate_pass` — checksum-bound passes the apply tooth consults.

Query them directly to answer "what reads this column", "is this name canonical", "what
issues are open". `python scripts/schema_gate.py status` prints the human view.

### Escalations
True taxonomy forks or an unparseable consumer needing a business call → record in
`core.schema_issue` AND route to the `orchestrator` on the parent bus → Sam (the live bus
is in the parent Renaissance repo; this repo only carries `bus/archive/`).

## Existing primitives the gate builds ON (don't reinvent)
- Writer flock (`core/db.py:_acquire_write_lock`, `scripts/with_warehouse_lock.sh`) —
  serializes all writers; gate-record + apply run inside it as one critical section.
- Numbered DDL + `scripts/setup_db.py` glob/apply → `core.db.apply_ddl_file` (the apply tooth).
- `core/orchestrator.py` glob entity discovery → auto-discovers `schema_manifest.py`.
- `scripts/refresh_sync_registry.py` `information_schema` self-discovery (catalog reuses the pattern).
- `scripts/warehouse_qa.py` — hosts the nightly catalog-vs-live drift + entity-contract backstop.
- One pip dep added: `sqlglot` (static SQL lineage).

## Spec
Full design + the Sam-facing forks: `deliverables/2026-06-18-db-review-agent/BUILD-SPEC.md`
in the parent Renaissance repo.
