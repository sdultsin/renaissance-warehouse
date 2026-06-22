# Renaissance Warehouse — Claude Code

Real running code: the DuckDB data-consolidation warehouse on the droplet. This repo
DOES push (`origin = github.com/sdultsin/renaissance-warehouse`, worktree → merge → main).

## Schema-gate — MANDATORY before ANY schema change (READ THIS)

There is exactly one shared warehouse and ~4 editors (Thomas/Sam/Darcy/David) + ~100
syncs reading it. An uncoordinated column rename/move/drop silently breaks a sync; a
careless `ADD COLUMN` drifts the schema into semantic dupes (`email` vs `email_address`).
The **Schema Moderator** is the review agent that prevents both. It is now an always-on droplet
**service** (BUILD-SPEC-v2) with a **two-layer gate**, BOTH required: a free deterministic FLOOR
(mechanical breakage — bare rename/drop with consumers, non-canonical names, missing intent) PLUS a
paid server-side **LLM deep-review** that judges semantic/intent/foreseeable-downstream breakage the
floor can't — and CAN block. You talk to it through `scripts/moderator_client.py` (the service is the
single authority; it re-gates server-side and is un-bypassable). Rules + the approval ledger live in
Postgres (`moderator` schema); the catalog/lineage stays in DuckDB.

**Whenever you propose a schema change — a new/edited `sql/ddl/NN_*.sql`, or an
`entities|sources|scripts/*.py` that changes an INSERT column list — you MUST:**

1. Claim the next DDL number from the moderator — `python scripts/moderator_client.py next-version`
   (the single authority across ALL writers; it reserves the number atomically so two people can't
   collide, the way David's stale-local v96 did 2026-06-22). The bus-local `ddl-number` wlock still
   exists but is now advisory — `next-version` is authoritative. Write the change in
   `sql/ddl/<that-number>_*.sql` and/or the entity `.py`.
2. Tag intent at the top of the DDL: `-- @gate: add | rename A->B | drop | alter-type`
   and `-- Depends on NN`. (So a rename reads as a rename and apply-order is checkable.)
3. Run the moderator and read the checklist — then drive the **bounded auto-fix loop**:
   ```
   python scripts/moderator_client.py loop --files sql/ddl/NN_*.sql
   ```
   `loop` reviews; if it BLOCKs it prints the prescribed fixes — YOU (the editor's Claude) apply
   them in the worktree and re-run `loop`. Repeat at most ~6 times; if still blocked, escalate to
   the `orchestrator` on the parent bus (a genuine taxonomy/business fork). When it passes, `loop`
   calls `record-pass`, which writes the content-hash-bound `moderator.approval_ledger` row — the
   ONLY thing the apply tooth trusts. (`review` = check only; `record` = record only; the pre-commit
   hook runs `review` automatically as a WARN-only backstop. Token/URL self-resolve like the
   `data-warehouse` skill — no minting, no asking Sam.)
4. Fix what the gate flags:
   - A **RENAME/DROP** of a column with consumers → switch to **expand/contract**
     (ADD the new name → dual-populate in the nightly → migrate each consumer one DDL at
     a time → DROP the old only when no consumer remains). NEVER a bare rename/drop.
   - A new column that's a **synonym** of an existing canonical column → use the canonical
     name (see `core.column_aliases`) or declare a new alias.
   - An **unparseable / dynamic-SQL consumer** the gate flagged `assumed` → annotate it
     once with `# @consumes: schema.table.col file:line` or `# @gate-resilient: <cols>`
     (if the file is column-name-agnostic by design).

**WARN-ONLY / ENFORCE FLIP HELD (current state).** The moderator records verdicts + findings, and
its `/review` already runs the full two-layer gate (incl. the LLM, which can return `block`), but
the apply tooth does NOT yet refuse un-recorded DDL: `SCHEMA_GATE_ENFORCE_APPLY=0` and all rules ship
`tier='warn'`. So commits/applies/the nightly proceed exactly as before — nobody is blocked. This is
deliberate (a gate that refused un-gated DDL pre-calibration would break the other editors' nightly).
The flip — R1–R4 → `tier='block'` + `SCHEMA_GATE_ENFORCE_APPLY=1` + `MODERATOR_LLM_FAIL_CLOSED=1`
(deep-review fails CLOSED) — is a separate, Sam-gated step after a clean WARN/calibration week.
Scopes (in `/opt/duckdb/allowed_tokens.txt`, 3rd column): `reader` < `editor` (review/record) < `admin` (rules).

NOTE — this WARN-hold is about the *content* gate (refusing un-reviewed DDL). It is **independent** of
the **`apply == commit`** binding (`MODERATOR_REQUIRE_COMMITTED=enforce`, ON by default), which is a
*drift* control, not a content judgement: `apply-now` refuses a DDL whose exact content isn't committed
at `origin/main`, and refuses a version-number collision — so the live DB can't drift ahead of the repo
(the 2026-06-22 v96 fix). That enforcement is live regardless of the content-gate flip.

### The queryable contract (the "manager with vision over the whole DB")
**Catalog/lineage — DuckDB** (read-derived, rebuilt nightly from live `information_schema` by
`entities/schema_manifest.py`; NOT a `*brain.md` — the runtime reads nothing from prose):
- `core.schema_catalog` — every (schema, table, column) + type + canonical_name + status.
- `core.schema_consumers` — who reads each column (file:line), tagged static/declared/assumed.
- `core.schema_gate_pass` — DuckDB-mirrored passes the apply tooth falls back to if Postgres is down.

**Rules + alias authority + ledgers — Postgres `moderator` schema** (concurrent multi-user writes):
- `moderator.rule` / `moderator.rules_version` — the versioned rule set (rules-as-data).
- `moderator.column_alias` — canonical_name ↔ synonym (the deterministic dupe authority).
- `moderator.issue` — the issue ledger; `moderator.approval_ledger` — append-only, content-hash-bound.

Query the live view through the service: `moderator_client.py rules | catalog | issues | ledger`
(or `GET /moderator/*`). The catalog stays directly queryable in DuckDB too.

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
