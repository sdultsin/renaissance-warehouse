# Warehouse skills (canonical, version-controlled)

These are the **current** Claude Code skills for the Renaissance DuckDB warehouse, committed here so
every writer installs **one current copy** (no stale local forks drifting out of date).

- **`warehouse-access/SKILL.md`** — the single source of truth for **accessing** the warehouse:
  READ (query API) and WRITE/EDIT (the moderator pipeline: author → `moderator_client.py loop` →
  record → `apply-enqueue` → `apply-now`). Run the **doctor** first. Covers the two-speed apply
  (recorded → nightly by default, OR `apply-now` to make it live in minutes) and every setup
  error→fix. Load this for anything about connecting, tokens, scopes, or editing.
- **`data-warehouse/SKILL.md`** — the **read query-navigation + anti-hallucination** reference (table
  map, canonical-source-per-metric, the NEVER list, cite-source-and-snapshot rules, flag-it). Carries
  a SUPERSEDED header pointing at `warehouse-access` for access/the read-write model.

## Install (writers — run once, then re-run after a `git pull` to stay current)

From your renaissance-warehouse clone:

```bash
mkdir -p ~/.claude/skills/warehouse-access ~/.claude/skills/data-warehouse
cp skills/warehouse-access/SKILL.md ~/.claude/skills/warehouse-access/SKILL.md
cp skills/data-warehouse/SKILL.md   ~/.claude/skills/data-warehouse/SKILL.md
```

This **overwrites** any older local copy (that's intended — these are the current ones). If you have
an old `data-warehouse` skill with read-only-forever / nightly-only framing, the overwrite fixes it.

These files are the source of truth; the doctor + apply commands they describe live in
`scripts/moderator_client.py` in this repo.
