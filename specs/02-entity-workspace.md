# 02 — Entity: workspace

**Phase:** 2 (parallel after foundation)
**Status:** spec'd 2026-05-30
**Owner:** Track A agent

## Goal

Ship the first canonical entity, end-to-end:
1. Raw snapshot table populated nightly from Instantly
2. Canonical `core.workspace` table with resolution rules applied
3. Registered in the orchestrator's `instantly` phase
4. Smoke-testable end-to-end on the droplet

This is also the **pattern proof** for every subsequent entity. Get this clean and the rest follow it.

## Inputs

**Primary source:** Instantly API. There are two endpoints depending on auth scope:
- Per-workspace key → `GET /api/v2/workspaces/current` (returns the one workspace that key authenticates against)
- Admin key (if Sam has one) → `GET /api/v2/workspaces` (returns all)

Since we have per-workspace keys in `.env.instantly` (see `core/credentials.py::instantly_workspace_keys()`), the practical pattern is: iterate the workspace keys, hit `/workspaces/current` with each, collect.

**Auth:** Bearer token. The instantly API blocks `User-Agent: Python-urllib/*` — use `httpx` with `User-Agent: "curl/8.4.0"` (already a working pattern per memory `reference_instantly_api_urllib_403_block.md`).

**Rate:** workspace keys are issued one at a time. 5-10 workspaces total. Trivial.

**Known workspace inventory** (from memory `reference_cc_workspace_kpi_wiring_20260526.md`):
- `renaissance-2` (Funding 5, CM EYVER)
- `renaissance-4` (Funding 1, CM SAMUEL)
- `renaissance-5` (Funding 2, CM IDO)
- `prospects-power` (Funding 3, CM LEO)
- `koi-and-destroy` (Funding 4, CM SAM)
- `renaissance-1` (Renaissance 1, CM unresolved)
- Plus retired: `the-dyad`, `renaissance-6`, `renaissance-3`, `automated-applications`, etc.

The agent must NOT hardcode the CM/offer mapping in the workspace table. That lives in a sibling canonical resolution (campaign-level CM derivation via regex on campaign name, per Sam's call).

## Outputs

### `raw_instantly_workspace` (append-only snapshot)

One row per (workspace, snapshot_date). Fields are whatever the API returns; copy verbatim.

```sql
CREATE TABLE IF NOT EXISTS raw_instantly_workspace (
  _loaded_at        TIMESTAMPTZ NOT NULL,
  _run_id           VARCHAR NOT NULL,
  workspace_id      VARCHAR NOT NULL,    -- Instantly's internal UUID
  slug              VARCHAR NOT NULL,    -- the URL-friendly name (renaissance-4, koi-and-destroy)
  name              VARCHAR,             -- display name
  plan              VARCHAR,             -- if exposed
  trial_active      BOOLEAN,
  organization_id   VARCHAR,
  api_response_raw  VARCHAR              -- full JSON for audit/recovery
);
```

PK: `(workspace_id, _loaded_at)`. Never delete rows. Multi-day history of workspace metadata changes lives here.

### `core.workspace` (canonical, one row per workspace)

```sql
CREATE TABLE IF NOT EXISTS core.workspace (
  workspace_id      VARCHAR PRIMARY KEY,
  slug              VARCHAR NOT NULL,
  name              VARCHAR,
  plan              VARCHAR,
  is_active         BOOLEAN NOT NULL,    -- T if reachable in the most recent run, F if last run reported it gone
  first_seen_at     TIMESTAMPTZ NOT NULL,
  last_seen_at      TIMESTAMPTZ NOT NULL,
  resolved_at       TIMESTAMPTZ NOT NULL
);
```

PK: `workspace_id`. `slug` and friends are pulled from the latest raw snapshot. `first_seen_at` is set on insert; `last_seen_at` updated on every run; `is_active = (last_seen_at = current_run.started_at)`.

## Resolution rules

- **Source of truth:** Instantly. If a workspace is in the env keys but unreachable, mark `is_active=false` and keep the last-known row. Never delete from canonical.
- **slug:** verbatim from Instantly response.
- **No CM / offer assignment in this entity.** Those flow from campaign-level inference.

## Implementation

Two files:

**`sources/instantly.py`** (new) — the Instantly REST client. Just the workspace endpoint to start; future entities will add to it.

```python
class InstantlyClient:
    def __init__(self, api_key: str): ...
    def get_current_workspace(self) -> dict: ...
```

**`entities/workspace.py`** (new) — the ingest function + register hook.

```python
def register(registry: Registry) -> None:
    registry.add_phase("instantly", "workspace", run_workspace_ingest)

def run_workspace_ingest(ctx: RunContext) -> PhaseResult:
    keys = ctx.credentials.instantly_workspace_keys()
    # iterate, hit /workspaces/current, insert into raw_instantly_workspace,
    # then upsert into core.workspace based on the latest raw rows for this run
    ...
```

**`sql/ddl/02_workspace.sql`** — both raw and canonical DDL.

## Definition of done

1. `python scripts/setup_db.py` applies `02_workspace.sql` (logged at version=2 in `core.schema_version`)
2. `python -m core.orchestrator --phase instantly` runs the workspace ingest end-to-end
3. `core.workspace` has rows for every available workspace key
4. `raw_instantly_workspace` has one row per workspace per run
5. Re-running the orchestrator updates `last_seen_at` without inserting duplicate canonical rows
6. The smoke test query `SELECT workspace_id, slug, is_active FROM core.workspace ORDER BY slug` returns sensible data

## Things to NOT do

- Do not hardcode workspace identities (no Python dict of `KNOWN_WORKSPACES`). The env keys are the source.
- Do not call MCP from inside the ingest. MCP is a dev tool; runtime uses REST + per-workspace keys.
- Do not derive CM, offer, or infra type at this layer. That's downstream.
- Do not write a custom retry / backoff framework. `httpx` + a simple `try/except + log + skip` per workspace is enough.
- Do not parallelize the workspace iteration. Per `feedback_instantly_list_accounts_serial_only.md` the Instantly API has fragile behavior under parallel reads. Serial.

## Open questions to surface

If the agent hits any of these, stop and surface to Sam:
- Workspace key that 401s — log + skip, do not retry indefinitely
- New workspace not in `.env.instantly` — surface, don't auto-discover
- Schema mismatch (Instantly returns a field we don't have a column for) — capture in `api_response_raw` and surface
