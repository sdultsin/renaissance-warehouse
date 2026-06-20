# 00 — Architecture Spec

**Status:** v1 design. Locked with Sam 2026-05-30.

This is a working spec — not user-facing documentation. Keep it terse. Update when shape changes, not when implementation does.

---

## Purpose

Consolidate every meaningful Renaissance data source into a single queryable layer that:
1. Kills manual-entry inaccuracy by deriving every derivable field from authoritative APIs
2. Surfaces the 5 dip-cause factors as continuous queries, not retrospective analyses
3. Provides the state layer for future agentic operations ("type → 100 meetings" aspirational)
4. Builds the schema discipline that supports the "data company" exit narrative

Single operator (Sam) consumes via SSH + DuckDB CLI + LLM-driven queries against a maintained SCHEMA.md.

## Non-goals (v1)

- Multi-user UI / collaboration
- Real-time freshness (nightly window is the contract)
- Replacement of CMs' operational tooling (they keep using Instantly + sheets until Sam migrates them)
- The 5-factor analytical views (v2 — the foundation lets them be added later)

## Cost / financial data (reframed from v1 non-goal → v1 schema-only)

**Reversed 2026-05-30** per Sam: build the cost SHAPE in v1 even though actual numbers are stubbed. Retrofitting cost into a frozen canonical layer is painful, and the acquirer story needs cost-aware queries inside 12 months. Concretely:

- One `core.cost_ledger` fact table — vendor × sku × period × attribution × USD.
- Nullable `cost_*_estimated` columns on the relevant canonical entities (`core.campaign` now; `core.sending_account` / `core.domain` / `core.meeting` when those Phase 3 entities land).
- Seed `core.cost_ledger` with reference rates from the infra strategy suite (Sam's known $/inbox, $/domain figures).
- Real ingest (Stripe, manual invoice CSVs) = Phase 2.

See `specs/13-financial-data-architecture.md` for the full design + phase split.

## Storage

- Primary: DuckDB single file at `/root/core/core.duckdb` on droplet `renaissance-worker`
- Backup: nightly `cp` to `/root/archive/mac-offload/core-YYYY-MM-DD.duckdb`, 14-day retention
- Compute: Python orchestrator runs on droplet (8 vCPU AMD, 16 GB RAM)
- No remote replica. Mac + agents access via SSH.

## Three layers

| Layer | Purpose | Contents | Mutation rule |
|---|---|---|---|
| **raw** | Append-only snapshots of every source. Source of truth for "what did the system say on day X." | `raw_<source>_<table>` per source × table. Plus `_loaded_at` and `_source_snapshot_id` on every row. | Never delete. Even if source deletes (Instantly lead deletions, sheet rewrites), raw preserves the historical snapshot. |
| **core** (canonical) | One table per real-world entity. Explicit resolution rules per field. The single answer to "what do we believe is true about X." | `workspace`, `campaign`, `campaign_marker_tag`, `campaign_sending_tag`, `sending_account`, `domain`, `recipient_domain`, `send_event`, `meeting`, `opportunity`, plus their change-event logs where applicable. | Rebuildable from raw at any time. Resolution rules documented per entity. Instantly wins by default. |
| **derived** | Analytical views built only on canonical. Nothing reads raw directly except canonical. | `v_*` views, `mv_*` materialized rollups. ESP×ESP, portfolio aggregates, factor-visibility (v2). | Pure functions of core. Drop and rebuild freely. |

**The discipline:** derived never reads raw. If a derived view needs a field, the field gets added to core first with proper resolution.

## Source-of-truth principle

**Instantly is the canonical source whenever it knows the answer.** Sheets contribute only fields Instantly can't know: human-assigned brand mappings, billing dates, contractor names, intent flags. When Inbox Hub says "Google" and Instantly says "Outlook," Instantly wins. When Domain Tech Sheet says "NOT USED" and Instantly shows active sends, Instantly wins. The sheet drift becomes a derived view, not a debate.

## Sync window (single observable run)

```
03:00 UTC  ─ existing lead-mirror sync (untouched, ~03:15 cron)
03:30 UTC  ─ core orchestrator starts
   3:30 ─ 3:45  raw_pipeline_*    (slim mirror from pipeline-supabase)
   3:45 ─ 4:00  raw_comms_*       (comms-orchestration snapshot)
   3:45 ─ 4:00  raw_outreachify_* (Outreachify Supabase snapshot)
   4:00 ─ 4:15  raw_instantly_*   (workspaces, campaigns, accounts, tags)
   4:15 ─ 4:30  raw_sheets_*      (Domain Tech Sheet, blacklist sheet)
   4:30 ─ 4:45  raw_account_truth (snapshot from droplet account-truth DuckDB)
   4:45 ─ 5:30  dns_sweep         (MX/A/SPF/DKIM/DMARC/PTR + 8 DNSBLs + redirect chain)
   5:30 ─ 5:40  canonical_refresh (rebuild canonical tables from raw)
   5:40 ─ 5:45  derived_refresh   (materialize derived views)
05:45 UTC  ─ done; write sync_run summary
```

Each phase is a registered ingest in the orchestrator. Failure of one phase logs and continues; orchestrator returns nonzero if any phase failed. Idempotent — re-running the same `run_id` is safe.

## Lead-level data approach (locked decision)

Change-event log, not full daily snapshots. Daily Instantly API call per active campaign for `(campaign_id, lead_email)`. Diff against previous snapshot to extract NEW and REMOVED memberships. Store only the change events in `raw_instantly_campaign_membership_event`. Reconstruct membership at any point via the changelog.

Reasoning: 1.65M sends/day is too much for full-snapshot, conversation_messages covers only 1.2% of sends so can't be the source, raw lead-list-per-day is bounded by lead count (~600-800k/day input → ~100-500k change events/day after diffing → trivial storage in DuckDB).

## Repo layout

```
renaissance-warehouse/
  README.md
  pyproject.toml         # python deps
  requirements.txt       # mirror for non-uv environments
  .gitignore
  specs/                 # working specs, not user docs
  core/                  # orchestrator + db + credentials
  entities/              # one module per canonical entity
  sources/               # one module per data source (Instantly, Supabase, Sheets)
  sql/
    ddl/                 # entity DDL files, numbered for sequence
  scripts/               # one-shot scripts, setup, cron entry
  SCHEMA.md              # LLM-readable schema doc (added in Phase 2)
```

## Tech stack

- Python 3.12 (droplet has it)
- DuckDB 1.5+ (already installed)
- `duckdb` Python bindings (primary DB driver)
- `psycopg2` (read from Supabase Postgres directly when MCP isn't enough)
- `requests` (Instantly + Outreachify REST)
- `dnspython` (DNS sweep — already a pattern in blocklist-surveillance)
- `python-dotenv` (.env loader)
- No web framework, no ORM, no migrations framework. Raw SQL files + Python.

## Public repo discipline

Repo is public on GitHub. Rules:
- `.env` and all secrets — never committed (`.gitignore` enforces)
- The DuckDB file — never committed (always on droplet)
- Data CSVs / output — never committed
- Schema DDL, sync scripts, integration logic, helpers — all public
- Analytical thresholds (kill rules, factor cutoffs) — go in a private companion repo if/when they emerge
- API tokens and per-workspace keys live in `/Users/sam/Documents/Claude Code/Renaissance/.env` and `.env.instantly` on the local Mac and droplet — never in repo

## Versioning

`core.schema_version (version int, applied_at timestamptz, sql_file text)`. Each DDL file applied once, idempotent. No alembic, no migrations framework. We're a single operator.

## Failure semantics

- Each phase logs its own start/end/duration/error to `core.sync_run_phase`
- Phase failure does not abort downstream phases (data freshness > strict consistency for v1)
- Re-running an entire run (`./scripts/nightly.sh --run-id <prior_id>`) is idempotent
- DuckDB writes are transactional per phase

## What changes from v1 to v2

- Factor-visibility views (anomaly detection, cohort heatmaps, copy-cluster monitoring)
- Cost / P&L integration
- Optional Supabase mirror of derived tables (for agentic remote access)
- Reply intent reclassification (existing `reply_data.intent` broke 2026-04-12)
- Webhook-based send-event capture (if change-event lead snapshots prove insufficient)

These do not require schema redesign — they extend, don't restructure.
