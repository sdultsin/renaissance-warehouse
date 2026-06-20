# 05 — SCHEMA.md (LLM-readable schema documentation)

**Phase:** 2 (parallel after foundation)
**Status:** spec'd 2026-05-30
**Owner:** Track D agent

## Goal

Produce `SCHEMA.md` at the repo root — a single document that any LLM agent (Claude, Codex, GPT) can read alongside a user question and produce correct SQL against the warehouse on the first try.

This is **the** primary interface for v1. There is no UI. Sam queries the warehouse by asking Claude, "Show me reply rate per workspace last week." Claude reads SCHEMA.md and writes the SQL. The quality of this document determines the usability of the entire warehouse.

## Format

A single markdown file. Structure:

```
# SCHEMA — renaissance-warehouse

## Architecture (3 paragraphs)
Raw → Canonical → Derived. The single rule: derived reads only canonical.

## How to use this doc (3 paragraphs, addressed to an LLM)
You are reading this to write SQL. Use DuckDB syntax. Tables live in 3 schemas:
raw (snapshots), core (canonical), and the default DuckDB schema (derived views).
Resolution rules below say which source wins when sources disagree.

## Conventions
- Naming: raw_<source>_<table>, core.<entity>, v_<view>, mv_<materialized>
- Time: every table has _loaded_at (when we ingested it). Most facts also have a
  source timestamp.
- IDs: source-system IDs are kept verbatim. No surrogate keys.
- Nulls: missing values are NULL, not -1, not "unknown".
- is_active flag: T if the entity was seen in the latest sync run, F if not.
  Canonical rows are never deleted.

## Entities (one section per canonical entity)
### core.workspace
- Source: Instantly /workspaces/current
- Resolution rule: Instantly wins. CM/offer attribution does NOT live here.
- Columns: ...
- Common queries: ...

### core.campaign
- Source: Instantly /campaigns
- Resolution rules:
  - is_mca: regex(?i)\b(isaac|mca|cheap leads)\b on campaign.name
  - cm: regex match on name → SAM/SAMUEL/LEO/IDO/EYVER/TOUKIR/...; null if ambiguous
  - offer: regex priority order (HELOC, Tariffs, s125, R&D, Funding); null if no match
- Columns: ...
- Common queries: ...

### core.campaign_marker_tag, core.campaign_sending_tag
- Source: Instantly /tags + per-campaign tag list
- Two distinct tag types; never join them as one
- Columns: ...

### raw_pipeline_* tables
- Source: pipeline-supabase Postgres, slim daily mirror
- Tables included: campaigns, campaign_data, campaign_daily_metrics,
  meetings_booked_raw, reply_data, lead_events, variant_copy, bounce_suppression
- Windows: 90-day rolling for time-series; full for stable refs
- Common queries: ...

## Common joins
- Campaign-day performance: campaign_daily_metrics × campaign × workspace
- Meeting attribution: meetings_booked_raw × campaign (matching on campaign name slug)
- Reply intent: reply_data × campaign_data (matching on campaign_id, step, variant)

## What is NOT here (yet)
- Lead-level event log (Phase 3)
- Recipient ESP attribution (Phase 3)
- DNS / blacklist sweep (Phase 3)
- Sending account state (Phase 3)
- Domain entity (Phase 3)
- Sent event capture (Phase 3 — derived from campaign membership snapshots)

## sync_run tables (operational)
### core.sync_run, core.sync_run_phase
- For debugging only. Latest run's status, per-phase timing, error messages.
- Don't read these for analytics. Read core.<entity> instead.
```

## Source material the agent must consult

- `specs/00-architecture.md` — load-bearing architectural decisions
- `specs/01-foundation-scaffold.md` — sync_run + schema_version
- `specs/02-entity-workspace.md` — workspace entity details
- `specs/03-entity-campaign.md` — campaign + tags entity details
- `specs/04-source-pipeline-supabase.md` — what's mirrored from pipeline-supabase
- `sql/ddl/*.sql` — actual column definitions (authoritative)
- The architecture decisions section of THIS chat's planning log (live in the orchestrator's CLAUDE.md / memory if a future Claude rebuilds the doc)

## Style rules for the doc

- **Address the LLM consumer directly.** "When you write SQL against this warehouse, ..." not "Users may query the data using..."
- **One table per section.** Every column gets a name, type, and a 1-2 sentence description that explains MEANING, not syntax.
- **Resolution rules in the entity section, not in an appendix.** When the LLM reads about `core.campaign`, the `is_mca` rule should be RIGHT THERE.
- **Common queries inline.** For each entity, list 3-5 representative queries that capture the kinds of questions Sam actually asks. These act as in-context examples for code-generation.
- **No marketing language.** No "powerful," "robust," "intuitive." This is reference material for a machine.
- **Use DuckDB-specific syntax in examples.** Not generic SQL. The LLM will copy these.

## Definition of done

1. `SCHEMA.md` at repo root
2. Every entity built in Phase 2 (workspace, campaign, marker_tag, sending_tag, raw_pipeline_*) is documented
3. Resolution rules from specs are inline in the entity sections, not buried
4. At least 5 "common queries" per major entity
5. A test: pick 3 plausible questions Sam might ask ("which CMs sent the most last week," "which campaigns are tagged Outlook in funding-3," "show me reply rate by infra for Funding 4"). Use ChatGPT or Claude + SCHEMA.md to write the SQL. If the SQL is wrong or the LLM had to guess at schema details, the doc isn't done.

## Things to NOT do

- Don't document derived views that don't exist yet. Phase 2 has none. Section them as "added in Phase 3."
- Don't include Python code snippets. The LLM is consuming this for SQL generation, not Python.
- Don't reference internal Renaissance jargon without defining it inline (CM, OTD, ESP, MCA, etc.). The LLM might not have that context.
- Don't try to be exhaustive on edge cases. If a behavior is rare, link to the relevant spec instead of duplicating the explanation.
- Don't write more than one page per entity. Density > breadth.

## Open questions to surface

- Should SCHEMA.md include sample data rows? Probably yes for tricky enums (status codes, intent codes). Surface if unclear what's tricky.
- Format for the "common queries" — full SQL blocks vs. one-line "to answer X, join Y to Z and group by W"? Default: full SQL blocks, copy-pasteable.
- Sam's actual question phrasing: ask Sam for 5 real questions he wishes he could just type. Use those as the validation set.
