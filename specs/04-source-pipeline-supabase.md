# 04 — Source: pipeline-supabase slim mirror

**Phase:** 2 (parallel after foundation)
**Status:** spec'd 2026-05-30
**Owner:** Track C agent

## Goal

Slim daily mirror of selected pipeline-supabase tables into `raw_pipeline_*` tables in DuckDB. NOT a full mirror — we deliberately leave the huge tables (conversation_messages 9.6M, contact_frequency_* 24M+, infra_account_daily_metrics 31M) in Supabase and query them in place when needed.

The point of the slim mirror is to make the **analytical** tables (campaign-level rollups, meetings, replies recent window) queryable from DuckDB without an SSH-to-Supabase trip for every query.

## Inputs

**Source:** Pipeline Supabase Postgres at the project URL stored in `.env`. Connection string in `PIPELINE_SUPABASE_DB_URL` (Sam will provide if not already there; the project ID is `edpyqbiqzduabtjhwfaa`).

**Auth:** psycopg2 with the connection string. Service role key auth is for PostgREST; Postgres direct uses URL with embedded password.

## Tables to mirror

Slim subset, optimized for analytical use:

| Source table | Rows | Window | Columns to keep | Notes |
|---|---|---|---|---|
| `public.campaigns` | 2,311 | full | all | campaign metadata |
| `public.campaign_data` | 41,750 | full | all | per (campaign, step, variant) canonical state |
| `public.campaign_daily_metrics` | 22,433 | last 90 days | all | per-day per-campaign sends/replies/opps |
| `public.meetings_booked_raw` | 948 | full | all | Slack-derived meetings (source of truth per Sam) |
| `public.reply_data` | 16,157 | last 90 days | all | individual replies with intent |
| `public.lead_events` | 50,138 | last 90 days | all | webhook-driven lead status changes |
| `public.variant_copy` | 1,431 | full | all | step/variant copy text |
| `public.bounce_suppression` | 222,649 | full | all | global bounce suppression |

Excluded explicitly (too large; queried in place via psycopg2 when needed):
- `conversation_messages` (9.6M)
- `contact_frequency_*` (24M+ × 3 tables)
- `infra_account_daily_metrics` (31M)
- `infra_domain_daily_metrics` (1.4M — borderline; defer)
- `sender_inboxes` (5.9M — sourced from account-truth ingestion instead)
- `webhook_delivery_log` (66k — operational, not analytical)
- All `cc_*` tables — these were migrated from CC, but they're operational (audit logs, run summaries, dashboard items) — defer to a later phase

## Outputs

`raw_pipeline_<table>` for each, plus the standard `_loaded_at` and `_run_id` columns. Use `CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM <source> WHERE FALSE` pattern on first run to bootstrap the schema (DuckDB will infer types from psycopg2 row dicts; coerce timestamps explicitly).

**Strategy per table:**
- Full-replace tables (small, stable): `campaigns`, `campaign_data`, `meetings_booked_raw`, `variant_copy`, `bounce_suppression` — `DELETE FROM raw_pipeline_X WHERE _run_id = ?; INSERT ...`. Append the new run's snapshot; keep prior runs for historical comparison.
- Window-rolling tables: `campaign_daily_metrics`, `reply_data`, `lead_events` — fetch last 90 days, upsert by source PK (`(campaign_id, date)` for metrics; row id for events).

**For v1, keep it simple:** every table is full-rewrite per run (delete by `_run_id`, insert fresh). Storage is cheap. We can optimize later if any table becomes a bottleneck.

## Implementation

**`sources/pipeline_supabase.py`** (new) — Postgres connection helper + per-table fetcher.

```python
class PipelineSupabase:
    def __init__(self, db_url: str): ...
    def fetch_table(self, table_name: str, where_clause: str | None = None) -> list[dict]: ...
```

**`entities/pipeline_mirror.py`** (new) — register the phase function.

```python
SLIM_TABLES = [
    ("campaigns", None),
    ("campaign_data", None),
    ("campaign_daily_metrics", "date >= current_date - interval '90 days'"),
    ("meetings_booked_raw", None),
    ("reply_data", "timestamp >= current_date - interval '90 days'"),
    ("lead_events", "timestamp >= current_date - interval '90 days'"),
    ("variant_copy", None),
    ("bounce_suppression", None),
]

def register(registry: Registry) -> None:
    registry.add_phase("pipeline_mirror", "all", run_pipeline_mirror)

def run_pipeline_mirror(ctx: RunContext) -> PhaseResult:
    db_url = ctx.credentials.require("PIPELINE_SUPABASE_DB_URL")
    pg = PipelineSupabase(db_url)
    rows_total = 0
    for table, where in SLIM_TABLES:
        rows = pg.fetch_table(table, where)
        write_raw(ctx.db, ctx.run_id, table, rows)
        rows_total += len(rows)
    return PhaseResult(rows_in=rows_total, rows_out=rows_total)
```

**`sql/ddl/04_pipeline_mirror.sql`** — one `CREATE TABLE IF NOT EXISTS raw_pipeline_<name>` per mirrored table. Define columns explicitly (don't rely on inference) so DuckDB types are stable.

## Resolution

This is a source-layer task only. No canonical resolution happens here. Canonical entities (`core.send_event`, `core.meeting`, etc.) consume these raws in a later phase.

## Definition of done

1. `python scripts/setup_db.py` applies `04_pipeline_mirror.sql` (version=4)
2. Running the orchestrator's `pipeline_mirror` phase fetches and persists every slim table
3. `SELECT count(*) FROM raw_pipeline_campaign_daily_metrics WHERE _run_id = <latest>` returns ~5-15k rows (90 days × 100 active campaigns)
4. `SELECT count(*) FROM raw_pipeline_meetings_booked_raw WHERE _run_id = <latest>` returns ~948 rows (or whatever current count is)
5. Re-running the phase replaces the prior run's rows for the same `_run_id` (idempotency within a single run)
6. Phase elapsed time on the droplet: under 5 minutes total

## Things to NOT do

- Don't mirror conversation_messages, contact_frequency_*, infra_*, sender_inboxes. Specifically excluded above.
- Don't write a sync watermark / incremental delta system. Full-rewrite per run is fine for v1.
- Don't try to derive canonical entities here — that's a later phase reading these raws.
- Don't use PostgREST REST API (the Supabase MCP wrapper). Direct Postgres via psycopg2 is the production pattern.
- Don't transform during fetch. Just COPY THROUGH. Transformations happen in the canonical layer.

## Open questions to surface

- If `PIPELINE_SUPABASE_DB_URL` is not in `.env`, surface to Sam. He can pull from Supabase dashboard. Format: `postgresql://postgres.<project_ref>:<password>@aws-1-<region>.pooler.supabase.com:5432/postgres`.
- If the pooler enforces statement_timeout (30 min per `feedback_supabase_pooler_30min_timeout.md`), surface — fetches must finish within that. None of our slim tables are big enough to hit this, but worth confirming.
- If a column type doesn't map cleanly to DuckDB (e.g., JSONB → string vs JSON), surface — choose one consistently.
