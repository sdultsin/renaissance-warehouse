# 12 — Pipeline-mirror COPY rewrite (v1.1)

**Status:** spec'd 2026-05-30, not started. Blocks Phase 3 entities that need `lead_events`, `reply_data`, etc.

## Goal

Rewrite `entities/pipeline_mirror.py` to use DuckDB's bulk `COPY` / `INSERT INTO ... FROM read_*` instead of per-row `executemany`. Fixes the v1 hang on tables with large JSONB columns (lead_events) or large row counts (bounce_suppression 220k, reply_data 380k).

Once this lands, restore the 4 deferred tables to `SLIM_TABLES`:
- `reply_data` (last 90d)
- `lead_events` (last 90d)
- `variant_copy` (full)
- `bounce_suppression` (full)

## Why current code hangs

Current pattern (see git history of `entities/pipeline_mirror.py`):
1. Stream rows from psycopg2 server-side cursor (good)
2. Chunk to 5k rows in memory (good)
3. `conn.executemany(insert_sql, batch)` of dicts coerced via per-row `json.dumps` (bad)

DuckDB's `executemany` is not optimized for thousands of rows in a single transaction. JSON encoding per row × 200k rows × 30 cols × dict iteration = single-threaded Python loop at full CPU for hours.

## Fix

Two patterns to choose from:

### Pattern A — DuckDB reads directly from Postgres (cleanest)

DuckDB has a `postgres_scanner` extension (or `postgres_query`) that lets it pull from Postgres natively. The whole thing becomes:

```python
conn.execute(f"INSTALL postgres; LOAD postgres;")
conn.execute(f"CREATE OR REPLACE TEMP TABLE _stage AS SELECT *, now() AS _loaded_at, '{run_id}' AS _run_id FROM postgres_query('{pg_url}', $${select_sql}$$);")
conn.execute(f"DELETE FROM raw_pipeline_{table} WHERE _run_id = ?", [run_id])
conn.execute(f"INSERT INTO raw_pipeline_{table} SELECT * FROM _stage")
```

Pros: zero Python row-handling, fastest, transactional. Cons: postgres extension may not be installed (verify on droplet); needs the full pg connection URL inline in SQL (security review).

### Pattern B — Stream to Parquet/CSV, then COPY (fallback)

```python
# Stream from pg to a temp file
with tempfile.NamedTemporaryFile(suffix='.parquet') as tf:
    df = pl.read_database(f"SELECT * FROM {table}", pg_conn)  # polars
    df.with_columns(...).write_parquet(tf.name)
    conn.execute(f"COPY raw_pipeline_{table} FROM '{tf.name}' (FORMAT PARQUET)")
```

Pros: works without the postgres extension. Cons: requires polars or pyarrow, materializes the table in memory once.

## Recommendation

Try Pattern A first (verify postgres extension is available). Fall back to Pattern B if not.

## Definition of done

1. `pipeline_mirror` phase completes in <2 min for the full 8-table set (currently 7m21s for 4 tables)
2. `lead_events`, `reply_data`, `variant_copy`, `bounce_suppression` all populated
3. Memory usage during the phase stays under 1 GB (no full-table materialization)
4. Re-runs are idempotent (delete-by-_run_id + insert)
5. Specs 09 + 10 + 11 can now consume their inputs

## Open questions

- Does `postgres_scanner` handle the `bytea` and `jsonb` columns cleanly, or does it serialize them to strings? Verify with a test query before committing to Pattern A.
- The pooler URL (`aws-1-...pooler.supabase.com:5432`) — does the postgres extension respect the 30-min statement_timeout? Big tables might want the direct (non-pooler) URL.
