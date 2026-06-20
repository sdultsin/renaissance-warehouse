#!/usr/bin/env python3
"""One-time chunked backfill of raw_pipeline_conversation_messages (~17.5M rows).

The orchestrator's single INSERT...SELECT OOM'd (wide body_text/body_html over 17.5M
rows > 12GB DuckDB limit). This loads via keyset pagination on the PK `id`
(indexed btree), bounded memory per page, restartable (ON CONFLICT DO NOTHING +
cursor recovered from max(id) already loaded). After this one-time load, the nightly
pipeline_mirror handles only the recent tail via the message_timestamp watermark.
"""
import duckdb, time, sys, os

WAREHOUSE = "/root/core/warehouse.duckdb"
PG = os.environ["PIPELINE_SUPABASE_DB_URL"]  # externalized hardcoded pipeline-Supabase DSN [2026-06-20]
PAGE = 400_000
RUN_ID = "manual-conv-backfill-20260606"

COLS = [
    "id","thread_id","campaign_id","workspace_id","lead_email","sender_email","sender_name",
    "recipient_email","recipient_name","direction","ue_type","body_text","body_html","subject",
    "message_timestamp","step_raw","step","variant","is_unread","interest_status",
    "ai_interest_value","content_preview","eaccount","subsequence_id","synced_at",
]
col_csv = ", ".join(COLS)
target_cols = "_key, " + col_csv + ", _loaded_at, _run_id"

con = duckdb.connect(WAREHOUSE)
con.execute("SET preserve_insertion_order=false")
con.execute("SET threads=3")
con.execute("INSTALL postgres"); con.execute("LOAD postgres")
con.execute(f"ATTACH '{PG}' AS pg (TYPE postgres, READ_ONLY)")

# recover cursor: highest id already loaded (string compare; pages pulled in id order)
last_id = con.execute("SELECT coalesce(max(id), '') FROM raw_pipeline_conversation_messages").fetchone()[0]
loaded = con.execute("SELECT count(*) FROM raw_pipeline_conversation_messages").fetchone()[0]
print(f"[start] already loaded={loaded:,} resume cursor id>'{last_id[:24]}'", flush=True)

page_no = 0
while True:
    page_no += 1
    esc = last_id.replace("'", "''")
    inner = (f"SELECT {col_csv} FROM public.conversation_messages "
             f"WHERE id > '{esc}' ORDER BY id LIMIT {PAGE}")
    t0 = time.time()
    con.execute("BEGIN")
    con.execute(
        f"INSERT INTO raw_pipeline_conversation_messages ({target_cols}) "
        f"SELECT CAST(id AS VARCHAR), {col_csv}, now(), '{RUN_ID}' "
        f"FROM postgres_query('pg', $q${inner}$q$) "
        f"ON CONFLICT (_key) DO NOTHING"
    )
    con.execute("COMMIT")
    row = con.execute("SELECT count(*), max(id) FROM raw_pipeline_conversation_messages").fetchone()
    new_total, new_max = row[0], row[1]
    got = new_total - loaded
    loaded = new_total
    dt = time.time() - t0
    print(f"[page {page_no}] +{got:,} -> {loaded:,} total ({dt:.0f}s) cursor id>'{(new_max or '')[:24]}'", flush=True)
    if new_max == last_id or got == 0:
        print("[done] no more rows", flush=True); break
    last_id = new_max

src = con.execute("SELECT count(*) FROM pg.public.conversation_messages").fetchone()[0]
print(f"[final] warehouse={loaded:,}  source={src:,}  delta={src-loaded:,}", flush=True)
con.close()
