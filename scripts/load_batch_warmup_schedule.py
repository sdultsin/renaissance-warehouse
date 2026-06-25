#!/usr/bin/env python3
"""Load core.batch_warmup_schedule (DDL 1015) from the gitignored seed CSV.

One row per provisioning cohort (inbox vendor x Instantly workspace x warmup-start date)
plus pending-upload / upload-error buckets — David's MilkBox/MailIn warmup -> go-live
schedule. Idempotent INSERT OR REPLACE on the cohort_id PK, so REFRESH = update the CSV
(seed_data/batch_warmup_schedule.csv) and re-run this; statuses/counts get replaced in
place, nothing is deleted. Flock-guarded (warehouse single-writer). Run box-side.

The seed is gitignored (seed_data/) so inbox-supplier names + infra scale stay out of the
PUBLIC repo — same boundary as scripts/load_account_registry.py.
"""
from __future__ import annotations

import fcntl
import os

import duckdb

DB = os.environ.get("CORE_DB_PATH", "/root/core/warehouse.duckdb")
WRITE_LOCK = "/root/core/warehouse.write.lock"
SEED = os.environ.get(
    "BATCH_WARMUP_SEED",
    "/root/renaissance-warehouse/seed_data/batch_warmup_schedule.csv",
)


def main() -> int:
    if not os.path.exists(SEED):
        print(f"seed CSV not found: {SEED} — nothing loaded.", flush=True)
        return 1

    lock_fd = os.open(WRITE_LOCK, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("WAREHOUSE WRITE LOCK BUSY. Exiting.", flush=True)
        return 1

    con = duckdb.connect(DB)
    con.execute("SET threads=2")

    # Read the seed once as VARCHAR and FAIL LOUD on the two silent-corruption paths a
    # hand-edited refresh can introduce: (a) duplicate cohort_id (INSERT OR REPLACE would
    # keep only the last -> undetected row loss), (b) a non-empty date/int that fails to
    # parse (TRY_CAST would NULL it silently).
    con.execute(
        f"CREATE TEMP TABLE _seed AS "
        f"SELECT * FROM read_csv_auto('{SEED}', header=true, nullstr='', all_varchar=true)"
    )
    # (a) cohort_id (the PK) must be unique, AND (b) the NATURAL key
    # (provider, batch_key, workspace, warmup_start_date) must be unique too — otherwise two
    # different cohort_ids for the same real cohort would both survive and double-count
    # n_accounts in the SUM rollups. NULL workspace/date (pending/error buckets) are
    # disambiguated by batch_key. Compare on a NULL-safe key string (the seed is all-VARCHAR).
    n_rows, n_ids, n_natural = con.execute(
        """
        SELECT count(*), count(DISTINCT cohort_id),
               count(DISTINCT coalesce(provider,'') || '|' || coalesce(batch_key,'') || '|'
                              || coalesce(workspace,'') || '|' || coalesce(warmup_start_date,''))
        FROM _seed
        """
    ).fetchone()
    if n_rows != n_ids:
        con.close()
        raise SystemExit(f"ABORT: {n_rows - n_ids} duplicate cohort_id(s) in {SEED} — fix the seed.")
    if n_rows != n_natural:
        con.close()
        raise SystemExit(
            f"ABORT: {n_rows - n_natural} duplicate natural key(s) "
            f"(provider, batch_key, workspace, warmup_start_date) in {SEED} — would double-count; fix the seed."
        )
    bad = con.execute(
        """
        SELECT count(*) FROM _seed
        WHERE (warmup_start_date  <> '' AND TRY_CAST(warmup_start_date  AS DATE)    IS NULL)
           OR (warmup_length_days =  '' OR  TRY_CAST(warmup_length_days AS INTEGER) IS NULL)
           OR (n_accounts         <> '' AND TRY_CAST(n_accounts         AS INTEGER) IS NULL)
           OR (as_of_date         <> '' AND TRY_CAST(as_of_date         AS DATE)    IS NULL)
        """
    ).fetchone()[0]
    if bad:
        con.close()
        raise SystemExit(f"ABORT: {bad} row(s) in {SEED} have an unparseable date/int — fix the seed.")
    # Enforce the warming<->date invariant that core.v_warmup_golive_daily relies on: a
    # 'warming' cohort MUST carry a warmup_start_date, otherwise its go_live_date is NULL and
    # it would silently drop out of the go-live ramp (under-counting fresh capacity). Pending /
    # error / active cohorts may legitimately have no date.
    bad_warming = con.execute(
        "SELECT count(*) FROM _seed WHERE status = 'warming' AND coalesce(warmup_start_date, '') = ''"
    ).fetchone()[0]
    if bad_warming:
        con.close()
        raise SystemExit(
            f"ABORT: {bad_warming} 'warming' row(s) in {SEED} have no warmup_start_date — "
            f"they would silently drop from the go-live ramp; set the date or change the status."
        )

    # Explicit column list -> bind by NAME, not position (robust to future column-order
    # drift in either the CSV or DDL 1015). Idempotent INSERT OR REPLACE on the cohort_id PK.
    con.execute(
        """
        INSERT OR REPLACE INTO core.batch_warmup_schedule
          (cohort_id, provider, batch_key, workspace, warmup_start_date, warmup_length_days,
           n_accounts, status, source, as_of_date, notes, _curated_at)
        SELECT
          cohort_id,
          provider,
          batch_key,
          workspace,
          TRY_CAST(warmup_start_date AS DATE)                   AS warmup_start_date,
          TRY_CAST(warmup_length_days AS INTEGER)               AS warmup_length_days,  -- guard ensures non-null
          TRY_CAST(n_accounts AS INTEGER)                       AS n_accounts,
          status,
          source,
          TRY_CAST(as_of_date AS DATE)                          AS as_of_date,
          notes,
          now()                                                 AS _curated_at
        FROM _seed
        """
    )

    cohorts, inboxes = con.execute(
        "SELECT count(*), SUM(n_accounts) FROM core.batch_warmup_schedule"
    ).fetchone()
    breakdown = con.execute(
        """
        SELECT provider, workspace, status, count(*) AS cohorts, SUM(n_accounts) AS inboxes
        FROM core.batch_warmup_schedule
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """
    ).fetchall()
    con.close()

    print(f"core.batch_warmup_schedule: {cohorts} cohorts, {inboxes} inboxes", flush=True)
    for provider, workspace, status, c, accts in breakdown:
        print(f"  {provider:<8} {str(workspace):<10} {status:<14} {c:>2} cohorts / {str(accts):>6} inboxes", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
