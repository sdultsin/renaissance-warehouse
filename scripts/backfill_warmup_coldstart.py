#!/usr/bin/env python3
"""Warmup + cold-start backfill into core.sending_account (#7). The warmup_started_at / warmup_score /
warmup_phase columns exist (DDL) but are EMPTY. Sources:
  - warmup_started_at  <- live /accounts timestamp_warmup_start (poll_live_accounts.py snapshot)
  - warmup_score       <- live stat_warmup_score
  - warmup_phase       <- live warmup_status (1=active warmup, 0=off, -1=error)
  - first_cold_send_at <- DERIVED: earliest date with actual_sends>0 per account in core.sending_account_daily
                          (the distinction Sam cares about — warmup-start vs first COLD send — was unmodeled)

WAREHOUSE WRITE — run in a coordinated single-writer window (take the warehouse.write.lock flock /
warehouse-writer wlock; do NOT run alongside the 3:30 nightly). Republish the serving snapshot after.
Idempotent (re-derives from current snapshot each run). first_cold_send_at requires DDL 77
(ALTER TABLE core.sending_account ADD COLUMN first_cold_send_at TIMESTAMP) — see 77_first_cold_send.sql.

Usage: python3 backfill_warmup_coldstart.py --live /opt/duckdb/warehouse_current.duckdb is WRONG —
point --db at the LIVE writer /root/core/warehouse.duckdb and --live-parquet at the snapshot.
"""
from __future__ import annotations
import argparse, fcntl, os
import duckdb

WRITE_LOCK = "/root/core/warehouse.write.lock"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/root/core/warehouse.duckdb", help="LIVE warehouse writer")
    ap.add_argument("--live-parquet", default="/root/core/live_accounts/latest.parquet")
    args = ap.parse_args()

    # warehouse single-writer flock (fcntl.flock == BSD flock == the nightly's `flock`); skip if busy
    lock_fd = os.open(WRITE_LOCK, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("WAREHOUSE WRITE LOCK BUSY (nightly or another writer active). Exiting.", flush=True)
        return 1

    con = duckdb.connect(args.db)  # writer; lock held above
    con.execute(f"CREATE OR REPLACE TEMP VIEW live AS SELECT lower(email) email, "
                f"timestamp_warmup_start, stat_warmup_score, warmup_status "
                f"FROM read_parquet('{args.live_parquet}')")

    # warmup state from live truth
    con.execute("""
        UPDATE core.sending_account sa
        SET warmup_started_at = TRY_CAST(l.timestamp_warmup_start AS TIMESTAMP),
            warmup_score      = TRY_CAST(l.stat_warmup_score AS DOUBLE),
            warmup_phase      = CASE l.warmup_status WHEN 1 THEN 'warming'
                                     WHEN 0 THEN 'off' WHEN -1 THEN 'error' ELSE NULL END
        FROM live l
        WHERE lower(sa.email) = l.email
    """)
    w = con.execute("SELECT count(warmup_started_at) FROM core.sending_account").fetchone()[0]

    # first COLD send per account = earliest date with actual_sends>0 (requires col from DDL 77)
    try:
        con.execute("""
            UPDATE core.sending_account sa
            SET first_cold_send_at = fc.first_send
            FROM (
              SELECT account_id, min(date) AS first_send
              FROM core.sending_account_daily WHERE actual_sends > 0 GROUP BY 1
            ) fc
            WHERE lower(sa.email) = lower(fc.account_id)
        """)
        c = con.execute("SELECT count(first_cold_send_at) FROM core.sending_account").fetchone()[0]
        print(f"first_cold_send_at populated: {c:,}", flush=True)
    except Exception as exc:
        print(f"first_cold_send_at skipped (run 77_first_cold_send.sql first): {exc}", flush=True)

    con.close()
    print(f"warmup_started_at populated: {w:,}", flush=True)
    print("DONE — now republish the serving snapshot.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
