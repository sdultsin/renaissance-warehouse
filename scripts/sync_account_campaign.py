#!/usr/bin/env python3
"""Sync the account<->active-campaign mapping from the account-truth box DB into the warehouse
(core.account_campaign, DDL 78), so account->campaign->offer is reachable on the serving snapshot.

Reads account_campaign_mappings (account-truth box DB; the campaign-inversion output) and TRUNCATE+
INSERTs into core.account_campaign on the LIVE warehouse writer. Idempotent (full replace). Takes the
warehouse single-writer flock — do NOT run alongside the nightly. Republish the serving snapshot after
(or let the 06:30 publisher pick it up). Cron daily AFTER the account-truth daily run for freshness.
"""
from __future__ import annotations
import fcntl, os, sys, time
import duckdb

WAREHOUSE = "/root/core/warehouse.duckdb"
WRITE_LOCK = "/root/core/warehouse.write.lock"
ACCOUNT_TRUTH = "/root/Renaissance/deliverables/2026-05-27-instantly-account-truth/account_truth.duckdb"


def main() -> int:
    # warehouse single-writer flock (same lock the nightly takes) — never two writers
    lock_fd = os.open(WRITE_LOCK, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("WAREHOUSE WRITE LOCK BUSY (nightly or another writer active). Exiting.", flush=True)
        return 1

    t0 = time.perf_counter()
    con = duckdb.connect(WAREHOUSE)
    con.execute(f"ATTACH '{ACCOUNT_TRUTH}' AS atdb (READ_ONLY)")

    src = con.execute("SELECT count(*) FROM atdb.account_campaign_mappings").fetchone()[0]
    if src == 0:
        print("source account_campaign_mappings is EMPTY — refusing to truncate (run the inversion first).", flush=True)
        return 2

    con.execute("TRUNCATE core.account_campaign")
    con.execute("""
        INSERT INTO core.account_campaign
        SELECT
            lower(email)          AS account_email,
            workspace_slug,
            campaign_id,
            campaign_name,
            campaign_status,
            campaign_status_label,
            now()                 AS _synced_at
        FROM atdb.account_campaign_mappings
        WHERE NULLIF(email,'') IS NOT NULL
    """)
    n = con.execute("SELECT count(*) FROM core.account_campaign").fetchone()[0]
    accts = con.execute("SELECT count(DISTINCT account_email) FROM core.account_campaign").fetchone()[0]
    # offer-resolution sanity via the view
    with_offer = con.execute(
        "SELECT count(*) FROM core.v_account_campaign_offer WHERE offer IS NOT NULL"
    ).fetchone()[0]
    con.close()
    print(f"core.account_campaign synced: {n:,} rows / {accts:,} accounts "
          f"(src {src:,}); {with_offer:,} rows resolve an offer. {time.perf_counter()-t0:.1f}s", flush=True)
    print("REMINDER: republish serving snapshot (or wait for 06:30 publisher).", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
