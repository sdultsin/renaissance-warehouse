#!/usr/bin/env python3
"""Load the staged MilkBox/MailIn cohort row_json CSVs into core.account_registry (DDL 79).
Parses row_json (JSON array of cell values) by position; filters to data rows (email contains '@',
which skips the preamble + header rows). Password/auth columns are NOT ingested. Idempotent
(INSERT OR REPLACE on email PK). Flock-guarded (warehouse single-writer). Run box-side.
"""
from __future__ import annotations
import fcntl, glob, json, os
import duckdb

DB = "/root/core/warehouse.duckdb"
WRITE_LOCK = "/root/core/warehouse.write.lock"
STAGING = "/root/core/cohort_staging"
# sheet column order (0-indexed): 0 domain,4 first,5 last,6 email,8 tag1,9 tag2,10 email_tag,
# 11 platform/partner(vendor),12 batch,13 workspace,14 inbox_type,15 status,16 gender,17 panel,18 offer
COHORT = {"milkbox": "MilkBox", "mailin": "MailIn"}


def jx(pos):  # json_extract_string by position, NULLIF-blank
    return f"NULLIF(json_extract_string(row_json, '$[{pos}]'), '')"


def main() -> int:
    lock_fd = os.open(WRITE_LOCK, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("WAREHOUSE WRITE LOCK BUSY. Exiting.", flush=True)
        return 1

    con = duckdb.connect(DB)
    con.execute("SET threads=2")
    total = 0
    for path in sorted(glob.glob(f"{STAGING}/*.csv")):
        base = os.path.basename(path).replace(".csv", "")        # milkbox_f1
        cohort = COHORT.get(base.split("_")[0], base.split("_")[0])
        con.execute(f"""
            INSERT OR REPLACE INTO core.account_registry
            SELECT
              lower({jx(6)})  AS email,
              lower({jx(0)})  AS domain,
              {jx(4)} AS first_name, {jx(5)} AS last_name,
              {jx(8)} AS rg_tag, {jx(9)} AS rg_range,
              {jx(10)} AS email_tag, {jx(11)} AS vendor,
              {jx(12)} AS batch_tag, {jx(13)} AS workspace_label,
              {jx(14)} AS inbox_type, {jx(15)} AS status, {jx(16)} AS gender,
              {jx(17)} AS panel, {jx(18)} AS offer,
              '{cohort}' AS cohort, '{base}' AS source_tab, now() AS _staged_at
            FROM read_csv_auto('{path}', header=true, ignore_errors=true)
            WHERE json_extract_string(row_json, '$[6]') LIKE '%@%'
        """)
        n = con.execute("SELECT count(*) FROM core.account_registry WHERE source_tab = ?", [base]).fetchone()[0]
        print(f"  {base} ({cohort}): {n:,} accounts", flush=True)
        total += n

    # summary
    rows = con.execute("""
        SELECT cohort, vendor, count(*) accts, count(DISTINCT domain) domains
        FROM core.account_registry GROUP BY 1,2 ORDER BY 1,3 DESC
    """).fetchall()
    grand = con.execute("SELECT count(*) FROM core.account_registry").fetchone()[0]
    con.close()
    print(f"\ncore.account_registry total: {grand:,}", flush=True)
    for cohort, vendor, accts, domains in rows:
        print(f"  {cohort:<10} {str(vendor):<16} {accts:>7,} accts / {domains:>5,} domains", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
