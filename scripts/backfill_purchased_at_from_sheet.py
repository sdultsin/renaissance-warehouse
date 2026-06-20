#!/usr/bin/env python3
"""Track I — backfill core.domain_registry.purchased_at (+ expires_at) from the Domain
Tech Sheet, already mirrored as raw_sheets_domain_tech_domains.

The sheet's "Domains" tab carries, per registrar account, a (domain, Expiration, Status)
triple. row_json is a positional JSON array, so account domain columns sit at indices
3, 6, 9, ... We unpivot those, parse the expiration, and — per Sam (2026-06-08): the
expiration is exactly one year after purchase — derive purchased_at = expiration − 1y.

Only fills purchased_at where NULL (never overwrites an exact registrar-API date) and
flags those rows purchased_at_is_derived = TRUE. Also backfills the exact expires_at
where missing. OTD vendor-provisioned domains aren't in the sheet → stay null (correct).

Needs the writer lock.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.backfill_purchased_at_from_sheet")

# Account domain columns in the positional row_json: 3, 6, ... (every 3rd). 102 covers
# all current accounts (Porkbun #1-8, Spaceship #1-6, Dynadot #1-17, ...).
UNPIVOT = """
WITH idx AS (SELECT unnest(generate_series(3, 120, 3)) AS i),
dom AS (
  SELECT lower(trim(json_extract_string(row_json, '$[' || i::VARCHAR || ']'))) AS domain,
         TRY_STRPTIME(json_extract_string(row_json, '$[' || (i+1)::VARCHAR || ']'),
                      '%b %d, %Y')::DATE AS expires_at
  FROM raw_sheets_domain_tech_domains CROSS JOIN idx
  WHERE row_index >= 2
)
SELECT domain, max(expires_at) AS expires_at
FROM dom
WHERE domain LIKE '%.%' AND expires_at IS NOT NULL
GROUP BY domain
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args(argv)
    conn = db_module.connect(Path(args.db) if args.db else None)

    def cov(where=""):
        return conn.execute(
            f"SELECT round(100.0*avg((purchased_at IS NOT NULL)::int),1) "
            f"FROM core.domain_registry WHERE registrar_account NOT ILIKE '%OTD%' {where}"
        ).fetchone()[0]

    before = cov()
    conn.execute("BEGIN")
    try:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE sheet_dates AS {UNPIVOT}")
        n_sheet = conn.execute("SELECT count(*) FROM sheet_dates").fetchone()[0]
        # exact expires_at where missing
        conn.execute("""
            UPDATE core.domain_registry r SET expires_at = s.expires_at
            FROM sheet_dates s WHERE s.domain = r.domain AND r.expires_at IS NULL
        """)
        # derived purchased_at = expiration - 1y, only where null
        conn.execute("""
            UPDATE core.domain_registry r
            SET purchased_at = (s.expires_at - INTERVAL 1 YEAR),
                purchased_at_is_derived = TRUE
            FROM sheet_dates s
            WHERE s.domain = r.domain AND r.purchased_at IS NULL
        """)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    after = cov()
    after_all = conn.execute(
        "SELECT round(100.0*avg((purchased_at IS NOT NULL)::int),1) FROM core.domain_registry"
    ).fetchone()[0]
    still_null = conn.execute(
        "SELECT count(*) FROM core.domain_registry WHERE purchased_at IS NULL "
        "AND registrar_account NOT ILIKE '%OTD%'"
    ).fetchone()[0]
    logger.info("sheet domains=%d | purchased_at excl-OTD: %.1f%% -> %.1f%% | all: %.1f%% | "
                "still-null(excl-OTD)=%d", n_sheet, before or 0, after, after_all, still_null)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
