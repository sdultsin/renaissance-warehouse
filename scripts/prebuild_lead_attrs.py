#!/usr/bin/env python3
"""
Pre-build core.lead_attrs from the lead mirror.

Runs standalone before the nightly orchestrator (cron: 02:30 UTC).
Reads lead_mirror.duckdb in READ-ONLY mode via ATTACH (safe to run while mirror is updating).
Writes core.lead_attrs into warehouse.duckdb — a slim, pre-materialized table of scraped
attrs for only the ~247k signal leads, so lead_spine.py can do a fast in-warehouse join
during the canonical phase instead of an expensive cross-file ATTACH.

Usage: python scripts/prebuild_lead_attrs.py
"""
import logging
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core import db as db_module  # warehouse-writer single-writer flock lives here

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scripts.prebuild_lead_attrs")

WAREHOUSE = "/root/core/warehouse.duckdb"
MIRROR = "/root/renaissance-worker/jobs/lead-mirror/lead_mirror.duckdb"


def main() -> int:
    if not Path(MIRROR).exists():
        logger.error("Lead mirror not found: %s", MIRROR)
        return 1

    logger.info("Connecting to warehouse (write mode)...")
    conn = db_module.connect(Path(WAREHOUSE))

    try:
        logger.info("Attaching lead mirror (read-only)...")
        conn.execute("DETACH DATABASE IF EXISTS leadmirror")
        conn.execute(f"ATTACH '{MIRROR}' AS leadmirror (READ_ONLY)")

        n_signal = conn.execute("SELECT count(*) FROM core.lead WHERE email IS NOT NULL").fetchone()[0]
        logger.info("Signal leads with email: %d", n_signal)

        if n_signal == 0:
            logger.warning("core.lead has no email-keyed leads — prebuild skipped (run after canonical)")
            return 0

        logger.info("Building core.lead_attrs from mirror for %d signal leads...", n_signal)
        conn.execute("DELETE FROM core.lead_attrs")
        conn.execute("""
            INSERT INTO core.lead_attrs (
                email, first_name, company_name, general_industry, specific_industry,
                seniority, company_size, city, state, source, source_list_name
            )
            SELECT
                lm.email,
                coalesce(lm.enrich_first_name, lm.first_name)      AS first_name,
                coalesce(lm.enrich_company_name, lm.company_name)   AS company_name,
                lm.general_industry,
                lm.specific_industry,
                lm.seniority,
                lm.company_size,
                lm.city,
                lm.state,
                lm.source,
                lm.source_list_name
            FROM core.lead cl
            JOIN leadmirror.mirror.leads_current lm
              ON cl.email IS NOT NULL AND lm.email = cl.email
            WHERE cl.email IS NOT NULL
        """)

        n_attrs = conn.execute("SELECT count(*) FROM core.lead_attrs").fetchone()[0]
        pct = round(n_attrs / n_signal * 100, 1) if n_signal else 0
        logger.info("core.lead_attrs materialized: %d rows (%.1f%% of signal leads matched)", n_attrs, pct)

        conn.execute("DETACH DATABASE IF EXISTS leadmirror")
        return 0

    except Exception as exc:
        logger.error("prebuild_lead_attrs failed: %s", exc, exc_info=True)
        try:
            conn.execute("DETACH DATABASE IF EXISTS leadmirror")
        except Exception:
            pass
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
