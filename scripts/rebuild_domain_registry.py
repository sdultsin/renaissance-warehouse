#!/usr/bin/env python3
"""Track I — rebuild core.domain_registry (DDL 58) from its four sources.

The table was created ad-hoc in the 06-08 hardening session, never versioned, and lost
in the 06-08/09 corruption recovery. This script reconstructs it from current state:

  universe = Domain Tech Sheet 'Domains' tab  (raw_sheets_domain_tech_domains, unpivoted)
           ∪ registrar APIs                   (raw_registrar_domains)
           ∪ warehouse                        (core.domain ∪ core.sending_account.domain)
           ∪ Cloudflare zones                 (raw_cloudflare_zones)

Column derivations (documented so the next rebuild isn't archaeology):
  tld                — suffix after the last dot.
  registrar/account  — registrar API row wins; else the sheet column header the domain
                       sits under ('Porkbun #3' → porkbun); else 'otd_managed'/'OTD' if
                       the domain only exists via OTD-esp sending accounts.
  status             — 'active' if the domain has status='active' sending accounts;
                       'assigned' if sheet says USED (or accounts/workspace exist) but
                       none active; 'unused' otherwise.
  assigned_workspace — dominant workspace_slug among the domain's sending accounts.
  inbox_count        — count of status='active' sending accounts (same semantics as
                       core.domain.inbox_count).
  expires_at         — sheet expiration (exact registrar dates come later via
                       backfill_purchased_at_registrars.py, which overwrites).
  purchased_at       — left NULL here; filled by the Track-I backfills
                       (registrar caches = exact, sheet expiry−1y = derived).

After this script, run (in order):
  1. backfill_domain_registry.py --ns /root/core/ns_sweep.parquet   (NS + CF flags)
  2. backfill_purchased_at_registrars.py --from-cache               (exact dates)
  3. backfill_purchased_at_from_sheet.py                            (derived dates)

Needs the single-writer lock (run in a quiet window).

Usage:
    python scripts/rebuild_domain_registry.py [--db PATH]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from core import db as db_module

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scripts.rebuild_domain_registry")

# Account-name columns of the sheet's positional row_json sit at indices 3, 6, 9, ...
# (cols 0-2 are a side summary block). 250 covers all current + future accounts.
SHEET_UNPIVOT = """
CREATE OR REPLACE TEMP TABLE sheet_dom AS
WITH hdr AS (
    SELECT i, json_extract_string(row_json, '$[' || i::VARCHAR || ']') AS account_name
    FROM raw_sheets_domain_tech_domains, (SELECT unnest(generate_series(3, 250, 3)) AS i)
    WHERE row_index = 0
      AND NULLIF(trim(json_extract_string(row_json, '$[' || i::VARCHAR || ']')), '') IS NOT NULL
),
cells AS (
    SELECT
        lower(trim(json_extract_string(d.row_json, '$[' || h.i::VARCHAR || ']')))  AS domain,
        h.account_name,
        TRY_STRPTIME(json_extract_string(d.row_json, '$[' || (h.i+1)::VARCHAR || ']'),
                     '%b %d, %Y')::DATE                                            AS expires_at,
        upper(trim(coalesce(json_extract_string(d.row_json, '$[' || (h.i+2)::VARCHAR || ']'), ''))) AS sheet_status
    FROM raw_sheets_domain_tech_domains d CROSS JOIN hdr h
    WHERE d.row_index >= 1
)
SELECT
    domain,
    arg_max(account_name, coalesce(expires_at, DATE '1970-01-01')) AS registrar_account,
    max(expires_at)                                                AS expires_at,
    bool_or(sheet_status = 'USED')                                 AS sheet_used
FROM cells
WHERE domain LIKE '%.%'
GROUP BY domain
"""

BUILD = """
CREATE OR REPLACE TEMP TABLE reg_api AS
SELECT domain,
       arg_max(lower(registrar), _loaded_at)       AS registrar,
       arg_max(registrar_account, _loaded_at)      AS registrar_account
FROM raw_registrar_domains
WHERE NULLIF(domain, '') IS NOT NULL
GROUP BY domain;

CREATE OR REPLACE TEMP TABLE acct AS
SELECT domain,
       count(*) FILTER (WHERE status = 'active')   AS inbox_count,
       mode(workspace_slug)                        AS assigned_workspace,
       bool_or(esp = 'otd')                        AS any_otd
FROM core.sending_account
WHERE NULLIF(domain, '') IS NOT NULL
GROUP BY domain;

CREATE OR REPLACE TEMP TABLE universe AS
SELECT domain FROM sheet_dom
UNION
SELECT domain FROM reg_api
UNION
SELECT lower(domain) FROM core.domain WHERE NULLIF(domain, '') IS NOT NULL
UNION
SELECT domain FROM acct
UNION
SELECT lower(domain) FROM raw_cloudflare_zones WHERE NULLIF(domain, '') IS NOT NULL;

DELETE FROM core.domain_registry;

INSERT INTO core.domain_registry
SELECT
    u.domain,
    lower(regexp_extract(u.domain, '\\.([a-z0-9-]+)$', 1))               AS tld,
    CASE
        WHEN r.registrar IS NOT NULL THEN r.registrar
        WHEN s.registrar_account IS NOT NULL
            THEN lower(regexp_extract(s.registrar_account, '^([A-Za-z]+)', 1))
        WHEN coalesce(a.any_otd, FALSE) THEN 'otd_managed'
        ELSE NULL
    END                                                                  AS registrar,
    coalesce(r.registrar_account, s.registrar_account,
             CASE WHEN coalesce(a.any_otd, FALSE) THEN 'OTD' END)        AS registrar_account,
    CASE
        WHEN coalesce(a.inbox_count, 0) > 0 THEN 'active'
        WHEN coalesce(s.sheet_used, FALSE) OR a.domain IS NOT NULL THEN 'assigned'
        ELSE 'unused'
    END                                                                  AS status,
    NULL::DATE                                                           AS purchased_at,
    FALSE                                                                AS purchased_at_is_derived,
    s.expires_at                                                         AS expires_at,
    NULL::VARCHAR                                                        AS nameserver_host,
    FALSE                                                                AS ns_at_cloudflare,
    a.assigned_workspace                                                 AS assigned_workspace,
    coalesce(a.inbox_count, 0)                                           AS inbox_count,
    concat_ws('+',
        CASE WHEN s.domain IS NOT NULL THEN 'sheet' END,
        CASE WHEN r.domain IS NOT NULL THEN 'registrar_api' END,
        CASE WHEN w.domain IS NOT NULL OR a.domain IS NOT NULL THEN 'warehouse' END,
        CASE WHEN c.domain IS NOT NULL THEN 'cloudflare' END)            AS source,
    (w.domain IS NOT NULL OR a.domain IS NOT NULL)                       AS in_warehouse,
    (c.domain IS NOT NULL)                                               AS in_cf,
    (s.domain IS NOT NULL)                                               AS in_sheet,
    (r.domain IS NOT NULL)                                               AS in_registrar_api,
    now()                                                                AS _loaded_at
FROM universe u
LEFT JOIN sheet_dom s ON s.domain = u.domain
LEFT JOIN reg_api   r ON r.domain = u.domain
LEFT JOIN acct      a ON a.domain = u.domain
LEFT JOIN (SELECT DISTINCT lower(domain) AS domain FROM core.domain)        w ON w.domain = u.domain
LEFT JOIN (SELECT DISTINCT lower(domain) AS domain FROM raw_cloudflare_zones) c ON c.domain = u.domain;
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args(argv)
    conn = db_module.connect(Path(args.db) if args.db else None)

    conn.execute("BEGIN")
    try:
        conn.execute(SHEET_UNPIVOT)
        n_sheet = conn.execute("SELECT count(*) FROM sheet_dom").fetchone()[0]
        logger.info("sheet domains unpivoted: %d", n_sheet)
        for stmt in BUILD.split(";"):
            if stmt.strip():
                conn.execute(stmt)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    total = conn.execute("SELECT count(*) FROM core.domain_registry").fetchone()[0]
    by_status = conn.execute(
        "SELECT status, count(*) FROM core.domain_registry GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    by_registrar = conn.execute(
        "SELECT coalesce(registrar,'NULL'), count(*) FROM core.domain_registry GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    tld_cov = conn.execute(
        "SELECT round(100.0*avg((tld IS NOT NULL)::int),1) FROM core.domain_registry"
    ).fetchone()[0]
    logger.info("core.domain_registry rebuilt: %d rows | tld %.1f%% | status %s | registrar %s",
                total, tld_cov, by_status, by_registrar)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
