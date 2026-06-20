"""core.domain canonical entity (spec 07).

Spine = raw_dns_sweep_domain (latest sweep). Enriched with:
  - esp / infra_provider / lifecycle / inbox_count aggregated from core.sending_account
  - ns_provider / registrar / acquisition_date / acquisition_batch from the Lucas/Tomer
    .co NS-handoff CSV (seed_data/domains/ns-handoff.csv, 2,002 rows)
  - cost_acquisition derived from the .co batch row in core.cost_ledger ($1.80/domain)

Registers under the 'canonical' phase, AFTER the dns_sweep phase has written
raw_dns_sweep_domain. No-op if the sweep table is empty (e.g. canonical run before
a sweep). Full rebuild each run.

ORDERING NOTE: within the canonical phase the orchestrator runs entities in sorted-
filename order, so `domain` (d) runs before `sending_account` (s). domain reads
core.sending_account for esp/infra/lifecycle aggregation — within one nightly pass
that is the PRIOR run's classification (core.sending_account isn't emptied until its
own entity runs later the same night). Effect: domain's esp/lifecycle is at most
one-run-stale (it converges next night; never empty after the first run). Inbox→ESP
classification is slow-moving, so this lag is immaterial. If it ever matters, run
`--phase canonical` twice, or rename this module to sort after sending_account.
"""
from __future__ import annotations

import logging
import os

from core.config import REPO_ROOT
from core.registry import Registry, RunContext
from core.sync_run import PhaseResult

logger = logging.getLogger("entities.domain")

SWEEP = "raw_dns_sweep_domain"
NS_CSV = os.environ.get(
    "DOMAIN_NS_CSV", str(REPO_ROOT / "seed_data" / "domains" / "ns-handoff.csv")
)
CO_BATCH_ID = "dynadot_2026-05-19_co_sale_14978"
# Full row-level .co-sale batch (14,978 domains across Dynadot #9-15), derived from
# the registrar registry. Drives $1.80/domain cost + batch tagging for the WHOLE
# purchase — not just the 2,002 NS-handoff subset given to Tomer/Lucas.
BATCH_CSV = os.environ.get(
    "DOMAIN_CO_BATCH_CSV", str(REPO_ROOT / "seed_data" / "domains" / "co-sale-batch.csv")
)


def register(registry: Registry) -> None:
    registry.add_phase("canonical", "domain", run_domain)


def run_domain(ctx: RunContext) -> PhaseResult:
    db = ctx.db

    have = db.execute(
        f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{SWEEP}'"
    ).fetchone()[0]
    if not have or db.execute(f"SELECT count(*) FROM {SWEEP}").fetchone()[0] == 0:
        logger.warning("%s empty/absent — skipping core.domain build", SWEEP)
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no dns sweep"})

    ns_exists = os.path.exists(NS_CSV)
    ns_cte = (
        f"SELECT lower(trim(domain)) AS domain, ns_provider, slot AS registrar_account, "
        f"TRY_CAST(registered_at AS DATE) AS acquisition_date "
        f"FROM read_csv_auto('{NS_CSV}')"
        if ns_exists
        else "SELECT NULL::VARCHAR AS domain, NULL::VARCHAR AS ns_provider, "
             "NULL::VARCHAR AS registrar_account, NULL::DATE AS acquisition_date WHERE FALSE"
    )
    if not ns_exists:
        logger.warning("NS handoff CSV not found at %s — ns_provider will be NULL", NS_CSV)

    batch_exists = os.path.exists(BATCH_CSV)
    batch_cte = (
        f"SELECT lower(trim(domain)) AS domain, registrar_account, "
        f"TRY_CAST(acquisition_date AS DATE) AS acquisition_date "
        f"FROM read_csv_auto('{BATCH_CSV}')"
        if batch_exists
        else "SELECT NULL::VARCHAR AS domain, NULL::VARCHAR AS registrar_account, "
             "NULL::DATE AS acquisition_date WHERE FALSE"
    )
    if not batch_exists:
        logger.warning("co-sale batch CSV not found at %s — only NS-handoff domains get batch cost", BATCH_CSV)

    db.execute("DELETE FROM core.domain")
    db.execute(
        f"""
        INSERT INTO core.domain (
          domain, registrar, registrar_account, acquisition_date, acquisition_batch, brand_prefix,
          esp, infra_provider, ns_provider,
          mx_provider, a_record_ip, a_record_24, spf_authorized_ips, dkim_selectors,
          dkim_tenant_prefix, dmarc_policy, dns_signature, redirect_chain, terminal_redirect,
          lifecycle_state, dns_configured_at, first_send_at, paused_at, retired_at,
          sheet_status, blacklist_count, any_blacklist_active, listed_on, inbox_count,
          cost_acquisition_usd_estimated, cost_renewal_annual_usd_estimated,
          is_active, first_seen_at, last_seen_at, resolved_at
        )
        WITH sw AS (
          SELECT * FROM {SWEEP}
          WHERE _run_id = (SELECT _run_id FROM {SWEEP} ORDER BY _loaded_at DESC LIMIT 1)
          QUALIFY ROW_NUMBER() OVER (PARTITION BY domain ORDER BY _loaded_at DESC) = 1
        ),
        acct AS (
          SELECT domain,
            COUNT(*) FILTER (WHERE is_active)                         AS inbox_count,
            mode(esp)                                                 AS esp,
            mode(infra_provider)                                      AS infra_provider,
            bool_or(is_active AND lifecycle_state = 'active')         AS has_active,
            bool_or(is_active AND lifecycle_state IN ('warming','warmed')) AS has_warming,
            bool_or(is_active AND lifecycle_state = 'paused')         AS has_paused
          FROM core.sending_account
          GROUP BY domain
        ),
        ns AS ({ns_cte}),
        batch AS ({batch_cte}),
        batch_rate AS (
          SELECT total_usd / NULLIF(unit_count, 0) AS per_domain
          FROM core.cost_ledger WHERE attribution_id = '{CO_BATCH_ID}' LIMIT 1
        )
        SELECT
          sw.domain,
          CASE WHEN batch.domain IS NOT NULL THEN 'dynadot' END        AS registrar,
          COALESCE(batch.registrar_account, ns.registrar_account)      AS registrar_account,
          COALESCE(batch.acquisition_date, ns.acquisition_date)        AS acquisition_date,
          CASE WHEN batch.domain IS NOT NULL THEN '{CO_BATCH_ID}' END  AS acquisition_batch,
          regexp_replace(sw.domain, '\\.[a-z0-9]+$', '')              AS brand_prefix,
          acct.esp,
          acct.infra_provider,
          ns.ns_provider,
          sw.mx_provider, sw.a_record_ip, sw.a_record_24, sw.spf_authorized_ips,
          sw.dkim_selectors_present                                   AS dkim_selectors,
          sw.dkim_tenant_prefix, sw.dmarc_policy, sw.dns_signature,
          sw.redirect_chain, sw.terminal_redirect,
          CASE
            WHEN acct.has_active  THEN 'in_use'
            WHEN acct.has_warming THEN 'dns_configured'
            WHEN acct.has_paused  THEN 'paused'
            ELSE 'in_use'
          END                                                         AS lifecycle_state,
          NULL, NULL, NULL, NULL,                                     -- dns_configured_at / first_send_at / paused_at / retired_at
          NULL                                                        AS sheet_status,
          sw.blacklist_count, sw.any_blacklist_active, sw.listed_on,
          COALESCE(acct.inbox_count, 0)                               AS inbox_count,
          CASE WHEN batch.domain IS NOT NULL THEN (SELECT per_domain FROM batch_rate) END AS cost_acquisition_usd_estimated,
          NULL                                                        AS cost_renewal_annual_usd_estimated,
          TRUE                                                        AS is_active,
          now(), now(), now()
        FROM sw
        LEFT JOIN acct  ON acct.domain  = sw.domain
        LEFT JOIN ns    ON ns.domain    = sw.domain
        LEFT JOIN batch ON batch.domain = sw.domain
        """
    )

    # Second source: owned-but-not-yet-sending .co-sale batch domains (the whole 14,978,
    # not just the 2,002 Tomer/Lucas NS-handoff). These have NO inboxes yet (absent from
    # the sweep spine) but per spec 07 belong in core.domain at lifecycle_state='acquired'
    # with their registrar/cost attribution. DNS fingerprint is NULL until they're swept.
    # NS-handoff subset (Tomer/Lucas) keeps esp=outlook/infra=MailIn + ns_provider; the
    # rest are owned-but-unprovisioned (esp/infra/ns NULL until configured).
    if batch_exists:
        db.execute(
            f"""
            INSERT INTO core.domain (
              domain, registrar, registrar_account, acquisition_date, acquisition_batch, brand_prefix,
              esp, infra_provider, ns_provider, lifecycle_state,
              sheet_status, blacklist_count, any_blacklist_active, listed_on, inbox_count,
              cost_acquisition_usd_estimated, cost_renewal_annual_usd_estimated,
              is_active, first_seen_at, last_seen_at, resolved_at
            )
            WITH ns AS ({ns_cte}),
            batch AS ({batch_cte}),
            batch_rate AS (
              SELECT total_usd / NULLIF(unit_count, 0) AS per_domain
              FROM core.cost_ledger WHERE attribution_id = '{CO_BATCH_ID}' LIMIT 1
            )
            SELECT
              batch.domain, 'dynadot',
              COALESCE(batch.registrar_account, ns.registrar_account),
              COALESCE(batch.acquisition_date, ns.acquisition_date),
              '{CO_BATCH_ID}', regexp_replace(batch.domain, '\\.[a-z0-9]+$', ''),
              CASE WHEN ns.domain IS NOT NULL THEN 'outlook' END AS esp,
              CASE WHEN ns.domain IS NOT NULL THEN 'MailIn'  END AS infra_provider,
              ns.ns_provider, 'acquired'     AS lifecycle_state,
              NULL, NULL, NULL, NULL, 0,
              (SELECT per_domain FROM batch_rate), NULL,
              TRUE, now(), now(), now()
            FROM batch
            LEFT JOIN ns ON ns.domain = batch.domain
            WHERE batch.domain NOT IN (SELECT domain FROM core.domain)
            """
        )

    n = db.execute("SELECT count(*) FROM core.domain").fetchone()[0]
    n_ns = db.execute("SELECT count(*) FROM core.domain WHERE ns_provider IS NOT NULL").fetchone()[0]
    n_bl = db.execute("SELECT count(*) FROM core.domain WHERE any_blacklist_active").fetchone()[0]
    logger.info("core.domain rebuilt: %d rows (%d NS-attributed, %d blacklisted)", n, n_ns, n_bl)
    return PhaseResult(rows_in=n, rows_out=n,
                       notes={"ns_attributed": n_ns, "blacklisted": n_bl})
