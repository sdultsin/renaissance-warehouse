"""core.recipient_domain — receiving-ESP classification of recipient (lead) domains.

Two-tier, volume-weighted (the recipient universe is ~3.25M domains in 30 days — a full
MX sweep would be ~40h; this classifies what carries the volume):

  1. CONSUMER MAP (instant, no MX): the known consumer ESPs. gmail alone is ~36% of
     send volume, so the map covers the majority of sends for free.
  2. MX LOOKUP (incremental): the top company domains BY SEND VOLUME not already
     classified, via sources/dns.py resolve_mx (MX-only — one query per domain, fast).
     Capped at RECIPIENT_MX_TOP_N (default 150k). Incremental: domains already in
     core.recipient_domain are skipped, so nightly runs only sweep new high-volume domains.

Volume is scored from the warehouse-local raw_pipeline_contact_frequency_campaign_daily
([2026-07-18 wave-2, MOF-10] repointed from pipeline-supabase postgres_scanner; the local
table is inbox-fed hourly + full history backfilled 2026-07-18, parity exact).

Registers under the 'dns_sweep' phase (it's a sweep; runs after the sender sweep).
Long tail stays unclassified (matrix buckets it as 'unknown').
"""
from __future__ import annotations

import logging
import os

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources import dns as dnslib

logger = logging.getLogger("entities.recipient_domain")

VOLUME_WINDOW_DAYS = int(os.environ.get("RECIPIENT_VOLUME_DAYS", "60"))
TOP_N = int(os.environ.get("RECIPIENT_MX_TOP_N", "150000"))
QPS = int(os.environ.get("RECIPIENT_MX_QPS", "64"))
CONTACT_FREQ = "contact_frequency_campaign_daily"

# Known consumer ESPs -> receiving ESP. Classified without MX.
CONSUMER_MAP: dict[str, str] = {
    "gmail.com": "google", "googlemail.com": "google",
    "outlook.com": "microsoft", "hotmail.com": "microsoft", "live.com": "microsoft",
    "msn.com": "microsoft", "hotmail.co.uk": "microsoft", "outlook.es": "microsoft",
    "yahoo.com": "yahoo", "ymail.com": "yahoo", "rocketmail.com": "yahoo",
    "yahoo.co.uk": "yahoo", "yahoo.ca": "yahoo", "aol.com": "yahoo",
    "icloud.com": "apple", "me.com": "apple", "mac.com": "apple",
    "comcast.net": "isp", "att.net": "isp", "sbcglobal.net": "isp",
    "bellsouth.net": "isp", "verizon.net": "isp", "charter.net": "isp",
    "cox.net": "isp", "optonline.net": "isp", "earthlink.net": "isp",
    "windstream.net": "isp", "frontier.com": "isp", "roadrunner.com": "isp",
    "mail.com": "other", "protonmail.com": "other", "gmx.com": "other", "zoho.com": "other",
}

# MX provider (dns.py classification) -> receiving ESP.
_MX_TO_ESP = {"google": "google", "outlook": "microsoft", "mimecast": "other",
              "barracuda": "other", "other": "other", "none": "unknown"}


def register(registry: Registry) -> None:
    registry.add_phase("dns_sweep", "recipient_domain", run_recipient_domain)


def run_recipient_domain(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id

    # --- 1. Score recipient domains by send volume (warehouse-local) ------------
    # [2026-07-18 wave-2] reads raw_pipeline_contact_frequency_campaign_daily directly —
    # no legacy-Supabase attach.
    conn.execute("DROP TABLE IF EXISTS _recip_vol")
    conn.execute(
        f"""
        CREATE TEMP TABLE _recip_vol AS
        SELECT lower(lead_domain) AS domain, SUM(sent_count)::BIGINT AS send_volume
        FROM raw_pipeline_{CONTACT_FREQ}
        WHERE send_date >= current_date - INTERVAL '{VOLUME_WINDOW_DAYS} days'
          AND lead_domain IS NOT NULL AND lead_domain <> ''
        GROUP BY 1
        """
    )
    n_domains = conn.execute("SELECT count(*) FROM _recip_vol").fetchone()[0]
    logger.info("recipient volume scored: %d domains (%dd window)", n_domains, VOLUME_WINDOW_DAYS)

    # --- 2. Consumer-map classification (idempotent upsert) ---------------------
    conn.execute(
        "CREATE TEMP TABLE _consumer (domain VARCHAR, recipient_esp VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO _consumer VALUES (?, ?)", list(CONSUMER_MAP.items())
    )
    conn.execute("DELETE FROM core.recipient_domain WHERE classification_method = 'consumer_map'")
    conn.execute(
        """
        INSERT INTO core.recipient_domain
          (domain, recipient_esp, mx_host, mx_provider, classification_method,
           send_volume, resolved_at, last_error)
        SELECT c.domain, c.recipient_esp, NULL, NULL, 'consumer_map',
               COALESCE(v.send_volume, 0), now(), NULL
        FROM _consumer c
        LEFT JOIN _recip_vol v ON v.domain = c.domain
        ON CONFLICT (domain) DO UPDATE SET
          recipient_esp = excluded.recipient_esp,
          classification_method = 'consumer_map',
          send_volume = excluded.send_volume,
          resolved_at = excluded.resolved_at
        """
    )
    n_consumer = conn.execute(
        "SELECT count(*) FROM core.recipient_domain WHERE classification_method='consumer_map'"
    ).fetchone()[0]

    # --- 3. Top company domains by volume, not yet classified -> MX sweep --------
    to_sweep = [
        r[0] for r in conn.execute(
            """
            SELECT v.domain FROM _recip_vol v
            LEFT JOIN _consumer c ON c.domain = v.domain
            LEFT JOIN core.recipient_domain rd ON rd.domain = v.domain
            WHERE c.domain IS NULL AND rd.domain IS NULL
            ORDER BY v.send_volume DESC
            LIMIT ?
            """,
            [TOP_N],
        ).fetchall()
    ]
    logger.info("MX sweep target: %d company domains (top %d by volume, incremental)",
                len(to_sweep), TOP_N)

    swept = {"n": 0}
    mx_buf: list[tuple] = []
    rd_buf: list[tuple] = []

    def _flush():
        if mx_buf:
            conn.executemany(
                "INSERT INTO raw_recipient_mx (domain, mx_host, mx_provider, mx_error, _loaded_at, _run_id) "
                "VALUES (?, ?, ?, ?, now(), ?)", mx_buf)
            mx_buf.clear()
        if rd_buf:
            conn.executemany(
                """INSERT INTO core.recipient_domain
                   (domain, recipient_esp, mx_host, mx_provider, classification_method,
                    send_volume, resolved_at, last_error)
                   VALUES (?, ?, ?, ?, 'mx_lookup', ?, now(), ?)
                   ON CONFLICT (domain) DO UPDATE SET
                     recipient_esp=excluded.recipient_esp, mx_host=excluded.mx_host,
                     mx_provider=excluded.mx_provider, classification_method='mx_lookup',
                     send_volume=excluded.send_volume, resolved_at=excluded.resolved_at,
                     last_error=excluded.last_error""", rd_buf)
            rd_buf.clear()

    vol_lookup = dict(conn.execute(
        "SELECT domain, send_volume FROM _recip_vol WHERE domain IN "
        "(SELECT domain FROM _recip_vol ORDER BY send_volume DESC LIMIT ?)", [TOP_N]
    ).fetchall())

    def _on_mx(r: dict):
        dom = r.get("domain")
        recs = r.get("mx_records") or []
        mx_host = recs[0] if recs else None
        prov = r.get("mx_provider") or "none"
        err = r.get("mx_error")
        esp = _MX_TO_ESP.get(prov, "other")
        mx_buf.append((dom, mx_host, prov, err, run_id))
        rd_buf.append((dom, esp, mx_host, prov, int(vol_lookup.get(dom, 0)), err))
        swept["n"] += 1
        if swept["n"] % 5000 == 0:
            _flush()
            logger.info("  MX swept %d/%d", swept["n"], len(to_sweep))

    if to_sweep:
        _mx_sweep(to_sweep, qps=QPS, on_result=_on_mx)
        _flush()

    n_total = conn.execute("SELECT count(*) FROM core.recipient_domain").fetchone()[0]
    by_esp = dict(conn.execute(
        "SELECT recipient_esp, count(*) FROM core.recipient_domain GROUP BY 1"
    ).fetchall())
    logger.info("core.recipient_domain: %d rows (consumer=%d, mx=%d) by_esp=%s",
                n_total, n_consumer, swept["n"], by_esp)
    return PhaseResult(rows_in=len(to_sweep), rows_out=n_total,
                       notes={"consumer": n_consumer, "mx_swept": swept["n"], "by_esp": by_esp})


def _mx_sweep(domains, qps, on_result):
    """MX-only fan-out (lighter than dns.sweep_domains — no SPF/DKIM/blacklist/redirect)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    workers = max(1, min(qps, 64))

    def _one(d):
        d = d.strip().lower().rstrip(".")
        try:
            res = dnslib.resolve_mx(d)
            res["domain"] = d
            return res
        except Exception as exc:  # noqa: BLE001
            return {"domain": d, "mx_records": [], "mx_provider": "none",
                    "mx_error": f"{exc.__class__.__name__}:{exc}"}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_one, d) for d in domains]
        for fut in as_completed(futures):
            on_result(fut.result())
