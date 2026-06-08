"""DNS + blacklist sweep -> raw_dns_sweep_domain + raw_blacklist_check.

Registry = DISTINCT active sending domains from core.sending_account (continuously
refreshed upstream). Engine = sources/dns.py (pure functions; all the resolver/
false-positive/error-handling logic lives there). This entity is the DB glue: fan out
the sweep, stream results into DuckDB in batches.

v1 blocklist set: a small set of domain zones (surbl, spamrl, spamhaus_dbl). IP zones +
Spamhaus DQS REST deferred to v1.1.

This is a long-running phase at scale and holds the DuckDB write lock for the duration
(single-writer). Run it in the background BEFORE the nightly window, not during cron:
    nohup python -m core.orchestrator --phase dns_sweep > dns_sweep.log 2>&1 &

Registers under the 'dns_sweep' phase.
"""
from __future__ import annotations

import json
import logging
import os

from core.registry import Registry, RunContext
from core.sync_run import PhaseResult
from sources import dns as dnslib

logger = logging.getLogger("entities.dns_sweep")

# v1 blocklists: the production-proven domain zones only.
V1_BLOCKLIST_NAMES = {"surbl", "spamrl", "spamhaus_dbl"}
# Production defaults (override via env). qps caps at 64 workers in dns.py.
# redirect OFF by default: the HTTP-HEAD probe is ~87% of per-domain wall time on our
# non-web sending domains (GAP F1) — off keeps the nightly sweep ~38 min, not ~6 h.
QPS = 50
DEFAULT_REDIRECT = "0"
FLUSH_EVERY = 2000  # domains buffered before a batch write

# raw_dns_sweep_domain column order (must match sql/ddl/18_dns.sql).
_SWEEP_COLS = [
    "domain", "mx_provider", "mx_records", "mx_error",
    "a_record_ip", "a_record_24", "a_records", "a_error",
    "spf_record", "spf_authorized_ips", "spf_includes_resolved", "spf_error",
    "dkim_selectors_present", "dkim_tenant_prefix", "dkim_error",
    "dmarc_policy", "dmarc_record", "dmarc_rua", "dmarc_error",
    "ptr", "ptr_error",
    "dns_signature", "redirect_chain", "terminal_redirect", "terminal_tld", "redirect_error",
    "blacklist_count", "any_blacklist_active", "listed_on",
    "sweep_error", "raw_json", "_loaded_at", "_run_id",
]


def _a24(ip: str | None) -> str | None:
    if not ip or ip.count(".") != 3:
        return None
    return ".".join(ip.split(".")[:3]) + ".0/24"


def _j(v) -> str | None:
    """JSON-encode a list/dict; pass None through."""
    if v is None:
        return None
    return json.dumps(v, default=str)


def _sweep_tuple(r: dict, run_id: str) -> tuple:
    return (
        r.get("domain"),
        r.get("mx_provider"), _j(r.get("mx_records")), r.get("mx_error"),
        r.get("a_record_ip"), _a24(r.get("a_record_ip")), _j(r.get("a_records")), r.get("a_error"),
        r.get("spf_record"), _j(r.get("spf_authorized_ips")), r.get("spf_includes_resolved"), r.get("spf_error"),
        _j(r.get("dkim_selectors_present")), r.get("dkim_tenant_prefix"), r.get("dkim_error"),
        r.get("dmarc_policy"), r.get("dmarc_record"), r.get("dmarc_rua"), r.get("dmarc_error"),
        r.get("ptr"), r.get("ptr_error"),
        r.get("dns_signature"), _j(r.get("redirect_chain")), r.get("terminal_redirect"),
        r.get("terminal_tld"), r.get("redirect_error"),
        r.get("blacklist_count"), r.get("any_blacklist_active"), _j(r.get("listed_on")),
        r.get("sweep_error"), _j(r), "now_placeholder", run_id,
    )


def _blacklist_rows(r: dict, run_id: str, blocklist_names: list[str]) -> list[tuple]:
    """One row per (domain, blocklist): listed | error | clean."""
    domain = r.get("domain")
    listed_on = set(r.get("listed_on") or [])
    errors = {e.get("blocklist"): e.get("reason") for e in (r.get("blacklist_errors") or [])}
    listings = {l.get("blocklist"): l.get("answer") for l in (r.get("blacklist_listings") or [])}
    rows = []
    for bl in blocklist_names:
        if bl in listed_on:
            rows.append((domain, bl, "listed", listings.get(bl), "now_placeholder", run_id))
        elif bl in errors:
            rows.append((domain, bl, "error", errors.get(bl), "now_placeholder", run_id))
        else:
            rows.append((domain, bl, "clean", None, "now_placeholder", run_id))
    return rows


def run_dns_sweep(ctx: RunContext) -> PhaseResult:
    conn = ctx.db
    run_id = ctx.run_id

    dnsbls = [bl for bl in dnslib.DEFAULT_DNSBLS if bl.name in V1_BLOCKLIST_NAMES]
    blocklist_names = [bl.name for bl in dnsbls]
    qps = int(os.environ.get("DNS_SWEEP_QPS", QPS))
    include_redirect = os.environ.get("DNS_SWEEP_REDIRECT", DEFAULT_REDIRECT) != "0"
    logger.info("dns_sweep blocklists=%s qps=%d redirect=%s",
                blocklist_names, qps, include_redirect)

    domains = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT domain FROM core.sending_account "
            "WHERE is_active AND domain IS NOT NULL AND domain <> ''"
        ).fetchall()
    ]
    limit = os.environ.get("DNS_SWEEP_LIMIT")
    if limit:
        domains = domains[: int(limit)]
        logger.info("DNS_SWEEP_LIMIT=%s -> testing on %d domains", limit, len(domains))
    logger.info("dns_sweep registry: %d active domains", len(domains))
    if not domains:
        return PhaseResult(rows_in=0, rows_out=0, notes={"skipped": "no domains"})

    # Idempotent within a run.
    conn.execute("DELETE FROM raw_dns_sweep_domain WHERE _run_id = ?", [run_id])
    conn.execute("DELETE FROM raw_blacklist_check WHERE _run_id = ?", [run_id])

    sweep_buf: list[tuple] = []
    bl_buf: list[tuple] = []
    counts = {"swept": 0, "listed": 0, "errored": 0}

    sweep_sql = (
        f"INSERT INTO raw_dns_sweep_domain ({', '.join(_SWEEP_COLS)}) "
        f"VALUES ({', '.join('now()' if c == '_loaded_at' else '?' for c in _SWEEP_COLS)})"
    )
    bl_sql = (
        "INSERT INTO raw_blacklist_check (domain, blocklist, status, detail, checked_at, _run_id) "
        "VALUES (?, ?, ?, ?, now(), ?)"
    )

    def _flush():
        if sweep_buf:
            # Drop the now_placeholder slot for _loaded_at (handled by now() in SQL).
            conn.executemany(sweep_sql, [t[:-2] + (t[-1],) for t in sweep_buf])
            sweep_buf.clear()
        if bl_buf:
            conn.executemany(bl_sql, [(d, b, s, det, rid) for (d, b, s, det, _ph, rid) in bl_buf])
            bl_buf.clear()

    def _on_result(r: dict):
        sweep_buf.append(_sweep_tuple(r, run_id))
        bl_buf.extend(_blacklist_rows(r, run_id, blocklist_names))
        counts["swept"] += 1
        if r.get("any_blacklist_active"):
            counts["listed"] += 1
        if r.get("sweep_error"):
            counts["errored"] += 1
        if counts["swept"] % FLUSH_EVERY == 0:
            _flush()
            logger.info("  swept %d/%d (listed=%d errored=%d)",
                        counts["swept"], len(domains), counts["listed"], counts["errored"])

    dnslib.sweep_domains(
        domains,
        qps=qps,
        dnsbls=dnsbls,
        include_redirect=include_redirect,
        include_spamhaus_dqs=False,
        on_result=_on_result,
    )
    _flush()

    n_sweep = conn.execute(
        "SELECT count(*) FROM raw_dns_sweep_domain WHERE _run_id = ?", [run_id]
    ).fetchone()[0]
    n_bl = conn.execute(
        "SELECT count(*) FROM raw_blacklist_check WHERE _run_id = ?", [run_id]
    ).fetchone()[0]
    logger.info("dns_sweep done: %d domains, %d blacklist checks, %d listed, %d errored",
                n_sweep, n_bl, counts["listed"], counts["errored"])
    return PhaseResult(
        rows_in=len(domains), rows_out=n_sweep,
        notes={"blacklist_checks": n_bl, "listed": counts["listed"],
               "errored": counts["errored"], "blocklists": blocklist_names},
    )


def register(registry: Registry) -> None:
    registry.add_phase("dns_sweep", "sweep", run_dns_sweep)
