"""Shared DNS + blacklist sweep library.

PURE LIBRARY — no DB writes, no ``register(registry)`` hook. The ``domain`` and
``recipient_domain`` entities (specs 07 / 08) import these functions to build
``raw_dns_sweep_domain`` and ``raw_blacklist_check`` rows.

Design notes / gotchas (from specs/07-entity-domain-dns-sweep.md +
specs/07a-blocklist-surveillance-prep-notes.md + the production code at
/root/renaissance-worker/jobs/blocklist-surveillance/{checker,config}.py):

  * **Local recursive resolver is MANDATORY.** ``DNS_RESOLVER`` defaults to
    ``$BLS_DNS_RESOLVER`` or ``127.0.0.2``. SURBL / Spamhaus / URIBL refuse or
    poison queries arriving from large public resolvers (8.8.8.8, 1.1.1.1): they
    return sentinel "you are using a public/abusive resolver" A-records that look
    exactly like real listings. The droplet runs a local recursive resolver on
    127.0.0.2; any host running this sweep MUST too, or every blacklist result is
    garbage. (prep-notes §2, §6.3)

  * **FALSE_POSITIVE_IPS guard.** Some DNSBLs answer with 127.0.0.1 /
    127.255.255.254 / 127.255.255.255 to signal "blocked resolver" / "deprecated
    mirror" / "rate exceeded", NOT a real listing. We drop those. This set matches
    the production checker.py byte-for-byte.

  * **Timeout / SERVFAIL == 'error', NEVER 'clean'.** The existing cron recorded
    timeouts as ``listed=False`` (clean), producing silent false-negatives
    (prep-notes §3: 92 Spamhaus 504s swallowed). Here a DNS timeout / SERVFAIL /
    sentinel-only answer on a blacklist zone goes into ``errors`` and the zone is
    NOT added to ``listed_on``. The caller decides how to treat a domain that
    errored on a given blocklist; we never assume clean on error.

  * **DNSBL query shape differs by blocklist family.** Two conventions:
      - *Domain / URI blocklists* (SURBL, URIBL, Spamhaus DBL, SORBS DBL, Spamrl)
        are queried as ``<domain>.<zone>`` — the registered domain appended verbatim.
      - *IP blocklists* (Barracuda, UCEPROTECT, SpamCop) expect the **reversed
        A-record IP** ``<d.c.b.a>.<zone>``, not the domain. Feeding a bare domain
        to an IP zone just NXDOMAINs (looks clean) and is semantically wrong.
    ``check_blacklists`` honors this via each entry's ``kind`` ('domain' | 'ip').
    NOTE: this deviates from the existing cron, which queries ALL zones as
    ``<domain>.<zone>``. The existing cron only configures domain-zones (spamrl,
    surbl) so it never hit this; once Barracuda/UCEPROTECT/SpamCop (IP zones) are
    added the reversed-IP form is required for correct results.

  * **MailIn tenant fingerprint (factor 3).** ``resolve_dkim`` chases the CNAME on
    ``selector1._domainkey.<domain>``; if it lands on ``*.onmicrosoft.com`` we
    capture the tenant prefix. Homogeneous tenant prefixes across many domains are
    the homogeneous-provisioning signal (memory: reference_mailin_tenant_fingerprint).

  * **spamrl is moribund.** db.spamrl.com returned zero listings across 51k prod
    queries (prep-notes §3) — kept in DEFAULT_DNSBLS for continuity but flagged
    dead; treat its results with suspicion. SORBS (dbl.sorbs.net) was largely shut
    down in 2024 and may NXDOMAIN everything — also flagged.

This module depends only on ``dnspython`` (DNS) and ``requests`` (redirect chain +
optional Spamhaus DQS REST). No DuckDB, no psycopg2. dnspython 2.34.2 confirmed
present on the droplet (python3.12).
"""

from __future__ import annotations

import hashlib
import ipaddress
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import dns.exception
import dns.resolver
import dns.reversename

logger = logging.getLogger("sources.dns")


# --------------------------------------------------------------------------- #
# Resolver
# --------------------------------------------------------------------------- #

# MANDATORY local recursive resolver. See module docstring. Override only if the
# host genuinely has a different local recursive resolver address.
DNS_RESOLVER: str = os.environ.get("BLS_DNS_RESOLVER", "127.0.0.2")

# Per-query timeout (seconds) and overall lifetime for a single name lookup.
DNS_TIMEOUT: float = float(os.environ.get("BLS_DNS_TIMEOUT", "5.0"))
DNS_LIFETIME: float = float(os.environ.get("BLS_DNS_LIFETIME", "8.0"))

# Sentinel answers that mean "your resolver is blocked / this is a test", NOT a
# real listing. Mirrors production checker.py:FALSE_POSITIVE_IPS.
FALSE_POSITIVE_IPS: frozenset[str] = frozenset(
    {"127.0.0.1", "127.255.255.254", "127.255.255.255"}
)


def _resolver(nameserver: str | None = None) -> dns.resolver.Resolver:
    """Build a Resolver pinned to our local recursive resolver.

    Constructed with configure=False so we never accidentally fall back to the
    host's /etc/resolv.conf (which on a laptop is a public resolver and would
    poison SURBL/Spamhaus). Always explicit.
    """
    r = dns.resolver.Resolver(configure=False)
    r.nameservers = [nameserver or DNS_RESOLVER]
    r.timeout = DNS_TIMEOUT
    r.lifetime = DNS_LIFETIME
    return r


def _query(
    name: str,
    rdtype: str,
    nameserver: str | None = None,
) -> tuple[list[Any], str | None]:
    """Run one DNS query. Returns (answers, error).

    answers: list of rdata objects (empty on NXDOMAIN / NoAnswer).
    error:   None on a definitive answer-or-empty; a short string on
             timeout / SERVFAIL / other transient failure. NXDOMAIN and NoAnswer
             are NOT errors — they are legitimate "no such record" answers.

    Note: dns.resolver.LifetimeTimeout (raised by the async resolver in the
    existing cron) is a subclass of dns.exception.Timeout, so the Timeout handler
    below covers both.
    """
    try:
        ans = _resolver(nameserver).resolve(name, rdtype, raise_on_no_answer=False)
        return (list(ans) if ans.rrset is not None else []), None
    except dns.resolver.NXDOMAIN:
        return [], None
    except dns.resolver.NoAnswer:
        return [], None
    except dns.resolver.NoNameservers as exc:
        # SERVFAIL from the local resolver — transient / upstream problem. Error.
        return [], f"no_nameservers:{exc.__class__.__name__}"
    except dns.exception.Timeout:
        return [], "timeout"
    except dns.exception.DNSException as exc:
        return [], f"dns_error:{exc.__class__.__name__}"


# --------------------------------------------------------------------------- #
# MX
# --------------------------------------------------------------------------- #

def _classify_mx(mx_hosts: Iterable[str]) -> str:
    """Classify MX provider from MX target hostnames.

    Rules (spec 07 / task): google | outlook | mimecast | barracuda | other.
    Matching is on the lowercased, trailing-dot-stripped hostname.
    """
    hosts = [h.lower().rstrip(".") for h in mx_hosts]
    for h in hosts:
        if (
            h.endswith(".google.com")
            or h.endswith(".googlemail.com")
            or h == "aspmx.l.google.com"
            or h.endswith(".aspmx.l.google.com")
        ):
            return "google"
    for h in hosts:
        if (
            h.endswith(".outlook.com")
            or h.endswith(".protection.outlook.com")
            or h.endswith(".mail.protection.outlook.com")
        ):
            return "outlook"
    for h in hosts:
        if h.endswith(".mimecast.com"):
            return "mimecast"
    for h in hosts:
        if h.endswith(".barracudanetworks.com"):
            return "barracuda"
    return "other"


def resolve_mx(domain: str, nameserver: str | None = None) -> dict[str, Any]:
    """Resolve MX records and classify the provider.

    Returns {mx_records: [host, ...] sorted by (preference, host),
             mx_provider: 'google'|'outlook'|'mimecast'|'barracuda'|'other',
             mx_error: <str|None>}.
    On no MX, mx_records=[] and mx_provider='other'.
    """
    answers, err = _query(domain, "MX", nameserver)
    pairs: list[tuple[int, str]] = []
    for rd in answers:
        host = str(getattr(rd, "exchange", "")).rstrip(".")
        pref = int(getattr(rd, "preference", 0))
        if host:
            pairs.append((pref, host))
    pairs.sort(key=lambda p: (p[0], p[1]))
    records = [h for _, h in pairs]
    return {
        "mx_records": records,
        "mx_provider": _classify_mx(records),
        "mx_error": err,
    }


# --------------------------------------------------------------------------- #
# A record
# --------------------------------------------------------------------------- #

def resolve_a(domain: str, nameserver: str | None = None) -> dict[str, Any]:
    """Resolve the A record. Returns {a_record_ip, a_records, a_record_24, a_error}.

    a_record_24 = first three octets + '.0/24' (clustering key). None if no A.
    If multiple A records, the first (sorted) is canonical; all kept in a_records.
    """
    answers, err = _query(domain, "A", nameserver)
    ips = sorted(str(rd) for rd in answers)
    ip = ips[0] if ips else None
    slash24: str | None = None
    if ip:
        try:
            addr = ipaddress.ip_address(ip)
            if addr.version == 4:
                a, b, c, _d = ip.split(".")
                slash24 = f"{a}.{b}.{c}.0/24"
        except ValueError:
            slash24 = None
    return {
        "a_record_ip": ip,
        "a_records": ips,
        "a_record_24": slash24,
        "a_error": err,
    }


# --------------------------------------------------------------------------- #
# SPF
# --------------------------------------------------------------------------- #

_IP4_RE = re.compile(r"ip4:([0-9./]+)", re.IGNORECASE)
_IP6_RE = re.compile(r"ip6:([0-9a-f:./]+)", re.IGNORECASE)
_INCLUDE_RE = re.compile(r"include:([^\s]+)", re.IGNORECASE)
_REDIRECT_RE = re.compile(r"redirect=([^\s]+)", re.IGNORECASE)


def _txt_strings(answers: list[Any]) -> list[str]:
    """Flatten TXT rdata (a tuple of byte-chunks) into joined strings."""
    out: list[str] = []
    for rd in answers:
        chunks = getattr(rd, "strings", None)
        if chunks is not None:
            out.append(b"".join(chunks).decode("utf-8", "replace"))
        else:
            out.append(str(rd).strip('"'))
    return out


def _spf_record_for(domain: str, nameserver: str | None) -> str | None:
    answers, _err = _query(domain, "TXT", nameserver)
    for txt in _txt_strings(answers):
        if txt.lower().startswith("v=spf1"):
            return txt
    return None


def resolve_spf(
    domain: str,
    nameserver: str | None = None,
    max_depth: int = 10,
) -> dict[str, Any]:
    """Resolve the SPF record and flatten its include:/redirect= chain.

    Returns {spf_record, spf_authorized_ips: [...], spf_includes_resolved: int,
             spf_error}.
    spf_authorized_ips collects every ip4:/ip6: CIDR across the recursively
    resolved chain. Recursion is capped at ~max_depth (RFC 7208's DNS-lookup limit
    is 10); we cap on distinct domains visited. Only ip4/ip6/include/redirect are
    expanded — a:/mx:/exists mechanisms are not (fingerprinting, not exhaustive
    authorization enumeration).
    """
    root = _spf_record_for(domain, nameserver)
    authorized: list[str] = []
    visited: set[str] = set()
    resolved_count = 0

    def _walk(name: str, record: str | None, depth: int) -> None:
        nonlocal resolved_count
        if record is None or depth > max_depth or len(visited) >= max_depth:
            return
        if name in visited:
            return
        visited.add(name)
        authorized.extend(m.group(1) for m in _IP4_RE.finditer(record))
        authorized.extend(m.group(1) for m in _IP6_RE.finditer(record))
        for inc in _INCLUDE_RE.finditer(record):
            target = inc.group(1).rstrip(".")
            if target and target not in visited:
                resolved_count += 1
                _walk(target, _spf_record_for(target, nameserver), depth + 1)
        rdir = _REDIRECT_RE.search(record)
        if rdir:
            target = rdir.group(1).rstrip(".")
            if target and target not in visited:
                resolved_count += 1
                _walk(target, _spf_record_for(target, nameserver), depth + 1)

    if root is not None:
        _walk(domain, root, 0)

    seen: set[str] = set()
    deduped = [ip for ip in authorized if not (ip in seen or seen.add(ip))]
    return {
        "spf_record": root,
        "spf_authorized_ips": deduped,
        "spf_includes_resolved": resolved_count,
        "spf_error": None,
    }


# --------------------------------------------------------------------------- #
# DKIM
# --------------------------------------------------------------------------- #

DEFAULT_DKIM_SELECTORS: list[str] = [
    "selector1",
    "selector2",
    "google",
    "default",
    "k1",
    "k2",
]

# Any *.onmicrosoft.com CNAME target — capture the tenant label.
_ONMICROSOFT_TENANT_RE = re.compile(r"([a-z0-9-]+)\.onmicrosoft\.com", re.IGNORECASE)

# Modern M365 DKIM CNAME target shape, e.g.
#   selector1-<domain-dashed>._domainkey.<tenant>.<region>-v1.dkim.mail.microsoft
# capture the <tenant> label that sits immediately after `._domainkey.`.
_MS_DKIM_TENANT_RE = re.compile(
    r"\._domainkey\.([a-z0-9]+)\.[^.]+\.dkim\.mail\.microsoft", re.IGNORECASE
)


def resolve_dkim(
    domain: str,
    selectors: list[str] | None = None,
    nameserver: str | None = None,
) -> dict[str, Any]:
    """Probe common DKIM selectors at ``<selector>._domainkey.<domain>``.

    A selector is "present" if its name resolves to either a CNAME or a TXT
    (v=DKIM1 / p=...). Returns {dkim_selectors_present: [...], dkim_tenant_prefix,
    dkim_error}.

    dkim_tenant_prefix: if selector1._domainkey.<domain> is a CNAME pointing at
    ``*.onmicrosoft.com`` we capture the onmicrosoft tenant label — the MailIn /
    M365 homogeneous-provisioning fingerprint (factor 3). The Microsoft pattern is
    typically selector1-<domain-dashed>._domainkey.<tenant>.onmicrosoft.com.
    """
    sels = selectors if selectors is not None else DEFAULT_DKIM_SELECTORS
    present: list[str] = []
    tenant_prefix: str | None = None

    for sel in sels:
        name = f"{sel}._domainkey.{domain}"
        cname_ans, _ = _query(name, "CNAME", nameserver)
        txt_ans, _ = _query(name, "TXT", nameserver)
        has_cname = bool(cname_ans)
        has_txt = any(
            t.lower().startswith("v=dkim1") or "p=" in t.lower()
            for t in _txt_strings(txt_ans)
        )
        if has_cname or has_txt:
            present.append(sel)

        if sel in ("selector1", "selector2") and has_cname and tenant_prefix is None:
            target = str(cname_ans[0].target).rstrip(".") if cname_ans else ""
            m = _ONMICROSOFT_TENANT_RE.search(target) or _MS_DKIM_TENANT_RE.search(target)
            if m:
                tenant_prefix = m.group(1)

    return {
        "dkim_selectors_present": present,
        "dkim_tenant_prefix": tenant_prefix,
        "dkim_error": None,
    }


# --------------------------------------------------------------------------- #
# DMARC
# --------------------------------------------------------------------------- #

_DMARC_POLICY_RE = re.compile(r"\bp=([a-z]+)", re.IGNORECASE)
_DMARC_RUA_RE = re.compile(r"rua=([^;]+)", re.IGNORECASE)


def resolve_dmarc(domain: str, nameserver: str | None = None) -> dict[str, Any]:
    """Resolve the DMARC policy at _dmarc.<domain>.

    Returns {dmarc_policy: 'none'|'quarantine'|'reject'|None, dmarc_record,
             dmarc_rua, dmarc_error}.
    dmarc_policy is None when there's no DMARC record (distinct from explicit p=none).
    """
    answers, err = _query(f"_dmarc.{domain}", "TXT", nameserver)
    record: str | None = None
    for txt in _txt_strings(answers):
        if txt.lower().startswith("v=dmarc1"):
            record = txt
            break
    policy: str | None = None
    rua: str | None = None
    if record:
        m = _DMARC_POLICY_RE.search(record)
        if m:
            p = m.group(1).lower()
            if p in ("none", "quarantine", "reject"):
                policy = p
        r = _DMARC_RUA_RE.search(record)
        if r:
            rua = r.group(1).strip()
    return {
        "dmarc_policy": policy,
        "dmarc_record": record,
        "dmarc_rua": rua,
        "dmarc_error": err,
    }


# --------------------------------------------------------------------------- #
# PTR (reverse DNS)
# --------------------------------------------------------------------------- #

def resolve_ptr(ip: str, nameserver: str | None = None) -> dict[str, Any]:
    """Reverse-DNS an IP. Returns {ptr, ptr_error}. ptr=None if no PTR."""
    if not ip:
        return {"ptr": None, "ptr_error": "no_ip"}
    try:
        rev = dns.reversename.from_address(ip)
    except dns.exception.SyntaxError:
        return {"ptr": None, "ptr_error": "bad_ip"}
    answers, err = _query(str(rev), "PTR", nameserver)
    ptr = str(answers[0].target).rstrip(".") if answers else None
    return {"ptr": ptr, "ptr_error": err}


# --------------------------------------------------------------------------- #
# Blacklists (DNSBL)
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Blocklist:
    """One DNSBL zone.

    name: short id used as the column / event key.
    zone: the DNSBL DNS zone.
    kind: 'domain' -> query <domain>.<zone>; 'ip' -> query <reversed-ip>.<zone>.
    note: human description / provenance.
    """

    name: str
    zone: str
    kind: str  # 'domain' | 'ip'
    note: str


# The 8 blocklists from spec 07 §"Blacklist sweep".
#   Existing 3 (blocklist-surveillance config.py):
#     - spamrl   : MORIBUND. 0 listings across 51k prod queries (prep-notes §3).
#                  db.spamrl.com is a DOMAIN blocklist. Kept for continuity.
#     - surbl    : multi.surbl.org — DOMAIN/URI blocklist. 99.9% of all our
#                  listings (prep-notes §3); carpet-bombs cheap .co/.info TLDs.
#                  High noise — measure baseline before acting.
#     - spamhaus : Spamhaus DBL is a DOMAIN blocklist. The public DNS zone
#                  dbl.spamhaus.org poisons queries from non-DQS resolvers; the
#                  reliable path is the DQS REST endpoint (check_spamhaus_dqs).
#                  Plain dbl.spamhaus.org is included but flagged: without a DQS
#                  key it returns the "blocked resolver" sentinel (FALSE_POSITIVE_IPS).
#   New 5 (deliverability handoff):
#     - barracuda  : b.barracudacentral.org — IP blocklist (reversed A record).
#     - sorbs      : dbl.sorbs.net — DOMAIN blocklist. WARNING: SORBS largely shut
#                    down 2024; may NXDOMAIN everything. Verify alive.
#     - uceprotect : dnsbl-1.uceprotect.net — IP blocklist (level 1).
#     - spamcop    : bl.spamcop.net — IP blocklist.
#     - uribl      : multi.uribl.com — DOMAIN/URI blocklist. Public resolvers get
#                    127.0.0.1 "query refused"; local resolver required.
DEFAULT_DNSBLS: list[Blocklist] = [
    Blocklist("spamrl", "db.spamrl.com", "domain",
              "Spamrl domain BL. MORIBUND: 0 listings in 51k prod queries (prep-notes §3)."),
    Blocklist("surbl", "multi.surbl.org", "domain",
              "SURBL multi URI BL. 99.9% of our listings; heavy .co/.info noise."),
    Blocklist("spamhaus_dbl", "dbl.spamhaus.org", "domain",
              "Spamhaus DBL (domain). Public zone poisons non-DQS resolvers; "
              "prefer DQS REST (check_spamhaus_dqs). Guarded by FALSE_POSITIVE_IPS."),
    Blocklist("barracuda", "b.barracudacentral.org", "ip",
              "Barracuda Reputation BL. IP-based: queries reversed A-record IP."),
    Blocklist("sorbs", "dbl.sorbs.net", "domain",
              "SORBS domain BL. WARNING: SORBS largely shut down 2024; verify alive."),
    Blocklist("uceprotect", "dnsbl-1.uceprotect.net", "ip",
              "UCEPROTECT level-1. IP-based: queries reversed A-record IP."),
    Blocklist("spamcop", "bl.spamcop.net", "ip",
              "SpamCop BL. IP-based: queries reversed A-record IP."),
    Blocklist("uribl", "multi.uribl.com", "domain",
              "URIBL multi (domain/URI). Public resolvers refused; local resolver required."),
]


def _reverse_ip(ip: str) -> str | None:
    """Reversed-octet form for a DNSBL IP query (a.b.c.d -> d.c.b.a). v4 only."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return None
    if addr.version != 4:
        return None  # IPv6 DNSBL nibble form not used by these zones
    return ".".join(reversed(ip.split(".")))


def check_blacklists(
    domain: str,
    dnsbls: list[Blocklist] | None = None,
    a_record_ip: str | None = None,
    nameserver: str | None = None,
) -> dict[str, Any]:
    """Check a domain against DNSBL zones.

    DOMAIN-kind zones: query ``<domain>.<zone>``.
    IP-kind zones:     query ``<reversed A-record IP>.<zone>`` (resolves A first if
                       a_record_ip not supplied).

    Result semantics (the false-negative fix):
      - NXDOMAIN / NoAnswer            -> clean for that zone (not added anywhere).
      - A-record answer not in
        FALSE_POSITIVE_IPS              -> LISTED -> appended to listed_on.
      - A-record answer only in
        FALSE_POSITIVE_IPS              -> 'error' (blocked/sentinel), NOT listed.
      - timeout / SERVFAIL             -> 'error' (NEVER treated as clean).
      - IP-zone but no A record         -> 'error' (can't check).

    Returns {listed_on: [name,...],
             errors: [{blocklist, zone, reason}],
             listings: [{blocklist, zone, answer}]}.
    """
    bls = dnsbls if dnsbls is not None else DEFAULT_DNSBLS
    listed_on: list[str] = []
    errors: list[dict[str, str]] = []
    listings: list[dict[str, str]] = []

    ip_for_checks = a_record_ip
    need_ip = any(bl.kind == "ip" for bl in bls)
    if need_ip and ip_for_checks is None:
        ip_for_checks = resolve_a(domain, nameserver).get("a_record_ip")

    for bl in bls:
        if bl.kind == "domain":
            query_name = f"{domain}.{bl.zone}"
        elif bl.kind == "ip":
            if not ip_for_checks:
                errors.append({"blocklist": bl.name, "zone": bl.zone,
                               "reason": "no_a_record_for_ip_blocklist"})
                continue
            rev = _reverse_ip(ip_for_checks)
            if rev is None:
                errors.append({"blocklist": bl.name, "zone": bl.zone,
                               "reason": "ipv6_or_bad_ip_unsupported"})
                continue
            query_name = f"{rev}.{bl.zone}"
        else:
            errors.append({"blocklist": bl.name, "zone": bl.zone,
                           "reason": f"unknown_kind:{bl.kind}"})
            continue

        answers, err = _query(query_name, "A", nameserver)
        if err is not None:
            # timeout / SERVFAIL -> error, NEVER clean (the false-negative fix).
            errors.append({"blocklist": bl.name, "zone": bl.zone, "reason": err})
            continue
        if not answers:
            continue  # NXDOMAIN / NoAnswer -> clean for this zone.
        ans_ips = [str(a) for a in answers]
        real = [a for a in ans_ips if a not in FALSE_POSITIVE_IPS]
        if not real:
            # Only sentinel answers -> blocked resolver / test, treat as error.
            errors.append({"blocklist": bl.name, "zone": bl.zone,
                           "reason": f"false_positive_sentinel:{','.join(ans_ips)}"})
            continue
        listed_on.append(bl.name)
        listings.append({"blocklist": bl.name, "zone": bl.zone,
                         "answer": ",".join(real)})

    return {"listed_on": listed_on, "errors": errors, "listings": listings}


# --------------------------------------------------------------------------- #
# Spamhaus DQS (optional REST, separate from the DNS DBL above)
# --------------------------------------------------------------------------- #

def check_spamhaus_dqs(domain: str, dqs_key: str | None = None) -> dict[str, Any]:
    """Optional Spamhaus DBL check via the DQS/WQS REST endpoint.

    Per prep-notes §4 + production checker.py the working key in prod is the
    ``.env`` BLS_SPAMHAUS_DQS_KEY and it hits
    ``apibl.spamhaus.net/lookup/v1/dbl/<domain>`` (200 = listed, 404 = clean) —
    NOT the documented <key>.dbl.dq.spamhaus.net DNS zone (the config.py comment is
    wrong, and its hardcoded fallback key is dead/HTTP-500).

    Returns {listed: bool|None, status: int|None, error: str|None}. listed=None
    means we couldn't determine (no key / transient error) — NEVER assume clean.
    """
    key = dqs_key or os.environ.get("BLS_SPAMHAUS_DQS_KEY")
    if not key:
        return {"listed": None, "status": None, "error": "no_dqs_key"}
    try:
        import requests  # local import: keep the DNS path dependency-light
    except ImportError:
        return {"listed": None, "status": None, "error": "requests_not_installed"}
    url = f"https://apibl.spamhaus.net/lookup/v1/dbl/{domain}"
    headers = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except Exception as exc:  # noqa: BLE001 - all network errors are 'unknown'
        return {"listed": None, "status": None,
                "error": f"request_failed:{exc.__class__.__name__}"}
    if resp.status_code == 200:
        return {"listed": True, "status": 200, "error": None}
    if resp.status_code == 404:
        return {"listed": False, "status": 404, "error": None}
    # 500/429/etc -> can't determine; do NOT treat as clean.
    return {"listed": None, "status": resp.status_code, "error": f"http_{resp.status_code}"}


# --------------------------------------------------------------------------- #
# Redirect chain (factor 5)
# --------------------------------------------------------------------------- #

def redirect_chain(
    domain: str,
    max_hops: int = 10,
    timeout: float = 8.0,
) -> dict[str, Any]:
    """Follow the HTTP redirect chain for a domain (factor 5: redirecting domains).

    Tries https://<domain>/ then http://<domain>/. Follows 301/302/303/307/308 up
    to max_hops via HEAD (falls back to GET if HEAD is rejected). Returns
    {chain: [url,...], terminal_url, terminal_tld, redirect_error}.
    """
    try:
        import requests
    except ImportError:
        return {"chain": [], "terminal_url": None, "terminal_tld": None,
                "redirect_error": "requests_not_installed"}

    chain: list[str] = []
    terminal_url: str | None = None
    last_error: str | None = None

    for scheme in ("https", "http"):
        start = f"{scheme}://{domain}/"
        try:
            session = requests.Session()
            url = start
            chain = [url]
            for _ in range(max_hops):
                try:
                    resp = session.head(url, allow_redirects=False, timeout=timeout)
                    if resp.status_code in (405, 501):
                        resp = session.get(url, allow_redirects=False,
                                           timeout=timeout, stream=True)
                except requests.RequestException:
                    resp = session.get(url, allow_redirects=False,
                                       timeout=timeout, stream=True)
                loc = resp.headers.get("location")
                if resp.status_code in (301, 302, 303, 307, 308) and loc:
                    nxt = requests.compat.urljoin(url, loc)
                    if nxt == url:
                        break
                    chain.append(nxt)
                    url = nxt
                    continue
                break
            terminal_url = chain[-1]
            last_error = None
            break  # https path succeeded; don't try http
        except requests.RequestException as exc:
            last_error = f"{scheme}_failed:{exc.__class__.__name__}"
            continue

    terminal_tld: str | None = None
    if terminal_url:
        try:
            host = requests.compat.urlparse(terminal_url).hostname or ""
            if host and "." in host:
                terminal_tld = host.rsplit(".", 1)[-1].lower()
        except Exception:  # noqa: BLE001
            terminal_tld = None

    return {
        "chain": chain if terminal_url else [],
        "terminal_url": terminal_url,
        "terminal_tld": terminal_tld,
        "redirect_error": last_error,
    }


# --------------------------------------------------------------------------- #
# DNS signature
# --------------------------------------------------------------------------- #

def dns_signature(
    mx: dict[str, Any],
    spf: dict[str, Any],
    dkim: dict[str, Any],
    dmarc: dict[str, Any],
) -> str:
    """SHA1 hex of the sorted, concatenated MX+SPF+DKIM+DMARC fingerprint.

    Two domains with the same signature share an identical DNS fingerprint — a
    homogeneous-provisioning (factor 3) and brand-clustering (factor 4) signal.
    Inputs are the dicts returned by the resolve_* functions; we use the stable
    sub-fields (sorted) so ordering / transient noise doesn't change the hash.
    """
    parts: list[str] = []
    parts.append("mx=" + ",".join(sorted(mx.get("mx_records") or [])))
    parts.append("mxp=" + str(mx.get("mx_provider") or ""))
    parts.append("spf=" + ",".join(sorted(spf.get("spf_authorized_ips") or [])))
    parts.append("dkim=" + ",".join(sorted(dkim.get("dkim_selectors_present") or [])))
    parts.append("tenant=" + str(dkim.get("dkim_tenant_prefix") or ""))
    parts.append("dmarc=" + str(dmarc.get("dmarc_policy") or ""))
    blob = "|".join(sorted(parts))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Convenience: full per-domain sweep
# --------------------------------------------------------------------------- #

def sweep_domain(
    domain: str,
    nameserver: str | None = None,
    dnsbls: list[Blocklist] | None = None,
    dkim_selectors: list[str] | None = None,
    include_redirect: bool = True,
    include_spamhaus_dqs: bool = False,
) -> dict[str, Any]:
    """Run every per-domain check and return one merged dict.

    PURE: returns a plain dict, writes nothing. The domain entity maps these keys
    onto raw_dns_sweep_domain / raw_blacklist_check rows.
    """
    domain = domain.strip().lower().rstrip(".")
    mx = resolve_mx(domain, nameserver)
    a = resolve_a(domain, nameserver)
    spf = resolve_spf(domain, nameserver)
    dkim = resolve_dkim(domain, dkim_selectors, nameserver)
    dmarc = resolve_dmarc(domain, nameserver)
    ptr = (
        resolve_ptr(a.get("a_record_ip"), nameserver)
        if a.get("a_record_ip")
        else {"ptr": None, "ptr_error": "no_a_record"}
    )
    blacklist = check_blacklists(
        domain, dnsbls=dnsbls, a_record_ip=a.get("a_record_ip"), nameserver=nameserver
    )
    redirect = (
        redirect_chain(domain) if include_redirect
        else {"chain": [], "terminal_url": None, "terminal_tld": None,
              "redirect_error": None}
    )
    spamhaus_dqs = check_spamhaus_dqs(domain) if include_spamhaus_dqs else None

    merged: dict[str, Any] = {"domain": domain}
    merged.update(mx)
    merged.update(a)
    merged.update(spf)
    merged.update(dkim)
    merged.update(dmarc)
    merged.update(ptr)
    merged["dns_signature"] = dns_signature(mx, spf, dkim, dmarc)
    merged["listed_on"] = blacklist["listed_on"]
    merged["blacklist_errors"] = blacklist["errors"]
    merged["blacklist_listings"] = blacklist["listings"]
    merged["blacklist_count"] = len(blacklist["listed_on"])
    merged["any_blacklist_active"] = len(blacklist["listed_on"]) > 0
    merged["redirect_chain"] = redirect["chain"]
    merged["terminal_redirect"] = redirect["terminal_url"]
    merged["terminal_tld"] = redirect["terminal_tld"]
    merged["redirect_error"] = redirect["redirect_error"]
    if spamhaus_dqs is not None:
        merged["spamhaus_dqs"] = spamhaus_dqs
    return merged


# --------------------------------------------------------------------------- #
# Batch runner
# --------------------------------------------------------------------------- #

def sweep_domains(
    domains: Iterable[str],
    qps: int = 20,
    nameserver: str | None = None,
    dnsbls: list[Blocklist] | None = None,
    dkim_selectors: list[str] | None = None,
    include_redirect: bool = True,
    include_spamhaus_dqs: bool = False,
    on_result: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    """Threaded batch sweep. Returns one merged dict per domain.

    ``qps`` bounds the worker-pool size (rough QPS cap — each domain issues many
    serial sub-queries, so wall-clock QPS is approximate, intentionally
    conservative to stay under DNSBL rate limits). Per-domain functions remain
    independently callable; this is just a convenience fan-out.

    If ``on_result`` is given it's called with each result as it completes (lets
    the caller stream rows into DuckDB without buffering all of them). Exceptions
    in a single domain sweep are captured into that domain's result dict under
    ``sweep_error`` rather than killing the batch.
    """
    workers = max(1, min(qps, 64))
    results: list[dict[str, Any]] = []

    def _one(d: str) -> dict[str, Any]:
        try:
            return sweep_domain(
                d,
                nameserver=nameserver,
                dnsbls=dnsbls,
                dkim_selectors=dkim_selectors,
                include_redirect=include_redirect,
                include_spamhaus_dqs=include_spamhaus_dqs,
            )
        except Exception as exc:  # noqa: BLE001 - never let one domain kill the batch
            logger.exception("sweep_domain failed for %s", d)
            return {"domain": d, "sweep_error": f"{exc.__class__.__name__}:{exc}"}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_one, d.strip().lower().rstrip(".")): d for d in domains
        }
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            if on_result is not None:
                on_result(res)
    return results


__all__ = [
    "DNS_RESOLVER",
    "FALSE_POSITIVE_IPS",
    "DEFAULT_DKIM_SELECTORS",
    "DEFAULT_DNSBLS",
    "Blocklist",
    "resolve_mx",
    "resolve_a",
    "resolve_spf",
    "resolve_dkim",
    "resolve_dmarc",
    "resolve_ptr",
    "check_blacklists",
    "check_spamhaus_dqs",
    "redirect_chain",
    "dns_signature",
    "sweep_domain",
    "sweep_domains",
]
