-- Phase 3: DNS + blacklist sweep raw tables (spec 07).
-- Applied at schema version 18 by scripts/setup_db.py / orchestrator DDL applier.
--
-- TWO raw objects (canonical core.domain is built separately in a later DDL):
--   raw_dns_sweep_domain  — one row per (domain, _run_id): full DNS fingerprint.
--   raw_blacklist_check   — append-only event log, one row per (domain, blocklist, run).
--
-- Source: sources/dns.py sweep_domain() merged dict, written by entities/dns_sweep.py.
-- Arrays are JSON-encoded VARCHAR (mx_records, a_records, spf_authorized_ips, etc.).
-- The full merged dict is preserved verbatim in raw_json for fields not promoted to
-- a scalar column.
--
-- v1 blocklist set = a small set of domain zones (surbl, spamrl, spamhaus_dbl).
-- IP zones (barracuda/uceprotect/spamcop) + Spamhaus DQS REST are deferred to v1.1.
-- Queries go through a local recursive resolver (some zones return sentinel answers
-- to large shared resolvers; dns.py handles this).

CREATE TABLE IF NOT EXISTS raw_dns_sweep_domain (
    domain                  VARCHAR,
    -- MX (receiver-side; for our owned domains usually matches the inbox ESP)
    mx_provider             VARCHAR,   -- google | outlook | mimecast | barracuda | other | none
    mx_records              VARCHAR,   -- JSON array of MX hostnames
    mx_error                VARCHAR,
    -- A record + /24 for IP clustering (factor 3)
    a_record_ip             VARCHAR,
    a_record_24             VARCHAR,   -- <a.b.c>.0/24 of a_record_ip
    a_records               VARCHAR,   -- JSON array
    a_error                 VARCHAR,
    -- SPF
    spf_record              VARCHAR,
    spf_authorized_ips      VARCHAR,   -- JSON array of resolved authorized IP ranges
    spf_includes_resolved   INTEGER,
    spf_error               VARCHAR,
    -- DKIM (+ MailIn tenant fingerprint, factor 3)
    dkim_selectors_present  VARCHAR,   -- JSON array of selectors that resolved
    dkim_tenant_prefix      VARCHAR,   -- *.onmicrosoft.com tenant prefix when present
    dkim_error              VARCHAR,
    -- DMARC
    dmarc_policy            VARCHAR,   -- none | quarantine | reject
    dmarc_record            VARCHAR,
    dmarc_rua               VARCHAR,
    dmarc_error             VARCHAR,
    -- Reverse DNS
    ptr                     VARCHAR,
    ptr_error               VARCHAR,
    -- Fingerprint + redirect (factor 5)
    dns_signature           VARCHAR,   -- SHA1 of sorted MX+SPF+DKIM+DMARC
    redirect_chain          VARCHAR,   -- JSON array of hops
    terminal_redirect       VARCHAR,
    terminal_tld            VARCHAR,
    redirect_error          VARCHAR,
    -- Blacklist rollup (detail lives in raw_blacklist_check)
    blacklist_count         INTEGER,
    any_blacklist_active    BOOLEAN,
    listed_on               VARCHAR,   -- JSON array of blocklist names currently listing
    -- Whole-sweep error (one domain's sweep blew up); NULL on success
    sweep_error             VARCHAR,
    raw_json                VARCHAR,   -- full merged dict (json.dumps) for fidelity
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- Append-only: the history of each (domain, blocklist) check is the whole point
-- (new listings, delistings, repeat cycles). Never DELETE except by _run_id within a run.
CREATE TABLE IF NOT EXISTS raw_blacklist_check (
    domain        VARCHAR,
    blocklist     VARCHAR,   -- surbl | spamrl | spamhaus_dbl | ...
    status        VARCHAR,   -- listed | clean | error
    detail        VARCHAR,   -- answer IP(s) when listed; reason when error
    checked_at    TIMESTAMPTZ NOT NULL,
    _run_id       VARCHAR
);

CREATE INDEX IF NOT EXISTS ix_raw_dns_sweep_domain     ON raw_dns_sweep_domain (domain);
CREATE INDEX IF NOT EXISTS ix_raw_blacklist_check_dom  ON raw_blacklist_check (domain);
CREATE INDEX IF NOT EXISTS ix_raw_blacklist_check_bl   ON raw_blacklist_check (blocklist, status);
