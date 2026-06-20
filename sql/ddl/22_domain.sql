-- Phase 3: core.domain canonical entity (spec 07).
-- Applied at schema version 22 by scripts/setup_db.py / orchestrator DDL applier.
--
-- One row per sending domain. Spine = raw_dns_sweep_domain (the DNS/blacklist
-- fingerprint of every active sending domain). Enriched with:
--   - esp / infra_provider / lifecycle aggregated from core.sending_account (the
--     inboxes hosted on the domain)
--   - ns_provider / registrar / acquisition_batch / acquisition_date from the
--     Lucas/Tomer .co NS-handoff CSV (seed_data/domains/ns-handoff.csv, 2,002 rows)
--   - cost_acquisition from the .co batch ($1.80) for batch-tagged domains
--
-- redirect_chain / terminal_redirect are NULL in v1 (factor-5 redirect probe deferred
-- from the bulk sweep for speed — see GAPS F1). sheet_status NULL until sheet ingest.
-- Built under the 'canonical' phase, full rebuild each run.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.domain (
    domain                VARCHAR PRIMARY KEY,
    registrar             VARCHAR,        -- 'dynadot' | ... (known only for NS-handoff domains in v1)
    registrar_account     VARCHAR,        -- e.g. 'Dynadot #11' (NS-handoff slot)
    acquisition_date      DATE,
    acquisition_batch     VARCHAR,        -- FK to core.cost_ledger.attribution_id (attribution_dim='batch')
    brand_prefix          VARCHAR,        -- domain label minus TLD (factor-4 brand clustering)

    -- Two-column infra classification (inherited from the inboxes on the domain)
    esp                   VARCHAR,        -- google | outlook | otd | NULL
    infra_provider        VARCHAR,        -- OTD resolvable; vendor brand for Outlook/Google NULL (GAP B6)
    ns_provider           VARCHAR,        -- Renaissance | Lucas | Tomer | Max-OTD (NS-handoff only in v1)

    -- DNS sweep fingerprint (sender-side)
    mx_provider           VARCHAR,
    a_record_ip           VARCHAR,
    a_record_24           VARCHAR,
    spf_authorized_ips    VARCHAR,        -- JSON array
    dkim_selectors        VARCHAR,        -- JSON array
    dkim_tenant_prefix    VARCHAR,        -- *.onmicrosoft.com tenant (factor-3 homogeneous provisioning)
    dmarc_policy          VARCHAR,
    dns_signature         VARCHAR,        -- SHA1 fingerprint (factor-3 clustering)
    redirect_chain        VARCHAR,        -- NULL in v1 (GAP F1)
    terminal_redirect     VARCHAR,        -- NULL in v1 (GAP F1)

    -- Lifecycle (mirrors sending_account lattice)
    lifecycle_state       VARCHAR NOT NULL,  -- acquired | dns_configured | in_use | paused | retired
    dns_configured_at     TIMESTAMPTZ,
    first_send_at         TIMESTAMPTZ,
    paused_at             TIMESTAMPTZ,
    retired_at            TIMESTAMPTZ,

    -- Operational
    sheet_status          VARCHAR,        -- USED | NOT USED | TBD (NULL until sheet ingest)
    blacklist_count       INTEGER,
    any_blacklist_active  BOOLEAN,
    listed_on             VARCHAR,        -- JSON array of blocklist names
    inbox_count           INTEGER,        -- # active inboxes on this domain (from sending_account)

    -- Cost projection (spec 13)
    cost_acquisition_usd_estimated     DOUBLE,   -- per-domain share of the acquisition batch
    cost_renewal_annual_usd_estimated  DOUBLE,   -- NULL in v1 (v3 derivation)

    is_active             BOOLEAN NOT NULL,
    first_seen_at         TIMESTAMPTZ,
    last_seen_at          TIMESTAMPTZ,
    resolved_at           TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_core_domain_esp          ON core.domain (esp);
CREATE INDEX IF NOT EXISTS ix_core_domain_infra        ON core.domain (infra_provider);
CREATE INDEX IF NOT EXISTS ix_core_domain_ns           ON core.domain (ns_provider);
CREATE INDEX IF NOT EXISTS ix_core_domain_signature    ON core.domain (dns_signature);
CREATE INDEX IF NOT EXISTS ix_core_domain_a24          ON core.domain (a_record_24);
CREATE INDEX IF NOT EXISTS ix_core_domain_brand        ON core.domain (brand_prefix);
CREATE INDEX IF NOT EXISTS ix_core_domain_blacklisted  ON core.domain (any_blacklist_active);
