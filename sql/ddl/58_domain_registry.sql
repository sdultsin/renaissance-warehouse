-- Version 58 (2026-06-11) — core.domain_registry: the Track-I domain master, now in
-- versioned DDL.
--
-- History: created ad-hoc during the 06-08 hardening session (Track I), never written
-- into sql/ddl/, and therefore silently lost in the 06-08/09 corruption recovery —
-- version tracking had no record of it (see 57_recovery_reassert.sql). Three nightly
-- Track-I steps (backfill_domain_registry / backfill_purchased_at_registrars /
-- backfill_purchased_at_from_sheet) WARNed nightly until the 2026-06-11 rebuild.
--
-- This is the comprehensive domain table (~150-160k rows): TLD / registrar /
-- lifecycle / purchase dates / NS state for the full fleet. core.domain is the
-- NARROW sending-active slice with DNS-fingerprint columns — do not confuse the two
-- (see deliverables/2026-06-06-longtail-scoping/duckdb-domain-account-sanity.md).
--
-- Population: scripts/rebuild_domain_registry.py builds the universe from four
-- sources (Domain Tech Sheet ∪ registrar APIs ∪ warehouse domains ∪ Cloudflare
-- zones), then the Track-I backfills fill nameserver_host / ns_at_cloudflare
-- (ns_sweep parquet) and purchased_at / expires_at (registrar caches + sheet
-- expiry−1y derivation).

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.domain_registry (
    domain                  VARCHAR PRIMARY KEY,
    tld                     VARCHAR,            -- suffix after the last dot, 100% coverage
    registrar               VARCHAR,            -- porkbun / dynadot / spaceship / otd_managed
    registrar_account       VARCHAR,            -- sheet column header e.g. 'Porkbun #3'; 'OTD' for vendor-managed
    status                  VARCHAR,            -- active (sending inboxes) / assigned (USED, no active inboxes) / unused
    purchased_at            DATE,
    purchased_at_is_derived BOOLEAN DEFAULT FALSE,  -- TRUE = sheet expiry−1y derivation, FALSE = registrar-API exact
    expires_at              DATE,
    nameserver_host         VARCHAR,            -- first NS host, lowercased, no trailing dot
    ns_at_cloudflare        BOOLEAN DEFAULT FALSE,
    assigned_workspace      VARCHAR,            -- dominant Instantly workspace among the domain's sending accounts
    inbox_count             INTEGER,            -- count of status='active' sending accounts on the domain
    source                  VARCHAR,            -- provenance summary, e.g. 'sheet+registrar_api'
    in_warehouse            BOOLEAN DEFAULT FALSE,  -- in core.domain or core.sending_account
    in_cf                   BOOLEAN DEFAULT FALSE,  -- in raw_cloudflare_zones
    in_sheet                BOOLEAN DEFAULT FALSE,  -- in the Domain Tech Sheet 'Domains' tab
    in_registrar_api        BOOLEAN DEFAULT FALSE,  -- in raw_registrar_domains
    _loaded_at              TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_core_domain_registry_registrar ON core.domain_registry (registrar);
CREATE INDEX IF NOT EXISTS ix_core_domain_registry_status    ON core.domain_registry (status);
CREATE INDEX IF NOT EXISTS ix_core_domain_registry_tld       ON core.domain_registry (tld);
