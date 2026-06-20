# 07 — Entity: domain + DNS sweep + blacklist

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

`core.domain` — one row per sending domain Renaissance owns, with DNS fingerprint + blacklist status as continuously-refreshed fields. Absorbs the existing `/root/renaissance-worker/jobs/blocklist-surveillance/` system.

This is the entity that powers most of the 5-factor visibility surfaces (especially factor 3: homogeneous provisioning, factor 4: brand clustering, factor 5: redirecting domains).

## Inputs

**Domain registry:** intersection of:
- `core.sending_account.domain` (DISTINCT — every domain that hosts an active inbox)
- Pipeline-supabase `infra_domain_registry` if it exists
- `raw_domain_tech_sheet` (the trustworthy sheet at `1wkrkX_02bdXaj_j-E03vLHIFRw8howadd96LOC4lONo`)

**DNS sweep** (nightly, async, all domains in registry):
- MX records → resolve targets, classify provider
- A record → IP + /24 block for clustering
- SPF record → resolve full chain, extract authorized IP ranges
- DKIM selectors (probe common: `selector1`, `selector2`, `google`, `default`, `k1`, `k2`, plus extracted from any historical email headers we have)
- DMARC policy + report URI
- ARPA reverse DNS on A-record IP
- HTTP HEAD chain → terminal redirect destination (factor 5)
- DNS signature hash = SHA1 of sorted MX+SPF+DKIM+DMARC strings

**Blacklist sweep** (nightly, async, all domains):
- Absorb the 3 existing blocklists from `blocklist-surveillance`: `spamrl`, `surbl`, `spamhaus_dbl`
- Add the 5 new ones (per the deliverability handoff): Barracuda RBL, SORBS, UCEPROTECT, SpamCop, URIBL
- All via DNSBL (no API keys needed, public)
- Plus optional Spamhaus DBL via DQS REST API (existing key in blocklist-surveillance config)
- Honor sheet status: don't blacklist-check domains marked NOT USED in domain tech sheet

## Outputs

### `raw_dns_sweep_domain` (one row per (domain, _run_id))
Captures every field from the nightly DNS sweep, JSON-encoded for arrays.

### `raw_blacklist_check` (append-only event log)
One row per (domain, blocklist, check_time, status). Append-only — history is the point.

### `raw_domain_tech_sheet` (daily snapshot)
Copy-through of the sheet contents.

### `core.domain`
```sql
CREATE TABLE core.domain (
  domain                VARCHAR PRIMARY KEY,
  registrar             VARCHAR,                 -- 'dynadot' | 'porkbun' | 'spaceship' | 'namecheap' | etc.
  registrar_account     VARCHAR,                 -- e.g. 'Dynadot #11' (from Domain Tech Sheet)
  acquisition_date      DATE,
  acquisition_batch     VARCHAR,                 -- FK to core.cost_ledger.attribution_id when attribution_dim='batch'
  brand_prefix          VARCHAR,                 -- extracted brand string

  -- Two-column infra classification (per Sam 2026-05-30)
  esp                   VARCHAR,                 -- 'google' | 'outlook' | 'otd' | 'other'  (recipient-side / inbox-tech)
  infra_provider        VARCHAR,                 -- 'OTD' | 'MailIn' | 'Reseller' | 'Folderly' | 'Maxify' | 'Tucows' | 'Warmly'  (vendor brand)
  ns_provider           VARCHAR,                 -- 'Renaissance' | 'Lucas' | 'Tomer' | 'Max-OTD' | etc.  (who manages NS records)

  -- DNS sweep (sender-side fingerprint; populated by nightly sweep)
  mx_provider           VARCHAR,                 -- google | outlook | mimecast | other (RECEIVER-side MX; usually matches esp for our owned domains)
  a_record_ip           VARCHAR,
  a_record_24           VARCHAR,                 -- /24 of A-record IP for clustering
  spf_authorized_ips    VARCHAR,                 -- JSON array
  dkim_selectors        VARCHAR,                 -- JSON array
  dkim_tenant_prefix    VARCHAR,                 -- *.onmicrosoft.com prefix per memory `reference_mailin_tenant_fingerprint`
  dmarc_policy          VARCHAR,                 -- none | quarantine | reject
  dns_signature         VARCHAR,                 -- SHA1 hash
  redirect_chain        VARCHAR,                 -- JSON array
  terminal_redirect     VARCHAR,

  -- Domain lifecycle (mirrors sending_account; per Sam 'lattice work')
  lifecycle_state       VARCHAR NOT NULL,        -- 'acquired' | 'dns_configured' | 'in_use' | 'paused' | 'retired'
  dns_configured_at     TIMESTAMPTZ,             -- DNS records set up (MX/SPF/DKIM/DMARC live)
  first_send_at         TIMESTAMPTZ,             -- first outbound observed via an account on this domain
  paused_at             TIMESTAMPTZ,
  retired_at            TIMESTAMPTZ,

  -- Operational metadata
  sheet_status          VARCHAR,                 -- USED | NOT USED | TBD (from Domain Tech Sheet)
  blacklist_count       INTEGER,                 -- count of blocklists currently listing this domain
  any_blacklist_active  BOOLEAN,

  -- Cost projection columns (per spec 13). NULL until v3 derivation logic populates.
  cost_acquisition_usd_estimated      DOUBLE,    -- per-domain share of acquisition_batch.total / batch.unit_count
  cost_renewal_annual_usd_estimated   DOUBLE,    -- annual recurring (.co ~$9, .info ~$9, .com ~$12)

  is_active             BOOLEAN NOT NULL,
  first_seen_at, last_seen_at, resolved_at  TIMESTAMPTZ
);
```

## Source data for the .co bulk batch allocation

The 2026-05-19/20 .co sale (14,978 domains) splits three ways. We have the per-domain allocation already on disk:

- **Lucas (1,001 domains):** `deliverables/2026-05-30-infra-strategy-suite/lucas-1001-domains.csv` — columns: domain, bucket, slot, dynadot_username, ns_provider='Lucas', registered_at='2026-05-19' or '2026-05-20'
- **Tomer (1,001 domains):** subtract Lucas's set from `tools/domain-acquisition/data/exports/ns-handoff-tomer-lucas-2026-05-29.csv` (2,002 rows total, ns_provider column distinguishes Lucas vs Tomer)
- **Max OTD (12,976 domains):** remainder of the 14,978 (per memory `project_cf_ns_diversification_max_owns_20260529`: Max manages CF NS for the in-house pool)

These CSVs become the seed input for `core.domain` for this batch. Per-domain field defaults:

| Field | Lucas batch | Tomer batch | Max OTD batch |
|---|---|---|---|
| `infra_provider` | `MailIn` | `MailIn` | `OTD` |
| `esp` | `outlook` | `outlook` | `otd` |
| `ns_provider` | `Lucas` | `Tomer` | `Max-OTD` |
| `acquisition_batch` | `dynadot_2026-05-19_co_sale_14978` | (same) | (same) |
| `cost_acquisition_usd_estimated` | `1.80` (derived from batch row in cost_ledger) | (same) | (same) |
| `lifecycle_state` | `acquired` initially, → `dns_configured` when DNS set up, → `in_use` on first send | (same) | (same) |

This is the canonical example for how a bulk-acquired cohort lands in `core.domain`. Same pattern repeats for every future batch: sale event → 1 cost_ledger row + N domain rows tagged with the batch.

### `core.domain_blacklist_event` (derived, but worth materializing)
One row per (domain, blocklist, first_listed_at, status). Surfaces NEW listings, delistings, repeat-cycles.

## Resolution rules

- **Domain registry:** intersect Instantly-derived (via `core.sending_account.domain`) ∩ pipeline `infra_domain_registry` if exists. Drop the manually-tagged "NOT USED" entries from the sweep (sheet status wins for "should we even check").
- **Sheet status:** verify against Instantly. If sheet says NOT USED but `core.sending_account` shows active inboxes on this domain → flag for review (surface in a `v_domain_sheet_drift` view). Instantly wins for actually-used flag.
- **Mx provider classification:** `*.google.com / *.googlemail.com / aspmx.l.google.com` → Google. `*.outlook.com / *.protection.outlook.com / *.mail.protection.outlook.com` → Outlook. `*.mimecast.com` → Mimecast. `*.barracudanetworks.com` → Barracuda. else → "Other" (capture for manual classification).
- **DKIM tenant prefix:** `dig CNAME selector1._domainkey.<domain>` → if response matches `*.onmicrosoft.com`, capture the tenant prefix (per `reference_mailin_tenant_fingerprint_20260526.md`). This is the factor-3 (homogeneous provisioning) detection mechanism.

## Migration from existing blocklist-surveillance

The existing system at `/root/renaissance-worker/jobs/blocklist-surveillance/`:
- Keeps running its 08:00 UTC cron (don't break it)
- We read its `state/last_run.json` as one input to the warehouse blacklist event log (backfill historical state)
- v1.5: warehouse takes over, blocklist-surveillance retired

## Definition of done

1. DDL at version 7
2. ~50,000 domains in `core.domain`, classified by infra
3. DNS sweep completes nightly in <60min
4. Blacklist sweep completes nightly in <30min, 8 blocklists checked
5. `core.domain_blacklist_event` log materializes hourly
6. SCHEMA.md updated; sheet-drift surface documented

## Things to NOT do

- Don't run the sweep against domains marked NOT USED unless explicitly probing for sheet drift
- Don't try to pause accounts from the warehouse (existing blocklist-surveillance has a DRY_RUN pause-action path; warehouse only OBSERVES, never ACTS in v1)
- Don't ingest the existing JSON state files as canonical — they're inputs to backfill, not authority
- Don't add Spamhaus DBL REST API as a 9th blocklist initially — same data via DNSBL is free
