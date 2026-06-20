# 08 — Entity: recipient_domain (MX-derived ESP classification)

**Phase:** 3
**Status:** spec'd 2026-05-30, not started

## Goal

`core.recipient_domain` — one row per distinct recipient domain we've ever sent to, with MX-derived ESP classification. This is the recipient axis of the ESP×ESP matrix Sam's been wanting.

## Inputs

**Distinct recipient domains** — sourced from the lead-mirror DuckDB on droplet (`/root/renaissance-worker/jobs/lead-mirror/lead_mirror.duckdb`, 21M leads). Extract `lower(split_part(email, '@', 2))` distinct values that have ever been sent to (cross-reference with pipeline-supabase `contact_frequency_events` for "actually contacted" filter).

**MX lookups:** `dig MX <domain>` via dnspython, incremental — only resolve domains we haven't already cached.

## Outputs

### `core.recipient_domain`
```sql
CREATE TABLE core.recipient_domain (
  domain               VARCHAR PRIMARY KEY,
  mx_records           VARCHAR,              -- JSON array of MX targets
  esp_classification   VARCHAR,              -- google | outlook | mimecast | barracuda | proofpoint | other
  esp_tenant           VARCHAR,              -- for outlook: the *.onmicrosoft.com tenant
  resolved_at          TIMESTAMPTZ,
  ttl_expires_at       TIMESTAMPTZ           -- resolved_at + 30 days; re-resolve on/after
);
```

No `_is_active`, no `first_seen_at`. This is a static lookup table — once we know a domain's MX, it doesn't change often.

### `core.recipient_domain_resolution_queue` (operational)
Tracks which domains are pending resolution. Populated nightly from "domains in lead mirror not yet in `core.recipient_domain` OR whose `ttl_expires_at < now()`".

## Resolution rules

- **`esp_classification`:** same patterns as `core.domain.mx_provider` (spec 07) — `*.google.com` / `*.googlemail.com` → Google, `*.outlook.com` / `*.protection.outlook.com` → Outlook, etc. else → 'other'.
- **`esp_tenant`:** for Outlook recipients, extract the `*.onmicrosoft.com` tenant prefix (for clustering analysis — many Outlook recipients share tenants, esp. corporate).
- **TTL:** 30 days. After expiry, re-resolve incrementally.

## Why we DON'T snapshot per send event

For the analytical use case (Sam asks "what's my RR by recipient ESP for Funding 4 last week"), we don't need per-send MX. We need per-day per-campaign distribution of recipient ESPs. That's derived by joining `conversation_messages` (or future `core.send_event`) to `core.recipient_domain` on the lead_email's domain.

The membership change-event log (spec 11) gives us the campaign-level distribution; this entity gives us the per-domain classification. The join is the analytical view.

## Definition of done

1. DDL at version 8
2. Initial sweep populates ~500k-2M `core.recipient_domain` rows (depending on lead diversity)
3. ESP classification distribution surfaces sensible cuts (e.g. for B2B: ~40% Google, ~50% Outlook, ~10% other — Sam will sanity-check)
4. Incremental re-resolution working: only domains with expired TTL hit the network
5. SCHEMA.md updated with a "Common queries" example for ESP×ESP

## Things to NOT do

- Don't do per-send MX lookups — wasteful, the recipient domain doesn't change between sends
- Don't snapshot the full lead-mirror into raw — we read from it in place, only extract distinct domains
- Don't try to resolve internal domains (the lead-mirror has internal-test emails that never get sent — filter for `verification_status IN ('valid', 'catch_all_valid')` upstream)
- Don't cache MX in JSON files outside DuckDB — keep canonical
