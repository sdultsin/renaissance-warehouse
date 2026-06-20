-- 76_domain_tld.sql  [2026-06-16 infra-data-truth / C3]
-- TLD dimension, derived from the domain (e.g. .co / .info / .com). Trivially derivable, so a view
-- keeps it always-correct with zero maintenance. Joins to core.domain and (via domain) to
-- core.sending_account. Last dot-segment only — multi-part TLDs (.co.uk) report the final label
-- ('uk'); Renaissance's fleet is overwhelmingly single-label TLDs, so this is sufficient. If a stored
-- column is later wanted for performance, materialize this into core.domain in the domain builder.
CREATE OR REPLACE VIEW core.v_domain_tld AS
SELECT
  domain,
  lower(regexp_extract(domain, '\.([a-z0-9-]+)$', 1)) AS tld
FROM core.domain
WHERE domain IS NOT NULL;
