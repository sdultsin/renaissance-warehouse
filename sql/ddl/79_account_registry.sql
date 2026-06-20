-- 79_account_registry.sql  [2026-06-17 infra-data-truth / C3]
-- Source-of-truth account registry for the MilkBox + MailIn cohorts (and future "Email Accounts" sheet
-- batches). Authoritative VENDOR = the sheet's Platform/Partner Tag (MilkBox/MailIn/Outreach Today/
-- Reseller/Inboxing) — the ONLY source that separates MilkBox from MailIn (both Outlook MX), which the
-- Darcy brief flagged as unresolved. Loaded by load_account_registry.py from Mac-staged row_json CSVs
-- (droplet has no Google creds). Credentials (password/auth) are deliberately NOT ingested. Identity-
-- sensitive → droplet-local only.

CREATE TABLE IF NOT EXISTS core.account_registry (
  email           VARCHAR PRIMARY KEY,
  domain          VARCHAR,
  first_name      VARCHAR,
  last_name       VARCHAR,
  rg_tag          VARCHAR,   -- Tag1 (RG#)
  rg_range        VARCHAR,   -- Tag2 (RG#-#)
  email_tag       VARCHAR,   -- ESP per the sheet: Google / Outlook / SMTP
  vendor          VARCHAR,   -- Platform/Partner Tag (AUTHORITATIVE): MilkBox/MailIn/Outreach Today/Reseller/Inboxing
  batch_tag       VARCHAR,
  workspace_label VARCHAR,   -- "Funding 1" / "Funding 2" / "Funding 4"
  inbox_type      VARCHAR,
  status          VARCHAR,
  gender          VARCHAR,
  panel           VARCHAR,
  offer           VARCHAR,
  cohort          VARCHAR,   -- MilkBox | MailIn (the batch group ingested)
  source_tab      VARCHAR,
  _staged_at      TIMESTAMP WITH TIME ZONE
);

-- Authoritative vendor per account, overlaid on the derived classifier (so MilkBox is now separable).
CREATE OR REPLACE VIEW core.v_account_vendor_authoritative AS
SELECT
  lower(ar.email)                         AS account_email,
  ar.vendor                               AS vendor_authoritative,
  v.vendor_category                       AS vendor_derived,
  COALESCE(ar.vendor, v.vendor_category)  AS vendor_resolved,
  ar.domain, ar.workspace_label, ar.email_tag AS esp_sheet, ar.cohort
FROM core.account_registry ar
LEFT JOIN core.sending_account_vendor v ON lower(v.account_email) = lower(ar.email);

-- Domain -> cohort/vendor. Does NOT touch core.domain (C8-owned); a join-able projection instead.
-- Domains are vendor-uniform by construction (provisioned per batch), so any_value is the dominant vendor.
CREATE OR REPLACE VIEW core.v_domain_cohort AS
SELECT
  lower(domain)        AS domain,
  any_value(vendor)    AS vendor,
  any_value(cohort)    AS cohort,
  count(*)             AS accounts,
  count(DISTINCT vendor) AS distinct_vendors  -- >1 flags a non-uniform domain to investigate
FROM core.account_registry
WHERE NULLIF(domain, '') IS NOT NULL
GROUP BY lower(domain);
