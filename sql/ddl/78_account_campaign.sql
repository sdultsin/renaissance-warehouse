-- 78_account_campaign.sql  [2026-06-17 infra-data-truth / C3]
-- Warehouse-native account <-> active-campaign mapping (the campaign_id-level attachment behind the
-- active_campaign_count fix), so account->campaign->offer is reachable ON THE SERVING SNAPSHOT at full
-- inventory coverage. Source = account_campaign_mappings in the account-truth box DB; synced by
-- sync_account_campaign.py (the warehouse only ever had active_campaign_count + a names-string on
-- core.sending_account_daily, never a clean mapping). Consumed by C5 portal Accounts tab + C4 analyst.
--
-- Table is loaded by the sync (TRUNCATE+INSERT each run); empty until the first sync. The view layers
-- offer / cm / is_mca / lead_type / esp / vendor onto it.

CREATE TABLE IF NOT EXISTS core.account_campaign (
  account_email          VARCHAR NOT NULL,
  workspace_slug         VARCHAR NOT NULL,
  campaign_id            VARCHAR NOT NULL,
  campaign_name          VARCHAR,
  campaign_status        INTEGER,
  campaign_status_label  VARCHAR,
  _synced_at             TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (workspace_slug, account_email, campaign_id)
);

CREATE OR REPLACE VIEW core.v_account_campaign_offer AS
SELECT
  ac.account_email,
  ac.workspace_slug,
  ac.campaign_id,
  ac.campaign_name,
  c.offer,
  c.cm,
  c.is_mca,
  lt.lead_type,
  sa.esp,
  v.vendor_category,
  sa.status AS account_status
FROM core.account_campaign ac
LEFT JOIN core.campaign c             ON c.campaign_id = ac.campaign_id
LEFT JOIN core.v_campaign_lead_type lt ON lt.campaign_id = ac.campaign_id
LEFT JOIN core.sending_account sa     ON lower(sa.email) = ac.account_email
LEFT JOIN core.sending_account_vendor v ON lower(v.account_email) = ac.account_email;
