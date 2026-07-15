-- @gate: add
-- Depends on 1101
-- Depends on 1102
-- ============================================================================
-- 1104_campaign_dim_patch.sql — core.campaign_dim_patch (evidence-carrying upsert
-- source for two measured dim defects) + core.v_campaign_dim_unified (the coalesced
-- campaign dimension incl. still_live_in_instantly).
--
-- PATCH SOURCES (deliverables/2026-07-14-cold-email-bi/, snapshot warehouse_20260714_141517_221):
--   A. dim-patch-a-nullws-campaign-workspace.csv — 52 campaign_ids whose
--      raw_pipeline_campaign_daily_metrics.workspace_id is NULL (orphaned deleted
--      campaigns, 1.05M sends); workspace recovered via core.variant_copy join.
--   B. dim-patch-b-missing-campaigns.csv — 72 campaign_ids with daily metrics but no
--      raw_pipeline_campaigns row; 56 names recovered (44 core.campaign, 8
--      raw_pipeline_campaign_data, 4 raw_instantly_campaign_analytics_*), 8 UNRECOVERED
--      (kept with NULL name — honesty rows), 8 synthetic __ledger_recon__ rows EXCLUDED
--      (they are evicted from campaign-grain surfaces, not patched — see DDL 1105/1106).
--   Rows present in both A and B are merged (patch_kind marks the union).
--
-- NEVER updates raw tables in place — this is a SIDE patch consumers coalesce via
-- core.v_campaign_dim_unified. Reversible: DROP VIEW + DROP TABLE.
--
-- still_live_in_instantly (metrics-cut Defect D1: raw_pipeline_campaigns.status says
-- 'active' for 905/957 scope campaigns while only ~105 exist in Instantly): liveness
-- derives from raw_instantly_campaign_dim.last_seen_at — the dim IS the nightly census
-- (refreshed by entities/instantly_analytics_daily.py). A campaign is still_live iff
-- seen within 2 days of the dim's freshest sighting. Campaigns absent from the dim
-- (pre-census era or deleted before 2026-06) are NOT live; liveness_basis says why.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.campaign_dim_patch (
  campaign_id      VARCHAR PRIMARY KEY,
  workspace_slug   VARCHAR,            -- recovered canonical slug (NULL = unrecovered)
  workspace_source VARCHAR,
  campaign_name    VARCHAR,            -- recovered name (NULL = unrecovered)
  name_source      VARCHAR,
  patch_kind       VARCHAR NOT NULL,   -- a_nullws_workspace | b_missing_campaign | a_nullws_workspace+b_missing_name
  evidence         VARCHAR,
  sent_context     BIGINT,             -- lifetime sends at patch time (context)
  snapshot_id      VARCHAR,
  _source          VARCHAR DEFAULT 'deliverable-2026-07-14-cold-email-bi',
  _loaded_at       TIMESTAMPTZ DEFAULT now()
);

INSERT INTO core.campaign_dim_patch
  (campaign_id, workspace_slug, workspace_source, campaign_name, name_source, patch_kind,
   evidence, sent_context, snapshot_id)
VALUES
  ('42c52f31-5aab-408d-aca8-c3102f041c60','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 58 to  65 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',91946,'warehouse_20260714_141517_221.duckdb'),
  ('66522882-616d-44fe-b17b-1b2d7280e34c','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 10 to 17 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',77574,'warehouse_20260714_141517_221.duckdb'),
  ('0fa51fb3-4cae-44f4-b854-5e78f3b6ee50','the-gatekeepers','variant_copy.workspace_id join (patch A)','OUTLOOK - TEST 1 (SAMUEL)','raw_instantly_campaign_analytics_*','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: the-gatekeepers; hard-deleted in Instantly)',76000,'warehouse_20260714_141517_221.duckdb'),
  ('7c15a526-79d9-4c83-a5f5-8b929105f686','renaissance-4','variant_copy.workspace_id join (patch A)','CX - OTD - General Test June 1st (SAMUEL)','raw_instantly_campaign_analytics_*','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-4; hard-deleted in Instantly)',60312,'warehouse_20260714_141517_221.duckdb'),
  ('863d053d-e628-453e-8194-1efa120b25ea','the-gatekeepers','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: the-gatekeepers; hard-deleted in Instantly)',56000,'warehouse_20260714_141517_221.duckdb'),
  ('6a03a444-5637-485a-8ea7-4b014fa0ee0a','prospects-power','variant_copy.workspace_id join (patch A)','OFF - Outlook - ARC 16 - Human (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',48041,'warehouse_20260714_141517_221.duckdb'),
  ('b81a924a-6b2a-40d9-a4ca-759c249a3d44','prospects-power','variant_copy.workspace_id join (patch A)','OFF - Outlook - ARC 5  - Beauty (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',45649,'warehouse_20260714_141517_221.duckdb'),
  ('b73ff343-de61-4a94-a2cf-0a3374c57fd4','prospects-power','variant_copy.workspace_id join (patch A)','OFF - Outlook - ARC 2 - Housing (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',42306,'warehouse_20260714_141517_221.duckdb'),
  ('42f3c9fe-e84c-4256-90cc-90610c12c612','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 2 to 9 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',39303,'warehouse_20260714_141517_221.duckdb'),
  ('a807cef2-25f1-4213-bfb9-7dae7a9c36db','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 26 to 33 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',35904,'warehouse_20260714_141517_221.duckdb'),
  ('7a4e0f75-683d-410c-8434-9e23431bf26d','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 18 to 25 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',31503,'warehouse_20260714_141517_221.duckdb'),
  ('7bccccd1-d2cd-412f-b827-ce20a913c45d','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 177 to 184 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',30433,'warehouse_20260714_141517_221.duckdb'),
  ('2124b9f6-e724-44d0-9dc0-a347f3083e43','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 188 to 195 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',30180,'warehouse_20260714_141517_221.duckdb'),
  ('0f8a1275-441c-4dca-a363-c56574d51f08','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 26 to 33 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',29959,'warehouse_20260714_141517_221.duckdb'),
  ('833c25a1-b4bd-4853-981b-4c3feffe926d','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 161 to 168 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',29416,'warehouse_20260714_141517_221.duckdb'),
  ('d12b620e-951e-49e3-a695-be79825173c8','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 169 to 176 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',26430,'warehouse_20260714_141517_221.duckdb'),
  ('b5e5094f-2eef-490e-89ee-d309de6ebe5e','prospects-power','variant_copy.workspace_id join (patch A)','OFF - ARC 4 - General (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',24909,'warehouse_20260714_141517_221.duckdb'),
  ('bf8779d6-ef1c-4c9f-8b35-b349a035c846','prospects-power','variant_copy.workspace_id join (patch A)','OFF - ARC 4 - General (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',24852,'warehouse_20260714_141517_221.duckdb'),
  ('64911bbe-96e6-449a-8aed-290afa99d2e7','prospects-power','variant_copy.workspace_id join (patch A)','OFF - ARC 4 - General (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',24559,'warehouse_20260714_141517_221.duckdb'),
  ('4a755a1c-8543-4296-9b71-aa618e929476','renaissance-2','variant_copy.workspace_id join (patch A)','ON - OTD - GENERAL MATRIX TEST ( 18 to 25 ) - (EYVER)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',23473,'warehouse_20260714_141517_221.duckdb'),
  ('2015d76f-cf29-42e1-8ac2-eff96e542562','equinox','variant_copy.workspace_id join (patch A)','OFF - ARC 2 - General (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',20000,'warehouse_20260714_141517_221.duckdb'),
  ('dc68bc85-8a92-4298-98ab-132ae1f3e088','prospects-power','variant_copy.workspace_id join (patch A)','OFF - ARC 4 - General (LEO)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: prospects-power; hard-deleted in Instantly)',19146,'warehouse_20260714_141517_221.duckdb'),
  ('a1832075-943e-4eb3-a2fe-9fac52cdcff2','koi-and-destroy','variant_copy.workspace_id join (patch A)','BOUNCED F4 - GEN - GOOGLE - M2 (SAM)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',7574,'warehouse_20260714_141517_221.duckdb'),
  ('3d029395-e266-48ec-af91-a7ba15560a2c','automated-applications','variant_copy.workspace_id join (patch A)','General','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',791,'warehouse_20260714_141517_221.duckdb'),
  ('e0e3cebf-7859-4109-be97-64fa5e80efe1','outlook-3','variant_copy.workspace_id join (patch A)','ZZ Folderly MailIn RESEND (securedusyou.co) [2026-06-06]','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',40,'warehouse_20260714_141517_221.duckdb'),
  ('ea944d2e-5514-475c-8a64-d0463af9ffdc','koi-and-destroy','variant_copy.workspace_id join (patch A)','ZZ Folderly OTD pool test [2026-06-06]','raw_instantly_campaign_analytics_*','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',40,'warehouse_20260714_141517_221.duckdb'),
  ('5a24e346-35ad-4e89-8d37-b9bacb9b227b','outlook-3','variant_copy.workspace_id join (patch A)','ZZ Folderly Placement Test - MailIn (securedusyou.co) [2026-06-05]','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',40,'warehouse_20260714_141517_221.duckdb'),
  ('d88c1006-3852-4983-abd1-ecd23090f3c6','koi-and-destroy','variant_copy.workspace_id join (patch A)','ZZ Folderly Placement Test - OTD (goldberg-registry.info) [2026-06-05]','raw_instantly_campaign_analytics_*','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',40,'warehouse_20260714_141517_221.duckdb'),
  ('e18332e2-136b-4118-9d6a-ea27c6cbece4','renaissance-2','variant_copy.workspace_id join (patch A)','ZZ Folderly Placement Test - Google (charthelphealth.co) [2026-06-05]','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-2; hard-deleted in Instantly)',40,'warehouse_20260714_141517_221.duckdb'),
  ('d31ce6e5-d527-4073-9bbd-d59635754816','section-125-2','variant_copy.workspace_id join (patch A)','Healthcare employers','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('04dda76e-51d1-4ec2-9c0f-9358509f93e6','koi-and-destroy','variant_copy.workspace_id join (patch A)','F4 - PROSVC - OTD - W2 (SAM)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('57723be4-70c9-4717-bca6-09257c9bc80d','koi-and-destroy','variant_copy.workspace_id join (patch A)','GTM - HIRING - GTM Engineer - Reseller Active (SAM) [2026-07-06]','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('8124dbb5-43fa-44a2-8b70-3eebed07cc47','koi-and-destroy','variant_copy.workspace_id join (patch A)','F4 - MCA - GOOGLE (SAM)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('7f1d4c4b-b1ca-4623-82f8-060ec486b782','koi-and-destroy','variant_copy.workspace_id join (patch A)','F4 - RETAIL - OTD - W2 (SAM)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('9eb7b0aa-e066-4be5-a367-1eeba210efe9','koi-and-destroy','variant_copy.workspace_id join (patch A)','F4 - TRADES - OTD - W2 (SAM)','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('50391583-f312-4dc4-a8b4-f0d49ccf08e8','renaissance-1','variant_copy.workspace_id join (patch A)','TEST','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: 404 tombstone (key: renaissance-1; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('58e3d4cf-6b44-4a8e-b0ae-def0a3b9a8da','section-125-2','variant_copy.workspace_id join (patch A)','Executive Directors ONLY, Nonprofits (sniper) - Ayman','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('02482dc4-3250-473e-bc13-1561a2806c50','section-125-2','variant_copy.workspace_id join (patch A)','Education Leaders (Heads/Supers) ONLY - Ayman','core.campaign','a_nullws_workspace+b_missing_name','core.variant_copy.workspace_id join | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('5032ce59-865b-40dc-8262-6f8e474d3150','None','variant_copy.workspace_id join (patch A)','ON - Gen Pair 38 (ANDRES) Z','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',52824,'warehouse_20260714_141517_221.duckdb'),
  ('8ba64b6d-1049-4fb1-84ea-b8856d80c60f','None','variant_copy.workspace_id join (patch A)','Old - Gen Pair 38 (ANDRES) Z','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',27361,'warehouse_20260714_141517_221.duckdb'),
  ('2bc48108-bc39-44d2-a446-4574f524a6bc','None','variant_copy.workspace_id join (patch A)','ON - Pair 8 - General - Quickcred - (SHAAN)','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',17125,'warehouse_20260714_141517_221.duckdb'),
  ('3f6dbec7-a003-4e24-8c01-cf9c2288f55f','None','variant_copy.workspace_id join (patch A)','P1 - Construction - Apexlink Capital - (SHAAN)','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',15163,'warehouse_20260714_141517_221.duckdb'),
  ('f85f2eb8-bf83-4e08-a112-64269a993a29','None','variant_copy.workspace_id join (patch A)','ON - Pair 2 - CEO''s - Angels Funding - (SHAAN)','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',14958,'warehouse_20260714_141517_221.duckdb'),
  ('3b032b59-5d53-4194-aac8-24a1817465d9','None','variant_copy.workspace_id join (patch A)','RG3570 - Restaurants(TOMI) x MA','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',13782,'warehouse_20260714_141517_221.duckdb'),
  ('f0e88915-6b54-425b-9b6d-8962ed53c663','None','variant_copy.workspace_id join (patch A)','ON - Gen te Pair 38 (ANDRES) Z','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',11878,'warehouse_20260714_141517_221.duckdb'),
  ('e3655a76-b1da-41d6-8a3c-c8fa4f507ce0','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('ce7a599b-69d6-4024-bafd-dae009e27eba','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('7a90f590-a36b-4a06-a485-5f8123749e38','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('01cc9659-93f4-46b6-b2d1-cf2b990cb0b6','None','variant_copy.workspace_id join (patch A)','ON - Pair 18 - Construction (CARLOS)','raw_pipeline_campaign_data','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('594c30f1-f87b-4d52-a87d-13abef9ba04e','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('590f4f72-cf65-4876-af01-224ec45226f9','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('6c09c1c1-90e0-45bd-8299-700b1effe80e','None','variant_copy.workspace_id join (patch A)',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','a_nullws_workspace+b_missing_name','UNRESOLVED (no variant_copy rows) | patch B: not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('0c4eb4b2-f07c-44db-9a14-153dc763027f','renaissance-5','fact_table','F2 - GEN - OTD-MS-WKND - 7-12-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',14378,'warehouse_20260714_141517_221.duckdb'),
  ('3ab84655-6742-489b-9645-384987ef073f','koi-and-destroy','fact_table','F4 - GEN - OTD-MS-WKND - 7-12-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',12650,'warehouse_20260714_141517_221.duckdb'),
  ('c7f239a6-dc7d-46a5-ba74-5a0ac6718a9b','warm-leads','fact_table','Big Think Capital - No show','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',4156,'warehouse_20260714_141517_221.duckdb'),
  ('cd3e8340-dcdd-40f7-8549-4feac81efd62','warm-leads','fact_table','GreenBridge Capital - No show','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',3892,'warehouse_20260714_141517_221.duckdb'),
  ('568b4e71-cf74-4f69-9760-246e196d7716','koi-and-destroy','fact_table','F4 - GEN - RESELLER-GOOG-WKND - 7-12-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=404 tombstone (key: koi-and-destroy; hard-deleted in Instantly)',2735,'warehouse_20260714_141517_221.duckdb'),
  ('1cf78c6e-b49a-49e1-ac79-98272ad79de7','warm-leads','fact_table','GoQualifi - No show','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',2387,'warehouse_20260714_141517_221.duckdb'),
  ('f3c41cf4-8d61-49f5-a0f8-e4980e774f82','renaissance-5','fact_table','F2 - GEN - RESELLER-GOOG-WKND - 7-12-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=404 tombstone (key: renaissance-5; hard-deleted in Instantly)',1174,'warehouse_20260714_141517_221.duckdb'),
  ('d966f344-da1e-42b2-8474-347abba8d85c','renaissance-4','fact_table',NULL,'UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404)','b_missing_campaign','name_source=UNRECOVERED (absent from all warehouse name tables; API GET /campaigns/{id}=404); api_meta=404 tombstone (key: renaissance-4; hard-deleted in Instantly)',0,'warehouse_20260714_141517_221.duckdb'),
  ('e7a63c72-c870-46eb-9abd-b8a4c0dab3f5','renaissance-4','fact_table','My Campaign','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('f5b18022-b23a-4e19-983f-aa00ba034028','koi-and-destroy','fact_table','F4 - TAX-941 - 7-13-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('83b30795-f4ea-46fb-bcd5-d8122a5f2a94','koi-and-destroy','fact_table','F4 - TAX-BROAD - 7-13-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb'),
  ('00df05c3-1c31-41fd-9f03-dc5651ffd030','koi-and-destroy','fact_table','F4 - HELOC - 7-13-01 (SAM)','core.campaign','b_missing_campaign','name_source=core.campaign; api_meta=not attempted (dead-workspace key or 0-sent)',0,'warehouse_20260714_141517_221.duckdb')
ON CONFLICT (campaign_id) DO NOTHING;

-- ── The coalesced campaign dimension ─────────────────────────────────────────
-- Name priority prefers REAL names over 'Unknown campaign …' placeholders:
--   live census dim > escrow history dim > patch > pipeline dim.
-- Workspace priority: live census dim > escrow history dim > patch > alias-normalized
-- pipeline dim (fixes the 5 display-name-contaminated workspace_id rows via 1102).
CREATE OR REPLACE VIEW core.v_campaign_dim_unified AS
WITH ids AS (
  SELECT campaign_id FROM main.raw_pipeline_campaigns
  UNION SELECT campaign_id FROM raw_instantly_campaign_dim
  UNION SELECT CAST(campaign_id AS VARCHAR) FROM main.raw_instantly_campaign_dim_history
  UNION SELECT campaign_id FROM core.campaign_dim_patch
  UNION SELECT DISTINCT campaign_id FROM main.raw_pipeline_campaign_daily_metrics
         WHERE NOT contains(campaign_id, '__ledger_recon__')
),
rpc AS (
  SELECT campaign_id,
         NULLIF(name, '') AS name,
         NULLIF(workspace_id, '') AS workspace_id,
         status AS pipeline_status
  FROM main.raw_pipeline_campaigns
),
live_dim AS (
  SELECT campaign_id, workspace_slug, campaign_name, campaign_status,
         first_seen_at, last_seen_at
  FROM raw_instantly_campaign_dim
),
hist_dim AS (
  SELECT CAST(campaign_id AS VARCHAR) AS campaign_id, workspace_slug, name, status,
         timestamp_created, source
  FROM main.raw_instantly_campaign_dim_history
),
freshest AS (SELECT max(last_seen_at) AS max_seen FROM raw_instantly_campaign_dim)
SELECT
  i.campaign_id,
  COALESCE(
    NULLIF(ld.campaign_name, ''),
    NULLIF(hd.name, ''),
    p.campaign_name,
    CASE WHEN rpc.name NOT ILIKE 'Unknown campaign%' THEN rpc.name END,
    rpc.name
  ) AS campaign_name,
  COALESCE(ld.workspace_slug, hd.workspace_slug, p.workspace_slug, wn.warehouse_slug,
           rpc.workspace_id) AS workspace_slug,
  CASE
    WHEN ld.campaign_id IS NOT NULL AND ld.last_seen_at >= f.max_seen - INTERVAL 2 DAY THEN TRUE
    ELSE FALSE
  END AS still_live_in_instantly,
  CASE
    WHEN ld.campaign_id IS NOT NULL AND ld.last_seen_at >= f.max_seen - INTERVAL 2 DAY
      THEN 'census_dim_seen_recently'
    WHEN ld.campaign_id IS NOT NULL THEN 'census_dim_stale (deleted after capture)'
    WHEN hd.campaign_id IS NOT NULL THEN 'escrow_dim_only (deleted; frozen 2026-07-15 capture)'
    ELSE 'pre_census_era (never in live dim; liveness unknown->false)'
  END AS liveness_basis,
  ld.last_seen_at AS census_last_seen_at,
  ld.first_seen_at AS census_first_seen_at,
  hd.timestamp_created AS created_at_instantly,
  rpc.pipeline_status,          -- Defect D1: stale for deleted campaigns; do NOT read as liveness
  p.patch_kind, p.evidence AS patch_evidence,
  CASE WHEN ld.campaign_id IS NOT NULL THEN 'raw_instantly_campaign_dim'
       WHEN hd.campaign_id IS NOT NULL THEN 'raw_instantly_campaign_dim_history'
       WHEN p.campaign_id IS NOT NULL THEN 'campaign_dim_patch'
       WHEN rpc.campaign_id IS NOT NULL THEN 'raw_pipeline_campaigns'
       ELSE 'fact_table_only' END AS identity_source
FROM ids i
LEFT JOIN live_dim ld USING (campaign_id)
LEFT JOIN hist_dim hd USING (campaign_id)
LEFT JOIN core.campaign_dim_patch p USING (campaign_id)
LEFT JOIN rpc USING (campaign_id)
LEFT JOIN core.v_workspace_slug_norm wn ON wn.alias_lower = lower(rpc.workspace_id)
CROSS JOIN freshest f;
