-- v_channel_offer — dedup the SMS leg to the latest brand row [2026-06-24]
--
-- Fix to DDL 1009: raw_comms_brand is an APPEND-per-run mirror (comms_mirror deletes only the current
-- run_id then re-inserts, so it accumulates one full copy of comms.brand per nightly — e.g. 760 rows =
-- 35 brands × 26 runs). 1009's SMS leg filtered `WHERE offer_type IS NOT NULL` only, which is correct
-- for exactly one populated nightly but returns DUPLICATE brands once ≥2 nightlies have run with the
-- offer_type column present. Dedup to the latest row per brand id (by _loaded_at) so the SMS leg always
-- yields one row per brand. Pure CREATE OR REPLACE of the view — additive, no data change.
--
-- @gate: add
-- Depends on 03 16 1009
-- Depends on: core.campaign (DDL 03), core.channel_offer_map + raw_comms_brand.offer_type (DDL 1009/16)

CREATE OR REPLACE VIEW core.v_channel_offer AS
  SELECT 'email'    AS channel, CAST(campaign_id AS VARCHAR) AS source_key, offer,
         'sales'    AS offer_kind, 'confirmed' AS confidence
    FROM core.campaign
   WHERE offer IS NOT NULL
  UNION ALL
  SELECT 'whatsapp' AS channel, source_key, offer, offer_kind, confidence
    FROM core.channel_offer_map
   WHERE channel = 'whatsapp'
  UNION ALL
  -- SMS leg: dedup raw_comms_brand to the latest row per brand id, then normalize the comms enum
  -- (funding/pre_ipo) onto the canonical labels. Self-activating: 0 rows until comms_mirror populates
  -- offer_type, then exactly one row per brand thereafter.
  SELECT 'sms' AS channel, b.id AS source_key,
         CASE WHEN b.offer_type = 'pre_ipo' THEN 'Pre-IPO'
              WHEN b.offer_type = 'funding' THEN 'Business Funding'
              ELSE b.offer_type END AS offer,
         'sales' AS offer_kind, 'confirmed' AS confidence
    FROM (
      SELECT id, offer_type,
             row_number() OVER (PARTITION BY id ORDER BY _loaded_at DESC, _run_id DESC) AS rn
        FROM raw_comms_brand
    ) b
   WHERE b.rn = 1 AND b.offer_type IS NOT NULL;
