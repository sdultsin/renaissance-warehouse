-- @gate: add
-- Depends on 1120 (view v1), 1071, 1103, 1079
-- ============================================================================
-- 1121_infra_google_family_reseller.sql [2026-07-17] — Sam ruling R32 addendum, 2026-07-17:
-- **anything Google-family-TAGGED = RESELLER** ('Google' / 'I-Google' / 'GooglePanel'
-- tag vendors fold into Reseller; supersedes the 1120/1079 Google-ambiguity hold by
-- Sam's explicit authority). View v2 — only the vendor mapping changes vs 1120.
--
-- Original R32 contract (1120):
-- (recorded in handoffs/2026-07-15-cold-email-bi-VISION-AND-STATE.md rulings table).
--
-- DETERMINISTIC SENDING-INFRA ATTRIBUTION, simplified to Sam's three infras:
--   'Reseller Active'            -> Reseller
--   'Outreach Today Active'      -> OTD
--   'MilkBox Active 25/49/75'    -> Milkbox   (the exact three Milkbox active tags,
--        verified live in the campaign dim; Milkbox campaigns exist ONLY in
--        Funding 1 / 2 / 4 = renaissance-4 / renaissance-5 / koi-and-destroy — verified)
--
-- WHY A VIEW (repo convention, no new sync): the pipeline ALREADY self-maintains —
--   * LIVE: entities/instantly_analytics_daily.py re-pulls every keyed workspace's
--     campaign list + email_tag_list nightly -> raw_instantly_campaign_dim.tag_labels
--     (fresh API tag pull; tag-mapping reads, no per-campaign sweeps). Tariffs joins
--     the roster via WAREHOUSE_INSTANTLY_ANALYTICS_EXTRA_SLUGS (scripts/nightly.sh,
--     same PR).
--   * entities/campaign_infra.py classifies nightly (dim_tag > census_majority >
--     frozen_tag_table precedence) into core.campaign_infra.
--   * FROZEN: core.campaign_sending_tag (tag sync frozen 2026-06-14/15) — Sam's R32
--     ruling supersedes the freeze for SENDING-INFRA tags only, satisfied via the
--     live dim/census paths above; the frozen table remains the deleted-campaign
--     evidence source, labeled as such.
--   This view = the R32 contract over that stack, keyed on core.campaign_offer_scope
--   (the canonical 3,352-campaign universe incl. pre-May deleted campaigns that the
--   registry's universe never covered; offer_scope auto-appends new campaigns
--   nightly). It rides every promote + MotherDuck publish automatically.
--
-- infra_basis taxonomy (complete, per R32):
--   'tag_live'             — live tag evidence: dim_tag / census_majority (+ the 9
--                            manifest_pump_day OTD rows + any manual override: live-
--                            era evidence stronger than a name, folded here)
--   'tag_frozen_20260614'  — frozen ≤2026-06-14/15 tag table evidence
--   'name_hint'            — validated name tokens ONLY: OTD / O2D -> OTD (O2D=OTD,
--                            Sam 07-16) and MILKBOX -> Milkbox. These two classes
--                            measured 100% precision vs tag truth (DDL 1079);
--                            GOOGLE/OUTLOOK/RESELLER name tokens measured 68-80% and
--                            are NEVER applied (stay unattributed — data honesty).
--   'unattributed'         — genuinely nothing provable (incl. non-3-infra vendors
--                            like CheapInboxes/GooglePanel/MSPanel/raw frozen tags:
--                            the raw vendor stays visible in infra_vendor_raw).
--
-- Registry rows whose campaign_id is missing from offer_scope (a handful of
-- no-identity frozen stubs) are intentionally out — offer_scope IS the campaign
-- universe of record.
-- ============================================================================

CREATE OR REPLACE VIEW core.v_campaign_infra_simple AS
WITH j AS (
    SELECT s.campaign_id,
           s.campaign_name,
           s.workspace_slug,
           s.owner_cm,
           s.owner_basis,
           s.sent_lifetime,
           ci.infra_vendor,
           ci.matched_tag,
           ci.mixed_infra,
           ci.mixed_detail,
           ci.n_accounts,
           ci.derivation_source,
           regexp_matches(upper(COALESCE(s.campaign_name,'')), '(^|[^A-Z])(OTD|O2D)([^A-Z]|$)')  AS name_otd,
           regexp_matches(upper(COALESCE(s.campaign_name,'')), '(^|[^A-Z])MILKBOX([^A-Z]|$)')     AS name_milkbox
    FROM core.campaign_offer_scope s
    LEFT JOIN core.campaign_infra ci USING (campaign_id)
)
SELECT campaign_id,
       campaign_name,
       workspace_slug,
       owner_cm,
       owner_basis,
       sent_lifetime,
       CASE
           WHEN infra_vendor = 'OTD'      THEN 'OTD'
           WHEN infra_vendor = 'Reseller' THEN 'Reseller'
           -- Sam 07-17: Google-family tags -> Reseller (rules the 1079 ambiguity)
           WHEN infra_vendor IN ('Google','I-Google','GooglePanel') THEN 'Reseller'
           WHEN infra_vendor = 'MilkBox'  THEN 'Milkbox'
           WHEN name_otd                  THEN 'OTD'
           WHEN name_milkbox              THEN 'Milkbox'
           ELSE NULL
       END AS infra,
       CASE
           WHEN infra_vendor IN ('OTD','Reseller','MilkBox','Google','I-Google','GooglePanel') THEN
               CASE derivation_source
                   WHEN 'frozen_tag_table' THEN 'tag_frozen_20260614'
                   WHEN 'name_heuristic'   THEN 'name_hint'
                   ELSE 'tag_live'   -- dim_tag / census_majority / manifest_pump_day / manual
               END
           WHEN name_otd OR name_milkbox THEN 'name_hint'
           ELSE 'unattributed'
       END AS infra_basis,
       infra_vendor        AS infra_vendor_raw,
       matched_tag,
       mixed_infra,
       mixed_detail,
       n_accounts,
       derivation_source   AS derivation_source_raw
FROM j;
