-- @gate: add
-- Depends on 1104
-- Depends on 1106
-- ============================================================================
-- 1135_v_meeting_truth_ws_dim_backstop.sql — workspace attribution backstop for
-- core.v_meeting_truth via the campaign dim (renovation fix, KPI-daily lane).
--
-- DEFECT (measured 2026-07-17): v_meeting_truth workspace_slug attribution
-- collapsed for July-2026 rows — 661/2,158 email-ours meetings attributed
-- (31%) vs 97-99% Mar-Jun. Cause: era2 rows (core.meeting, source='sheet')
-- carry workspace_slug from the legacy meeting entity's own resolution, which
-- does not know the July campaign-naming generation ("F2 - GEN - RESELLER-GOOG
-- - R1 (SAM)" class) — the workspace token moved into the campaign string.
-- BUT those same rows DO carry a matched campaign_id (match_method sheet_norm),
-- and core.v_campaign_dim_unified resolves 1,487/1,487 of the July unattributed
-- campaign_ids to a workspace (renaissance-2 432 · renaissance-5 332 ·
-- koi-and-destroy 280 · prospects-power 258 · renaissance-4 170 · gatekeepers 10
-- · warm-leads 5).
--
-- FIX (view-level, additive): workspace_slug = COALESCE(entity-resolved slug,
-- campaign-dim slug). Applied to BOTH era legs (era1 portal rows also carry a
-- best-effort campaign_id; the backstop only ever FILLS NULLs/empties, never
-- overrides an entity-resolved slug). View column list/order UNCHANGED —
-- consumers (v_kpi_email, v_funnel, conversion feeds, 1124 meeting events
-- entity, v_dialer_feed) are shape-safe. campdim is deduped to one slug per
-- campaign_id (MAX) so the join cannot fan out meeting rows.
--
-- The rest of both legs is reproduced VERBATIM from the live definition
-- (= DDL 1106; verified identical via duckdb_views() 2026-07-17).
--
-- Reversible: re-apply the 1106 view definition (CREATE OR REPLACE back).
-- ============================================================================

CREATE OR REPLACE VIEW core.v_meeting_truth AS
WITH campdim AS (
  SELECT campaign_id, MAX(workspace_slug) AS ws_slug
  FROM core.v_campaign_dim_unified
  WHERE campaign_id IS NOT NULL AND workspace_slug IS NOT NULL AND workspace_slug <> ''
  GROUP BY 1
)
SELECT mr.meeting_key, mr.era, mr.meeting_date, mr.posted_at, mr.lead_email,
       mr.channel_norm, mr.channel_basis,
       COALESCE(NULLIF(mr.workspace_slug, ''), cd1.ws_slug) AS workspace_slug,
       mr.offer, mr.in_funding_scope, mr.is_ours, mr.partner, mr.campaign_string,
       mr.campaign_id, mr.match_method, mr.cm, mr."source", mr.portal_source, mr.raw_text
FROM core.meeting_rebuilt mr
LEFT JOIN campdim cd1 ON cd1.campaign_id = mr.campaign_id
WHERE mr.era = 'slack_era_portal'
UNION ALL
SELECT m.meeting_id AS meeting_key,
       'post_cutover_core_meeting' AS era,
       m.meeting_date,
       m.posted_at,
       m.lead_email,
       CASE WHEN m.channel IN ('Email', 'SMS', 'WhatsApp', 'LinkedIn', 'Call') THEN m.channel
            WHEN m.channel IS NULL OR m.channel = '' THEN 'Other'
            ELSE 'Other' END AS channel_norm,
       'native_channel' AS channel_basis,
       COALESCE(NULLIF(m.workspace_slug, ''), cd2.ws_slug) AS workspace_slug,
       m.offer,
       CASE WHEN COALESCE(m.offer, '') = 'Pre-IPO' THEN FALSE
            WHEN cos.in_funding_scope IS NOT NULL THEN cos.in_funding_scope
            ELSE TRUE END AS in_funding_scope,
       NOT regexp_matches(lower(COALESCE(m.campaign_name_raw, '') || ' ' || COALESCE(m.raw_text, '')), 'iskra') AS is_ours,
       m.partner_key AS partner,
       m.campaign_name_raw AS campaign_string,
       m.campaign_id,
       m.match_method,
       m.cm,
       m."source",
       NULL AS portal_source,
       m.raw_text
FROM core.meeting m
LEFT JOIN core.campaign_offer_scope cos ON cos.campaign_id = m.campaign_id
LEFT JOIN campdim cd2 ON cd2.campaign_id = m.campaign_id
WHERE m."source" = 'sheet' AND m.meeting_date >= DATE '2026-06-01';
