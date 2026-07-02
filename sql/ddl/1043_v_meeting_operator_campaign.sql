-- Canonical cross-operator meeting attribution (Max vs Eyver vs funding, etc). Version 1043.
-- @gate: add
-- Depends on 03, 20, 107
--
-- WHY: cross-operator meeting analysis ("Max's workspace vs Eyver vs the funding workspaces")
-- kept appearing "impossible warehouse-side" because scans joined core.meeting to
-- main.raw_pipeline_campaigns -- the RETIRING pipeline-supabase dim, which carries 0 rows for
-- Max's workspace, so Max's campaigns (and their variant-copy) fell under "(no dim)". That is a
-- WRONG-DIM artifact, not a modeling gap: Max IS fully modeled --
--   * core.workspace          has "Max's workspace" (9e822ccc-549d-4d91-ac13-a9c313af8fd3),
--   * core.campaign           (the CANONICAL campaign dim) has Max's 77 campaigns,
--   * core.meeting            has Max's 25 meetings, campaign-attributed (23/24), with
--                             workspace_canonical = "Max's workspace" reconciled via core.workspace_alias.
-- Verified 2026-06-29: the canonical join below returns every operator INCLUDING Max
-- (Max's workspace = 24/24 email meetings attributed), alongside Funding 5 (Eyver) 581, Warm leads
-- 1045, the funding workspaces, RE Wholesale, etc.
--
-- USE THIS for cross-operator meeting analysis. It joins core.meeting -> core.campaign (by
-- campaign_id) -> core.workspace (by workspace_id), so EVERY sending workspace is covered by
-- construction and "(no dim)" cannot happen. Do NOT join main.raw_pipeline_campaigns (retiring;
-- missing Max + others). Grain: one row per attributed meeting (campaign_id NOT NULL). SMS/WhatsApp
-- bookings that route through sub-account rather than campaign_id are out of scope here (see
-- core.v_sms_booking_attribution for the SMS path).
CREATE OR REPLACE VIEW core.v_meeting_operator_campaign AS
SELECT
    w.workspace_id,
    w.name                       AS operator_workspace,
    m.campaign_id,
    c.name                       AS campaign_name,
    c.cm,
    c.offer,
    c.is_active                  AS campaign_is_active,
    m.channel,
    m.meeting_date,
    m.meeting_id,
    m.program,
    m.partner,
    m.cm                         AS meeting_cm,
    m.cm_workspace
FROM core.meeting m
JOIN core.campaign c   ON m.campaign_id = c.campaign_id
JOIN core.workspace w  ON c.workspace_id = w.workspace_id
WHERE m.campaign_id IS NOT NULL;
