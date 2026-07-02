-- v_sms_sends_by_offer — bake the PER-WORKSPACE offer rule in as the PRIMARY key [2026-06-29]
-- @gate: alter-type   (CREATE OR REPLACE VIEW — change offer derivation; no column add/rename/drop)
-- Depends on 1044 (v_sms_sends_by_offer, core.sms_campaign_offer, core.sms_offer_override)
--
-- Sam + Grace confirmed (2026-06-29) that SMS offer is designated PER SENDIVO SUB-ACCOUNT (workspace):
--   Renaissance 1 = Business Funding · Renaissance 2 = Pre-IPO ONLY · Renaissance 3 = Business Funding
--   (R3 is Sam's own workspace, taken over 2026-06-29 for funding + SMS AIM; was a stale/near-idle ws).
-- This == Grace's UI method exactly:  Funding = All-Outbound − Renaissance-2 SMS-sent ; Pre-IPO = R2.
--
-- DDL 1044 keyed the split off copy-keyword classification (core.sms_campaign_offer). Copy faithfully
-- tracks the workspace (each ws runs one offer's copy) and reconciles to Grace, BUT the keyword classifier
-- has a false-positive class — a re-subscribe/follow-up system blast in R1 (e.g. campaigns 2602/3257) trips
-- the IPO regex ("invest/shares") and lands ~394k R1 funding sends in Pre-IPO. Per-workspace is the
-- ground truth and is SELF-ENFORCING for new campaigns (incl. R3 as it ramps).
--
-- Precedence:  manual override (core.sms_offer_override)  >  workspace rule  >  copy-classifier fallback.
-- The per-row sub_account_name replicates Grace's PER-DAY method (handles the one campaign — 2477 — whose
-- sub-account label legitimately changed across days). NULL-sub_account rows carry no send volume, so the
-- copy fallback is immaterial (kept only for safety / future-proofing).
-- core.sms_campaign_offer (copy) is RETAINED as a per-campaign CROSS-CHECK: it now flags any campaign whose
-- copy disagrees with its workspace (e.g. Pre-IPO copy in a Funding workspace) for review, rather than
-- driving the served number.

CREATE OR REPLACE VIEW main.v_sms_sends_by_offer AS
SELECT
    d.metric_date,
    CASE
        WHEN ovr.offer IS NOT NULL                THEN ovr.offer               -- 1. manual override wins
        WHEN d.sub_account_name = 'Renaissance 2' THEN 'Pre-IPO'               -- 2. Grace's rule: R2 = Pre-IPO
        WHEN d.sub_account_name IS NOT NULL       THEN 'Business Funding'       -- 3. every other workspace = Funding
        ELSE COALESCE(cpy.offer, '(offer-unknown)')                            -- 4. NULL sub-account -> copy fallback
    END                              AS offer,
    SUM(d.sent)                      AS sent,
    COUNT(DISTINCT d.campaign_id)    AS campaigns
FROM main.v_sms_campaign_performance AS d
LEFT JOIN core.sms_offer_override  AS ovr USING (campaign_id)
LEFT JOIN core.sms_campaign_offer  AS cpy USING (campaign_id)
WHERE d.campaign_id IS NOT NULL
GROUP BY 1, 2;
