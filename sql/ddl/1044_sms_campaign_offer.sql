-- 1044_sms_campaign_offer.sql  (2026-06-29)
-- @gate: add
-- Depends on: main.v_sms_campaign_performance, core.meeting (existing objects)
-- SMS campaign -> offer (Business Funding vs Pre-IPO vs Section 125 / Tariffs / R&D Credit),
-- classified from the actual BLAST COPY. Nothing else in the warehouse can produce the send-side
-- offer split: sub-account, campaign name and sender brand are throwaway/funding-skinned and carry
-- NO offer signal (verified). The message COPY does ("growth capital / line of credit" = funding;
-- "early shares in OpenAI & Anthropic, accredited investors / invest before IPO / Lumara Investment
-- Partners" = Pre-IPO). Campaigns are 100% offer-pure (0 mixed of 64 on 06-26).
-- Validated vs Grace 06-24/25/26: v_sms_campaign_performance JOIN this map reproduces her Funding/IPO
-- split across a 77->40->58% IPO swing (copy=truth; ~10% deltas = Grace ET-boundary/manual).
-- POPULATED BY entities/sms_campaign_offer.py (samples /sms/logs copy for NEW campaign_ids; keyword+LLM).

CREATE TABLE IF NOT EXISTS core.sms_campaign_offer (
    campaign_id         BIGINT,
    campaign_name       VARCHAR,
    offer               VARCHAR,
    method              VARCHAR,
    confidence          DOUBLE,
    sample_copy         VARCHAR,
    classifier_version  INTEGER,
    classified_at       TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);

CREATE TABLE IF NOT EXISTS core.sms_offer_override (
    campaign_id   BIGINT,
    offer         VARCHAR,
    note          VARCHAR,
    set_at        TIMESTAMPTZ
);

CREATE OR REPLACE VIEW main.v_sms_sends_by_offer AS
SELECT
    d.metric_date,
    COALESCE(o.offer, c.offer, '(offer-unknown)')   AS offer,
    SUM(d.sent)                                      AS sent,
    COUNT(DISTINCT d.campaign_id)                    AS campaigns
FROM main.v_sms_campaign_performance d
LEFT JOIN core.sms_campaign_offer  c USING (campaign_id)
LEFT JOIN core.sms_offer_override  o USING (campaign_id)
WHERE d.campaign_id IS NOT NULL
GROUP BY 1, 2;
