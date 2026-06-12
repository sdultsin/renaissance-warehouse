-- Version 62 (2026-06-12) — KPI dashboard views: v_kpi_email + v_kpi_sms.
--
-- Sam's KPI redefinition (handoffs/2026-06-12-meeting-attribution-audit.md §6):
--   * KPI = emails sent ÷ meetings booked  ("how many emails to book a meeting")
--           — an EFFICIENCY RATIO, not a target. Lower is better.
--   * EOP = emails sent ÷ opportunities.
--   * Reply rate split human vs auto (campaign_daily carries both columns).
--
-- Grain rules:
--   * v_kpi_email — date × infra × cm × is_mca. Daily/weekly/monthly = GROUP BY
--     over this view. The ratio columns here are DAILY-ROW ratios; for any
--     multi-day window recompute from the summed measures (Σsent ÷ Σmeetings),
--     NEVER average the daily ratios. All KPI ratios are PERIOD ratios (sends
--     and meetings from the same window), not cohort-attributed — most
--     meaningful at weekly/monthly grain.
--   * v_kpi_sms — date × Sendivo campaign for send-side measures; SMS meetings
--     only exist at date grain (campaign_id IS NULL on SMS bookings), so they
--     ride on a separate date-level row with campaign_id NULL and
--     campaign_name '(sms meetings)'. GROUP BY date for daily totals.
--
-- Channel split: a meeting is SMS if its raw Slack text / raw campaign name
-- carries an SMS-channel keyword (sendivo / sms / whatsapp / iskra) — the
-- heuristic the audit sanctioned. SMS-flagged meetings are EXCLUDED from
-- v_kpi_email even when they fuzzy-matched an email campaign (e.g. the
-- "Sendivo opps RV" catch-all), and are what v_kpi_sms counts.
--
-- Attribution: campaign dims come from raw_pipeline_campaigns
-- (feedback_prefer_pipeline_derived_over_regex); is_mca from core.campaign.
-- Meetings that match no campaign land in an explicit '(unattributed)' bucket
-- (ship-with-bucket-visible decision) — they count toward date totals but not
-- per-infra/per-CM cuts.

CREATE OR REPLACE VIEW v_kpi_email AS
WITH dims AS (
    SELECT
        c.campaign_id,
        COALESCE(NULLIF(rp.infra_type, ''), 'unknown')        AS infra,
        COALESCE(NULLIF(rp.cm_name, ''), NULLIF(c.cm, ''), '(no cm)') AS cm,
        COALESCE(c.is_mca, FALSE)                             AS is_mca
    FROM core.campaign c
    LEFT JOIN raw_pipeline_campaigns rp USING (campaign_id)
),
sends AS (
    SELECT
        cd.date,
        d.infra, d.cm, d.is_mca,
        SUM(cd.sent)            AS sent,
        SUM(cd.opportunities)   AS opportunities,
        SUM(cd.replies_human)   AS replies_human,
        SUM(cd.replies_auto)    AS replies_auto,
        SUM(cd.bounces)         AS bounces
    FROM core.campaign_daily cd
    JOIN dims d USING (campaign_id)
    GROUP BY 1, 2, 3, 4
),
email_meetings AS (
    SELECT
        CAST(m.posted_at AS DATE)                  AS date,
        COALESCE(d.infra, '(unattributed)')        AS infra,
        COALESCE(d.cm, NULLIF(m.cm, ''), '(unattributed)') AS cm,
        COALESCE(d.is_mca, FALSE)                  AS is_mca,
        COUNT(*)                                   AS meetings
    FROM core.meeting m
    LEFT JOIN dims d ON m.campaign_id = d.campaign_id
    WHERE m.source = 'slack'
      AND NOT regexp_matches(
            lower(COALESCE(m.campaign_name_raw, '') || ' ' || COALESCE(m.raw_text, '')),
            'sendivo|\bsms\b|whatsapp|iskra')
    GROUP BY 1, 2, 3, 4
)
SELECT
    COALESCE(s.date,   mt.date)    AS date,
    COALESCE(s.infra,  mt.infra)   AS infra,
    COALESCE(s.cm,     mt.cm)      AS cm,
    COALESCE(s.is_mca, mt.is_mca)  AS is_mca,
    COALESCE(s.sent, 0)            AS sent,
    COALESCE(s.opportunities, 0)   AS opportunities,
    COALESCE(s.replies_human, 0)   AS replies_human,
    COALESCE(s.replies_auto, 0)    AS replies_auto,
    COALESCE(s.bounces, 0)         AS bounces,
    COALESCE(mt.meetings, 0)       AS meetings,
    -- Daily-row ratios. Recompute from sums for any multi-day window.
    CAST(s.sent AS DOUBLE) / NULLIF(s.opportunities, 0)      AS eop,
    CAST(s.sent AS DOUBLE) / NULLIF(mt.meetings, 0)          AS kpi_emails_per_meeting,
    CAST(mt.meetings AS DOUBLE) / NULLIF(s.opportunities, 0) AS opp_to_meeting_rate,
    CAST(s.replies_human AS DOUBLE) / NULLIF(s.sent, 0)      AS reply_rate_human,
    CAST(s.replies_auto AS DOUBLE) / NULLIF(s.sent, 0)       AS reply_rate_auto
FROM sends s
FULL OUTER JOIN email_meetings mt
    ON  s.date = mt.date
    AND s.infra = mt.infra
    AND s.cm = mt.cm
    AND s.is_mca IS NOT DISTINCT FROM mt.is_mca;


CREATE OR REPLACE VIEW v_kpi_sms AS
-- Send-side: per Sendivo campaign per day (from v_sms_campaign_performance).
SELECT
    p.metric_date                AS date,
    p.campaign_id,
    p.campaign_name,
    p.sub_account_name,
    p.sent,
    p.delivered,
    p.replies,
    p.positive_replies           AS opportunities,   -- "opps" = positive replies
    p.opt_outs,
    p.cost_usd,
    0                            AS meetings,         -- meetings exist at date grain only
    CAST(p.positive_replies AS DOUBLE) / NULLIF(p.delivered, 0) AS opp_rate,
    CAST(p.replies AS DOUBLE)          / NULLIF(p.delivered, 0) AS reply_rate
FROM v_sms_campaign_performance p
UNION ALL
-- Meeting-side: SMS bookings carry no Sendivo campaign linkage (campaign_id is
-- the EMAIL campaign id and NULL/catch-all for SMS) — date-level rows only.
SELECT
    CAST(m.posted_at AS DATE)    AS date,
    NULL                         AS campaign_id,
    '(sms meetings)'             AS campaign_name,
    NULL                         AS sub_account_name,
    0 AS sent, 0 AS delivered, 0 AS replies, 0 AS opportunities, 0 AS opt_outs,
    0.0 AS cost_usd,
    COUNT(*)                     AS meetings,
    NULL AS opp_rate, NULL AS reply_rate
FROM core.meeting m
WHERE m.source = 'slack'
  AND regexp_matches(
        lower(COALESCE(m.campaign_name_raw, '') || ' ' || COALESCE(m.raw_text, '')),
        'sendivo|\bsms\b|whatsapp|iskra')
GROUP BY 1;
