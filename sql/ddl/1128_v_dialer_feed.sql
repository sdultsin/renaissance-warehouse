-- @gate: add
-- Depends on 1124
-- Depends on 1125
-- Depends on 1126
-- Depends on 1127
-- ============================================================================
-- 1128_v_dialer_feed.sql — core.v_dialer_feed: THE warm-callers' list (R14/R24/R25).
--
-- SEMANTICS (one row per callable PERSON = lead_email):
--   IN:  current-state label ∈ {opportunity, engagement, confused} in ANY workspace
--        (the overlaid v_reply_label_current — booked leads already read
--        'meeting_booked' there and drop out = "NOT meeting_booked-current")
--   OUT: booked-meeting DNC (raw_comms_suppression booked_exclusivity/form_delivered)
--        PLUS caller-harm suppressions: verbal_opt_out (lead told a caller no) and
--        partner_application (active partner pipeline) — matched on BOTH email keys
--        and phone keys, last-10-digit normalized. sms_opt_out/stop_keyword are
--        deliberately NOT excluded: R25 ruled the DNC settlement SMS-specific and
--        calling permitted; excluding them would gut the callable pool against
--        that ruling.
--   AND: phone present (lead-mirror phone master via raw_lead_dialer_attrs;
--        core.lead.phone_e164 fallback)
--   Workspace/campaign attribution = the (workspace, lead) pair with the most
--   recent current-positive reply.
--
-- HONESTY COLUMNS:
--   sending_domain = the sending account domain of the lead's latest reply that
--        carries eaccount (core.reply; ~47% of reply rows carry eaccount — NULL
--        when never captured).
--   brand = BEST-EFFORT prettified domain root. There is NO domain→brand mapping
--        on-box (copy-level brands like "Blue Haven" live in variant signatures,
--        not resolvable per-domain) — treat as a hint, sending_domain is the truth.
--   caller_dnc = reserved, always NULL today: no federal DNC list exists on-box.
--        The column is the integration point for one.
--   email_opt_out = the lead unsubscribed from EMAIL — calling stays permitted
--        (R25); carried so callers have context.
--
-- Reversible: DROP VIEW.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE VIEW core.v_dialer_feed AS
WITH callable AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY lower(lead_email)
               ORDER BY current_label_message_ts DESC, labeled_at DESC
           ) AS rn
    FROM core.v_reply_label_current
    WHERE current_label IN ('opportunity', 'engagement', 'confused')
),
pick AS (
    SELECT * FROM callable WHERE rn = 1
),
dnc_email AS (
    SELECT DISTINCT lower(prospect_number) AS lead_email
    FROM main.raw_comms_suppression
    WHERE reason IN ('booked_exclusivity', 'form_delivered', 'verbal_opt_out', 'partner_application')
      AND prospect_number LIKE '%@%'
),
dnc_phone AS (
    SELECT DISTINCT right(regexp_replace(prospect_number, '[^0-9]', '', 'g'), 10) AS phone10
    FROM main.raw_comms_suppression
    WHERE reason IN ('booked_exclusivity', 'form_delivered', 'verbal_opt_out', 'partner_application')
      AND prospect_number NOT LIKE '%@%'
      AND length(regexp_replace(prospect_number, '[^0-9]', '', 'g')) >= 10
),
lead_phone_fallback AS (
    SELECT lower(email) AS lead_email, max(phone_e164) AS phone_e164
    FROM core.lead
    WHERE email IS NOT NULL AND phone_e164 IS NOT NULL
    GROUP BY 1
),
message_current AS (
    -- latest verdict per message, ranked across the FULL verdict set (incl. gate
    -- classes auto/bot; only 'labeler_error' ignored) so a re-gated message can
    -- never fall back to a stale positive label (two-key reviewer finding).
    SELECT *,
           row_number() OVER (
               PARTITION BY message_ref_table, message_ref_id
               ORDER BY labeled_at DESC, labeler_version DESC
           ) AS rn
    FROM main.raw_reply_label_event
    WHERE label <> 'labeler_error'
      AND message_ts IS NOT NULL
),
episodes AS (
    -- opportunity EPISODES per person: an opportunity verdict whose previous
    -- message-grain verdict (any workspace; auto/bot verdicts count as
    -- non-opportunity boundaries) was not opportunity starts an episode
    SELECT lead_email, count(*) AS opp_episodes
    FROM (
        SELECT lower(lead_email) AS lead_email, label,
               lag(label) OVER (
                   PARTITION BY lower(lead_email)
                   ORDER BY message_ts, labeled_at, event_id
               ) AS prev_label
        FROM message_current
        WHERE rn = 1
    )
    WHERE label = 'opportunity'
      AND (prev_label IS NULL OR prev_label <> 'opportunity')
    GROUP BY 1
),
brandsrc AS (
    SELECT lower(lead_email) AS lead_email,
           split_part(eaccount, '@', 2) AS sending_domain
    FROM core.reply
    WHERE eaccount IS NOT NULL AND eaccount LIKE '%@%'
    QUALIFY row_number() OVER (
        PARTITION BY lower(lead_email) ORDER BY reply_timestamp DESC
    ) = 1
)
SELECT
    p.lead_email                                        AS email,
    a.first_name,
    a.last_name,
    COALESCE(a.company_clean, a.company_raw)            AS company,
    a.company_clean_source,
    COALESCE(a.phone, lf.phone_e164)                    AS phone,
    p.current_label,
    b.bucket,
    b.days_since_reply,
    b.last_positive_at,
    COALESCE(ep.opp_episodes, 0)                        AS opp_episodes,
    p.workspace_slug                                    AS workspace,
    p.current_campaign_id                               AS campaign_id,
    bs.sending_domain,
    CASE WHEN bs.sending_domain IS NULL THEN NULL
         ELSE array_to_string(
                  list_transform(
                      str_split(
                          replace(
                              regexp_extract(
                                  regexp_replace(bs.sending_domain,
                                                 '\.(com|net|org|co|io|us|biz|info)$', ''),
                                  '([^.]+)$', 1),
                              '-', ' '),
                          ' '),
                      w -> upper(substr(w, 1, 1)) || substr(w, 2)),
                  ' ')
    END                                                 AS brand,
    a.city,
    a.state,
    a.timezone,
    CAST(NULL AS BOOLEAN)                               AS caller_dnc,
    p.current_opt_out                                   AS email_opt_out,
    p.current_label_message_ts                          AS last_labeled_reply_at,
    (a.lead_email IS NOT NULL)                          AS attrs_matched
FROM pick p
LEFT JOIN main.raw_lead_dialer_attrs a ON a.lead_email = lower(p.lead_email)
LEFT JOIN lead_phone_fallback lf       ON lf.lead_email = lower(p.lead_email)
LEFT JOIN core.v_lead_bucket_current b ON b.lead_email = lower(p.lead_email)
LEFT JOIN episodes ep                  ON ep.lead_email = lower(p.lead_email)
LEFT JOIN brandsrc bs                  ON bs.lead_email = lower(p.lead_email)
LEFT JOIN dnc_email de                 ON de.lead_email = lower(p.lead_email)
LEFT JOIN dnc_phone dp
       ON dp.phone10 = right(regexp_replace(COALESCE(a.phone, lf.phone_e164), '[^0-9]', '', 'g'), 10)
WHERE COALESCE(a.phone, lf.phone_e164) IS NOT NULL
  AND length(regexp_replace(COALESCE(a.phone, lf.phone_e164), '[^0-9]', '', 'g')) >= 10
  AND de.lead_email IS NULL
  AND dp.phone10 IS NULL;
