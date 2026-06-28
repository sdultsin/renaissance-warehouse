-- 97_sms_booking_attribution.sql  (LEAN design, revised 2026-06-28)
-- SMS booking -> blast attribution. Replaces Sendivo's last-reply `deals_won` for the copy KPI.
-- Spec: deliverables/2026-06-28-sendivo-dealswon-recon/ATTRIBUTION-BUILD-SPEC.md (REVISED LEAN DESIGN)
--
-- DESIGN (mirrors Instantly: copy + replies + outcomes, NOT every message):
--   * Blast copy        -> /blasts (~800 rows; small).
--   * Replies           -> raw_sendivo_inbound (already synced).
--   * Booked-lead->blast -> raw_sendivo_blast_deal, fed by Sendivo's per-deal blast export (the
--                          click-through Larry is building). ONE row per booked deal (~2k), NOT a
--                          1M/day outbound scrape. Wants the ORIGINATING (first-reply) blast for our
--                          KPI + the last-touch blast for reconciling against Sendivo's deals_won.
--
-- VERDICT context: deals_won = BOOKED, never funded. core.deal_funded stays GBC-only; nothing here
-- touches it. Historical pre-06-26 attribution + this export are vendor-gated (see G0-FINDINGS.md).

-- ---------------------------------------------------------------------------
-- Per-booked-deal blast attribution from Sendivo (vendor export). Lean: ~2k rows, not 1M/day.
CREATE TABLE IF NOT EXISTS raw_sendivo_blast_deal (
    deal_id                 VARCHAR,
    contact_phone10         VARCHAR,
    contact_email           VARCHAR,
    origin_blast_id         BIGINT,     -- first-reply / conversation-originating blast (OUR KPI)
    origin_blast_name       VARCHAR,
    last_touch_blast_id     BIGINT,     -- Sendivo's last-reply blast (= how deals_won is counted)
    campaign_id             BIGINT,
    sub_account_id          BIGINT,
    deal_status             VARCHAR,    -- e.g. 'closed-won'
    is_closed_won           BOOLEAN,    -- label "Booked"
    booked_at               TIMESTAMPTZ,
    _loaded_at              TIMESTAMPTZ NOT NULL,
    _run_id                 VARCHAR
);

-- ---------------------------------------------------------------------------
-- Booking truth (SMS) with phone, recovered from the Funding Form by email.
-- NOTE (measured 2026-06-28): ~78% of core.meeting SMS bookings join to an FF-SMS row with a phone;
-- the ~22% gap is a core.meeting<->Funding-Form reconciliation gap (bookings aged out of the live
-- sheet / mapped to SMS from another source), NOT a phone/email capture gap. Tracked, not hidden.
CREATE OR REPLACE VIEW core.v_sms_booking_phone AS
WITH ff AS (
    SELECT
        lower(trim(json_extract_string(row_json, '$[9]')))                       AS email,
        right(regexp_replace(json_extract_string(row_json, '$[10]'), '[^0-9]', '', 'g'), 10) AS phone10
    FROM main.raw_sheets_funding_form_data
    WHERE _run_id = (SELECT max(_run_id) FROM main.raw_sheets_funding_form_data)
      AND upper(trim(json_extract_string(row_json, '$[3]'))) = 'SMS'
)
SELECT DISTINCT
    m.meeting_id,
    lower(m.lead_email)                                  AS email,
    ff.phone10,
    m.program,
    m.sendivo_sub_account,
    m.meeting_date,
    coalesce(m.submission_ts, (m.meeting_date)::TIMESTAMP) AS booking_ts
FROM core.meeting m
LEFT JOIN ff ON ff.email = lower(m.lead_email)
WHERE m.channel = 'SMS';

-- ---------------------------------------------------------------------------
-- One row per SMS booking with the blast that PRODUCED it (origin) + Sendivo's last-touch blast.
-- attribution_method is ALWAYS set so nothing is silently dropped.
CREATE OR REPLACE VIEW core.v_sms_booking_attribution AS
SELECT
    b.meeting_id,
    b.email,
    b.phone10,
    b.program,
    b.sendivo_sub_account,
    b.booking_ts,
    d.origin_blast_id                                    AS attributed_blast_id,
    d.origin_blast_name                                  AS attributed_blast_name,
    d.last_touch_blast_id                                AS sendivo_last_reply_blast_id,
    CASE
        WHEN b.phone10 IS NULL              THEN 'unattributed_no_phone'
        WHEN d.origin_blast_id IS NOT NULL  THEN 'origin_blast'
        WHEN d.last_touch_blast_id IS NOT NULL THEN 'last_touch_fallback'
        ELSE 'unattributed_no_deal_export'
    END                                                  AS attribution_method
FROM core.v_sms_booking_phone b
LEFT JOIN raw_sendivo_blast_deal d
       ON d.contact_phone10 = b.phone10
      OR lower(d.contact_email) = b.email;

-- ---------------------------------------------------------------------------
-- Per-blast performance: OUR bookings-attributed (origin) vs Sendivo's last-touch deals_won.
CREATE OR REPLACE VIEW core.v_sms_blast_performance AS
SELECT
    coalesce(attributed_blast_id, sendivo_last_reply_blast_id) AS blast_id,
    any_value(attributed_blast_name)                           AS blast_name,
    count(*) FILTER (WHERE attribution_method = 'origin_blast')        AS bookings_origin_ours,
    count(*) FILTER (WHERE sendivo_last_reply_blast_id IS NOT NULL)    AS deals_won_sendivo_lasttouch,
    any_value(program)                                         AS program,
    any_value(sendivo_sub_account)                             AS sub_account
FROM core.v_sms_booking_attribution
WHERE coalesce(attributed_blast_id, sendivo_last_reply_blast_id) IS NOT NULL
GROUP BY 1;
