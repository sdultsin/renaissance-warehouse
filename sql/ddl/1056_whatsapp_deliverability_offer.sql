-- 1056 — WhatsApp (ISKRA) deliverability + offer split + meeting reconcile  [2026-06-30]
-- @gate: add
-- Depends on 82 1028 110
--
-- Handoff (2026-06-30 BUILD-whatsapp-iskra-deliverability-and-launch-attribution): the daily report's
-- WhatsApp numbers are misleading. ISKRA "sent" includes a ~30-37% FAILURE rate (verified 06-29/06-30),
-- launch attribution is missing, and the meeting count is unreconciled. This DDL ships the WhatsApp
-- analogue of the SMS deliverability layer (v_sms_deliverability*, DDL 1053) + offer split
-- (v_sms_sends_by_offer, DDL 1052) + a meeting reconcile, all PURELY ADDITIVE (no existing view's
-- column contract is touched here; the v_sms_dash_wa_daily meeting-bucket fix is the separate DDL 1057).
--
-- WHAT IS / ISN'T AVAILABLE (verified live against the ISKRA public API + warehouse, 2026-06-30):
--   * status (delivered/read/failed/sent/queued) and direction are 100% populated on raw_iskra_messages
--     -> deliverability IS fully derivable at the message grain. (delivered := delivered|read; failed;
--     pending := sent|queued = in-flight / no delivery receipt yet.)
--   * campaign_id/campaign_name/template_id/template_name are still 100% NULL from the API (0/100 outbound,
--     and there is NO /campaigns | /launches | /templates endpoint — all 404; only 6 read scopes exist).
--     So PER-LAUNCH ("campaign") attribution CANNOT be produced today — it is BLOCKED on Arseny populating
--     those fields. entities/iskra.py already captures the columns (DDL 1028) so they auto-fill the moment
--     he does. The closest reproducible "launch-ish" dimension we DO have is the offer (pipeline) below
--     plus the per-opener copy variant (v_whatsapp_copy_performance, DDL 1028).
--   * messages carry NO sending-number id, so deliverability-by-NUMBER is not derivable from the message
--     grain (raw_iskra_numbers cannot be joined to messages). Grain here is day x offer. A message<->number
--     link is a second Arseny ask.
--
-- OFFER (the WhatsApp Funding/Pre-IPO split). The only campaign-ish dimension ISKRA exposes is
-- conversations.pipeline_id. Verified 2026-06-30 there are two real pipelines + noise:
--   * a2484184-... = Business Funding outreach (49,138 June sends; 47,010/47,015 funding-keyword openers,
--     e.g. "business capital and lines of credit up to $500k").
--   * cd397669-... = WhatsApp number VERIFICATION / OTP template traffic (30,542 "sends" whose body is
--     "...is your verification code / OTP Code..."; active only Jun 3-9). NOT outreach -> is_outreach=FALSE,
--     excluded from the outreach offer/deliverability rollups so it cannot inflate the sent denominator.
--   * Pre-IPO WhatsApp outreach has NO active ISKRA pipeline today (sends = 0). The handoff's ISKRA
--     "Renaissance_Offer2 = Pre-IPO" workspace is not visible through our single read key; when a Pre-IPO
--     pipeline (or Arseny's campaign_name) appears, add one row to core.iskra_pipeline_offer — no DDL.
-- The mapping is a DATA table (core.iskra_pipeline_offer), not hardcoded UUIDs in view bodies, so Sam/Arseny
-- extend it with an INSERT. Mirrors the SMS pattern (core.sms_campaign_offer / sms_offer_override).

-- ---------------------------------------------------------------------------------------------
-- (0) Pipeline -> offer reference map (data-driven; idempotent seed).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.iskra_pipeline_offer (
    pipeline_id   VARCHAR PRIMARY KEY,
    offer         VARCHAR,        -- 'Business Funding' | 'Pre-IPO' | '(system/verification)'
    is_outreach   BOOLEAN,        -- FALSE = system/OTP traffic, excluded from outreach metrics
    note          VARCHAR
);
INSERT INTO core.iskra_pipeline_offer (pipeline_id, offer, is_outreach, note) VALUES
  ('a2484184-4c18-4faf-b2b9-dff9a4cadb25', 'Business Funding',      TRUE,
   'Funding LOC/MCA WhatsApp outreach (Thomas/rotating personas).'),
  ('cd397669-6bd3-4c74-b23b-59fc223f830c', '(system/verification)', FALSE,
   'WhatsApp number-verification / OTP template traffic (Jun 3-9). NOT outreach; excluded.')
ON CONFLICT (pipeline_id) DO NOTHING;

-- ---------------------------------------------------------------------------------------------
-- (1) v_whatsapp_deliverability_daily — atomic grain (day x offer/pipeline): attempted / delivered /
--     failed / pending + percentages. The WhatsApp analogue of v_sms_deliverability_daily (DDL 1053).
--     delivered := delivered|read ; failed := failed ; pending := sent|queued (in-flight, no receipt yet).
--     deliv_pct/fail_pct are of SENT/attempted (what the report must show instead of raw sent);
--     NB: column `sent` = all outbound attempted; the status='sent' value is the in-flight subset counted in `pending`.
--     resolved_deliv_pct is delivered/(delivered+failed) — the apples-to-apples vs the SMS 90% target.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW main.v_whatsapp_deliverability_daily AS
WITH m AS (
  SELECT
    (msg.created_at AT TIME ZONE 'UTC')::date AS metric_date,
    c.pipeline_id,
    msg.status
  FROM raw_iskra_messages AS msg
  LEFT JOIN raw_iskra_conversations AS c ON c.id = msg.conversation_id
  WHERE msg.direction = 'outbound'
)
SELECT
  m.metric_date,
  m.pipeline_id,
  COALESCE(po.offer, '(offer-unknown)')                         AS offer,
  COALESCE(po.is_outreach, FALSE)                               AS is_outreach,
  count(*)                                                      AS sent,
  count(*) FILTER (WHERE m.status IN ('delivered','read'))      AS delivered,
  count(*) FILTER (WHERE m.status = 'failed')                   AS failed,
  count(*) FILTER (WHERE m.status IN ('sent','queued'))         AS pending,
  ROUND(100.0 * count(*) FILTER (WHERE m.status IN ('delivered','read')) / NULLIF(count(*),0), 1) AS deliv_pct,
  ROUND(100.0 * count(*) FILTER (WHERE m.status = 'failed')              / NULLIF(count(*),0), 1) AS fail_pct,
  ROUND(100.0 * count(*) FILTER (WHERE m.status IN ('sent','queued'))    / NULLIF(count(*),0), 1) AS pending_pct,
  ROUND(100.0 * count(*) FILTER (WHERE m.status IN ('delivered','read'))
        / NULLIF(count(*) FILTER (WHERE m.status IN ('delivered','read','failed')), 0), 1)        AS resolved_deliv_pct
FROM m
LEFT JOIN core.iskra_pipeline_offer AS po ON po.pipeline_id = m.pipeline_id
GROUP BY 1, 2, 3, 4;

-- ---------------------------------------------------------------------------------------------
-- (2) v_whatsapp_deliverability_14d — trailing-14d rolling summary by offer (OUTREACH only); flags the
--     sub-90% resolved-delivery floor. Mirrors v_sms_deliverability_14d (DDL 1053).
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW main.v_whatsapp_deliverability_14d AS
SELECT
  d.offer,
  SUM(d.sent)       AS sent,
  SUM(d.delivered)  AS delivered,
  SUM(d.failed)     AS failed,
  SUM(d.pending)    AS pending,
  ROUND(100.0 * SUM(d.delivered) / NULLIF(SUM(d.sent), 0), 1)                       AS deliv_pct,
  ROUND(100.0 * SUM(d.failed)    / NULLIF(SUM(d.sent), 0), 1)                       AS fail_pct,
  ROUND(100.0 * SUM(d.delivered) / NULLIF(SUM(d.delivered) + SUM(d.failed), 0), 1)       AS resolved_deliv_pct,
  ROUND(100.0 * SUM(d.delivered) / NULLIF(SUM(d.delivered) + SUM(d.failed), 0), 1) < 90  AS below_target_90
FROM main.v_whatsapp_deliverability_daily AS d
WHERE d.metric_date >= CURRENT_DATE - INTERVAL 14 DAY
  AND d.is_outreach
GROUP BY d.offer;

-- ---------------------------------------------------------------------------------------------
-- (3) v_whatsapp_sends_by_offer — daily outreach sends (attempted/delivered/failed) split by offer.
--     The WhatsApp analogue of v_sms_sends_by_offer (DDL 1052). OUTREACH only (OTP/verification excluded).
--     Today this is 100% Business Funding; a Pre-IPO row appears automatically once a Pre-IPO pipeline is
--     mapped in core.iskra_pipeline_offer.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW main.v_whatsapp_sends_by_offer AS
SELECT
  d.metric_date,
  d.offer,
  SUM(d.sent)      AS sent,
  SUM(d.delivered) AS delivered,
  SUM(d.failed)    AS failed
FROM main.v_whatsapp_deliverability_daily AS d
WHERE d.is_outreach
GROUP BY 1, 2;

-- ---------------------------------------------------------------------------------------------
-- (4) v_whatsapp_meeting_reconcile — the three WhatsApp meeting signals side-by-side, per day, LABELED
--     and NEVER summed. Resolves the handoff's "9 vs 3" double-count concern by naming ONE source of truth:
--       * sheet_meetings  = SOURCE OF TRUTH: partner-logged WhatsApp meetings (core.meeting, channel
--         'WhatsApp'), bucketed by meeting_date (the day the meeting occurs) — the same SoT every other
--         channel uses. Split Funding/Pre-IPO via the sheet's offer column.
--       * iskra_ai_booked = ENGAGEMENT ONLY: ISKRA's AI meeting_status='booked' tag (raw_iskra_meetings),
--         by tag day. Inflated (204 all-time vs ~181 true) — DO NOT report as booked; shown only to track
--         the gap (Sam: portal/sheet is truth, not ISKRA's count).
--       * imb_wa_phone_match = CROSS-CHECK: portal im_bookings (Funding booking SoT) rows whose phone
--         matches a WhatsApp-messaged contact, bucketed by im_bookings.`date` (the real booking date —
--         im_bookings.meeting_date is ~100% NULL and created_at is the snapshot timestamp, both unusable).
--         im_bookings has no channel field and is Funding-only, so it is a cross-check, not the per-channel
--         SoT. NOTE the three columns use DIFFERENT time bases (meeting day / ISKRA tag day / booking day)
--         and DIFFERENT populations — they are shown side-by-side to reconcile, and must NEVER be summed.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW main.v_whatsapp_meeting_reconcile AS
WITH sheet AS (
  SELECT meeting_date AS metric_date,
         count(*)                                            AS sheet_meetings,
         count(*) FILTER (WHERE offer = 'Business Funding')  AS sheet_funding,
         count(*) FILTER (WHERE offer = 'Pre-IPO')           AS sheet_preipo
  FROM core.meeting
  WHERE source = 'sheet' AND channel = 'WhatsApp' AND is_duplicate_of IS NULL AND meeting_date IS NOT NULL
  GROUP BY 1
),
iskra AS (
  SELECT (tagged_at AT TIME ZONE 'UTC')::date AS metric_date,
         count(*) FILTER (WHERE meeting_status = 'booked') AS iskra_ai_booked
  FROM raw_iskra_meetings WHERE tagged_at IS NOT NULL GROUP BY 1
),
wa_phones AS (
  SELECT DISTINCT right(regexp_replace(contact_phone, '[^0-9]', '', 'g'), 10) AS phone10
  FROM raw_iskra_messages WHERE direction = 'outbound' AND contact_phone IS NOT NULL
),
imb AS (
  SELECT b.bk_date AS metric_date, count(*) AS imb_wa_phone_match
  FROM (
    SELECT DISTINCT
           right(regexp_replace(phone, '[^0-9]', '', 'g'), 10) AS phone10,
           try_cast(date AS DATE)                              AS bk_date  -- the real booking date
    FROM main.raw_im_bookings
    WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM main.raw_im_bookings)
      AND (deleted_at IS NULL OR deleted_at = '') AND phone IS NOT NULL
  ) b
  JOIN wa_phones w ON w.phone10 = b.phone10
  WHERE b.bk_date IS NOT NULL
  GROUP BY 1
)
SELECT
  COALESCE(s.metric_date, i.metric_date, b.metric_date) AS metric_date,
  COALESCE(s.sheet_meetings, 0)    AS sheet_meetings,      -- SOURCE OF TRUTH (report uses this)
  COALESCE(s.sheet_funding, 0)     AS sheet_funding,
  COALESCE(s.sheet_preipo, 0)      AS sheet_preipo,
  COALESCE(i.iskra_ai_booked, 0)   AS iskra_ai_booked,     -- engagement only; inflated; do NOT report
  COALESCE(b.imb_wa_phone_match,0) AS imb_wa_phone_match   -- cross-check (Funding bookings, WhatsApp-touched)
FROM sheet s
FULL OUTER JOIN iskra i ON i.metric_date = s.metric_date
FULL OUTER JOIN imb   b ON b.metric_date = COALESCE(s.metric_date, i.metric_date);
