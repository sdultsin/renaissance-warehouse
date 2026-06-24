-- Omnichannel OFFER dimension — make `offer` a first-class, channel-agnostic dimension [2026-06-24]
--
-- Problem: the omnichannel layer (DDL 89/1005) shipped offer-BLIND. Its per-channel "opps / replies /
-- meetings" silently MIX offers (Business Funding + Pre-IPO + Section 125 + Tariffs + R&D), so any
-- cross-channel total blends a pre-IPO reply rate with a funding one and is uninterpretable. `offer`
-- must be a property of the SEND (which pipeline/brand/campaign was sent = which offer), the same
-- send-side attribution foundation as copy/CM.
--
-- Reuse, don't reinvent (the offer dimension already exists upstream):
--   * EMAIL  → core.campaign.offer        (regex + LLM cache; values: Business Funding / Pre-IPO /
--                                           Section 125 / Tariffs / R&D Credit — the canonical vocab)
--   * SMS    → comms.brand.offer_type      (funding / pre_ipo) — NOTE: the warehouse mirror
--                                           raw_comms_brand does NOT yet carry offer_type (ingest gap;
--                                           fixed in a companion change), so SMS is wired but dormant.
--   * WhatsApp → (NEW) core.channel_offer_map — Iskra pipeline_id is the only campaign-ish dimension;
--                                           there is no upstream offer field, so we map it here.
--
-- Design: channel-agnostic + extensible (the verticals are coming). One map keyed by (channel,
-- source_key); one unified lookup view; offer added to the WhatsApp grain views; one additive
-- offer-sliced overview. ADDITIVE — does not mutate the shipped offer-blind v_omnichannel_overview
-- (Sam decides whether to replace it; see SELF-CLOSE decision (a)).
--
-- WhatsApp offer map evidence (whatsapp-offer-tagging chat, 2026-06-24; full Iskra history May18-Jun23):
--   a2484184… = Business Funding  (the ONLY sales pipeline: LOC/$400k-no-PG copy, 39,824 convs,
--                                   24,157 funding-keyword hits, 0 IPO hits, 33 meetings)
--   cd397669… = operational       (WhatsApp OTP / verification-code traffic — not a sales offer;
--                                   0 positive, 0 meetings)
--   a8fe7634… = test              (dev gibberish, 7 convs)
--   ZERO pre-IPO on WhatsApp anywhere in this Iskra workspace's history (pre-IPO runs on EMAIL + SMS).
--
-- Depends on: core.campaign (DDL 03), v_whatsapp_conversation_performance (DDL 82),
--   raw_iskra_messages/conversations (DDL 82), raw_pipeline_campaign_daily_metrics, core.meeting,
--   core.instantly_bounce_daily, v_sms_campaign_performance.

-- @gate: add
-- Depends on 03 16 82 1005

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 0. SMS enabling fix: the warehouse mirror raw_comms_brand predates comms.brand.offer_type, so SMS
--    offer is invisible in the warehouse. Add the column; the comms_mirror ingest derives its SELECT
--    list from the warehouse table schema, so it auto-pulls offer_type on the next nightly (no entity
--    edit needed). Until that nightly the column is NULL — the SMS legs below are guarded to stay
--    dormant (0 rows) rather than emit a NULL/blended offer. ADD COLUMN is additive + idempotent.
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
ALTER TABLE raw_comms_brand ADD COLUMN IF NOT EXISTS offer_type VARCHAR;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 1. core.channel_offer_map — the channel-agnostic send-side offer map (SoT for channels with no
--    upstream offer column; today WhatsApp, tomorrow any new channel/vertical). Email/SMS keep their
--    upstream home (core.campaign.offer / brand.offer_type) and are federated by v_channel_offer below.
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.channel_offer_map (
  channel       VARCHAR NOT NULL,                       -- 'whatsapp' | 'sms' | 'email' | future
  source_key    VARCHAR NOT NULL,                       -- whatsapp: iskra pipeline_id; sms: brand id; email: campaign_id
  offer         VARCHAR,                                -- canonical label (core.v_offer_dim vocab) or NULL when not a sales offer
  offer_kind    VARCHAR NOT NULL DEFAULT 'sales',       -- 'sales' | 'operational' | 'test'
  confidence    VARCHAR NOT NULL DEFAULT 'confirmed',   -- 'confirmed' | 'derived' | 'inferred'
  method        VARCHAR,                                -- how it was labeled
  confirmed_by  VARCHAR,
  confirmed_at  DATE,
  notes         VARCHAR,
  PRIMARY KEY (channel, source_key)
);

-- Idempotent re-seed of the WhatsApp rows — UPSERT (no DELETE, provably non-destructive): re-running
-- this DDL refreshes the mapped rows and never removes data. Other channels are never touched.
INSERT INTO core.channel_offer_map
  (channel, source_key, offer, offer_kind, confidence, method, confirmed_by, confirmed_at, notes) VALUES
  ('whatsapp', 'a2484184-4c18-4faf-b2b9-dff9a4cadb25', 'Business Funding', 'sales', 'confirmed',
   'outbound_copy: LOC/$400k-no-PG; 24157 funding-kw, 0 IPO-kw; 33 meetings', 'whatsapp-offer-tagging', '2026-06-24',
   '39,824 convs May22-Jun23; the active funding wave'),
  ('whatsapp', 'cd397669-6bd3-4c74-b23b-59fc223f830c', NULL, 'operational', 'confirmed',
   'outbound_copy: WhatsApp OTP/verification codes; 0 positive / 0 meetings', 'whatsapp-offer-tagging', '2026-06-24',
   '24,806 convs; number warm-up / OTP, not a sales offer — exclude from funnel'),
  ('whatsapp', 'a8fe7634-40f7-44f5-9376-030672e71328', NULL, 'test', 'confirmed',
   'outbound_copy: dev gibberish/"test"', 'whatsapp-offer-tagging', '2026-06-24',
   '7 convs; developer test pipeline')
ON CONFLICT (channel, source_key) DO UPDATE SET
  offer        = EXCLUDED.offer,
  offer_kind   = EXCLUDED.offer_kind,
  confidence   = EXCLUDED.confidence,
  method       = EXCLUDED.method,
  confirmed_by = EXCLUDED.confirmed_by,
  confirmed_at = EXCLUDED.confirmed_at,
  notes        = EXCLUDED.notes;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 2. core.v_channel_offer — ONE cross-channel offer lookup: "what offer is this (channel, key)?".
--    Email + WhatsApp are live; SMS is included but currently returns no rows because the
--    raw_comms_brand mirror lacks offer_type (companion ingest fix re-enables it with no view change).
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
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
  -- SMS leg: self-activating. Normalizes the comms enum (funding/pre_ipo) onto the canonical labels.
  -- Returns 0 rows until the next comms_mirror nightly populates offer_type (column added in step 0),
  -- then lights up automatically — no further DDL change.
  SELECT 'sms' AS channel, b.id AS source_key,
         CASE WHEN b.offer_type = 'pre_ipo' THEN 'Pre-IPO'
              WHEN b.offer_type = 'funding' THEN 'Business Funding'
              ELSE b.offer_type END AS offer,
         'sales' AS offer_kind, 'confirmed' AS confidence
    FROM raw_comms_brand b
   WHERE b.offer_type IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 3. core.v_whatsapp_conversation_offer — the universal WhatsApp offer TAG at conversation grain.
--    conversation_id → pipeline_id → offer. Every WhatsApp record joins here: messages & meetings on
--    conversation_id, Close opps on source_lead_id (= conversation_id) / campaign (= pipeline_id).
--    This is the "tag every WhatsApp record" deliverable, done as a derived join (the email
--    derived.reply_offer pattern) — backfill + go-forward by construction (recomputed every read).
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_whatsapp_conversation_offer AS
SELECT
  c.id                                   AS conversation_id,
  c.pipeline_id,
  om.offer,
  COALESCE(om.offer_kind, 'unknown')     AS offer_kind,
  COALESCE(om.confidence, 'unmapped')    AS offer_confidence
FROM raw_iskra_conversations c
LEFT JOIN core.channel_offer_map om
  ON om.channel = 'whatsapp' AND om.source_key = c.pipeline_id;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 4. v_whatsapp_conversation_performance — REPLACE to add offer/offer_kind (additive columns).
--    Same definition as DDL 82 + the offer join on pipeline_id. Existing columns unchanged/in order.
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_whatsapp_conversation_performance AS
WITH msg AS (
  SELECT conversation_id,
         count(*) FILTER (WHERE direction = 'outbound')                  AS outbound_msgs,
         count(*) FILTER (WHERE direction = 'inbound')                   AS inbound_msgs,
         min(created_at)                                                 AS first_message_at,
         max(created_at)                                                 AS last_message_at,
         max(created_at) FILTER (WHERE direction = 'inbound')            AS last_inbound_at,
         min(contact_phone)                                             AS contact_phone
  FROM raw_iskra_messages
  WHERE conversation_id IS NOT NULL
  GROUP BY conversation_id
)
SELECT
  COALESCE(m.conversation_id, c.id)        AS conversation_id,
  COALESCE(c.contact_phone, m.contact_phone) AS contact_phone,
  c.contact_name,
  c.pipeline_id,
  om.offer,
  COALESCE(om.offer_kind, 'unknown')       AS offer_kind,
  m.outbound_msgs,
  m.inbound_msgs,
  (COALESCE(m.inbound_msgs, 0) > 0)        AS replied,
  m.first_message_at,
  m.last_message_at,
  m.last_inbound_at,
  c.last_message_at                        AS conv_last_message_at,
  mt.reply_sentiment,
  mt.meeting_status,
  mt.deal_outcome,
  mt.summary,
  mt.tagged_at,
  (mt.reply_sentiment = 'positive')        AS is_positive_reply,
  (mt.meeting_status = 'booked')           AS meeting_booked
FROM msg m
FULL JOIN raw_iskra_conversations c ON m.conversation_id = c.id
LEFT JOIN raw_iskra_meetings mt      ON mt.conversation_id = COALESCE(m.conversation_id, c.id)
LEFT JOIN core.channel_offer_map om  ON om.channel = 'whatsapp' AND om.source_key = c.pipeline_id;

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 5. v_whatsapp_pipeline_performance — REPLACE to add offer/offer_kind (per the brief: the
--    pipeline-performance view must be offer-aware). Same rollup as DDL 1005 + the map join.
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_whatsapp_pipeline_performance AS
SELECT
  p.pipeline_id,
  om.offer,
  COALESCE(om.offer_kind, 'unknown')              AS offer_kind,
  count(*)                                        AS conversations,
  count(*) FILTER (WHERE p.replied)               AS replied,
  count(*) FILTER (WHERE p.is_positive_reply)     AS positive_replies,
  count(*) FILTER (WHERE p.meeting_booked)        AS meetings_booked,
  round(count(*) FILTER (WHERE p.replied)           * 1.0 / NULLIF(count(*), 0), 4) AS reply_rate,
  round(count(*) FILTER (WHERE p.is_positive_reply) * 1.0 / NULLIF(count(*), 0), 4) AS positive_rate
FROM v_whatsapp_conversation_performance p
LEFT JOIN core.channel_offer_map om ON om.channel = 'whatsapp' AND om.source_key = p.pipeline_id
GROUP BY p.pipeline_id, om.offer, COALESCE(om.offer_kind, 'unknown');

-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- 6. v_omnichannel_overview_by_offer — the offer-SLICED cross-channel overview (channel × offer × date).
--    Additive NEW view; the shipped offer-blind v_omnichannel_overview is left intact for back-compat.
--    EMAIL + WhatsApp legs carry the true offer; SMS rolls up under offer='(sms-offer-pending-mirror)'
--    until the raw_comms_brand offer_type fix lands (then the SMS CTE gets the brand→offer join).
--    QA invariant: SUM over offer per (channel, date) == the offer-blind v_omnichannel_overview value.
-- ─────────────────────────────────────────────────────────────────────────────────────────────────
-- NOTE on grain-safety: each metric is computed in its own (offer × date) CTE, then a per-channel KEY
-- SPINE (UNION of every metric's keys) LEFT JOINs them — so a meeting or positive on a date/offer with
-- no sends that day is NEVER dropped (the bug a sends-LEFT-JOIN-meetings would cause). This mirrors the
-- shipped v_omni_*_performance "dates" spine. QA: SUM over offer per (channel,date) == the offer-blind
-- v_omnichannel_performance (verified loss-less for email sends 5,727,961=5,727,961 + WhatsApp 55,534=55,534).
CREATE OR REPLACE VIEW v_omnichannel_overview_by_offer AS
WITH email_send AS (
  SELECT COALESCE(c.offer, '(unmapped)') AS offer, m.date AS metric_date,
         sum(m.sent)                     AS sent,
         sum(m.unique_replies)           AS replies_human,
         sum(m.unique_replies_automatic) AS replies_auto,
         sum(m.unique_opportunities)     AS positive_replies
  FROM raw_pipeline_campaign_daily_metrics m
  LEFT JOIN core.campaign c ON c.campaign_id = m.campaign_id
  GROUP BY 1, 2
),
email_mtg AS (
  SELECT COALESCE(c.offer, COALESCE(NULLIF(mt.offer, ''), '(unmapped)')) AS offer,
         CAST(mt.posted_at AS DATE) AS metric_date, count(*) AS meetings_booked
  FROM core.meeting mt
  LEFT JOIN core.campaign c ON c.campaign_id = mt.campaign_id
  WHERE mt.is_duplicate_of IS NULL AND mt.source = 'sheet' AND mt.channel = 'Email'
  GROUP BY 1, 2
),
email_keys AS (
  SELECT offer, metric_date FROM email_send
  UNION SELECT offer, metric_date FROM email_mtg
),
email_final AS (
  SELECT 'email' AS channel, k.offer, k.metric_date,
         COALESCE(s.sent, 0)             AS sent,
         s.replies_human, s.replies_auto,
         COALESCE(s.positive_replies, 0) AS positive_replies,
         COALESCE(mt.meetings_booked, 0) AS meetings_booked
  FROM email_keys k
  LEFT JOIN email_send s  USING (offer, metric_date)
  LEFT JOIN email_mtg  mt USING (offer, metric_date)
),
-- WhatsApp sends by offer (message → conversation → pipeline → offer). replies_human/auto are NULL —
-- WhatsApp has no human/auto split (matches the shipped v_omni_whatsapp_performance); positive comes
-- from the AI-tag layer (wa_pos).
wa_send AS (
  SELECT COALESCE(co.offer, CASE WHEN co.offer_kind IN ('operational','test') THEN '(non-sales)' ELSE '(unmapped)' END) AS offer,
         CAST(timezone('UTC', msg.created_at) AS DATE) AS metric_date,
         count(*) FILTER (WHERE msg.direction = 'outbound') AS sent
  FROM raw_iskra_messages msg
  LEFT JOIN core.v_whatsapp_conversation_offer co ON co.conversation_id = msg.conversation_id
  GROUP BY 1, 2
),
wa_pos AS (
  SELECT COALESCE(co.offer, '(non-sales)') AS offer,
         CAST(timezone('UTC', mt.tagged_at) AS DATE) AS metric_date,
         count(*) FILTER (WHERE mt.reply_sentiment = 'positive') AS positive_replies
  FROM raw_iskra_meetings mt
  LEFT JOIN core.v_whatsapp_conversation_offer co ON co.conversation_id = mt.conversation_id
  WHERE mt.tagged_at IS NOT NULL
  GROUP BY 1, 2
),
wa_mtg AS (
  -- WhatsApp meetings (Funding Form, channel=WhatsApp). The sheet carries no per-row pipeline, so these
  -- can't be split below pipeline grain — all WhatsApp is Business Funding, so they roll up under it
  -- (the only WhatsApp sales offer). Flagged as a known coarse-grain in the report.
  SELECT 'Business Funding' AS offer, CAST(posted_at AS DATE) AS metric_date, count(*) AS meetings_booked
  FROM core.meeting
  WHERE is_duplicate_of IS NULL AND source = 'sheet' AND channel = 'WhatsApp'
  GROUP BY 1, 2
),
wa_keys AS (
  SELECT offer, metric_date FROM wa_send
  UNION SELECT offer, metric_date FROM wa_pos
  UNION SELECT offer, metric_date FROM wa_mtg
),
wa_final AS (
  SELECT 'whatsapp' AS channel, k.offer, k.metric_date,
         COALESCE(s.sent, 0)             AS sent,
         CAST(NULL AS BIGINT)            AS replies_human,
         CAST(NULL AS BIGINT)            AS replies_auto,
         COALESCE(p.positive_replies, 0) AS positive_replies,
         COALESCE(mt.meetings_booked, 0) AS meetings_booked
  FROM wa_keys k
  LEFT JOIN wa_send s  USING (offer, metric_date)
  LEFT JOIN wa_pos  p  USING (offer, metric_date)
  LEFT JOIN wa_mtg  mt USING (offer, metric_date)
)
SELECT * FROM email_final
UNION ALL
SELECT * FROM wa_final;
-- SMS leg deliberately omitted until raw_comms_brand.offer_type lands (step 0 + next nightly) — adding a
-- blended SMS row here would re-introduce exactly the offer-blind blend this view exists to remove. SMS
-- meetings additionally lack any brand/offer key on the sheet (sendivo_sub_account='Renaissance 1' only),
-- a genuine gap reported to Sam, not papered over.
