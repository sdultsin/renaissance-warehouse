-- 82 — Iskra WhatsApp source (the WhatsApp analogue of Sendivo's SMS, DDL 25/34).
--
-- Iskra public API (https://xglfamaaotmwulglwcui.supabase.co/functions/v1/public-api, Bearer
-- ISKRA_API_KEY, read-only) — Renaissance's WhatsApp outreach (Thomas-persona LOC/funding).
-- Mirrors the Sendivo warehouse pattern; the cleaner Iskra API closes the SMS-side gaps by
-- design: per-message analytics (G4), per-conversation meeting attribution + sentiment, deals
-- (G8), and a numbers asset-health inventory (G2). Facts mirror the Iskra API exactly
-- (warehouse-query-prompt contract) — we do NOT re-derive opp/positive-reply logic here; the
-- raw stats funnel is stored verbatim as the reconciliation source-of-truth row.
--
-- IMPORTANT — "opportunities" in raw_iskra_stats is Iskra's OWN field (≈ any inbound: ~86% of
-- sends as of 2026-06-18). It is NOT Renaissance's positive-reply/opp definition and must not be
-- mapped onto it. The positive-intent signal for CRM/attribution is reply_sentiment='positive'
-- and/or meeting_status='booked' on raw_iskra_meetings (see v_whatsapp_conversation_performance).
--
-- Entities: entities/iskra.py (phase 'iskra'). Applied at schema version 82 by scripts/setup_db.py.

-- =====================================================================
-- raw_iskra_messages — one row per WhatsApp message. PK id; idempotent UPSERT (status evolves
-- sent->delivered, so we update status/_loaded_at on conflict). Incremental by created_at.
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_messages (
    id                   VARCHAR PRIMARY KEY,
    channel              VARCHAR,
    direction            VARCHAR,        -- inbound | outbound
    body                 VARCHAR,
    status               VARCHAR,        -- delivered | received | ...
    provider_message_id  VARCHAR,
    conversation_id      VARCHAR,
    contact_phone        VARCHAR,
    contact_name         VARCHAR,
    created_at           TIMESTAMPTZ,
    _loaded_at           TIMESTAMPTZ NOT NULL,
    _run_id              VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_msg_created ON raw_iskra_messages (created_at);
CREATE INDEX IF NOT EXISTS ix_iskra_msg_conv    ON raw_iskra_messages (conversation_id);

-- =====================================================================
-- raw_iskra_conversations — one row per conversation. PK id; UPSERT. Incremental by created_at
-- (the /conversations feed's sort key; server `since` is ignored -> client-side early-stop).
-- Mutable fields (last_message_at, unread_count) only refresh within the overlap window; the
-- authoritative activity truth is the message grain. conv_last_message_at in the view is a
-- best-effort snapshot.
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_conversations (
    id                 VARCHAR PRIMARY KEY,
    contact_phone      VARCHAR,
    contact_name       VARCHAR,
    last_message_text  VARCHAR,
    last_message_at    TIMESTAMPTZ,
    unread_count       INTEGER,
    assigned_user_id   VARCHAR,
    pipeline_id        VARCHAR,
    created_at         TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ NOT NULL,
    _run_id            VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_conv_lastmsg ON raw_iskra_conversations (last_message_at);

-- =====================================================================
-- raw_iskra_meetings — AI-tagged per-conversation meeting/sentiment state (the WhatsApp meeting-
-- attribution join SMS lacks). One row per conversation (latest tag); PK conversation_id; UPSERT.
-- reply_sentiment + meeting_status here are the POSITIVE-INTENT signal (NOT stats.opportunities).
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_meetings (
    conversation_id  VARCHAR PRIMARY KEY,
    meeting_status   VARCHAR,        -- booked | none | ...
    meeting_evidence VARCHAR,
    deal_outcome     VARCHAR,
    reply_sentiment  VARCHAR,        -- positive | negative | neutral
    summary          VARCHAR,
    tagged_at        TIMESTAMPTZ,
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_meet_tagged ON raw_iskra_meetings (tagged_at);

-- =====================================================================
-- raw_iskra_deals — deal pipeline. PK id; UPSERT (stage/amount evolve). Incremental by updated_at.
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_deals (
    id               VARCHAR PRIMARY KEY,
    title            VARCHAR,
    stage_id         VARCHAR,
    pipeline_id      VARCHAR,
    contact_name     VARCHAR,
    contact_phone    VARCHAR,
    amount           DOUBLE,
    currency         VARCHAR,
    conversation_id  VARCHAR,
    created_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ,
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_deal_stage ON raw_iskra_deals (stage_id);
CREATE INDEX IF NOT EXISTS ix_iskra_deal_conv  ON raw_iskra_deals (conversation_id);

-- =====================================================================
-- raw_iskra_numbers — sending-asset (WhatsApp number) health inventory. APPEND per run (one
-- snapshot per id per run) so we keep an asset-health TIME SERIES; v_whatsapp_number_health
-- picks the latest per id. (Fills the SMS-side G2 gap for WhatsApp.)
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_numbers (
    id                   VARCHAR,
    phone_number         VARCHAR,
    label                VARCHAR,
    display_name         VARCHAR,
    status               VARCHAR,        -- ready | restricted | banned | inactive | warming
    quality_rating       VARCHAR,        -- GREEN | YELLOW | RED | UNKNOWN | NULL
    messaging_limit      VARCHAR,
    daily_send_limit     BIGINT,
    warmup_day           INTEGER,
    country_code         VARCHAR,
    workspace_id         VARCHAR,
    provider_app_id      VARCHAR,
    business_manager_id  VARCHAR,
    last_health_sync_at  TIMESTAMPTZ,
    created_at           TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ,
    _loaded_at           TIMESTAMPTZ NOT NULL,
    _run_id              VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_num_id ON raw_iskra_numbers (id);

-- raw_iskra_numbers_snapshot — the aggregate health roll-up. APPEND per run (time series).
CREATE TABLE IF NOT EXISTS raw_iskra_numbers_snapshot (
    captured_at      TIMESTAMPTZ,
    total            BIGINT,
    n_banned         BIGINT,
    n_restricted     BIGINT,
    n_inactive       BIGINT,
    n_warming        BIGINT,
    n_ready          BIGINT,
    q_green          BIGINT,
    q_yellow         BIGINT,
    q_red            BIGINT,
    q_unknown        BIGINT,
    total_daily_cap  BIGINT,
    raw_json         VARCHAR,         -- full payload (by_status/by_quality keys may evolve)
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
);

-- =====================================================================
-- raw_iskra_stats — the agency funnel (sent/delivered/replies/opportunities/meetings/deals_won)
-- for a window. This is the RECONCILIATION source-of-truth row (like Sendivo /delivery-metrics).
-- APPEND per (window, run); v_whatsapp_performance picks the latest run per window.
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_iskra_stats (
    channel             VARCHAR,
    window_from         DATE,
    window_to           DATE,
    messages_sent       BIGINT,
    messages_delivered  BIGINT,
    delivery_rate       DOUBLE,
    replies             BIGINT,
    reply_rate          DOUBLE,
    opportunities       BIGINT,        -- Iskra's OWN field (≈ any inbound). NOT a Renaissance opp.
    meetings_booked     BIGINT,
    deals_won           BIGINT,
    captured_at         TIMESTAMPTZ,
    raw_json            VARCHAR,
    _loaded_at          TIMESTAMPTZ NOT NULL,
    _run_id             VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_iskra_stats_win ON raw_iskra_stats (window_from, window_to);

-- =====================================================================
-- v_whatsapp_performance — the agency WhatsApp funnel tile. Latest snapshot per window.
-- Mirrors v_sms_performance (DDL 25). Facts = Iskra stats/summary exactly.
-- =====================================================================
CREATE OR REPLACE VIEW v_whatsapp_performance AS
WITH latest AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY channel, window_from, window_to ORDER BY captured_at DESC) AS rn
  FROM raw_iskra_stats
)
SELECT channel, window_from, window_to,
       messages_sent, messages_delivered, delivery_rate,
       replies, reply_rate,
       opportunities,        -- Iskra "any-inbound" opp; see header caveat
       meetings_booked, deals_won, captured_at
FROM latest WHERE rn = 1;

-- v_whatsapp_number_health — latest health row per number (asset inventory).
CREATE OR REPLACE VIEW v_whatsapp_number_health AS
WITH latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _loaded_at DESC) AS rn
  FROM raw_iskra_numbers
)
SELECT id, phone_number, label, display_name, status, quality_rating,
       messaging_limit, daily_send_limit, warmup_day, country_code,
       last_health_sync_at, updated_at
FROM latest WHERE rn = 1;

-- =====================================================================
-- v_whatsapp_conversation_performance — the ATTRIBUTION GRAIN. Per conversation: outbound/inbound
-- message counts (from the upserted message table — PK id already dedups, no run-rank needed),
-- joined to the meeting/sentiment tag and the conversation snapshot. Mirrors the structure of
-- v_sms_campaign_performance (DDL 34) at the conversation grain.
--
-- is_positive_reply / meeting_booked here are the POSITIVE-INTENT flags that gate the Phase-2
-- WhatsApp-opp -> Close CRM push (NOT stats.opportunities). pipeline_id is the only campaign-ish
-- dimension Iskra exposes (no blast/campaign object — Open Question 1 for Arseny).
-- =====================================================================
CREATE OR REPLACE VIEW v_whatsapp_conversation_performance AS
WITH msg AS (
  SELECT conversation_id,
         count(*) FILTER (WHERE direction = 'outbound') AS outbound_msgs,
         count(*) FILTER (WHERE direction = 'inbound')  AS inbound_msgs,
         min(created_at)                                AS first_message_at,
         max(created_at)                                AS last_message_at,
         max(created_at) FILTER (WHERE direction = 'inbound') AS last_inbound_at,
         min(contact_phone)                             AS contact_phone
  FROM raw_iskra_messages
  WHERE conversation_id IS NOT NULL
  GROUP BY conversation_id
)
SELECT
  COALESCE(m.conversation_id, c.id)                     AS conversation_id,
  COALESCE(c.contact_phone, m.contact_phone)            AS contact_phone,
  c.contact_name,
  c.pipeline_id,
  m.outbound_msgs,
  m.inbound_msgs,
  (COALESCE(m.inbound_msgs, 0) > 0)                     AS replied,
  m.first_message_at,
  m.last_message_at,
  m.last_inbound_at,
  c.last_message_at                                     AS conv_last_message_at,
  mt.reply_sentiment,
  mt.meeting_status,
  mt.deal_outcome,
  mt.summary,
  mt.tagged_at,
  (mt.reply_sentiment = 'positive')                     AS is_positive_reply,
  (mt.meeting_status = 'booked')                        AS meeting_booked
FROM msg m
FULL OUTER JOIN raw_iskra_conversations c ON m.conversation_id = c.id
LEFT JOIN raw_iskra_meetings mt
  ON mt.conversation_id = COALESCE(m.conversation_id, c.id);
