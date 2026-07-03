-- =====================================================================
-- SMS + WHATSAPP REPLY-TIME SLA  (Version 1077 — the §6 channel siblings of DDL 1070)
-- =====================================================================
-- @gate: none (new raw mirror + new fact + views; nothing existing is altered/dropped)
-- Depends on 1070 (grammar/clock), 52 (comms outbound-mirror precedent), 82 (iskra), spec-14 (raw_sendivo_inbound)
--
-- WHAT THIS ADDS (ITEM-3, SMS/WA program 2026-07-03 — Sam's decided clock):
--   The email §6 reply-time metric (DDL 1070 / core.sla_reply_time) gets its SMS and
--   WhatsApp siblings: per-desk first-reply -> first-response latency on the SAME
--   12:00-20:00 ET Mon-Fri business-minute clock, with the RAW wall-clock median
--   carried alongside (Sam 2026-07-03: SMS/WA response norms are faster than email
--   and answering runs ~24/7, so the clamped median alone can read 0.0 — both are shipped).
--
-- SOURCES / HONESTY BOUNDARIES (measured 2026-07-03, ITEM3-SLA coverage audit):
--   * SMS inbound  = raw_sendivo_inbound (webhook_receipt-parsed; COMPLETE — the worker's
--     comms.message drops suppressed STOP inbounds, the webhook mirror drops nothing).
--     Population = first NON-OPT-OUT inbound per (sub_account, phone10). STOP-class
--     inbounds get no response BY DESIGN (suppression), so they owe no SLA pair.
--   * SMS outbound = comms.sendivo_outbound_recovered mirrored here as
--     raw_sendivo_outbound_recovered (NEW, below). Reply-type sends are blast_id IS NULL
--     (blast steps carry blast_id; verified: 1,087/1,087 known AIM sends 06-29..30 appear
--     as blast_id-NULL rows; 97.6% of 253 SMS bookings 06-28..07-01 show a reply-type send).
--     CAPTURE ERA: reply-type capture jumped 4-10x on 2026-06-27 (R1 175->580+/day,
--     R2 26->491+/day) — BEFORE 06-27 reply capture is partial (the old ~22-24% era), so
--     the fact FLOORS the SMS population at first-reply >= 2026-06-27 (100%-or-wipe).
--     One pairing for ALL THREE desks: R3's AIM sends and R1/R2's manual sends both land
--     in recovered, so the metric stays valid as AIM expands across desks.
--   * WhatsApp     = raw_iskra_messages (both directions, integrity-anchored ingest;
--     hard-fails on truncation). Population = first inbound per conversation_id; response
--     = first outbound after it (recent Iskra rows carry no campaign/template ids, but
--     hand-checked timelines show first-outbound-after is a genuine contextual reply).
--   * SIM-PROOF: sendivo webhooks / Sendivo logs / Iskra API only carry REAL traffic —
--     AIM simulator rows (comms.message.is_simulation) never enter these sources.
--
-- FRESHNESS (structural, for consumers): the recovered outbound lands on the worker at
--   08:15/13:30 UTC and mirrors into the warehouse at the NEXT nightly (03:45Z), so at
--   render time the SMS response side is ~D-2 (the two newest clock-open days read as
--   unanswered and back-fill). Iskra is nightly (~D-1). The renderer's weekly window
--   treats this exactly like email's SYNC-7 drain (understated-days WARN, threshold 3).
--
-- GRAIN: SMS = (sub_account_name, phone10) — NOT brand: one desk conversation can span
--   multiple brands/sender-numbers of the same sub-account (hand-check case 6195086649:
--   inbound to FUNDING4U answered via FUNDING4DOCTORS). WA = Iskra conversation_id.
--   Unanswered firsts are KEPT (NULL latencies) so answered-rate is derivable — for SMS/WA
--   that rate is LOAD-BEARING (measured ~23-35% SMS, ~12% WA: most non-opt-out replies are
--   hostile/junk that the desk/AIM correctly declines to answer; a median without the
--   answered-rate would read as "we answer everything fast").

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------
-- 1. RAW MIRROR — comms.sendivo_outbound_recovered with the columns the SLA needs.
--    The existing raw_comms_sendivo_outbound (DDL 52) mirrors a PG view that exposes only
--    phone10/message/sent_at/status_group — no sub_account and no blast discriminator, so
--    reply-type sends can't be told from blast steps there. This mirror reads the base
--    table. Append-only + dedupe view, incremental by recovered_at (discovery time — a
--    sent_at watermark would miss the recovery job's deep back-fills; entity:
--    entities/sendivo_recovered_mirror.py, comms_mirror phase).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_sendivo_outbound_recovered (
  sendivo_log_id   VARCHAR,          -- unique per send in the source
  phone10          VARCHAR,          -- prospect phone, last-10-digit normalized
  sent_at          TIMESTAMPTZ,
  recovered_at     TIMESTAMPTZ,      -- when the worker's recovery job DISCOVERED the row — the
                                     -- incremental watermark (the recovery job anti-join-backfills
                                     -- rows with OLD sent_at, PR #179; sent_at would miss those)
  sub_account_name VARCHAR,          -- Renaissance 1/2/3 (the SMS desk)
  campaign_name    VARCHAR,
  blast_id         BIGINT,           -- NULL = reply-type (conversational) send; NOT NULL = blast step
  message_content  VARCHAR,
  _loaded_at       TIMESTAMPTZ NOT NULL,
  _run_id          VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_sor_phone_sent ON raw_sendivo_outbound_recovered (phone10, sent_at);
CREATE INDEX IF NOT EXISTS ix_sor_sent       ON raw_sendivo_outbound_recovered (sent_at);

-- Dedupe view (overlap re-pulls are idempotent through it): one row per sendivo_log_id.
CREATE OR REPLACE VIEW v_sendivo_outbound_recovered AS
SELECT * FROM raw_sendivo_outbound_recovered
QUALIFY row_number() OVER (PARTITION BY sendivo_log_id ORDER BY _loaded_at DESC) = 1;

-- ---------------------------------------------------------------------
-- 2. RESPONSE-LEVEL FACT — one row per conversation's FIRST qualifying prospect reply.
--    Built by scripts/build_sla_reply_time.py (same run as the email fact, same verbatim
--    §6 clamp + self-check). DROP+CREATE each run, like core.sla_reply_time.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS core.sla_reply_time_smswa;

CREATE TABLE IF NOT EXISTS core.sla_reply_time_smswa (
  response_id         TEXT NOT NULL,   -- sms: 'sms|<sub>|<phone10>' ; wa: 'wa|<conversation_id>'
  channel             TEXT NOT NULL,   -- 'sms' | 'whatsapp'
  desk                TEXT NOT NULL,   -- 'Renaissance 1'|'Renaissance 2'|'Renaissance 3'|'WhatsApp (ISKRA)'
  conversation_key    TEXT,            -- phone10 (sms) / iskra conversation_id (wa)
  prospect_msg_ts     TIMESTAMPTZ,     -- first non-opt-out inbound (sms) / first inbound (wa), full history
  our_reply_ts        TIMESTAMPTZ,     -- first qualifying outbound after it; NULL if (yet) unanswered
  biz_latency_minutes DOUBLE,          -- §6 business-minute clock (12-20 ET Mon-Fri); NULL if unanswered
  raw_latency_minutes DOUBLE,          -- raw wall-clock minutes (Sam: reported alongside); NULL if unanswered
  clock_open_date     DATE,            -- ET date the SLA clock OPENS (the report bucket day)
  _built_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  _run_id             VARCHAR
);
-- @@INDEXES@@
CREATE INDEX IF NOT EXISTS ix_sla_smswa_clockopen ON core.sla_reply_time_smswa (clock_open_date);
CREATE INDEX IF NOT EXISTS ix_sla_smswa_desk      ON core.sla_reply_time_smswa (desk);

-- ---------------------------------------------------------------------
-- 3. ROLLUP MACRO — business-minute + raw stats over an arbitrary clock-open range,
--    recomputed from response-level rows (never average daily percentiles). Includes
--    population + answered-rate (load-bearing for SMS/WA — see header).
-- ---------------------------------------------------------------------
CREATE OR REPLACE MACRO sla_reply_time_smswa_rollup(start_date, end_date) AS TABLE
  SELECT
    channel,
    desk,
    ANY_VALUE(CAST(start_date AS DATE))            AS period_start,
    ANY_VALUE(CAST(end_date   AS DATE))            AS period_end,
    count(*)                                       AS n_first_replies,
    count(biz_latency_minutes)                     AS n_answered,
    round(100.0 * count(biz_latency_minutes) / count(*), 1) AS answered_pct,
    median(biz_latency_minutes)                    AS median_biz_min,
    avg(biz_latency_minutes)                       AS avg_biz_min,
    median(raw_latency_minutes)                    AS median_raw_min,
    quantile_cont(biz_latency_minutes, 0.75)       AS q75_biz_min
  FROM core.sla_reply_time_smswa
  WHERE clock_open_date >= CAST(start_date AS DATE)
    AND clock_open_date <= CAST(end_date   AS DATE)
  GROUP BY channel, desk;
