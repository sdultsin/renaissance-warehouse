-- raw_sendivo_outbound_message — per-message Sendivo outbound carrying blast_id [2026-06-29]
--
-- The booking->blast attribution KPI ("which script books leads") needs (phone10 x blast_id) at the
-- MESSAGE grain. /sms/logs exposes blast_id per message from 2026-06-26 (Larry shipped it then), and
-- the nightly already captures it into the comms Supabase (comms.sendivo_outbound_recovered, via
-- entities/sendivo_logs.py with SENDIVO_BODY_CAPTURE=1). This table mirrors the BLAST-CARRYING subset
-- into the warehouse so core.v_sms_booking_attribution can derive the originating (first-reply) blast
-- ourselves -- WITHOUT Sendivo's unreliable last-reply deals_won and WITHOUT a manual vendor export.
--
-- Fed by entities/sendivo_outbound_blast_mirror.py (comms_mirror phase, single-writer-safe,
-- incremental by sent_at, append-only raw layer + the de-dup view below).
--
-- @gate: add
-- Depends on 52
CREATE TABLE IF NOT EXISTS raw_sendivo_outbound_message (
    sendivo_log_id   VARCHAR,
    phone10          VARCHAR,
    blast_id         BIGINT,
    blast_name       VARCHAR,
    campaign_name    VARCHAR,
    sub_account_name VARCHAR,
    sent_at          TIMESTAMPTZ,
    _loaded_at       TIMESTAMPTZ NOT NULL,
    _run_id          VARCHAR
);

-- De-dup for queries: the latest row per sendivo_log_id (the append-only raw layer can carry overlap
-- re-pulls). Only blast-carrying rows are exposed (the attribution-relevant set).
CREATE OR REPLACE VIEW v_sendivo_outbound_blast AS
SELECT sendivo_log_id, phone10, blast_id, blast_name, campaign_name, sub_account_name, sent_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY sendivo_log_id ORDER BY _loaded_at DESC) AS rn
  FROM raw_sendivo_outbound_message
  WHERE blast_id IS NOT NULL
) WHERE rn = 1;
