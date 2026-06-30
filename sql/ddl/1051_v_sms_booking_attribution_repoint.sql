-- v_sms_booking_attribution -- repoint to warehouse-native first-reply blast attribution [2026-06-29]
--
-- DDL 1034 wired this view to raw_sendivo_blast_deal (the MANUAL vendor export), which is empty -> every
-- booking came back 'unattributed_no_deal_export'. The per-message blast data now lives in the warehouse
-- (raw_sendivo_outbound_message / v_sendivo_outbound_blast, DDL 1050), so derive the ORIGINATING
-- (first-reply) blast ourselves: for each SMS booking, the blast of the most-recent outbound at/before
-- the booker's FIRST inbound reply -- the real "which script booked them" KPI, which Sendivo's last-reply
-- deals_won cannot answer. Attribution priority (each row LABELED, nothing silently dropped):
--   1. vendor export (raw_sendivo_blast_deal.origin_blast_id), when/if Larry's richer export lands -> 'vendor_export'
--   2. first-reply blast (this build)                                                               -> 'first_reply'
--   3. last outbound blast at/before booking (no inbound reply captured)                            -> 'last_outbound'
--   else                                                                                            -> 'unattributed_no_blast' / 'unattributed_no_phone'
-- sendivo_last_reply_blast_id = blast of the outbound at/before the LAST reply <= booking, to reconcile
-- against Sendivo's last-reply deals_won. Coverage is go-forward (>= 2026-06-26, where blast_id exists);
-- pre-06-26 attribution closes automatically when Larry backfills historical blast_id.
-- Validated read-only 2026-06-29: 197 go-forward booked phones -> 114 attributed (90 first_reply, 24 last_outbound).
-- Output columns unchanged from DDL 1034 (v_sms_blast_performance depends on them).
--
-- @gate: add
-- Depends on 1034 1050
CREATE OR REPLACE VIEW core.v_sms_booking_attribution AS
WITH bk AS (
  SELECT meeting_id, email, phone10, program, sendivo_sub_account, booking_ts
  FROM core.v_sms_booking_phone
),
-- first inbound reply at/before booking (per booked phone)
fr AS (
  SELECT b.meeting_id, b.phone10, min(i.received_at) AS first_reply_at
  FROM bk b
  JOIN main.raw_sendivo_inbound i
    ON right(regexp_replace(i.prospect_number, '[^0-9]', '', 'g'), 10) = b.phone10
   AND i.received_at <= b.booking_ts
  WHERE b.phone10 IS NOT NULL
  GROUP BY 1, 2
),
-- last inbound reply at/before booking (for the Sendivo-style reconciliation)
lr AS (
  SELECT b.meeting_id, b.phone10, max(i.received_at) AS last_reply_at
  FROM bk b
  JOIN main.raw_sendivo_inbound i
    ON right(regexp_replace(i.prospect_number, '[^0-9]', '', 'g'), 10) = b.phone10
   AND i.received_at <= b.booking_ts
  WHERE b.phone10 IS NOT NULL
  GROUP BY 1, 2
),
-- blast of the latest outbound at/before the FIRST reply (our KPI)
origin AS (
  SELECT fr.meeting_id, o.blast_id, o.blast_name
  FROM fr
  ASOF JOIN v_sendivo_outbound_blast o
    ON o.phone10 = fr.phone10 AND fr.first_reply_at >= o.sent_at
),
-- blast of the latest outbound at/before the LAST reply (reconcile vs Sendivo deals_won)
lastblast AS (
  SELECT lr.meeting_id, o.blast_id
  FROM lr
  ASOF JOIN v_sendivo_outbound_blast o
    ON o.phone10 = lr.phone10 AND lr.last_reply_at >= o.sent_at
),
-- fallback: blast of the latest outbound at/before booking (no reply captured)
lob AS (
  SELECT b.meeting_id, o.blast_id, o.blast_name
  FROM bk b
  ASOF JOIN v_sendivo_outbound_blast o
    ON o.phone10 = b.phone10 AND b.booking_ts >= o.sent_at
  WHERE b.phone10 IS NOT NULL
)
SELECT
  b.meeting_id,
  b.email,
  b.phone10,
  b.program,
  b.sendivo_sub_account,
  b.booking_ts,
  COALESCE(vd.origin_blast_id, origin.blast_id, lob.blast_id)        AS attributed_blast_id,
  COALESCE(vd.origin_blast_name, origin.blast_name, lob.blast_name)  AS attributed_blast_name,
  COALESCE(vd.last_touch_blast_id, lastblast.blast_id)               AS sendivo_last_reply_blast_id,
  CASE
    WHEN b.phone10 IS NULL               THEN 'unattributed_no_phone'
    WHEN vd.origin_blast_id IS NOT NULL  THEN 'vendor_export'
    WHEN origin.blast_id IS NOT NULL     THEN 'first_reply'
    WHEN lob.blast_id IS NOT NULL        THEN 'last_outbound'
    ELSE 'unattributed_no_blast'
  END                                                                AS attribution_method
FROM bk b
LEFT JOIN raw_sendivo_blast_deal vd ON vd.contact_phone10 = b.phone10
LEFT JOIN origin    ON origin.meeting_id    = b.meeting_id
LEFT JOIN lastblast ON lastblast.meeting_id = b.meeting_id
LEFT JOIN lob       ON lob.meeting_id       = b.meeting_id;
