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
--
-- Implementation notes (addresses two-key reviewer on PR #110):
--   * "Latest outbound at/before T" is picked with a standard row_number() window (NOT ASOF JOIN),
--     deterministic via (sent_at DESC, sendivo_log_id DESC). Verified read-only 2026-06-29 to give the
--     identical result as the ASOF form AND an independent Python re-implementation:
--     197 go-forward booked phones -> 114 attributed (90 first_reply, 24 last_outbound).
--   * raw_sendivo_blast_deal (DDL 1034) supplies contact_phone10 / origin_blast_id / origin_blast_name /
--     last_touch_blast_id; it is DE-DUPed to one row per phone here so the LEFT JOIN can never fan-out
--     booking rows when the vendor export eventually populates it.
--   * attributed_blast_id and _name are taken from the SAME source via aligned CASE (never mixed).
-- Output columns unchanged from DDL 1034 (v_sms_blast_performance depends on them).
--
-- @gate: add
-- Depends on 1034 1050
CREATE OR REPLACE VIEW core.v_sms_booking_attribution AS
WITH bk AS (
  SELECT meeting_id, email, phone10, program, sendivo_sub_account, booking_ts
  FROM core.v_sms_booking_phone
),
-- vendor export de-duped to one row per phone (empty today; future-proofs the LEFT JOIN against fan-out)
vd AS (
  SELECT contact_phone10, origin_blast_id, origin_blast_name, last_touch_blast_id
  FROM (
    SELECT contact_phone10, origin_blast_id, origin_blast_name, last_touch_blast_id,
           row_number() OVER (PARTITION BY contact_phone10 ORDER BY _loaded_at DESC) AS rn
    FROM raw_sendivo_blast_deal
  ) WHERE rn = 1
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
-- latest outbound blast at/before the FIRST reply (our KPI)
origin AS (
  SELECT meeting_id, blast_id, blast_name FROM (
    SELECT fr.meeting_id, o.blast_id, o.blast_name,
           row_number() OVER (PARTITION BY fr.meeting_id ORDER BY o.sent_at DESC, o.sendivo_log_id DESC) AS rn
    FROM fr
    JOIN v_sendivo_outbound_blast o
      ON o.phone10 = fr.phone10 AND o.sent_at <= fr.first_reply_at
  ) WHERE rn = 1
),
-- latest outbound blast at/before the LAST reply (reconcile vs Sendivo deals_won)
lastblast AS (
  SELECT meeting_id, blast_id FROM (
    SELECT lr.meeting_id, o.blast_id,
           row_number() OVER (PARTITION BY lr.meeting_id ORDER BY o.sent_at DESC, o.sendivo_log_id DESC) AS rn
    FROM lr
    JOIN v_sendivo_outbound_blast o
      ON o.phone10 = lr.phone10 AND o.sent_at <= lr.last_reply_at
  ) WHERE rn = 1
),
-- fallback: latest outbound blast at/before booking (no inbound reply captured)
lob AS (
  SELECT meeting_id, blast_id, blast_name FROM (
    SELECT b.meeting_id, o.blast_id, o.blast_name,
           row_number() OVER (PARTITION BY b.meeting_id ORDER BY o.sent_at DESC, o.sendivo_log_id DESC) AS rn
    FROM bk b
    JOIN v_sendivo_outbound_blast o
      ON o.phone10 = b.phone10 AND o.sent_at <= b.booking_ts
    WHERE b.phone10 IS NOT NULL
  ) WHERE rn = 1
)
SELECT
  b.meeting_id,
  b.email,
  b.phone10,
  b.program,
  b.sendivo_sub_account,
  b.booking_ts,
  CASE
    WHEN vd.origin_blast_id IS NOT NULL THEN vd.origin_blast_id
    WHEN origin.blast_id    IS NOT NULL THEN origin.blast_id
    WHEN lob.blast_id       IS NOT NULL THEN lob.blast_id
  END                                                       AS attributed_blast_id,
  CASE
    WHEN vd.origin_blast_id IS NOT NULL THEN vd.origin_blast_name
    WHEN origin.blast_id    IS NOT NULL THEN origin.blast_name
    WHEN lob.blast_id       IS NOT NULL THEN lob.blast_name
  END                                                       AS attributed_blast_name,
  COALESCE(vd.last_touch_blast_id, lastblast.blast_id)      AS sendivo_last_reply_blast_id,
  CASE
    WHEN b.phone10 IS NULL               THEN 'unattributed_no_phone'
    WHEN vd.origin_blast_id IS NOT NULL  THEN 'vendor_export'
    WHEN origin.blast_id IS NOT NULL     THEN 'first_reply'
    WHEN lob.blast_id IS NOT NULL        THEN 'last_outbound'
    ELSE 'unattributed_no_blast'
  END                                                       AS attribution_method
FROM bk b
LEFT JOIN vd        ON vd.contact_phone10  = b.phone10
LEFT JOIN origin    ON origin.meeting_id   = b.meeting_id
LEFT JOIN lastblast ON lastblast.meeting_id = b.meeting_id
LEFT JOIN lob       ON lob.meeting_id      = b.meeting_id;
