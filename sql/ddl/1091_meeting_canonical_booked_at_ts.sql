-- @gate: add
-- Depends on 107
-- ============================================================================
-- 1091: core.v_meeting_canonical — ADD booked_at_ts (real intra-day booking timestamp).
--
-- WHY (warehouse-flags #25): `posted_at` is midnight-stamped (00:00:00 UTC) on 2,225/2,253
-- (98.8%) portal-era rows (source='sheet', posted_at >= 2026-06-29), so any intra-day
-- ordering / reply->booking cohort join silently breaks (a same-day booking sorts BEFORE
-- the reply that produced it — this manufactured a fake "conversion collapse at Jun-29"
-- in a WL analysis until caught). Real timestamps EXIST in raw_im_bookings.created_at.
--
-- FIX (ADDITIVE ONLY — no existing column's name, order, or semantics changes):
-- append `booked_at_ts` = COALESCE(TRY_CAST(imb.created_at AS TIMESTAMPTZ), posted_at),
-- via a LEFT JOIN to raw_im_bookings deduped to the latest _snapshot_date per id.
--   - Join key: v_meeting_canonical.source_event_id = CAST(raw_im_bookings.id AS VARCHAR).
--     NOT raw_im_bookings.booking_id — that column is a decoy (NULL/empty on ~90% of rows).
--   - Fallback posted_at covers slack-era rows + the 28 non-portal sheet rows (verified:
--     the non-joining sheet set == exactly the set that already has real posted_at times).
--   - No fan-out: ROW_NUMBER dedup to rn=1 gives <=1 imb row per id; verified row count
--     unchanged (37,954) on snapshot warehouse_20260709_143726_149.duckdb.
--   - Cross-source collision checked on the same snapshot: 0/30,410 slack rows join.
--
-- SEMANTICS of booked_at_ts: the portal ROW-ENTRY time (raw_im_bookings.created_at) —
-- always on/after the nominal booking day. Measured skew vs posted_at: same-day 1,477,
-- +1d 551, +3d 163, +4d 34 (the +3/+4d cluster = July-4-weekend backlog entered late).
-- Right for time-of-day, intra-day ordering, and reply->booking cohort joins; NOT a
-- reason to re-date posted_at's nominal day. `meeting_date` stays the canonical day-bucket.
--
-- Verified on snapshot warehouse_20260709_143726_149.duckdb: 2,253/2,253 portal-era rows
-- get a non-midnight booked_at_ts, 0 NULLs, booked_at_ts >= posted_at on every row.
-- READ-ONLY on core.meeting / raw_im_bookings. View replace only — no table change.
-- ============================================================================

CREATE OR REPLACE VIEW core.v_meeting_canonical AS
SELECT
  m.meeting_id, m.source, m.source_event_id,
  m.meeting_date,                       -- CANONICAL (col A)
  m.posted_at, m.submission_ts,         -- audit (col C)
  m.channel, m.partner, m.partner_key,
  m.campaign_id, m.campaign_name_raw, m.match_method, m.match_confidence,
  m.cm                AS cm_raw,         -- raw sheet col-17 (audit; Grace types IDO on non-CM work)
  m.cm_workspace      AS cm,             -- WORKSPACE-credited CM (D6) — NULL for non-funding-CM workspaces
  m.workspace_name, m.workspace_slug, m.workspace_canonical,
  m.offer, m.program, m.sendivo_sub_account,
  CASE
    WHEN m.workspace_canonical = 'Section 125' THEN 'Section 125 (frozen)'   -- D2 frozen-historical
    WHEN m.workspace_slug IS NULL OR m.workspace_canonical IS NULL THEN '(unmapped)'
    ELSE m.workspace_canonical                                               -- e.g. 'Funding 2 (Ido)','Warm Leads'
  END AS reporting_segment,
  m.lead_email, m.advisor, m.advisor_name, m.advisor_partner, m.inbox_manager,
  -- booked_at_ts: real intra-day booking timestamp (portal row-entry time from
  -- raw_im_bookings.created_at; falls back to posted_at where no portal booking joins).
  -- +1..+4d late-entry tail exists (see header) — use for time-of-day/ordering/cohorts,
  -- never as a replacement for meeting_date's day-bucket. (warehouse-flags #25)
  COALESCE(TRY_CAST(imb.created_at AS TIMESTAMPTZ), m.posted_at) AS booked_at_ts
FROM core.meeting m
LEFT JOIN (
  SELECT id, created_at,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY _snapshot_date DESC) AS rn
  FROM raw_im_bookings
) imb
  ON imb.rn = 1 AND m.source_event_id = CAST(imb.id AS VARCHAR);
