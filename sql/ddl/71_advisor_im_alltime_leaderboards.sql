-- Version 71 (2026-06-14) — ALL-TIME Advisor + Inbox-Manager leaderboard views.
-- Applied via apply_ddl_file(conn, <this file>, version=71) in a post-cutover idle writer
-- window (outside 03:30-05:45 UTC) under the warehouse write lock. Idempotent
-- CREATE OR REPLACE; no table writes, no backfill — pure projection over existing data.
-- Live schema_version was 70 immediately before this file.
--
-- WHY (Sam, 2026-06-14): the Advisor and Inbox-Manager (IM) leaderboards in core.meeting
-- only carry the SHEET era (>= 2026-06-01, ~2.5k rows) — DDL 70 backfilled advisor/IM from
-- the Funding-Form sheet, but the Slack-era rows (< 2026-06-01) never had those dims. The
-- pre-Jun-1 advisor/IM HISTORY lives only in the legacy bookings-portal table
-- (raw_im_bookings). These views UNION the two eras so the leaderboards are ALL-TIME, not
-- just the ~2.5k sheet rows. Same fact-driven principle as the CM-leaderboard fix
-- (scripts/portal_data.py §CM leaderboard logic).
--
-- SOURCES (no re-ingestion):
--   * Pre-Jun-1  -> raw_im_bookings, the FROZEN darcy analysis snapshot
--                   (_snapshot_date = 2026-05-31, _source='darcy_portal_im_bookings').
--                   36,806 rows, all parseable dates, ALL < 2026-06-01 (range
--                   2024-08-19 .. 2026-05-02), advisor 96.9% / inbox_manager 87.1%.
--                   CHOSEN over the live nightly snapshot (2026-06-12) because the nightly
--                   pull carries 23 dirty/typo dates (e.g. '0202-...', '2027-...') and
--                   bleeds past the cutover; the frozen darcy snapshot is clean, fully
--                   pre-cutover, and is the genuine "frozen ~Jun-2" history.
--   * Jun-1+     -> core.meeting source='sheet' (the DDL 70 advisor/IM backfill).
--
-- DEDUP AT THE JUN-1 SEAM: the two source slices are DISJOINT by construction —
--   legacy slice  = WHERE TRY_CAST(date AS DATE) <  2026-06-01   (max observed = 2026-05-02)
--   sheet slice   = source='sheet' (posted_at >= 2026-06-01 by meeting.py's own cutover)
-- so the boundary cannot double-count. This is byte-for-byte the same cutover boundary
-- entities/meeting.py uses (CUTOVER='2026-06-01', '<' vs '>='). The gap-verification
-- proved the eras are the same underlying bookings with no overlap; legacy OWNS pre-Jun-1,
-- the sheet OWNS Jun-1+. (Slack-era core.meeting rows carry NO advisor/IM and therefore
-- contribute nothing here — correct: their advisor/IM history is exactly what raw_im_bookings
-- supplies.)
--
-- NORMALIZATION (matches the DDL 70 / entities/meeting.py backfill):
--   * ADVISOR. In raw_im_bookings `advisor` is already a BARE full name ("Jake Goldstein").
--     In core.meeting the sheet stores "<PARTNER_PREFIX>: <Full Name>" and meeting.py split
--     out advisor_name = the name AFTER the prefix. So the comparable, partner-prefix-free
--     name is: legacy.advisor  ==  meeting.advisor_name. We carry advisor_partner too:
--     legacy has no prefix (advisor_partner = NULL / '(unknown)'); sheet carries the mapped
--     partner from meeting.advisor_partner.
--   * INBOX MANAGER. The sheet's first-name-only IM values were resolved to full names by
--     meeting.py (_IM_FIRST_TO_FULL); raw_im_bookings already stores full names. We apply the
--     SAME first->full map to BOTH sides (a no-op on the already-full legacy values, except it
--     keeps any stray first-name spelling unified) so a person never splits across spellings.
--     "Jamie" is intentionally left unresolved on both sides (ambiguous: Jamie Isla vs Jamie
--     Solis) — legacy data only ever spells it "Jamie Isla" (already full), so no collision.
--
-- NOISE EXCLUSION: drop NULL / blank / email-like / obvious-junk advisor & IM values
--   (test / n/a / none / tbd / unknown / instantly / max / placeholders). Measured 2026-06-14:
--   ZERO such junk tokens exist in the frozen pre-Jun-1 slice, so the filter is a safety net,
--   not a cull. (The only "short" advisor values are first-name-only spellings of real
--   advisors, e.g. "Cameron" — those are REAL and intentionally KEPT; only an advisor
--   first->full normalization, which the spec does not ask for, would merge them. Documented.)
--
-- WINDOW-ABLE BY CONSTRUCTION (same principle as the CM/workspace fact-driven views): the
--   base views are at BOOKING GRAIN and carry `booking_date`. An advisor/IM appears in any
--   time window IFF they have a booking fact in it — GROUP/FILTER the base view by date for
--   MTD / last-7 / any range, with no static allowlist. The *_summary views are convenience
--   all-time rollups for the leaderboard tiles.
--
-- IM-NULL COVERAGE CAVEAT: ~12% of legacy bookings have inbox_manager NULL (87.1% populated
--   on the frozen slice; ~88% on the sheet) — those bookings exist as advisor/partner facts
--   but are simply absent from the IM leaderboard. Expected; do not treat as data loss.
--
-- Migration-agnostic standard SQL (must port off single-file DuckDB unchanged).

CREATE SCHEMA IF NOT EXISTS derived;

-- ============================================================================
-- BASE: derived.v_advisor_alltime — one row per booking, advisor + booking_date.
--   Window-able: filter by booking_date for any time cut; an advisor appears in a
--   window iff they have a booking fact in it.
-- ============================================================================
CREATE OR REPLACE VIEW derived.v_advisor_alltime AS
WITH legacy AS (  -- pre-Jun-1: frozen darcy snapshot, disjoint < cutover
  SELECT
    'im_bookings_legacy'              AS source,
    TRY_CAST(date AS DATE)            AS booking_date,
    trim(advisor)                     AS advisor_name,
    CAST(NULL AS VARCHAR)             AS advisor_partner   -- legacy carries no partner prefix
  FROM raw_im_bookings
  WHERE _snapshot_date = DATE '2026-05-31'
    AND _source = 'darcy_portal_im_bookings'
    AND TRY_CAST(date AS DATE) IS NOT NULL
    AND TRY_CAST(date AS DATE) < DATE '2026-06-01'
    AND advisor IS NOT NULL
    AND trim(advisor) <> ''
    AND advisor NOT LIKE '%@%'
    AND lower(trim(advisor)) NOT IN
        ('test','n/a','na','none','tbd','unknown','null','-','.','x','xxx','instantly','max')
),
sheet AS (  -- Jun-1+: the DDL 70 sheet backfill (advisor_name = name after the prefix)
  SELECT
    'sheet'                           AS source,
    CAST(posted_at AS DATE)           AS booking_date,
    trim(advisor_name)                AS advisor_name,
    advisor_partner                   AS advisor_partner
  FROM core.meeting
  WHERE source = 'sheet'
    AND advisor_name IS NOT NULL
    AND trim(advisor_name) <> ''
    AND advisor_name NOT LIKE '%@%'
    AND lower(trim(advisor_name)) NOT IN
        ('test','n/a','na','none','tbd','unknown','null','-','.','x','xxx','instantly','max')
)
SELECT source, booking_date, advisor_name,
       COALESCE(advisor_partner, '(unknown)') AS advisor_partner
FROM legacy
UNION ALL
SELECT source, booking_date, advisor_name,
       COALESCE(advisor_partner, '(unknown)') AS advisor_partner
FROM sheet;

-- Convenience all-time rollup (the leaderboard tile). Window cuts read the base view above.
CREATE OR REPLACE VIEW derived.v_advisor_alltime_summary AS
SELECT
  advisor_name,
  count(*)                                              AS bookings_all_time,
  count(*) FILTER (WHERE source = 'im_bookings_legacy') AS bookings_pre_jun1,
  count(*) FILTER (WHERE source = 'sheet')              AS bookings_jun1_plus,
  min(booking_date)                                     AS first_booking,
  max(booking_date)                                     AS last_booking,
  any_value(advisor_partner)                            AS advisor_partner_any
FROM derived.v_advisor_alltime
GROUP BY advisor_name
ORDER BY bookings_all_time DESC;

-- ============================================================================
-- BASE: derived.v_inbox_manager_alltime — one row per booking, IM + booking_date.
--   first->full normalization applied to BOTH eras (no-op on already-full legacy).
--   Window-able (filter booking_date). NULL IMs are excluded (the ~12% caveat).
-- ============================================================================
CREATE OR REPLACE VIEW derived.v_inbox_manager_alltime AS
WITH legacy AS (
  SELECT
    'im_bookings_legacy'              AS source,
    TRY_CAST(date AS DATE)            AS booking_date,
    CASE trim(inbox_manager)
      WHEN 'Anjanette' THEN 'Anjanette Manayao'
      WHEN 'April'     THEN 'April Bagahansol'
      WHEN 'Erwell'    THEN 'Erwell Pacot'
      WHEN 'Frank'     THEN 'Frank Intong'
      WHEN 'Jamil'     THEN 'Jamil Matias'
      WHEN 'Jessica'   THEN 'Jessica Dumlao'
      WHEN 'Kenneth'   THEN 'Kenneth Bondoc'
      WHEN 'Larrabel'  THEN 'Larrabel Cardoza'
      WHEN 'Madel'     THEN 'Madel Pantaleon'
      WHEN 'Monique'   THEN 'Monique Andrade'
      WHEN 'Nikko'     THEN 'Nikko Macarandan'
      WHEN 'Norman'    THEN 'Norman Pascua'
      WHEN 'Ramir'     THEN 'Ramir Velasquez'
      WHEN 'Robert'    THEN 'Robert Bat-og'
      WHEN 'William'   THEN 'William Isla'
      ELSE trim(inbox_manager)        -- 'Jamie' deliberately unresolved (ambiguous)
    END                               AS inbox_manager
  FROM raw_im_bookings
  WHERE _snapshot_date = DATE '2026-05-31'
    AND _source = 'darcy_portal_im_bookings'
    AND TRY_CAST(date AS DATE) IS NOT NULL
    AND TRY_CAST(date AS DATE) < DATE '2026-06-01'
    AND inbox_manager IS NOT NULL
    AND trim(inbox_manager) <> ''
    AND inbox_manager NOT LIKE '%@%'
    AND lower(trim(inbox_manager)) NOT IN
        ('test','n/a','na','none','tbd','unknown','null','-','.','x','xxx','instantly','max')
),
sheet AS (
  -- meeting.inbox_manager is ALREADY first->full normalized by entities/meeting.py; re-applying
  -- the same map here is a safe no-op that guarantees both eras share the canonical spelling.
  SELECT
    'sheet'                           AS source,
    CAST(posted_at AS DATE)           AS booking_date,
    CASE trim(inbox_manager)
      WHEN 'Anjanette' THEN 'Anjanette Manayao'
      WHEN 'April'     THEN 'April Bagahansol'
      WHEN 'Erwell'    THEN 'Erwell Pacot'
      WHEN 'Frank'     THEN 'Frank Intong'
      WHEN 'Jamil'     THEN 'Jamil Matias'
      WHEN 'Jessica'   THEN 'Jessica Dumlao'
      WHEN 'Kenneth'   THEN 'Kenneth Bondoc'
      WHEN 'Larrabel'  THEN 'Larrabel Cardoza'
      WHEN 'Madel'     THEN 'Madel Pantaleon'
      WHEN 'Monique'   THEN 'Monique Andrade'
      WHEN 'Nikko'     THEN 'Nikko Macarandan'
      WHEN 'Norman'    THEN 'Norman Pascua'
      WHEN 'Ramir'     THEN 'Ramir Velasquez'
      WHEN 'Robert'    THEN 'Robert Bat-og'
      WHEN 'William'   THEN 'William Isla'
      ELSE trim(inbox_manager)
    END                               AS inbox_manager
  FROM core.meeting
  WHERE source = 'sheet'
    AND inbox_manager IS NOT NULL
    AND trim(inbox_manager) <> ''
    AND inbox_manager NOT LIKE '%@%'
    AND lower(trim(inbox_manager)) NOT IN
        ('test','n/a','na','none','tbd','unknown','null','-','.','x','xxx','instantly','max')
)
SELECT source, booking_date, inbox_manager FROM legacy
UNION ALL
SELECT source, booking_date, inbox_manager FROM sheet;

-- Convenience all-time rollup (the leaderboard tile). Window cuts read the base view above.
CREATE OR REPLACE VIEW derived.v_inbox_manager_alltime_summary AS
SELECT
  inbox_manager,
  count(*)                                              AS bookings_all_time,
  count(*) FILTER (WHERE source = 'im_bookings_legacy') AS bookings_pre_jun1,
  count(*) FILTER (WHERE source = 'sheet')              AS bookings_jun1_plus,
  min(booking_date)                                     AS first_booking,
  max(booking_date)                                     AS last_booking
FROM derived.v_inbox_manager_alltime
GROUP BY inbox_manager
ORDER BY bookings_all_time DESC;
