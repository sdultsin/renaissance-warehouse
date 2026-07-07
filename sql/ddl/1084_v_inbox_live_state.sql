-- 1084_v_inbox_live_state.sql  [2026-07-07]
-- ONE canonical live-state per inbox — the single source of truth for "how many inboxes
-- are live / active / broken / gone", so every surface (portal, daily report, inbox hub,
-- dashboards) reads the SAME number instead of each inventing its own filter.
--
-- WHY THIS EXISTS
--   The raw core.sending_account.status column is unreliable for counting: when an inbox
--   DEPARTS the Instantly census, the generator carries its LAST status forward
--   (entities/sending_account.py line ~94: status = COALESCE(c.conn_state, h.status)), so
--   ~551k long-departed inboxes still carry a legacy status='active' and phantom-inflate any
--   `WHERE status='active'` count (portal showed ~551k "active" when the live fleet is ~352k).
--   This view derives state from GROUND TRUTH instead: presence in the latest Instantly
--   census (= the live /accounts poll, cross-checked 2026-07-07: active=352,277 vs poll
--   352,278) + the disambiguated connection axis (conn_state) + a grace window.
--
-- THE STATE MODEL (mutually exclusive, one row per inbox that has ever existed)
--   active              present in latest census AND conn_active   (healthy, sending now)
--   paused              present AND conn_paused
--   disconnected        present AND connection_error / sending_error (still in Instantly, may reconnect)
--   pending_retirement  ABSENT from latest census but <= GRACE_DAYS since last seen — held,
--                       NOT retired. Covers provider delete-then-re-add cycles (an inbox
--                       removed today and re-added tomorrow is never wrongly retired; it
--                       re-appears in the census and flips back to live, matched by email).
--   retired             absent > GRACE_DAYS — genuinely gone; history + tags preserved.
--                       Auto-un-retires if the email ever reappears in the census.
--
--   GRACE_DAYS = 7 (one week; comfortably longer than any provider re-add cycle).
--   "live" (present in Instantly right now) = active + paused + disconnected.
--
-- ADDITIVE: new views; read existing objects; drop nothing.
-- @gate: add
-- Depends on 105
CREATE OR REPLACE VIEW core.v_inbox_live_state AS
WITH latest AS (SELECT max(census_date) AS d FROM core.account_census),
last_present AS (                                    -- last census day each email appeared
  SELECT lower(email) AS email_lc, max(census_date) AS last_census_date
  FROM core.account_census GROUP BY 1
),
cur AS (                                             -- latest-census connection axis
  SELECT lower(email) AS email_lc, conn_state FROM core.v_account_census_state
)
SELECT
  sa.email,
  sa.workspace_slug,
  (c.email_lc IS NOT NULL)                                         AS is_live,   -- present in Instantly now
  CASE WHEN c.email_lc IS NOT NULL THEN 0
       ELSE (SELECT d FROM latest) - lp.last_census_date END       AS days_absent,
  CASE
    WHEN c.email_lc IS NOT NULL THEN
      CASE
        WHEN c.conn_state = 'conn_active'                      THEN 'active'
        WHEN c.conn_state = 'conn_paused'                      THEN 'paused'
        WHEN c.conn_state IN ('connection_error','sending_error') THEN 'disconnected'
        ELSE 'active'                                    -- live but unlabelled conn_state
      END
    WHEN (SELECT d FROM latest) - lp.last_census_date <= 7        THEN 'pending_retirement'
    ELSE 'retired'
  END                                                              AS live_state,
  lp.last_census_date,
  (SELECT d FROM latest)                                           AS latest_census_date
FROM core.sending_account sa
LEFT JOIN cur c          ON c.email_lc  = lower(sa.email)
LEFT JOIN last_present lp ON lp.email_lc = lower(sa.email);

COMMENT ON VIEW core.v_inbox_live_state IS
  'CANONICAL live-state per inbox (2026-07-07). One row per inbox ever seen. live_state in '
  '(active|paused|disconnected|pending_retirement|retired), derived from latest Instantly '
  'census presence + conn_state + a 7-day grace window (NOT the phantom-prone raw status '
  'column). "live" = present in Instantly = active+paused+disconnected. Every inbox count '
  'should read this view so all surfaces agree. Cross-checked to the live /accounts poll.';

-- Official fleet counts — the ONE rollup every dashboard/report should read.
CREATE OR REPLACE VIEW core.v_inbox_state_summary AS
SELECT
  live_state,
  count(*)                                              AS inboxes,
  count(*) FILTER (WHERE is_live)                       AS present_in_instantly
FROM core.v_inbox_live_state
GROUP BY 1
ORDER BY inboxes DESC;

COMMENT ON VIEW core.v_inbox_state_summary IS
  'Official fleet counts by live_state (reads core.v_inbox_live_state). active = healthy & '
  'sending; active+paused+disconnected = present in Instantly (the true live fleet). '
  'Use THIS instead of ad-hoc status=''active'' counts.';
