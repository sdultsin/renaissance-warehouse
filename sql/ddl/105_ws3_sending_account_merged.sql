-- ============================================================================
-- 105_ws3_sending_account_merged.sql
-- @gate: add
-- Depends on 104
-- @gate-note: status RELABEL to conn_* is written into core.sending_account.status by the
--   COMPANION generator entities/sending_account.py (line ~88: status = COALESCE(c.conn_state, h.status));
--   conn_state literals {conn_active,conn_paused,connection_error,sending_error} match every repoint
--   filter below EXACTLY. DDL + generator land in the SAME gated apply; the orchestrator build runs the
--   generator (relabel + rebuild) and only THEN promotes serving — consumers never see the pre-relabel state.
-- WS3 (MERGED) — rebuild core.sending_account as a CENSUS-DERIVED registry,
--   + census status/warmup RELABEL (kill the active/paused axis collision),
--   + census-COMPLETENESS alarm (main.v_census_completeness),
--   + the census_summary surface ASSERT 1 / the generator precondition depend on.
--
-- Snapshot of record (validation): warehouse_20260621_063139_227.duckdb
-- Live MAX(core.schema_version) at draft = 104 (104_account_census.sql). 105 is FREE
--   (SELECT count(*) FROM core.schema_version WHERE version=105  => 0).
-- The moderator re-runs SELECT max(version)+1 immediately before apply and bumps the
--   whole remaining block by any delta if the nightly moved the floor (C1/C7).
--   apply_ddl_file PK-dedupes on version => a TAKEN/duplicate version silently no-ops
--   the WHOLE migration, so the free-slot check above is mandatory.
--
-- Depends on (ALL VERIFIED LIVE this snapshot):
--   core.account_census            (BASE TABLE, 314,887 rows, census_date 2026-06-21, 1 date)
--   core.sending_account_daily     (BASE TABLE; cols date,account_id,workspace_slug,esp,actual_sends,...)
--   core.sending_account           (BASE TABLE, 1,359,514 rows; retired_at TIMESTAMPTZ, is_active BOOL)
--   core.workspace                 (BASE TABLE; slug, workspace_id, name, is_active)
--
-- Binds CONTRACT C2 column names: retired_at (departure), is_active (BOOL). NO departed_on.
-- All statements idempotent. DuckDB has no ADD COLUMN IF NOT EXISTS; the duplicate-column
--   error on the ALTERs is tolerated by the runner (matches the 19_*/31_* ALTER pattern).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Additive provenance columns on core.sending_account
--    (retired_at / is_active / first_cold_send_at ALREADY EXIST — do NOT re-add.
--     warmup_state is NEW: carries the disambiguated WARMUP axis, separate from `status`.)
-- ----------------------------------------------------------------------------
ALTER TABLE core.sending_account ADD COLUMN inventory_source     VARCHAR;  -- 'instantly_census' | 'census_diff_departed' | 'bootstrap_departed'
ALTER TABLE core.sending_account ADD COLUMN census_date_resolved DATE;     -- the census date this row was resolved against
ALTER TABLE core.sending_account ADD COLUMN warmup_state         VARCHAR;  -- disambiguated WARMUP axis: warmup_on | warmup_off | warmup_banned
-- retired_at  : EXISTING (TIMESTAMP WITH TIME ZONE) — NULL = live; stamped = first census it was absent (cast from DATE; implicit promote).
-- is_active   : EXISTING (BOOLEAN) — TRUE iff present in latest census (retired_at IS NULL).
-- first_cold_send_at : EXISTING (TIMESTAMPTZ) — lives HERE; WS4 (v106) backfills; WS3 passes it through.
-- status      : EXISTING (VARCHAR) — WS3 now sources it from the disambiguated CONNECTION axis (see v_account_census_state).
-- last_seen_at: EXISTING — frozen-history last-activity anchor (no duplicate column added).

CREATE INDEX IF NOT EXISTS ix_core_sending_account_retired ON core.sending_account (retired_at);

-- ----------------------------------------------------------------------------
-- 2. CENSUS STATUS/WARMUP RELABEL — kill the VERIFIED label collision.
--    Live census uses the SAME words on TWO axes:
--      status_label   in {active, paused, connection_error, sending_error}   (CONNECTION axis; codes 1/2/-1/-3)
--      warmup_status_label in {active, paused, banned}                       (WARMUP axis;     codes 1/0/-1)
--    e.g. (status_label='active', warmup_status_label='active') 291,526
--         (status_label='active', warmup_status_label='paused') 5,152
--         (status_label='active', warmup_status_label='banned')   572
--    => the word "active"/"paused" is ambiguous unless the axis is named. This view emits
--       DISAMBIGUATED labels so sending_account.status / .warmup_state never carry the collision.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_account_census_state AS
SELECT
    c.*,
    -- CONNECTION axis (was status_label) -> conn_active / conn_paused / connection_error / sending_error
    CASE c.status
         WHEN  1 THEN 'conn_active'
         WHEN  2 THEN 'conn_paused'
         WHEN -1 THEN 'connection_error'
         WHEN -3 THEN 'sending_error'
         ELSE 'conn_' || COALESCE(c.status_label, CAST(c.status AS VARCHAR))
    END AS conn_state,
    -- WARMUP axis (was warmup_status_label) -> warmup_on / warmup_off / warmup_banned
    CASE c.warmup_status
         WHEN  1 THEN 'warmup_on'
         WHEN  0 THEN 'warmup_off'
         WHEN -1 THEN 'warmup_banned'
         ELSE 'warmup_' || COALESCE(c.warmup_status_label, CAST(c.warmup_status AS VARCHAR))
    END AS warmup_state
FROM core.account_census c
WHERE c.census_date = (SELECT max(census_date) FROM core.account_census);

-- ----------------------------------------------------------------------------
-- 3. CENSUS SUMMARY surface — the ground-truth count object ASSERT 1 and the generator
--    precondition both read. (Live check: core.account_census_summary did NOT exist as a
--    table/view this snapshot — only account_census + v_account_census_latest/_current.
--    The v2 generator+ASSERT reference it, so the merged WS3 block CREATES it here as a
--    view, exposing `overall_accounts` per census_date.)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.account_census_summary AS
SELECT
    census_date,
    COUNT(*)                                  AS overall_accounts,
    COUNT(DISTINCT workspace_slug)            AS workspace_count,
    COUNT(DISTINCT lower(email))              AS distinct_emails
FROM core.account_census
GROUP BY census_date;

-- ----------------------------------------------------------------------------
-- 4. CENSUS-COMPLETENESS ALARM — catch a key rotation silently dropping a workspace.
--    Per-workspace latest-census count vs the prior census date's count (when 2+ dates
--    exist) and vs an absolute floor. With only 1 census date today it reports the per-ws
--    counts + flags any workspace below a hard floor (a workspace dropping to ~0 = a missing
--    per-ws key). WS3 ASSERT 4 (safety guard) is the hard gate; this view is the human alarm.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW main.v_census_completeness AS
WITH latest AS (SELECT max(census_date) AS d FROM core.account_census),
prev AS (
    SELECT max(census_date) AS d FROM core.account_census
    WHERE census_date < (SELECT d FROM latest)
),
cur AS (
    SELECT workspace_slug, COUNT(*) AS n_now
    FROM core.account_census WHERE census_date = (SELECT d FROM latest)
    GROUP BY 1
),
pri AS (
    SELECT workspace_slug, COUNT(*) AS n_prev
    FROM core.account_census WHERE census_date = (SELECT d FROM prev)
    GROUP BY 1
)
SELECT
    (SELECT d FROM latest)                                            AS census_date,
    COALESCE(cur.workspace_slug, pri.workspace_slug)                 AS workspace_slug,
    COALESCE(cur.n_now, 0)                                           AS n_now,
    pri.n_prev                                                       AS n_prev,
    COALESCE(cur.n_now,0) - COALESCE(pri.n_prev,0)                   AS delta,
    -- drop alarm: present yesterday, (near-)gone today
    (pri.n_prev IS NOT NULL AND COALESCE(cur.n_now,0) < pri.n_prev * 0.5) AS workspace_dropped,
    -- absolute floor: any account-bearing workspace under 100 today is suspicious (smallest live ws ~1,527)
    (COALESCE(cur.n_now,0) < 100)                                    AS below_floor
FROM cur
FULL OUTER JOIN pri ON pri.workspace_slug = cur.workspace_slug;

-- ----------------------------------------------------------------------------
-- 5. LIVE registry view — THE object live/current dashboards read (≈314K).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_account_live AS
SELECT * FROM core.sending_account
WHERE retired_at IS NULL;                 -- live == not tombstoned == in the latest census

-- ----------------------------------------------------------------------------
-- 6. Reconciliation view (OBSERVED reporting; partition_ok is an internal CHECK, not the gate).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_account_reconcile AS
SELECT
    census_date_resolved                              AS census_date,
    COUNT(*) FILTER (WHERE retired_at IS NULL)        AS live_accounts,
    COUNT(*) FILTER (WHERE retired_at IS NOT NULL)    AS departed_accounts,
    COUNT(*)                                          AS universe_accounts
FROM core.sending_account
GROUP BY census_date_resolved;

-- ----------------------------------------------------------------------------
-- 7. Departed-aware daily view (frozen history preserved; live tiles filter retired_at IS NULL).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sending_account_daily_live AS
SELECT
    d.*,
    sa.retired_at,
    sa.last_seen_at,
    (sa.retired_at IS NULL)                                          AS is_live_account,
    (sa.retired_at IS NULL OR d.date < sa.retired_at::date)         AS is_active_on_date
FROM core.sending_account_daily d
LEFT JOIN core.sending_account sa
       ON lower(sa.email) = lower(d.account_id);   -- account_id in daily IS the email

-- ============================================================================
-- 8. CONSUMER MEANING-FLIP REPOINT (F4 — the highest-blast-radius item, companion-in-gate).
-- ----------------------------------------------------------------------------
-- WS3 changes what an "active sending account" MEANS, on TWO independent axes, the moment the
-- generator rebuilds core.sending_account:
--
--   (A) is_active flips from "ever-seen" (OLD: 1,299,431 — incl. 535,340 connection_error +
--       36,061 paused + 2,967 sending-error rows) to "present in the latest census"
--       (NEW: == retired_at IS NULL == core.v_sending_account_live ≈ 314,887). Any consumer
--       filtering `WHERE is_active` silently drops from 1.3M to 314K. (This is the intended
--       correction — but we repoint each such consumer to core.v_sending_account_live so the
--       source of "live inventory" is explicit and one-definition, per R5.)
--
--   (B) status stops carrying the bare connection words. The relabel (block 2 +
--       v_account_census_state) makes LIVE rows carry the DISAMBIGUATED connection axis:
--         'active'         -> 'conn_active'        (live count this snapshot: 297,250)
--         'paused'         -> 'conn_paused'        (1,612)
--         'connection_error' -> 'connection_error' (11,709, unchanged)
--         'sending error'  -> 'sending_error'      (4,316; note OLD value had a SPACE)
--       Departed rows keep their prior VARCHAR status from the carry-forward (so 'active' still
--       exists on tombstoned rows) — therefore a live "active inbox" count MUST key on
--       'conn_active' (which only live census rows ever receive), NOT on bare 'active'.
--       => EVERY consumer filtering `status='active'` / `status='paused'` / `status IN
--          ('active','paused',...)` MATCHES NOTHING for live rows after the rebuild (a SILENT
--          correctness regression, no error thrown) unless repointed to the conn_* vocabulary.
--
-- This block repoints EVERY live consumer found by grepping the live schema
-- (information_schema.views referencing sending_account) for is_active / bare-status filters.
-- It is part of the SAME gated apply as the rebuild so no consumer ever reads the broken value.
--
-- Consumers handled OUTSIDE this DDL (file edits, noted for the apply step):
--   * scripts/portal_data.py — ACTIVE_INBOX_WHERE = "status = 'active'" + WARMUP_WHERE; the
--     "active_inboxes" portal headline. Repoint default to "status = 'conn_active'" (or set
--     env PORTAL_ACTIVE_INBOX_FILTER). Post-flip headline reads ≈297,250 (live, healthy &
--     sending), NOT 725,063 and NOT 1.3M. SHELL: scripts/refresh_portal_feed.sh INVOKES
--     portal_data.py + the lens generators — the value-fix lives in those .py files, the .sh
--     itself carries no sending_account SQL; no .sh edit required beyond confirming it re-runs
--     the patched generators against the serving snapshot.
--   * scripts/dashboard_data.py — `WHERE is_active` (auto-corrects to 314K; repoint to
--     core.v_sending_account_live for one-definition parity).
--   * WS6 cube (v108) — reads core.sending_account.lifecycle_state (NOT status/is_active);
--     WS3 carries lifecycle_state forward UNCHANGED (COALESCE(h.lifecycle_state,'unknown')),
--     so the WS3 status-relabel does NOT touch it. The WS6 lowercase-'active' / case-alignment
--     fix is a v108-internal edit per the gate-review §a WS6 row — handled in that unit, not here.
--
-- NOTE: core.sending_account_daily — NO schema change, NO row deletion (D2 frozen history).
-- The TABLE rebuild of core.sending_account itself is performed by the GENERATOR
--   (entities/sending_account.py, staged alongside) in the SAME nightly canonical pass,
--   AFTER the census phase, AFTER this DDL lands the ALTER columns + views.
-- ----------------------------------------------------------------------------

-- (B) STATUS-FILTER consumers — remap bare connection words to the conn_* vocabulary.
-- derived.v_sending_account_capacity: is_active filter (axis A) -> v_sending_account_live.
CREATE OR REPLACE VIEW derived.v_sending_account_capacity AS
SELECT
    workspace_slug,
    count_star() FILTER (WHERE is_active)                                            AS active,
    count_star() FILTER (WHERE COALESCE(has_errors, FALSE))                          AS errored,
    count_star() FILTER (WHERE (is_active AND (NOT COALESCE(has_errors, FALSE))))    AS total_active,
    COALESCE(sum(daily_limit) FILTER (WHERE (is_active AND (NOT COALESCE(has_errors, FALSE)))), 0) AS sendable_daily_limit
FROM core.v_sending_account_live          -- R5: live == latest census (≈314,887); is_active is TRUE on every row here
GROUP BY workspace_slug
ORDER BY workspace_slug;

-- main.v_sending_account_freshness: is_active filter (axis A) -> v_sending_account_live for active_accounts.
CREATE OR REPLACE VIEW main.v_sending_account_freshness AS
SELECT
    max(_snapshot_date)                                              AS latest_snapshot,
    (current_date - max(_snapshot_date))                            AS days_stale,
    CASE WHEN ((current_date - max(_snapshot_date)) > 2) THEN 'STALE'
         WHEN ((current_date - max(_snapshot_date)) > 1) THEN 'WARNING'
         ELSE 'FRESH' END                                          AS freshness_status,
    count_star()                                                   AS total_accounts,
    count_star() FILTER (WHERE retired_at IS NULL)                 AS active_accounts   -- live == in latest census (≈314,887)
FROM core.sending_account;

-- main.v_infra_capacity_daily: status IN ('active','paused','connection_error','missing') -> conn_* vocabulary.
--   live "active" rows carry conn_active; departed rows are excluded by keying on conn_active.
CREATE OR REPLACE VIEW main.v_infra_capacity_daily AS
SELECT
    _snapshot_date                                                                          AS date,
    workspace_slug,
    infra_label(esp)                                                                        AS infra,
    count_star()                                                                            AS accounts_total,
    count_star() FILTER (WHERE (status = 'conn_active'))                                     AS accounts_sendable,
    count_star() FILTER (WHERE (status IN ('conn_paused','connection_error','sending_error')
                                  OR retired_at IS NOT NULL))                                AS accounts_blocked,
    COALESCE(sum(daily_limit), 0)                                                            AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE (status = 'conn_active')), 0)                    AS sendable_per_day
FROM core.sending_account
GROUP BY _snapshot_date, workspace_slug, infra_label(esp);

-- main.v_sending_account_anomalies: is_active filter (axis A) -> v_sending_account_live.
CREATE OR REPLACE VIEW main.v_sending_account_anomalies AS
WITH esp_norms AS (
    SELECT esp, "mode"(daily_limit) AS norm_limit, median(daily_limit) AS median_limit
    FROM core.v_sending_account_live
    WHERE (daily_limit IS NOT NULL) AND (esp IS NOT NULL)
    GROUP BY esp
)
SELECT
    sa.account_id, sa.email, sa."domain", sa.esp, sa.infra_provider, sa.workspace_slug,
    w."name" AS workspace_name, sa.daily_limit, n.norm_limit AS esp_norm,
    (sa.daily_limit - n.norm_limit) AS deviation,
    round(((abs((sa.daily_limit - n.norm_limit)) * 100.0) / "nullif"(n.norm_limit, 0)), 1) AS deviation_pct,
    sa.lifecycle_state, sa.created_at, CAST(sa.created_at AS DATE) AS created_date,
    split_part(sa."domain", '.', 1) AS domain_stem
FROM core.v_sending_account_live AS sa
INNER JOIN esp_norms AS n ON ((n.esp = sa.esp))
LEFT JOIN core.workspace AS w ON ((w.slug = sa.workspace_slug))
WHERE ((sa.daily_limit IS NOT NULL) AND (sa.daily_limit != n.norm_limit));

-- main.v_account_health: status IN ('active'/'paused'/'connection_error'/'missing') -> conn_* + retired_at.
CREATE OR REPLACE VIEW main.v_account_health AS
SELECT
    workspace_slug,
    infra_label(esp)                                                                          AS infra,
    count_star()                                                                              AS accounts_total,
    count_star() FILTER (WHERE (status = 'conn_active'))                                       AS active,
    count_star() FILTER (WHERE (status = 'conn_paused'))                                       AS paused,
    count_star() FILTER (WHERE (status = 'connection_error'))                                  AS connection_error,
    count_star() FILTER (WHERE (retired_at IS NOT NULL))                                       AS missing,   -- 'missing' == departed (tombstoned), not a live status
    count_star() FILTER (WHERE (status IN ('conn_paused','connection_error') OR retired_at IS NOT NULL)) AS unsendable,
    COALESCE(sum(daily_limit), 0)                                                              AS theoretical_per_day,
    COALESCE(sum(daily_limit) FILTER (WHERE (status = 'conn_active')), 0)                      AS sendable_per_day
FROM core.sending_account
GROUP BY workspace_slug, infra_label(esp);

-- main.v_accounts_per_domain: status='active' / status IN ('paused','connection_error','missing') -> conn_* + retired_at.
CREATE OR REPLACE VIEW main.v_accounts_per_domain AS
SELECT
    "domain",
    infra_label(esp)                                                                          AS infra,
    count_star()                                                                              AS accounts,
    count_star() FILTER (WHERE (status = 'conn_active'))                                       AS sendable,
    count_star() FILTER (WHERE (status IN ('conn_paused','connection_error') OR retired_at IS NOT NULL)) AS unsendable,
    COALESCE(sum(daily_limit), 0)                                                              AS daily_limit_total
FROM core.sending_account
GROUP BY "domain", infra_label(esp);

-- main.v_tag_coverage_gaps: is_active filter (axis A) -> v_sending_account_live.
CREATE OR REPLACE VIEW main.v_tag_coverage_gaps AS
WITH vendor_tags AS (
    SELECT tag_label FROM (VALUES ('Google'),('Reseller'),('Reseller PP'),('MailIn'),('Mailin'),
                                  ('CheapInboxes'),('Inboxing'),('Outreach Today')) AS t(tag_label)
),
account_vendor_tagged AS (
    SELECT DISTINCT t.email FROM core.sending_account_tag AS t
    WHERE (t.tag_label = ANY(SELECT tag_label FROM vendor_tags))
)
SELECT
    sa.workspace_slug, w."name" AS workspace_name, sa.esp,
    count_star() AS total_accounts,
    count(vt.email) AS tagged,
    (count_star() - count(vt.email)) AS untagged,
    round(((100.0 * count(vt.email)) / count_star()), 1) AS pct_covered
FROM core.v_sending_account_live AS sa
LEFT JOIN core.workspace AS w ON ((w.slug = sa.workspace_slug))
LEFT JOIN account_vendor_tagged AS vt ON ((vt.email = sa.email))
WHERE (sa.esp IS NOT NULL)
GROUP BY sa.workspace_slug, w."name", sa.esp
HAVING ((count_star() - count(vt.email)) > 0)
ORDER BY (count_star() - count(vt.email)) DESC;

-- dash.lens_overview__esp_distribution: is_active filter (axis A) -> v_sending_account_live.
CREATE OR REPLACE VIEW dash.lens_overview__esp_distribution AS
SELECT
    COALESCE(w."name", sa.workspace_slug) AS workspace,
    sa.esp,
    count_star()                          AS inboxes
FROM core.v_sending_account_live AS sa
LEFT JOIN core.workspace AS w ON ((w.slug = sa.workspace_slug))
WHERE (sa.esp IS NOT NULL)
GROUP BY 1, 2;

-- dash.warehouse_overview__esp: status='active' -> conn_active.
CREATE OR REPLACE VIEW dash.warehouse_overview__esp AS
SELECT
    esp,
    count_star()                AS inboxes,
    sum(daily_limit)            AS daily_capacity,
    count(DISTINCT "domain")    AS domains
FROM core.sending_account
WHERE ((status = 'conn_active') AND (esp IS NOT NULL))
GROUP BY esp
ORDER BY inboxes DESC;

-- dash.warehouse_overview__kpi: active_inboxes / warmup_inboxes keyed on status='active' -> conn_active.
--   (the meeting/sent sub-selects are unchanged; only the two sending_account status filters flip.)
CREATE OR REPLACE VIEW dash.warehouse_overview__kpi AS
SELECT
    (SELECT count_star() FILTER (WHERE (status = 'conn_active')) FROM core.sending_account) AS active_inboxes,
    (SELECT count_star() FROM core.sending_account
       WHERE ((status = 'conn_active')
              AND (lower(COALESCE(lifecycle_state, '')) ~~ '%warm%')
              AND (lower(COALESCE(lifecycle_state, '')) != 'warmed'))) AS warmup_inboxes,
    (SELECT count_star() FROM core.meeting AS m WHERE ((m.posted_at >= date_trunc('month', current_date)) AND (m.posted_at < (current_date + 1)) AND CASE  WHEN ((m."source" = 'sheet')) THEN ((m.channel = 'Email')) ELSE (NOT regexp_matches(lower(((COALESCE(m.campaign_name_raw, '') || ' ') || COALESCE(m.raw_text, ''))), 'sendivo|\bsms\b|whatsapp|iskra')) END)) AS mtd_meetings,
    (SELECT count_star() FROM core.meeting AS m WHERE CASE  WHEN ((m."source" = 'sheet')) THEN ((m.channel = 'Email')) ELSE (NOT regexp_matches(lower(((COALESCE(m.campaign_name_raw, '') || ' ') || COALESCE(m.raw_text, ''))), 'sendivo|\bsms\b|whatsapp|iskra')) END) AS all_time_meetings,
    (SELECT sum(sent) FROM main.raw_pipeline_campaign_daily_metrics WHERE (date >= date_trunc('month', current_date))) AS mtd_sent,
    round((CAST((SELECT sum(sent) FROM main.raw_pipeline_campaign_daily_metrics WHERE (date >= date_trunc('month', current_date))) AS DOUBLE) / "nullif"((SELECT count_star() FROM core.meeting AS m WHERE ((m.posted_at >= date_trunc('month', current_date)) AND (m.posted_at < (current_date + 1)) AND CASE  WHEN ((m."source" = 'sheet')) THEN ((m.channel = 'Email')) ELSE (NOT regexp_matches(lower(((COALESCE(m.campaign_name_raw, '') || ' ') || COALESCE(m.raw_text, ''))), 'sendivo|\bsms\b|whatsapp|iskra')) END)), 0))) AS mtd_sb_ratio,
    (SELECT CAST(posted_at AS DATE) FROM core.meeting AS m WHERE CASE  WHEN ((m."source" = 'sheet')) THEN ((m.channel = 'Email')) ELSE (NOT regexp_matches(lower(((COALESCE(m.campaign_name_raw, '') || ' ') || COALESCE(m.raw_text, ''))), 'sendivo|\bsms\b|whatsapp|iskra')) END GROUP BY 1 ORDER BY count_star() DESC LIMIT 1) AS record_day,
    (SELECT count_star() FROM core.meeting AS m WHERE CASE  WHEN ((m."source" = 'sheet')) THEN ((m.channel = 'Email')) ELSE (NOT regexp_matches(lower(((COALESCE(m.campaign_name_raw, '') || ' ') || COALESCE(m.raw_text, ''))), 'sendivo|\bsms\b|whatsapp|iskra')) END GROUP BY CAST(posted_at AS DATE) ORDER BY count_star() DESC LIMIT 1) AS record_day_meetings;

-- dash.lens_sending_truth: joins sa.status for eligibility — remap 'active'/'missing' to conn_* + departed.
CREATE OR REPLACE VIEW dash.lens_sending_truth AS
WITH classified AS (
    SELECT
        d.date, d.account_id, d.workspace_slug,
        COALESCE(w."name", d.workspace_slug) AS workspace_name,
        CASE WHEN ((d.esp = 'google')) THEN ('Google')
             WHEN ((d.esp = 'outlook')) THEN ('Outlook')
             WHEN ((d.esp = 'otd')) THEN ('OTD')
             ELSE COALESCE(d.esp, '(unknown)') END AS infra_type,
        sa."domain",
        sa.status AS account_status,
        d.daily_limit, d.expected_sends, d.actual_sends, d.active_campaign_count, d.fulfillment,
        ((COALESCE(sa.status, '') = 'conn_active') AND (COALESCE(d.expected_sends, 0) > 0)) AS is_eligible,
        ((COALESCE(sa.status, '') = 'conn_active') AND (COALESCE(d.expected_sends, 0) > 0) AND (COALESCE(d.active_campaign_count, 0) > 0)) AS is_campaign_assigned_eligible,
        CASE WHEN ((sa.status IS NULL) OR (sa.retired_at IS NOT NULL)) THEN ('missing_current_inventory')
             WHEN ((sa.status != 'conn_active')) THEN ('bad_status')
             WHEN ((COALESCE(d.daily_limit, 0) = 0)) THEN ('daily_limit_zero')
             WHEN ((COALESCE(d.active_campaign_count, 0) = 0)) THEN ('no_active_campaign')
             WHEN (((COALESCE(d.expected_sends, 0) > 0) AND (COALESCE(d.actual_sends, 0) >= (d.expected_sends * 0.95)))) THEN ('fully_utilized')
             ELSE 'assigned_but_undersent' END AS eligibility,
        CASE WHEN (((d.actual_sends = 0) AND (d.expected_sends > 0))) THEN ('zero')
             WHEN (((d.expected_sends > 0) AND (d.fulfillment < 0.25))) THEN ('under25')
             WHEN (((d.expected_sends > 0) AND (d.fulfillment < 0.50))) THEN ('under50')
             WHEN (((d.expected_sends > 0) AND (d.fulfillment < 0.85))) THEN ('under85')
             WHEN (((d.expected_sends > 0) AND (d.fulfillment >= 0.85))) THEN ('ok')
             ELSE 'none' END AS fulfillment_bucket
    FROM core.sending_account_daily AS d
    LEFT JOIN core.sending_account AS sa ON ((lower(sa.email) = lower(d.account_id)))
    LEFT JOIN core.workspace AS w ON ((w.slug = d.workspace_slug))
    WHERE (d.date >= (current_date - 92))
)
SELECT
    date, account_id, workspace_slug, workspace_name, infra_type, "domain", account_status,
    eligibility, fulfillment_bucket, daily_limit, expected_sends AS configured_capacity,
    actual_sends, active_campaign_count,
    CASE WHEN ((NOT is_eligible)) THEN (expected_sends) ELSE 0 END AS excluded_capacity,
    CASE WHEN (is_eligible) THEN (expected_sends) ELSE 0 END AS eligible_capacity,
    CASE WHEN (is_campaign_assigned_eligible) THEN (expected_sends) ELSE 0 END AS campaign_assigned_capacity,
    greatest((CASE WHEN (is_eligible) THEN (expected_sends) ELSE 0 END - actual_sends), 0) AS eligible_gap
FROM classified;
-- ============================================================================
-- PASSTHROUGH consumers (NO fix needed — verified): core.v_account_campaign_offer,
--   core.v_sending_account_vendor(_resolved), derived.v_batch_lifecycle(_summary),
--   dash.lens_overview__sending_truth_inventory — SELECT is_active/status but do NOT filter on
--   the bare words; is_active auto-corrects to census-membership, status surfaces conn_* as data.
-- ============================================================================
