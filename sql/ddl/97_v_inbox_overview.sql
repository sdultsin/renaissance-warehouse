-- 97_v_inbox_overview.sql  [2026-06-26]
-- core.v_inbox_overview — the company-wide INBOX OVERVIEW ("Inbox Hub" inside the warehouse).
-- ONE row per live inbox (keyed off the latest account_census snapshot), with everything we
-- sync from Instantly joined in: identity, provider/infra, lifecycle state, live status,
-- daily limit, warmup config + score, campaign membership, batch/RG tags, Instantly tags,
-- created / warmup-start / go-live dates, cold-send history, plus a derived Stage + health flag.
--
-- ADDITIVE / NON-DESTRUCTIVE: brand-new view name (does not touch the existing core.v_inbox_hub
-- or any base table). A VIEW recomputes on every nightly rebuild, so it self-refreshes for free.
-- Some columns are sparse until the upstream syncs are widened (tags ~56%, campaigns ~13%,
-- batch ~50% as of 2026-06-26) — coverage rises automatically as those feeders fill.
--
-- @gate: add
-- Depends on 96
CREATE OR REPLACE VIEW core.v_inbox_overview AS
WITH cur AS (
  SELECT lower(trim(email))               AS email,
         any_value("domain")              AS domain,
         any_value(workspace_slug)        AS workspace_slug,
         any_value(status_label)          AS status,
         any_value(daily_limit)           AS daily_limit,
         any_value(warmup_status_label)   AS warmup_state,
         any_value(warmup_limit)          AS warmup_limit,
         any_value(stat_warmup_score)     AS warmup_score,
         any_value(provider_code)         AS provider_code,
         any_value(timestamp_created)     AS created_at,
         any_value(timestamp_warmup_start) AS warmup_start,
         max(census_date)                 AS snapshot_date
  FROM core.account_census
  WHERE census_date = (SELECT max(census_date) FROM core.account_census)
  GROUP BY 1
),
lbl AS (
  SELECT lower(trim(email))              AS email,
         any_value(vendor)               AS vendor,
         any_value(infra)                AS infra,
         any_value(lifecycle)            AS state,
         any_value(cold_start)           AS go_live,
         any_value(last_cold_send_date)  AS last_cold_send,
         any_value(total_cold_sends_ever) AS total_cold_sends,
         any_value(cold_send_days)       AS cold_send_days
  FROM core.v_account_label_current GROUP BY 1
),
tg AS (
  SELECT lower(trim(email)) AS email, string_agg(DISTINCT tag_label, ' | ') AS tags
  FROM core.sending_account_tag GROUP BY 1
),
cp AS (
  SELECT lower(trim(account_email)) AS email, count(DISTINCT campaign_id) AS n_campaigns
  FROM core.account_campaign GROUP BY 1
),
bat AS (
  SELECT email, batch_key, rg_tag_1, rg_tag_2, provider_tag FROM (
    SELECT lower(trim(account_email)) AS email, batch_key, rg_tag_1, rg_tag_2, provider_tag,
           row_number() OVER (PARTITION BY lower(trim(account_email))
                              ORDER BY is_current_batch DESC NULLS LAST, _loaded_at DESC NULLS LAST) AS rn
    FROM core.sending_account_batch WHERE account_email IS NOT NULL
  ) WHERE rn = 1
)
SELECT
  c.email,
  c.domain                                          AS "domain",
  c.workspace_slug,
  COALESCE(l.vendor, bat.provider_tag)              AS provider,
  l.infra,
  l.state,                                          -- Active / Warmup (lifecycle)
  c.status,                                         -- active / connection_error / sending_error / paused
  (c.status = 'active')                             AS connected,
  c.daily_limit,
  c.warmup_state,                                   -- active / banned / paused
  c.warmup_limit,
  c.warmup_score,
  COALESCE(cp.n_campaigns, 0)                       AS n_campaigns,
  (COALESCE(cp.n_campaigns, 0) > 0)                 AS in_campaign,
  bat.batch_key,
  bat.rg_tag_1,
  bat.rg_tag_2,
  tg.tags,
  c.created_at,
  c.warmup_start,
  l.go_live,
  l.last_cold_send,
  l.total_cold_sends,
  l.cold_send_days,
  CASE
    WHEN c.status IN ('connection_error','sending_error') THEN 'Disconnected'
    WHEN c.warmup_state = 'banned'                         THEN 'Banned'
    WHEN c.status = 'paused'                               THEN 'Paused'
    WHEN l.state = 'Active'                                THEN 'Live'
    WHEN l.state = 'Warmup'                                THEN 'Warming'
    WHEN l.go_live IS NOT NULL                             THEN 'Live'
    ELSE 'Other'
  END                                               AS stage,
  (l.vendor IS NULL OR l.vendor IN ('(pending)','Unmapped')) AS needs_provider_tag,
  c.provider_code,
  c.snapshot_date
FROM cur c
LEFT JOIN lbl l   USING (email)
LEFT JOIN tg      USING (email)
LEFT JOIN cp      USING (email)
LEFT JOIN bat     USING (email);
