-- 1085_v_inbox_overview_dedup_tags.sql  [2026-07-07]
-- FIX the duplicate-row / over-count bug in core.v_inbox_overview.
--
-- SYMPTOM: v_inbox_overview returned 434,773 rows for only 399,194 distinct inboxes — so
-- core.inbox (is_live), the Inbox Hub, and any count(*) over-counted the live fleet by ~35k.
-- The real live count (distinct inboxes present in the latest Instantly census) is 399,194,
-- cross-checked to the live /accounts poll (396,606) and core.sending_account.is_active.
--
-- ROOT CAUSE: the `atg` CTE reads core.account_tags with NO per-email dedup. account_tags is
-- an additive per-(email,workspace) ledger that KEEPS ghost rows for inboxes that changed
-- workspace — 34,851 currently-live emails have >1 tag row. Every OTHER join in the view is
-- deduped to one row per email (GROUP BY / row_number rn=1); this one fanned out.
--
-- FIX: dedup `atg` to ONE row per email, keeping the most-recently-loaded tag row (its current
-- tags). Nothing else changes — byte-identical to 1083 except the `atg` CTE. Output columns
-- unchanged; row count drops 434,773 -> 399,194 (the true distinct-inbox count).
-- @gate: alter
-- Depends on 1083
CREATE OR REPLACE VIEW core.v_inbox_overview AS
WITH cur AS (
  SELECT lower(trim(email)) AS email, any_value("domain") AS "domain", any_value(workspace_slug) AS workspace_slug,
         any_value(status_label) AS status, any_value(daily_limit) AS daily_limit,
         any_value(warmup_status_label) AS warmup_state, any_value(warmup_limit) AS warmup_limit,
         any_value(stat_warmup_score) AS warmup_score, any_value(provider_code) AS provider_code,
         any_value(timestamp_created) AS created_at, any_value(timestamp_warmup_start) AS warmup_start,
         max(census_date) AS snapshot_date
  FROM core.account_census WHERE census_date=(SELECT max(census_date) FROM core.account_census) GROUP BY 1),
lbl AS (SELECT lower(trim(email)) AS email, any_value(vendor) AS vendor, any_value(infra) AS infra,
         any_value(lifecycle) AS state, any_value(cold_start) AS go_live, any_value(last_cold_send_date) AS last_cold_send,
         any_value(total_cold_sends_ever) AS total_cold_sends, any_value(cold_send_days) AS cold_send_days
  FROM core.v_account_label_current GROUP BY 1),
atg AS (SELECT email, tags,
         list_filter(tags_arr, x -> regexp_matches(lower(x),'^(outreach today|mailin|milkbox|tucows|reseller|cheap inboxes|inboxing|microsoft panel|ms panel|panel|maildoso)( |$)'))[1] AS prov_tag,
         list_filter(tags_arr, x -> regexp_matches(x,'^B[0-9]'))[1] AS batch_tag
  FROM (SELECT lower(trim(email)) AS email, tags, tags_arr,
          row_number() OVER (PARTITION BY lower(trim(email)) ORDER BY _loaded_at DESC NULLS LAST) AS rn
        FROM core.account_tags) WHERE rn=1),
cl AS (SELECT lower(trim(account_email)) AS email, max(n_campaigns) AS n_campaigns, max(n_active_campaigns) AS n_active
  FROM core.account_campaign_live GROUP BY 1),
ae AS (SELECT lower(trim(email)) AS email, any_value(error_string) AS disconnect_reason, any_value(error_code) AS disconnect_code
  FROM core.account_error GROUP BY 1),
bat AS (SELECT email, batch_key, rg_tag_1, rg_tag_2, provider_tag FROM (
    SELECT lower(trim(account_email)) AS email, batch_key, rg_tag_1, rg_tag_2, provider_tag,
      row_number() OVER (PARTITION BY lower(trim(account_email)) ORDER BY is_current_batch DESC NULLS LAST, _loaded_at DESC NULLS LAST) AS rn
    FROM core.sending_account_batch WHERE account_email IS NOT NULL) WHERE rn=1),
sev AS (SELECT lower(trim(account_id)) AS email, min(event_at) FILTER (WHERE new_state='active') AS connected_evt,
         max(event_at) FILTER (WHERE new_state='paused') AS paused_date, max(event_at) FILTER (WHERE new_state='retired') AS retired_date
  FROM core.sending_account_state_event GROUP BY 1),
firstact AS (SELECT lower(trim(email)) AS email, min(census_date) FILTER (WHERE status_label='active') AS first_active_day
  FROM core.account_census GROUP BY 1),
dns AS (SELECT dom, has_mx, mx_provider, has_spf, has_dkim, has_dmarc FROM (
    SELECT lower("domain") AS dom, (NULLIF(mx_records,'') IS NOT NULL) AS has_mx, mx_provider,
      (NULLIF(spf_record,'') IS NOT NULL) AS has_spf, (dkim_selectors_present IS NOT NULL) AS has_dkim,
      (NULLIF(dmarc_policy,'') IS NOT NULL) AS has_dmarc,
      row_number() OVER (PARTITION BY lower("domain") ORDER BY _loaded_at DESC) AS rn
    FROM raw_dns_sweep_domain) WHERE rn=1),
bl AS (SELECT lower("domain") AS dom, bool_or(status='listed' AND blocklist='spamhaus_dbl') AS blacklisted FROM raw_blacklist_check GROUP BY 1),
snd AS (SELECT lower(account_id) AS email,
         sum(actual_sends) FILTER (WHERE date >= (SELECT max(date) FROM core.sending_account_daily)-6) AS sends_7d,
         sum(actual_sends) FILTER (WHERE date >= (SELECT max(date) FROM core.sending_account_daily)-29) AS sends_30d
  FROM core.sending_account_daily WHERE date >= (SELECT max(date) FROM core.sending_account_daily)-29 GROUP BY 1),
fcs AS (SELECT email, first_cold_send_at FROM core.account_first_cold_send)
SELECT
  c.email, c."domain" AS "domain", c.workspace_slug,
  COALESCE(NULLIF(regexp_extract(atg.prov_tag,'(?i)^(Outreach Today|MailIn|MilkBox|Tucows|Reseller|Cheap Inboxes|Inboxing|Microsoft Panel|MS Panel|Panel|Maildoso)',0),''), l.vendor, bat.provider_tag) AS provider,
  l.infra,
  COALESCE(CASE WHEN atg.prov_tag ILIKE '%warm%' THEN 'Warmup' WHEN atg.prov_tag ILIKE '%active%' THEN 'Active' END, l.state) AS state,
  c.status, (c.status='active') AS connected, c.daily_limit, c.warmup_state, c.warmup_limit, c.warmup_score,
  COALESCE(cl.n_campaigns,0) AS n_campaigns, (COALESCE(cl.n_campaigns,0)>0) AS in_campaign, COALESCE(cl.n_active,0) AS n_active_campaigns,
  COALESCE(atg.batch_tag, bat.batch_key) AS batch_key, bat.rg_tag_1, bat.rg_tag_2, atg.tags,
  ae.disconnect_reason, ae.disconnect_code,
  dns.has_mx, dns.mx_provider, dns.has_spf, dns.has_dkim, dns.has_dmarc, COALESCE(bl.blacklisted,false) AS blacklisted,
  CASE c.provider_code WHEN 1 THEN 'IMAP/SMTP' WHEN 2 THEN 'Google' WHEN 3 THEN 'Microsoft' END AS esp,
  COALESCE(snd.sends_7d,0) AS sends_7d, COALESCE(snd.sends_30d,0) AS sends_30d,
  c.created_at, c.warmup_start, COALESCE(sev.connected_evt, CAST(firstact.first_active_day AS TIMESTAMP)) AS connected_date,
  CASE WHEN COALESCE(CASE WHEN atg.prov_tag ILIKE '%warm%' THEN 'Warmup' WHEN atg.prov_tag ILIKE '%active%' THEN 'Active' END, l.state)='Active'
       THEN fcs.first_cold_send_at ELSE NULL END AS go_live,
  l.last_cold_send, sev.paused_date, sev.retired_date, l.total_cold_sends, l.cold_send_days,
  CASE WHEN c.warmup_state='banned' THEN 'Banned'
       WHEN c.status IN ('connection_error','sending_error') THEN 'Disconnected' WHEN c.status='paused' THEN 'Paused'
       WHEN l.state='Active' THEN 'Live' WHEN l.state='Warmup' THEN 'Warming'
       WHEN l.go_live IS NOT NULL THEN 'Live' ELSE 'Other' END AS stage,
  CASE WHEN c.warmup_state='banned' THEN 'banned'
       WHEN c.status IN ('connection_error','sending_error') THEN 'broken' WHEN c.status='paused' THEN 'paused'
       WHEN COALESCE(bl.blacklisted,false) OR dns.has_mx=false THEN 'at-risk'
       WHEN c.status='active' THEN 'healthy' ELSE 'unknown' END AS health,
  CASE WHEN c.daily_limit>0 THEN round((COALESCE(snd.sends_7d,0)/7.0)/c.daily_limit,2) END AS utilization_7d,
  (atg.prov_tag IS NULL AND (l.vendor IS NULL OR l.vendor IN ('(pending)','Unmapped'))) AS needs_provider_tag,
  c.provider_code, c.snapshot_date
FROM cur c
LEFT JOIN lbl l USING (email) LEFT JOIN atg USING (email) LEFT JOIN cl USING (email) LEFT JOIN ae USING (email)
LEFT JOIN bat USING (email) LEFT JOIN sev USING (email) LEFT JOIN firstact USING (email)
LEFT JOIN snd USING (email) LEFT JOIN fcs USING (email)
LEFT JOIN dns ON dns.dom = lower(c."domain") LEFT JOIN bl ON bl.dom = lower(c."domain");
