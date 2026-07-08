-- 1088_chat_cancel_onetime_fix.sql
-- [2026-07-07] ONE-TIME / TEMPORARY fix (David-authorized). The sheet-sourced
-- rg_tag_dim.is_cancelled MISSES ~8k Avion/Shekhar domains actually cancelled via
-- the WhatsApp/Toukir chat (the authoritative reseller cancel path). Loads the
-- chat-verified cancelled domains (data delivered to the box out-of-band — NOT in
-- this public repo) and ORs them into core.v_inbox_attribution.is_cancelled.
-- Temporary until David's proper cancel-tracking system; then DROP TABLE
-- core.chat_cancel_domains + revert the view. Data file: /root/chat_cancel_domains_20260707.csv
CREATE TABLE IF NOT EXISTS core.chat_cancel_domains (domain VARCHAR, provider VARCHAR, cancel_date VARCHAR);
DELETE FROM core.chat_cancel_domains;
COPY core.chat_cancel_domains FROM '/root/chat_cancel_domains_20260707.csv' (HEADER true, AUTO_DETECT true);
CREATE OR REPLACE VIEW core.v_inbox_attribution AS
WITH tag_edges AS (SELECT lower(email) AS email, workspace_slug, unnest(tags_arr) AS tag FROM core.account_tags),
tag_attr AS (SELECT r.email, r.workspace_slug, string_agg(r.tag, ',' ORDER BY r.tag) FILTER (WHERE regexp_matches(r.tag, '^RG[0-9]')) AS rg_tags, bool_or(COALESCE(d.is_cancelled, CAST('f' AS BOOLEAN))) AS is_cancelled, max(d.partner) AS partner, max(d.rg_type) AS rg_type, max(d.renewal) AS renewal FROM tag_edges AS r LEFT JOIN core.rg_tag_dim AS d ON ((d.rg_tag = r.tag)) GROUP BY 1, 2),
batch AS (SELECT account_email, batch_key, batch_family, attribution_source, row_number() OVER (PARTITION BY account_email ORDER BY is_current_batch DESC NULLS LAST, batch_key DESC NULLS LAST) AS rn FROM core.sending_account_batch)
SELECT c.email, c.workspace_slug, c.workspace_current_name, c.status_label, c.daily_limit, t.rg_tags, t.partner,
  (COALESCE(t.is_cancelled, CAST('f' AS BOOLEAN)) OR (lower(split_part(c.email,'@',2)) IN (SELECT domain FROM core.chat_cancel_domains))) AS is_cancelled,
  t.rg_type, t.renewal, b.batch_key, b.batch_family, b.attribution_source, c.census_date
FROM core.v_account_census_latest AS c
LEFT JOIN tag_attr AS t ON (((t.email = lower(c.email)) AND (t.workspace_slug = c.workspace_slug)))
LEFT JOIN batch AS b ON (((b.account_email = lower(c.email)) AND (b.rn = 1)));
