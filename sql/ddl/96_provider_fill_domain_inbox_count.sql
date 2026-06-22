-- 96_provider_fill_domain_inbox_count.sql  [2026-06-21]
-- Reflect David's FINAL DATA edits into core.sending_account_batch, additively.
-- Two changes, both NON-DESTRUCTIVE (no DROP / RENAME / ALTER TYPE; no existing
-- value is overwritten; no rows deleted):
--
--   1. provider_tag backfill — fill ONLY rows where provider_tag IS NULL, using the
--      authoritative batch->provider map derived from FINAL DATA (provider is uniform
--      per named batch: 119 batches -> exactly 1 provider each). Existing non-null
--      provider_tag values are left untouched (the WHERE guard). Covers the 109,486
--      NULL-provider rows that carry a named batch. (Blank-batch rows are handled in a
--      follow-up that needs per-account data from the merged file.)
--
--   2. core.v_domain_inbox_count — a COMPUTED view of true inboxes per domain
--      (count DISTINCT account_email), so "accounts per domain" is always correct
--      (5, not the file's duplicate-inflated 10) and self-updates. Additive: new view.
--
-- @gate: add
-- Depends on 61

-- 1. additive provider backfill (NULL-only) ----------------------------------------
UPDATE core.sending_account_batch AS t
SET    provider_tag = m.provider
FROM   (VALUES
  ('1st_batch','Outreach Today'),
  ('2nd_batch','Outreach Today'),
  ('B10','Shekhar'),
  ('B100','Outreach Today'),
  ('B100-R','Outreach Today'),
  ('B101','Outreach Today'),
  ('B101-R','Outreach Today'),
  ('B102.1','Inboxing'),
  ('B102.3','Inboxing'),
  ('B102.4','Inboxing'),
  ('B102.5','Inboxing'),
  ('B103','Outreach Today'),
  ('B104','Outreach Today'),
  ('B106','Outreach Today'),
  ('B107','Outreach Today'),
  ('B108','Outreach Today'),
  ('B109','Outreach Today'),
  ('B110','Outreach Today'),
  ('B112','Outreach Today'),
  ('B13','Shekhar'),
  ('B14','Shekhar'),
  ('B16','Shekhar'),
  ('B17','Shekhar'),
  ('B21','Shekhar'),
  ('B23','Shekhar'),
  ('B24','Shekhar'),
  ('B25','Shekhar'),
  ('B26','Shekhar'),
  ('B28','Shekhar'),
  ('B29','Avion'),
  ('B30','Avion'),
  ('B31','Avion'),
  ('B33','Shekhar'),
  ('B36','Shekhar'),
  ('B37','Shekhar'),
  ('B39','Shekhar'),
  ('B40','Avion'),
  ('B41PA','Panel'),
  ('B42.1O','Inboxing'),
  ('B42.2O','Inboxing'),
  ('B42.3O','Inboxing'),
  ('B42.4O','Inboxing'),
  ('B42.5O','Inboxing'),
  ('B44.1G','Outreach Today'),
  ('B44.2G','Outreach Today'),
  ('B44.3G','Outreach Today'),
  ('B44.4G','Outreach Today'),
  ('B44.5G','Outreach Today'),
  ('B45','Inboxing'),
  ('B47','MailIn'),
  ('B48','Outreach Today'),
  ('B49','MailIn'),
  ('B51','Outreach Today'),
  ('B51-R','Outreach Today'),
  ('B52','MailIn'),
  ('B53','Outreach Today'),
  ('B53-R','Outreach Today'),
  ('B54','Outreach Today'),
  ('B54-R','Outreach Today'),
  ('B55','MailIn'),
  ('B56','MailIn'),
  ('B57','MailIn'),
  ('B58','Outreach Today'),
  ('B58-R','Outreach Today'),
  ('B59','Outreach Today'),
  ('B60','Outreach Today'),
  ('B60.2','Panel'),
  ('B61','Outreach Today'),
  ('B61-R','Outreach Today'),
  ('B62','MailIn'),
  ('B63','Outreach Today'),
  ('B63-R','Outreach Today'),
  ('B64','Outreach Today'),
  ('B65','Outreach Today'),
  ('B65-R','Outreach Today'),
  ('B66','Outreach Today'),
  ('B66-R','Outreach Today'),
  ('B67','MailIn'),
  ('B68','Outreach Today'),
  ('B68-R','Outreach Today'),
  ('B69','Outreach Today'),
  ('B69-R','Outreach Today'),
  ('B70','Outreach Today'),
  ('B71','MailIn'),
  ('B72','MailIn'),
  ('B73','MailIn'),
  ('B74','MailIn'),
  ('B75','MailIn'),
  ('B76-R','Outreach Today'),
  ('B77','Inboxing'),
  ('B79','MailIn'),
  ('B8','Shekhar'),
  ('B80','MailIn'),
  ('B81','MailIn'),
  ('B82','MailIn'),
  ('B83','MailIn'),
  ('B84','MailIn'),
  ('B85','MailIn'),
  ('B86','MailIn'),
  ('B88','Outreach Today'),
  ('B88-R','Outreach Today'),
  ('B89','Outreach Today'),
  ('B89-R','Outreach Today'),
  ('B90','MailIn'),
  ('B91','MailIn'),
  ('B92','MailIn'),
  ('B93','Outreach Today'),
  ('B94','Outreach Today'),
  ('B95','Outreach Today'),
  ('B95-R','Outreach Today'),
  ('B96','Outreach Today'),
  ('B96-R','Outreach Today'),
  ('B97','Outreach Today'),
  ('B97-R','Outreach Today'),
  ('B98','Outreach Today'),
  ('B99-R','Outreach Today'),
  ('MS Panel 6','Microsoft Panel'),
  ('MS Panel 7','Microsoft Panel'),
  ('MS Panel 8','Microsoft Panel')
       ) AS m(batch_key, provider)
WHERE  t.batch_key = m.batch_key
  AND  t.provider_tag IS NULL;        -- guard: only fill blanks, never overwrite

-- 2. additive computed view: true distinct inboxes per domain ----------------------
CREATE OR REPLACE VIEW core.v_domain_inbox_count AS
SELECT lower(domain)                  AS domain,
       count(DISTINCT account_email)  AS inbox_count
FROM   core.sending_account_batch
WHERE  NULLIF(domain, '') IS NOT NULL
GROUP  BY lower(domain);
