-- 1020_inbox_unified.sql  [2026-06-26]
-- core.inbox — THE canonical company inbox database (single VIEW; merge logic in one place,
-- so no two-creation-verb split-brain). One row per inbox that has EVER existed (~2.77M),
-- unifying the provisioning master (core.sending_account_batch) with the live hub
-- (core.v_inbox_overview). Retires the "FINAL DATA" framing: the live inbox hub is just
-- this view WHERE is_live, so the master can never again be missing a live inbox.
--
--   is_live = the email is present in core.v_inbox_overview, i.e. in the LATEST Instantly
--             account census = the current fleet (~433k). NOTE: that fleet includes
--             DISCONNECTED inboxes too — is_live means "currently EXISTS in Instantly"
--             (vs retired/deleted), NOT "connected". Use `status`/`stage` for connected-
--             vs-down. Retired/deleted inboxes (master-only) are is_live = FALSE.
--   provisioning cols (batch_family/offer/first_name/last_name/status_csv) come from the
--             master and are filled for every inbox THAT IS IN THE MASTER. A currently-live
--             inbox not yet written back to the master will have these NULL (it still gets
--             provider/batch/RG from its live Instantly tags via the COALESCE below).
--   overlapping cols (provider/domain/batch_key/rg) prefer the LIVE value, fall back to the
--             master CSV. All other live-state cols come straight from the hub (NULL when dead).
--
-- ADDITIVE: new view; reads existing objects; drops nothing. (Materialize for speed is a
-- clean follow-up once the shape settles.)
-- @gate: add
-- Depends on 101
CREATE OR REPLACE VIEW core.inbox AS
WITH m AS (
  SELECT email, batch_key, rg1, rg2, prov_csv, dom_csv, offer, batch_family, first_name, last_name, status_csv
  FROM (
    SELECT lower(trim(account_email)) AS email, batch_key, rg_tag_1 AS rg1, rg_tag_2 AS rg2,
           provider_tag AS prov_csv, "domain" AS dom_csv, offer, batch_family, first_name, last_name, status_csv,
           row_number() OVER (PARTITION BY lower(trim(account_email))
                              ORDER BY is_current_batch DESC NULLS LAST, _loaded_at DESC NULLS LAST) AS rn
    FROM core.sending_account_batch WHERE account_email IS NOT NULL
  ) WHERE rn = 1
)
SELECT
  COALESCE(l.email, m.email)              AS email,
  (l.email IS NOT NULL)                   AS is_live,
  COALESCE(l."domain", m.dom_csv)         AS "domain",
  COALESCE(l.provider, m.prov_csv)        AS provider,
  COALESCE(l.batch_key, m.batch_key)      AS batch_key,
  COALESCE(l.rg_tag_1, m.rg1)             AS rg_tag_1,
  COALESCE(l.rg_tag_2, m.rg2)             AS rg_tag_2,
  m.batch_family, m.offer, m.first_name, m.last_name, m.status_csv,
  l.* EXCLUDE (email, "domain", provider, batch_key, rg_tag_1, rg_tag_2)
FROM m FULL OUTER JOIN core.v_inbox_overview l ON l.email = m.email;

COMMENT ON VIEW core.inbox IS
  'CANONICAL COMPANY INBOX DATABASE — one row per inbox that has EVER existed (~2.77M): '
  'provisioning (batch/domain/provider/offer/RG/name) from the master + live state '
  '(status/dates/warmup/tags/campaigns/...) for the current fleet. is_live = present in the '
  'latest Instantly account census (the ~433k current fleet, connected OR disconnected; use '
  'status/stage for connected-vs-down); retired/deleted = is_live FALSE. The live inbox hub '
  '= this view WHERE is_live. Supersedes the old "FINAL DATA" master.';
