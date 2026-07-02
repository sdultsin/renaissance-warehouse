-- @gate: additive_views
-- Depends on 31
-- Fix v_tag_coverage_gaps: match the REAL tag convention.
--
-- BUG (shipped in DDL 31): the vendor_tags allowlist matched EXACT labels
--   ('Google','Reseller','Reseller PP','MailIn','Mailin','CheapInboxes','Inboxing',
--   'Outreach Today'), but the tag sync writes the provider+status convention
--   '<provider> Active' / '<provider> Warmup'. Verified live 2026-06-24: the only
--   labels on core.sending_account_tag are 'Outreach Today Active/Warmup' and
--   'Reseller Active/Warmup' (the allowlist was ALSO missing 'MilkBox'). So the
--   IN(...) matched 0 rows -> tagged=0 -> pct_covered=0.0 for EVERY workspace, which
--   read as "tag sync dead / total coverage gap" when the sync is in fact healthy
--   (~315k fresh rows refreshed daily).
--
-- FIX: match by SUFFIX ('% Active' / '% Warmup'). This is provider-agnostic, so it
--   does not go stale when a new provider (MilkBox, MailIn, ...) is added, and it
--   mirrors David's stated tagging convention ("everything tagged active or warmup").
--   Email join is made case-insensitive (lower() both sides) so a casing mismatch
--   cannot understate coverage. Workspace name still resolves via canonical
--   core.workspace (never the stale raw_pipeline_campaigns codename).
--
-- Verified live before ship (2026-06-24): OLD predicate -> 236,273 active / 0 tagged
--   / 0%. NEW predicate -> OTD 100.0% (1 untagged), Google 89.2% (4,346 untagged),
--   Outlook 5.6% (20,099 untagged) — reconciles with David: OTD/Reseller ~complete,
--   ~4-5k new Google untagged, ~20k untagged Cheap Inboxes (Outlook).
--
-- Original output columns are preserved; active/warmup/both-conflict breakdown columns
-- are appended (additive, non-breaking). Idempotent (CREATE OR REPLACE) and
-- non-destructive: view logic only, no data touched.
CREATE OR REPLACE VIEW v_tag_coverage_gaps AS
WITH account_vendor_tag AS (
    -- an account is "vendor-tagged" iff it carries a '<provider> Active' / '<provider> Warmup' tag
    SELECT lower(email) AS email,
           MAX(CASE WHEN tag_label LIKE '% Active' THEN 1 ELSE 0 END) AS has_active,
           MAX(CASE WHEN tag_label LIKE '% Warmup' THEN 1 ELSE 0 END) AS has_warmup
    FROM core.sending_account_tag
    WHERE tag_label LIKE '% Active' OR tag_label LIKE '% Warmup'
    GROUP BY lower(email)
)
SELECT
    sa.workspace_slug,
    w.name AS workspace_name,
    sa.esp,
    COUNT(*) AS total_accounts,
    COUNT(vt.email) AS tagged,
    COUNT(*) - COUNT(vt.email) AS untagged,
    ROUND(100.0 * COUNT(vt.email) / COUNT(*), 1) AS pct_covered,
    SUM(COALESCE(vt.has_active, 0)) AS tagged_active,
    SUM(COALESCE(vt.has_warmup, 0)) AS tagged_warmup,
    SUM(CASE WHEN vt.has_active = 1 AND vt.has_warmup = 1 THEN 1 ELSE 0 END) AS tagged_both_conflict
FROM core.sending_account sa
LEFT JOIN core.workspace w ON w.slug = sa.workspace_slug
LEFT JOIN account_vendor_tag vt ON vt.email = lower(sa.email)
WHERE sa.is_active AND sa.esp IS NOT NULL
GROUP BY sa.workspace_slug, w.name, sa.esp
HAVING (COUNT(*) - COUNT(vt.email)) > 0
ORDER BY (COUNT(*) - COUNT(vt.email)) DESC;
