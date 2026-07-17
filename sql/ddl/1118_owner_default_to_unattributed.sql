-- @gate: add
-- Depends on 1103
-- ============================================================================
-- 1118_owner_default_to_unattributed.sql [2026-07-17] — Sam ruling 2026-07-16
-- (owner-attribution REVERSAL, recorded in
-- handoffs/2026-07-15-cold-email-bi-VISION-AND-STATE.md R22-R27 block):
-- campaigns whose owner_cm came from the charter's no-parenthetical -> IDO
-- FALLBACK are NOT Ido's — they become UNATTRIBUTED.
--
-- WHAT: core.campaign_offer_scope rows WHERE owner_basis = 'default'
-- (the 1103 OWNER CAVEAT encoded exactly this swappability: only 'default'
-- rows may change; 'name' / 'workspace' rows are never touched).
-- 956 rows at authoring time = the 927 seeded fallback rows + nightly
-- auto-appends since; all currently owner_cm='IDO'.
--   owner_cm    -> NULL           (render '(unattributed)' at the view/report layer)
--   owner_basis -> 'unattributed' (self-describing; preserves identifiability of
--                                  the former fallback cohort for any future re-ruling)
--
-- The ONLY owner-carrying object from the 1101-1114 batch is this table.
-- (1102 workspace_alias_unified.owner_cm = workspace-level CM-era lineage — a
-- different concept, not the campaign fallback — deliberately unchanged.)
-- The nightly auto-append (entities/mof_bi_history.py) ships in the same PR so
-- new no-parenthetical campaigns append as NULL/'unattributed', never 'IDO'.
--
-- Reversible: core.campaign_offer_scope_owner_backup_1118 holds the exact
-- pre-change (campaign_id, owner_cm, owner_basis). Idempotent: a second run
-- matches 0 rows (no 'default' rows remain).
-- ============================================================================

-- (1) BACKUP FIRST — exact pre-change owner values of every row this file touches.
CREATE TABLE IF NOT EXISTS core.campaign_offer_scope_owner_backup_1118 AS
SELECT campaign_id, owner_cm, owner_basis, now() AS backed_up_at
FROM core.campaign_offer_scope
WHERE owner_basis = 'default';

-- (2) THE REVERSAL — fallback-derived owners become unattributed.
UPDATE core.campaign_offer_scope
SET owner_cm    = NULL,
    owner_basis = 'unattributed'
WHERE owner_basis = 'default';
