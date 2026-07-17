-- @gate: data-backfill (owner recompute; no schema change; no column add/rename/drop)
-- Depends on 1103 (core.campaign_offer_scope), 1102 (core.workspace_alias_unified), 1118
-- ============================================================================
-- 1119_owner_deterministic_r31.sql [2026-07-17] — Sam ruling R31, 2026-07-16/17
-- (recorded in handoffs/2026-07-15-cold-email-bi-VISION-AND-STATE.md rulings table).
--
-- SUPERSEDES the 1118 unattributed reversal (the ledger keeps both states via the
-- 1118/1119 backup tables): DETERMINISTIC 100% owner attribution, two-step:
--
--   STEP 1 — NAME TOKEN WINS ('name_parse'), four tiers, first hit wins:
--     t1  parenthetical owner token:  '... (Marcos)' / '(SAM)'  (any case)
--     t2  BARE TRAILING CM name:      '... - EYVER'  (Sam: 100% of Funding-5
--         campaigns end with an un-parenthesized Eyver; the old regex missed all)
--     t3  UNIQUE standalone CM token anywhere: exactly one distinct owner token
--         appears as a whole word ('OLD - A - OWNERS NOT 2 - LAUTARO (copy)') —
--         required to reproduce every pre-1119 'name'-basis owner (0-regression
--         proven on the 2026-07-17 serving snapshot before authoring)
--     t4  workspace-scoped contains-rules per Sam:
--           renaissance-5 (Funding 2): any SAM token  -> SAM
--           renaissance-1:             any IDO token  -> IDO
--   STEP 2 — WORKSPACE DEFAULT when no name token ('workspace_default'):
--     renaissance-4 -> SAMUEL   renaissance-5 -> IDO      prospects-power -> LEO
--     koi-and-destroy -> SAM    renaissance-2 -> EYVER    the-gatekeepers -> MAX
--     tariffs -> IDO            renaissance-1 -> INSTANTLY_DFY (unless IDO in name)
--     warm-leads -> WL          (named campaign inside warm-leads -> that name, via t1-t3)
--   DELETED / other workspaces: STEP 1 -> alias-map dominant CM where UNAMBIGUOUS
--     (core.workspace_alias_unified.owner_cm carries a single CM, no '/'):
--     'alias_dominant' -> else owner_cm NULL, basis 'unattributed' (render
--     '(unattributed)' at the view/report layer, 1118 convention).
--
-- owner_basis taxonomy after this file (complete):
--   'name_parse' | 'workspace_default' | 'alias_dominant' | 'unattributed'
--
-- OWNER TOKEN SET = the 13 historic CM tags observed in pre-1119 'name'-basis rows
-- (SAM LEO EYVER TOMI SAMUEL LAUTARO MARCOS ANDRES SHAAN CARLOS BRENDAN IDO AYMAN)
-- + MAX WL JESSICA KEN ISAAC (the unowned-campaigns-scan owner tag set) — non-owner
-- parentheticals like (ARCHIVE)/(copy)/(GENERAL) never match by construction.
--
-- Verified read-only on serving snapshot warehouse_20260717_022909_705 (dry-run):
--   name_parse 2,426 (t1 2,156 · t2 239 · t3 31) · workspace_default 506 ·
--   alias_dominant 27 · unattributed 393 (ALL in deleted/non-live workspaces;
--   live-workspace unattributed = 0). Old 'name'-basis owners changed: 0.
--
-- The nightly auto-append (entities/mof_bi_history.py::_OFFER_SCOPE_APPEND) ships
-- the SAME derivation in the same PR — keep the two in sync.
--
-- Reversible: core.campaign_offer_scope_owner_backup_1119 holds the full pre-change
-- (campaign_id, owner_cm, owner_basis). Idempotent: recompute is a pure function of
-- (campaign_name, workspace_slug, alias map); a second run rewrites identical values.
-- ============================================================================

-- (1) BACKUP FIRST — full pre-change owner state (every row may change basis).
CREATE TABLE IF NOT EXISTS core.campaign_offer_scope_owner_backup_1119 AS
SELECT campaign_id, owner_cm, owner_basis, now() AS backed_up_at
FROM core.campaign_offer_scope;

-- (2) THE DETERMINISTIC RECOMPUTE (all rows).
UPDATE core.campaign_offer_scope AS s
SET owner_cm    = d.new_owner,
    owner_basis = d.new_basis
FROM (
    WITH alias_cm AS (
        SELECT warehouse_slug, any_value(owner_cm) AS ws_cm
        FROM core.workspace_alias_unified
        GROUP BY 1
    ),
    base AS (
        SELECT sc.campaign_id, sc.campaign_name, sc.workspace_slug,
               list_filter(
                   regexp_extract_all(COALESCE(sc.campaign_name,''), '\(([A-Za-z]{2,10})\)', 1),
                   x -> list_contains(['SAM','IDO','LEO','EYVER','SAMUEL','TOMI','CARLOS','SHAAN','MAX',
                                       'LAUTARO','MARCOS','ANDRES','BRENDAN','AYMAN','WL','JESSICA','KEN','ISAAC'],
                                      upper(x))
               )[1] AS paren_tok,
               upper(regexp_extract(COALESCE(sc.campaign_name,''), '([A-Za-z]+)[^A-Za-z]*$', 1)) AS trail_tok,
               regexp_matches(COALESCE(sc.campaign_name,''), '(^|[^A-Za-z])[Ss][Aa][Mm]([^A-Za-z]|$)') AS has_sam,
               regexp_matches(COALESCE(sc.campaign_name,''), '(^|[^A-Za-z])[Ii][Dd][Oo]([^A-Za-z]|$)') AS has_ido,
               list_distinct(list_filter(
                   regexp_extract_all(upper(COALESCE(sc.campaign_name,'')), '[A-Z]+'),
                   x -> list_contains(['SAM','IDO','LEO','EYVER','SAMUEL','TOMI','CARLOS','SHAAN','MAX',
                                       'LAUTARO','MARCOS','ANDRES','BRENDAN','AYMAN','WL','JESSICA','KEN','ISAAC'], x)
               )) AS anywhere_toks,
               a.ws_cm
        FROM core.campaign_offer_scope sc
        LEFT JOIN alias_cm a ON a.warehouse_slug = sc.workspace_slug
    ),
    derived AS (
        SELECT *,
            CASE
                WHEN paren_tok IS NOT NULL THEN upper(paren_tok)
                WHEN list_contains(['SAM','IDO','LEO','EYVER','SAMUEL','TOMI','CARLOS','SHAAN','MAX',
                                    'LAUTARO','MARCOS','ANDRES','BRENDAN','AYMAN','WL','JESSICA','KEN','ISAAC'],
                                   trail_tok) THEN trail_tok
                WHEN len(anywhere_toks) = 1 THEN anywhere_toks[1]
                WHEN workspace_slug = 'renaissance-5' AND has_sam THEN 'SAM'
                WHEN workspace_slug = 'renaissance-1' AND has_ido THEN 'IDO'
                ELSE NULL
            END AS name_owner
        FROM base
    )
    SELECT campaign_id,
        CASE
            WHEN name_owner IS NOT NULL THEN name_owner
            WHEN workspace_slug = 'renaissance-4'   THEN 'SAMUEL'
            WHEN workspace_slug = 'renaissance-5'   THEN 'IDO'
            WHEN workspace_slug = 'prospects-power' THEN 'LEO'
            WHEN workspace_slug = 'koi-and-destroy' THEN 'SAM'
            WHEN workspace_slug = 'renaissance-2'   THEN 'EYVER'
            WHEN workspace_slug = 'the-gatekeepers' THEN 'MAX'
            WHEN workspace_slug = 'tariffs'         THEN 'IDO'
            WHEN workspace_slug = 'renaissance-1'   THEN 'INSTANTLY_DFY'
            WHEN workspace_slug = 'warm-leads'      THEN 'WL'
            WHEN ws_cm IS NOT NULL AND ws_cm NOT LIKE '%/%' THEN ws_cm
            ELSE NULL
        END AS new_owner,
        CASE
            WHEN name_owner IS NOT NULL THEN 'name_parse'
            WHEN workspace_slug IN ('renaissance-4','renaissance-5','prospects-power','koi-and-destroy',
                                    'renaissance-2','the-gatekeepers','tariffs','renaissance-1','warm-leads')
                THEN 'workspace_default'
            WHEN ws_cm IS NOT NULL AND ws_cm NOT LIKE '%/%' THEN 'alias_dominant'
            ELSE 'unattributed'
        END AS new_basis
    FROM derived
) AS d
WHERE s.campaign_id = d.campaign_id;
