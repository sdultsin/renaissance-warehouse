-- @gate: add
-- Depends on 1102
-- ============================================================================
-- 1122_alias_map_new_ws_live_keyed.sql [2026-07-17] — register the two NEW
-- live-keyed Instantly workspaces in core.workspace_alias_unified
-- (Sam-sanctioned 2026-07-17 new-workspace sync wiring):
--   * "Section 125"  workspace_id afeaedb1-b031-48ab-9581-64cdfc454fae
--     (created 2026-07-06, canonical slug section-125)
--   * "Growth 1"     workspace_id b29a7676-74d9-4bd9-a67b-0c8990c45ad8
--     (created 2026-07-14, canonical slug growth-1)
--
-- WHY: the alias-map ship seeded these as no-key entries (instantly_workspace_id
-- NULL, notes "no API key yet"). Keys are now live in the repo .env.instantly and
-- every droplet key store (warehouse .env, .env.threads, codex-ops Track-H map,
-- renaissance-worker twin); nightly pulls auto-discovered both on 07-16; thread
-- drain lanes + reply webhooks registered 07-17. The map must carry the workspace
-- UUIDs so uuid-stamped rows (raw thread/reply tables, webhook payloads) normalize
-- to the canonical slug — the daily labeler and reply-QA joins resolve workspace
-- via wa.alias = m.workspace_id.
--
-- WHAT (all idempotent; no row deletions):
--   1. Stamp instantly_workspace_id + live-keyed status on the three existing
--      rows (section-125 warehouse_slug; growth-1 warehouse_slug + display_name).
--   2. REPOINT the 'Section 125' display_name alias from the DELETED
--      section-125-2 generation (cancelled ~06-17) to the LIVE successor
--      workspace: current-state display-name joins must resolve to the live ws.
--      Prior mapping (section-125-2, deleted/frozen) preserved in notes.
--   3. INSERT the two uuid alias rows (WHERE NOT EXISTS guards).
-- ============================================================================

-- 1a. section-125 warehouse_slug row → live-keyed
UPDATE core.workspace_alias_unified
SET instantly_workspace_id = 'afeaedb1-b031-48ab-9581-64cdfc454fae',
    status_raw = 'live-keyed (2026-07-17)',
    notes = CASE WHEN notes LIKE '%[2026-07-17] live-keyed%' THEN notes
                 ELSE coalesce(notes, '') ||
                      ' | [2026-07-17] live-keyed: API key verified live (repo .env.instantly + droplet stores); wired into nightly pulls, Track-H daily metrics, thread-drain lanes (new-section125), pipeline reply webhooks (7 events). NOT in the comms funding sweep by design (King section=section_125, never business funding — R3).'
            END
WHERE warehouse_slug = 'section-125'
  AND alias = 'section-125'
  AND alias_kind = 'warehouse_slug';

-- 1b. growth-1 rows (warehouse_slug + display_name) → live-keyed
UPDATE core.workspace_alias_unified
SET instantly_workspace_id = 'b29a7676-74d9-4bd9-a67b-0c8990c45ad8',
    status_raw = 'live-keyed (2026-07-17)',
    notes = CASE WHEN notes LIKE '%[2026-07-17] live-keyed%' THEN notes
                 ELSE coalesce(notes, '') ||
                      ' | [2026-07-17] live-keyed: API key verified live (repo .env.instantly + droplet stores); wired into nightly pulls, Track-H daily metrics, thread-drain lanes (new-growth1), pipeline reply webhooks (7 events) + Comms Orch warm-call webhook; IN the comms funding sweep (King row GROWTH 1 section=funding, key live).'
            END
WHERE warehouse_slug = 'growth-1'
  AND alias IN ('growth-1', 'Growth 1');

-- 2. Repoint 'Section 125' display alias: deleted section-125-2 → live section-125
UPDATE core.workspace_alias_unified
SET warehouse_slug = 'section-125',
    instantly_workspace_id = 'afeaedb1-b031-48ab-9581-64cdfc454fae',
    status_class = 'live',
    status_raw = 'live-keyed (2026-07-17)',
    funding_relevant = 'n',
    notes = CASE WHEN notes LIKE '%[2026-07-17] REPOINTED%' THEN notes
                 ELSE coalesce(notes, '') ||
                      ' | [2026-07-17] REPOINTED display-name alias from section-125-2 (deleted generation, cancelled ~06-17) to the LIVE successor workspace afeaedb1 (created 07-06): current-state display-name joins must resolve to the live ws. The deleted generation stays reachable via its own warehouse_slug/uuid aliases.'
            END
WHERE alias = 'Section 125'
  AND alias_kind = 'display_name'
  AND warehouse_slug = 'section-125-2';

-- 3a. uuid alias row for section-125
INSERT INTO core.workspace_alias_unified
  (alias, alias_kind, warehouse_slug, instantly_workspace_id, status_class,
   status_raw, funding_relevant, owner_cm, notes)
SELECT 'afeaedb1-b031-48ab-9581-64cdfc454fae', 'uuid', 'section-125',
       'afeaedb1-b031-48ab-9581-64cdfc454fae', 'live', 'live-keyed (2026-07-17)',
       'n', NULL,
       '[2026-07-17] uuid alias for the live Section 125 workspace (created 07-06); added with the new-workspace sync wiring so uuid-stamped raw rows normalize to the canonical slug.'
WHERE NOT EXISTS (
  SELECT 1 FROM core.workspace_alias_unified
  WHERE alias = 'afeaedb1-b031-48ab-9581-64cdfc454fae'
);

-- 3b. uuid alias row for growth-1
INSERT INTO core.workspace_alias_unified
  (alias, alias_kind, warehouse_slug, instantly_workspace_id, status_class,
   status_raw, funding_relevant, owner_cm, notes)
SELECT 'b29a7676-74d9-4bd9-a67b-0c8990c45ad8', 'uuid', 'growth-1',
       'b29a7676-74d9-4bd9-a67b-0c8990c45ad8', 'live', 'live-keyed (2026-07-17)',
       'unknown', NULL,
       '[2026-07-17] uuid alias for the live Growth 1 workspace (created 07-14, offer TBD — classify by copy when it sends); added with the new-workspace sync wiring so uuid-stamped raw rows normalize to the canonical slug.'
WHERE NOT EXISTS (
  SELECT 1 FROM core.workspace_alias_unified
  WHERE alias = 'b29a7676-74d9-4bd9-a67b-0c8990c45ad8'
);
