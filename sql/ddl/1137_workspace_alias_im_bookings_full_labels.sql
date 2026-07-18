-- @gate: add
-- Depends on 107
-- 1137_workspace_alias_im_bookings_full_labels.sql  [2026-07-17]
-- Seed core.workspace_alias for the im_bookings.workspace FULL display labels that land
-- workspace_slug NULL in core.meeting (the nightly meeting entity warns: "workspace labels
-- unresolved by core.workspace_alias: ['Funding 5 (Eyver)', 'Funding 1 (Samuel)',
-- 'Funding 2 (Ido)', 'Funding 3 (Leo)', 'Tariffs', 'Close', 'Funding 4 (Sam)',
-- 'Max''s workspace']"). The 107.0a seed keys the BARE Grace col-O labels ('Funding 1');
-- the bookings portal writes the FULL canonical workspace names — same workspaces, different
-- alias_name key (PK). Same mechanism as 1058 (im_bookings labels supplemental seed).
--
-- Mapping — every alias below is 1:1 the LIVE core.workspace.name (verified read-only
-- 2026-07-17 against core.workspace(slug,name,workspace_id)); slug/uuid taken verbatim:
--   'Funding 1 (Samuel)' -> renaissance-4    cm SAMUEL  (177 unresolved meeting rows)
--   'Funding 2 (Ido)'    -> renaissance-5    cm IDO     (358)
--   'Funding 3 (Leo)'    -> prospects-power  cm LEO     (277)
--   'Funding 4 (Sam)'    -> koi-and-destroy  cm SAM     (315)
--   'Funding 5 (Eyver)'  -> renaissance-2    cm EYVER   (457)
--   'Max''s workspace'   -> the-gatekeepers  cm NULL    (14) — D6: NOT a funding-CM ws, never credits
--   'Tariffs'            -> tariffs          cm NULL    (6)  — the NEW live Tariffs ws (started
--                           07-06, id 36842873-c988-482d-bdbc-acd707db4cc9), NOT the dead erc-1
--                           lane (also named 'Tariffs' in core.workspace but deleted/key-402);
--                           label maps to the live lane. Out of funding scope, cm NULL.
--   'Close'              -> intentionally LEFT UNMAPPED: no matching core.workspace row exists
--                           (it is the Close-CRM booking source label, not an Instantly
--                           workspace); stays '(unmapped)' until a business mapping is decided.
-- cm on the five Funding rows follows the 107.0a design (D6: cm = the WORKSPACE-credited CM);
-- ASSERT 3b invariant holds — non-NULL cm only on the 5 whitelisted funding slugs
-- (renaissance-4/5, prospects-power, koi-and-destroy, renaissance-2).
-- INSERT ... ON CONFLICT DO NOTHING -> idempotent; never clobbers an existing alias row.
INSERT INTO core.workspace_alias (alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm) VALUES
  ('Funding 1 (Samuel)', 'Funding 1 (Samuel)', 'active', 'renaissance-4',   'cdae94c6-5a88-4614-92e2-09e28a073a2e', 'SAMUEL'),
  ('Funding 2 (Ido)',    'Funding 2 (Ido)',    'active', 'renaissance-5',   '88de6a7c-55db-4594-8851-ed7d56342a45', 'IDO'),
  ('Funding 3 (Leo)',    'Funding 3 (Leo)',    'active', 'prospects-power', 'd5ebf2bd-d7c8-4feb-8310-e57e6140e12a', 'LEO'),
  ('Funding 4 (Sam)',    'Funding 4 (Sam)',    'active', 'koi-and-destroy', '6ab744f5-be81-4c5b-8333-c0c119a19b80', 'SAM'),
  ('Funding 5 (Eyver)',  'Funding 5 (Eyver)',  'active', 'renaissance-2',   'f02d3d50-0e9f-4687-981d-6134e789baa4', 'EYVER'),
  ('Max''s workspace',   'Max''s workspace',   'active', 'the-gatekeepers', '9e822ccc-549d-4d91-ac13-a9c313af8fd3', NULL),
  ('Tariffs',            'Tariffs',            'active', 'tariffs',         '36842873-c988-482d-bdbc-acd707db4cc9', NULL)
ON CONFLICT (alias_name) DO NOTHING;
