-- @gate: add
-- Depends on 107
-- 1058_workspace_alias_im_bookings_labels.sql  [2026-06-30]
-- Seed core.workspace_alias for the bookings-portal im_bookings.workspace labels surfaced by the
-- core.meeting im_bookings cutover (DDL 1054/1055, PR #115). These labels were landing workspace_slug
-- NULL ('(unmapped)' segment) on the >=06-29 SMS-funding rows; mapping (per Sam 2026-06-30) routes
-- them to the correct canonical workspace. cm stays NULL — these are Sendivo SMS sub-accounts /
-- non-funding workspaces, which correctly carry NO CM credit under the D6 design (cm_workspace=NULL).
--
-- Mapping (Sam 2026-06-30):
--   'Sendivo', 'Sendivo R1', 'R1'  -> the SENDIVO Renaissance-1 SMS sub-account (slug
--                                     sendivo-renaissance-1) — NOT the Instantly cold-email ws.
--                                     (bare 'Sendivo' fallback: per-row phone->sub-account isn't
--                                     expressible in the label-based alias, so the dominant
--                                     SMS-funding ws per Sam.)
--   'Instantly'                    -> the Instantly Renaissance-1 cold-email ws (slug renaissance-1).
--   'Sendivo R3'                   -> NEW Sendivo Renaissance-3 SMS sub-account (mirrors the R1 form).
--   "Max's WS"                     -> Max's workspace (slug the-gatekeepers).
--   'Sendivo R4'..'R9'             -> intentionally LEFT UNMAPPED (junk; Sendivo only has Ren 1/2/3).
--   'Sendivo (Renaissance 1)'      -> intentionally NOT touched (already resolves to
--                                     sendivo-renaissance-1; remapping would shift frozen-FF rows).
-- INSERT ... ON CONFLICT DO NOTHING -> idempotent; never clobbers an existing alias row.
INSERT INTO core.workspace_alias (alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm) VALUES
  ('Sendivo',    'Sendivo (Renaissance 1)',   'active', 'sendivo-renaissance-1', NULL, NULL),
  ('Sendivo R1', 'Sendivo (Renaissance 1)',   'active', 'sendivo-renaissance-1', NULL, NULL),
  ('R1',         'Sendivo (Renaissance 1)',   'active', 'sendivo-renaissance-1', NULL, NULL),
  ('Sendivo R3', 'Sendivo (Renaissance 3)',   'active', 'sendivo-renaissance-3', NULL, NULL),
  ('Instantly',  'Renaissance 1 (Instantly)', 'active', 'renaissance-1',         NULL, NULL),
  ('Max''s WS',  'Max''s workspace',          'active', 'the-gatekeepers',       NULL, NULL)
ON CONFLICT (alias_name) DO NOTHING;
