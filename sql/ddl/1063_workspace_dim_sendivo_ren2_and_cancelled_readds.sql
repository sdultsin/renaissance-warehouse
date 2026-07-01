-- @gate: add
-- Depends on 103 107
-- 1063_workspace_dim_sendivo_ren2_and_cancelled_readds.sql  [2026-07-01]  DATA-2 + DATA-7
--
-- Meeting->workspace reconciliation dimension seeds (deliverables/2026-07-01-meeting-attribution-
-- reconciliation/RECONCILIATION.md, §3-B3/§3-B4/§5):
--
--  (1) DATA-2: a real core.workspace row for the SYNTHETIC Sendivo desk slug
--      'sendivo-renaissance-2', so the Pre-IPO partner-desk meetings (Collins/Summit -> Sendivo
--      Renaissance 2, entities/meeting.py step 2.5 B3 rule) carry a workspace_id and never fall
--      out of workspace-keyed KPIs. This is NOT the existing slug 'renaissance-2' (= Funding 5 /
--      Eyver, UUID f02d3d50) — reusing it would mis-credit Pre-IPO desk meetings to Funding 5.
--      core.workspace_alias already maps the label 'Sendivo (Renaissance 2)' -> this slug (DDL
--      107.0 seed); only the dim row was missing.
--  (2) DATA-7: re-add the 4 cancelled workspace slugs dropped from core.workspace when the
--      workspaces were cancelled ~2026-06-17 (outlook-2, renaissance-6, renaissance-7, erc-2).
--      1,396 all-time slack-source meetings resolve workspace ONLY through
--      raw_pipeline_campaigns.workspace_id(SLUG) -> core.workspace.slug (meeting.py step 2f);
--      without these dim rows the join misses. Names: curated display names (the live Instantly
--      API keys are DEAD 401/402 — see core.v_instantly_workspace_crosswalk, DDL 1022); erc-2 has
--      no recorded display name anywhere -> curated 'ERC 2' (mirrors the 'Outlook 2' convention).
--
-- workspace_id: no Instantly UUID exists for any of these (never mirrored / synthetic desk;
-- verified raw_instantly_workspace + raw_instantly_campaign carry ZERO rows for them). PK uses a
-- clearly-marked 'synthetic:<slug>' id so every UUID-keyed join (core.campaign.workspace_id,
-- account census organization, DDL 68's f.workspace_id fallback join) stays inert — ONLY
-- slug-keyed joins (meeting step 2f, portal_data workspace labels) resolve. Deliberately NOT the
-- bare slug: raw_pipeline_* facts carry slugs in workspace_id columns and a bare-slug PK would
-- silently activate the never-matching slug-vs-UUID joins (behavior change outside DATA-2 scope).
--
-- is_active = FALSE for all 5: the 4 are cancelled; the Sendivo desk is not a live Instantly
-- workspace (the nightly ingest flips non-polled rows FALSE anyway — seeding FALSE avoids flap).
-- NB: if a cancelled workspace were ever revived in Instantly, the ingest would add its real-UUID
-- row alongside this one (core.workspace.slug has no UNIQUE constraint) — a slug dup would fan out
-- every `ON w.slug = ...` join (dashboard_data.py, portal_data.py, 86_dash_views, meeting step 2f
-- feeds). Accepted as ~0-probability (keys dead/deleted upstream); on any revival DELETE the
-- synthetic row first. Step 2f itself stays single-valued per campaign (ROW_NUMBER pick).
--
-- NB (DATA-2 item "extend the 1058 label seed"): verified 2026-07-01 — NOTHING to extend. The 10
-- remaining unmapped im_bookings labels (Sendivo R4..R9 typos, 2 email-addresses-as-workspace,
-- 'F1', 'Whatsapp') are the documented junk set (RECONCILIATION.md §4-B1): leave unmapped, never
-- force phantom workspaces. The 'Sendivo (Renaissance 2)' alias already exists (DDL 107).
--
-- Idempotent: INSERT ... WHERE NOT EXISTS on both workspace_id and slug; safe to re-run.

INSERT INTO core.workspace
  (workspace_id, slug, name, is_active, first_seen_at, last_seen_at, resolved_at)
SELECT v.workspace_id, v.slug, v.name, v.is_active, now(), now(), now()
FROM (VALUES
  ('synthetic:sendivo-renaissance-2', 'sendivo-renaissance-2', 'Sendivo (Renaissance 2)', FALSE),
  ('synthetic:outlook-2',             'outlook-2',             'Outlook 2',               FALSE),
  ('synthetic:renaissance-6',         'renaissance-6',         'Renaissance 6',           FALSE),
  ('synthetic:renaissance-7',         'renaissance-7',         'Renaissance 7',           FALSE),
  ('synthetic:erc-2',                 'erc-2',                 'ERC 2',                   FALSE)
) AS v(workspace_id, slug, name, is_active)
WHERE NOT EXISTS (SELECT 1 FROM core.workspace w
                  WHERE w.workspace_id = v.workspace_id OR w.slug = v.slug);

-- Slug -> workspace_id crosswalk (103 pattern; the nightly ingest would auto-extend this from
-- core.workspace anyway — seeding here makes the DDL self-contained for the apply-now path).
-- Derived FROM core.workspace (post-insert) rather than a second VALUES list, so the crosswalk can
-- never reference a workspace_id the first INSERT skipped (no dangling alias under any guard path).
INSERT INTO core.workspace_slug_alias (workspace_slug, workspace_id, alias_source, resolved_at)
SELECT w.slug, w.workspace_id, 'curated', now()
FROM core.workspace w
JOIN (VALUES ('sendivo-renaissance-2'), ('outlook-2'), ('renaissance-6'),
             ('renaissance-7'), ('erc-2')) AS v(slug) ON v.slug = w.slug
WHERE w.slug NOT IN (SELECT workspace_slug FROM core.workspace_slug_alias);

-- core.workspace_alias label completeness (adversarial-review NIT 2026-07-01): the canonical label
-- form 'Sendivo (Renaissance 3)' had no alias row (DDL 1058 seeds only the 'Sendivo R3' junk-label
-- form), so a meeting step-2e phone match on a Renaissance-3 Sendivo send would set
-- sendivo_sub_account but resolve no workspace. Mirrors the DDL 107.0 Ren1/Ren2 rows; cm NULL
-- (Sendivo sub-accounts never credit a CM under D6).
INSERT INTO core.workspace_alias (alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
SELECT * FROM (VALUES
  ('Sendivo (Renaissance 3)', 'Sendivo (Renaissance 3)', 'active', 'sendivo-renaissance-3', NULL, NULL)
) AS v(alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
WHERE NOT EXISTS (SELECT 1 FROM core.workspace_alias a WHERE a.alias_name = v.alias_name);
