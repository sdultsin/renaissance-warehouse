-- @gate: add
-- Depends on 81
-- 103_workspace_name_normalization.sql  [2026-06-20]  WS1 portal-data-rebuild
-- Apply via apply_ddl_file(conn, <this file>, version=103).  RE-VERIFY MAX(version)+1 AT APPLY (C1).
-- Idempotent: CREATE TABLE IF NOT EXISTS + CREATE OR REPLACE VIEW + INSERT … WHERE NOT EXISTS.
-- Builds on the EXISTING UUID-keyed core.workspace (PK workspace_id, 16 rows) — does NOT replace it.
-- READ-ONLY on existing fact tables; writes only the 2 NEW side-tables, 2 additive core.workspace rows,
-- 1 new view, and CREATE OR REPLACE of 3 naming-consumer views (D4 re-point).

-- ─────────────────────────────────────────────────────────────────────────────
-- (A) core.workspace_slug_alias — historical-slug → stable workspace_id crosswalk (side-table,
--     email_domain_mx_relabel pattern; re-asserted nightly). FK → core.workspace.workspace_id.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.workspace_slug_alias (
  workspace_slug VARCHAR PRIMARY KEY,
  workspace_id   VARCHAR NOT NULL,
  alias_source   VARCHAR NOT NULL,    -- 'core_workspace' | 'raw_instantly_workspace' | 'curated' | 'live_api_slug'
  resolved_at    TIMESTAMPTZ NOT NULL
);

-- (A.1) Every core.workspace slug maps to its own UUID (covers the 16 keyed workspaces incl. erc-1,
--       koi-and-destroy, the-gatekeepers=Max's workspace, etc.).
INSERT INTO core.workspace_slug_alias (workspace_slug, workspace_id, alias_source, resolved_at)
SELECT w.slug, w.workspace_id, 'core_workspace', now()
FROM core.workspace w
WHERE w.slug IS NOT NULL
  AND w.slug NOT IN (SELECT workspace_slug FROM core.workspace_slug_alias);

-- (A.2) Auto-derive any extra slugs the poller saw in raw_instantly_workspace (multi-slug-per-UUID,
--       e.g. a future funding-4==koi-and-destroy). Today raw has no warm/dyad rows (verified 0).
INSERT INTO core.workspace_slug_alias (workspace_slug, workspace_id, alias_source, resolved_at)
SELECT DISTINCT r.slug, r.workspace_id, 'raw_instantly_workspace', now()
FROM main.raw_instantly_workspace r
WHERE r.slug IS NOT NULL
  AND r.slug NOT IN (SELECT workspace_slug FROM core.workspace_slug_alias);

-- (A.3) Curated aliases for the 3 fact slugs the poller never captured but that resolve to a known
--       workspace. 'tariffs' == erc-1 (same UUID 0d9ed15e). warm-leads uses the ORG UUID 58ae9dc4
--       (CONTRACT C4 — the account-census key), NOT the registry client_id 64606686. the-dyad uses
--       its live registry UUID 1265f3a5 (no org UUID exists; 0 live accounts, frozen daily rows only).
INSERT INTO core.workspace_slug_alias (workspace_slug, workspace_id, alias_source, resolved_at)
SELECT * FROM (VALUES
  ('tariffs',   '0d9ed15e-8fb9-4427-860e-99a403cea081', 'curated',       now()),  -- == erc-1 (Tariffs)
  ('the-dyad',  '1265f3a5-3e03-439a-81af-55842ce7fac3', 'live_api_slug', now()),  -- The Dyad (registry UUID)
  ('warm-leads','58ae9dc4-9bc0-46d6-beb2-a1dc3e99cbf5', 'curated',       now())   -- Warm leads (ORG UUID, C4)
) AS v(workspace_slug, workspace_id, alias_source, resolved_at)
WHERE v.workspace_slug NOT IN (SELECT workspace_slug FROM core.workspace_slug_alias);

-- (A.4) Backfill core.workspace dim rows for the-dyad + warm-leads so the alias FK + norm view land.
--       CONTRACT C4: warm-leads UUID 58ae9dc4 → 'Warm leads'. Names = live-API names. Idempotent on UUID.
--       NOTE: core.workspace has NOT NULL first_seen_at/last_seen_at/resolved_at (no default) — set to now().
INSERT INTO core.workspace
  (workspace_id, slug, name, is_active, first_seen_at, last_seen_at, resolved_at)
SELECT v.workspace_id, v.slug, v.name, v.is_active, now(), now(), now()
FROM (VALUES
  ('1265f3a5-3e03-439a-81af-55842ce7fac3','the-dyad',  'The Dyad',  FALSE),  -- no active paid plan (0 live accts)
  ('58ae9dc4-9bc0-46d6-beb2-a1dc3e99cbf5','warm-leads','Warm leads',TRUE)    -- 1,527 live accounts (C4 INCLUDE)
) AS v(workspace_id, slug, name, is_active)
WHERE v.workspace_id NOT IN (SELECT workspace_id FROM core.workspace);

-- ─────────────────────────────────────────────────────────────────────────────
-- (B) core.workspace_name_history — append-only rename trail (lineage). live_api slice rebuilt each
--     run; naming-doc slice (from workspace-rename-history.md) loaded once + preserved.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.workspace_name_history (
  workspace_id   VARCHAR     NOT NULL,
  name           VARCHAR     NOT NULL,
  name_source    VARCHAR     NOT NULL,    -- 'live_api' | 'naming_doc'
  first_seen_at  TIMESTAMPTZ NOT NULL,
  last_seen_at   TIMESTAMPTZ NOT NULL,
  is_current     BOOLEAN     NOT NULL,
  PRIMARY KEY (workspace_id, name, name_source)
);

DELETE FROM core.workspace_name_history WHERE name_source = 'live_api';
INSERT INTO core.workspace_name_history
  (workspace_id, name, name_source, first_seen_at, last_seen_at, is_current)
SELECT h.workspace_id, h.name, 'live_api', h.first_seen_at, h.last_seen_at, (h.name = w.name) AS is_current
FROM (
  SELECT workspace_id, TRIM(name) AS name,
         MIN(_loaded_at) AS first_seen_at, MAX(_loaded_at) AS last_seen_at
  FROM main.raw_instantly_workspace
  WHERE name IS NOT NULL AND TRIM(name) <> ''
  GROUP BY workspace_id, TRIM(name)
) h
LEFT JOIN core.workspace w ON w.workspace_id = h.workspace_id;

-- (B.1) Naming-doc lineage (from workspace-rename-history.md) → historical names per UUID, never
--       overriding the live current name (is_current=FALSE; live row above carries is_current=TRUE).
INSERT INTO core.workspace_name_history
  (workspace_id, name, name_source, first_seen_at, last_seen_at, is_current)
SELECT * FROM (VALUES
  ('6ab744f5-be81-4c5b-8333-c0c119a19b80','Big Think Capital','naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('6ab744f5-be81-4c5b-8333-c0c119a19b80','Koi and Destroy',  'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('6ab744f5-be81-4c5b-8333-c0c119a19b80','Funding 4',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-10', FALSE),
  ('9e822ccc-549d-4d91-ac13-a9c313af8fd3','The Gatekeepers',  'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('9e822ccc-549d-4d91-ac13-a9c313af8fd3','Funding 6',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-11', FALSE),
  ('9e822ccc-549d-4d91-ac13-a9c313af8fd3','Pre-IPO',          'naming_doc',TIMESTAMPTZ '2026-06-11', TIMESTAMPTZ '2026-06-16', FALSE),
  ('cdae94c6-5a88-4614-92e2-09e28a073a2e','Renaissance 4',    'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('cdae94c6-5a88-4614-92e2-09e28a073a2e','Funding 1',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-10', FALSE),
  ('88de6a7c-55db-4594-8851-ed7d56342a45','Renaissance 5',    'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('88de6a7c-55db-4594-8851-ed7d56342a45','Funding 2',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-10', FALSE),
  ('d5ebf2bd-d7c8-4feb-8310-e57e6140e12a','Power Prospect',   'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('d5ebf2bd-d7c8-4feb-8310-e57e6140e12a','Funding 3',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-10', FALSE),
  ('f02d3d50-0e9f-4687-981d-6134e789baa4','Renaissance 2',    'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('f02d3d50-0e9f-4687-981d-6134e789baa4','Funding 5',        'naming_doc',TIMESTAMPTZ '2026-05-14', TIMESTAMPTZ '2026-06-10', FALSE),
  ('634b4eac-8903-48a6-9361-bf1d52a13476','GreenBridge Capital','naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('634b4eac-8903-48a6-9361-bf1d52a13476','Equinox',          'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('7d4e8e68-db7c-427c-a5eb-9675c0d1f3e8','Automated Application','naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('ddbeb975-fafb-4412-ae8f-d3b478f6abff','Outlook 1',        'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-05-14', FALSE),
  ('0d9ed15e-8fb9-4427-860e-99a403cea081','ERC 1',            'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-04-20', FALSE),
  ('0d9ed15e-8fb9-4427-860e-99a403cea081','Tariffs + Funding','naming_doc',TIMESTAMPTZ '2026-04-20', TIMESTAMPTZ '2026-05-20', FALSE),
  ('0d9ed15e-8fb9-4427-860e-99a403cea081','Tariffs + Pre-IPO','naming_doc',TIMESTAMPTZ '2026-05-01', TIMESTAMPTZ '2026-05-20', FALSE),
  ('396288e0-48cc-456b-94f8-a49f093e90eb','R&D Credit + Pre-IPO','naming_doc',TIMESTAMPTZ '2026-05-01', TIMESTAMPTZ '2026-06-11', FALSE),
  ('587765d7-e9ed-4057-85d1-eca48bcc9384','Renaissance 1',    'naming_doc',TIMESTAMPTZ '2025-10-26', TIMESTAMPTZ '2026-06-10', FALSE)
) AS v(workspace_id, name, name_source, first_seen_at, last_seen_at, is_current)
WHERE NOT EXISTS (
  SELECT 1 FROM core.workspace_name_history h
  WHERE h.workspace_id = v.workspace_id AND h.name = v.name AND h.name_source = v.name_source
);

-- ─────────────────────────────────────────────────────────────────────────────
-- (C) core.v_workspace_norm — THE canonical normalizer: any historical slug → stable workspace_id
--     + CURRENT name. Dashboards READ this; never re-derive a name.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW core.v_workspace_norm AS
SELECT a.workspace_slug,
       a.workspace_id,
       COALESCE(w.name, a.workspace_slug) AS current_name,
       w.is_active
FROM core.workspace_slug_alias a
LEFT JOIN core.workspace w ON w.workspace_id = a.workspace_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- (D) Re-point main.v_workspace_name (campaign-side shim) at the normalizer. Keeps the existing
--     81_workspace_performance.sql joins working; now resolves account-side slugs + tariffs too.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW main.v_workspace_name AS
SELECT workspace_slug AS slug, current_name AS ws_name
FROM core.v_workspace_norm;

-- ─────────────────────────────────────────────────────────────────────────────
-- (E) D4 re-point of the two consumers that derive names INDEPENDENTLY + emit STALE names today.
-- ─────────────────────────────────────────────────────────────────────────────

-- (E.1) derived.v_workspace_send_daily: its core.workspace join was slug-vs-UUID mismatched (0 rows)
--       so it emitted the stale f.workspace_name ("Koi and Destroy","The Gatekeepers",…). Re-point to
--       the normalizer on slug so it shows the CURRENT name on all dates (D4). Preserve columns.
CREATE OR REPLACE VIEW derived.v_workspace_send_daily AS
SELECT f.date,
       COALESCE(f.workspace_id, '(unknown)')                         AS workspace_id,
       COALESCE(n.current_name, f.workspace_name, f.workspace_id)    AS workspace_name,
       COALESCE(n.current_name, f.workspace_name, f.workspace_id)    AS workspace_label,
       (n.is_active = FALSE)                                          AS workspace_deleted,
       sum(f.sent)                        AS sent,
       sum(f.unique_opportunities)        AS unique_opportunities,
       sum(f.unique_replies)              AS unique_replies,
       sum(f.unique_replies_automatic)    AS unique_replies_automatic
FROM main.raw_pipeline_campaign_daily_metrics f
LEFT JOIN core.v_workspace_norm n ON n.workspace_slug = f.workspace_id   -- f.workspace_id holds the SLUG
GROUP BY 1,2,3,4,5;
-- NOTE: drops the deleted_at/last_active_date columns the old view exposed (sourced from the dead
-- core.workspace UUID join). v_workspace_send_mtd reads only workspace_label/workspace_deleted/
-- last_active_date(max) — see E.2 which is updated to not reference last_active_date.

-- (E.2) derived.v_workspace_send_mtd reads v_workspace_send_daily; rebuild to match E.1's columns
--       (no last_active_date dependency) so it inherits current names.
CREATE OR REPLACE VIEW derived.v_workspace_send_mtd AS
SELECT workspace_id,
       any_value(workspace_label)  AS workspace_label,
       bool_or(workspace_deleted)  AS workspace_deleted,
       sum(sent)                   AS sent_mtd,
       sum(unique_replies)         AS replies_mtd,
       sum(unique_opportunities)   AS opps_mtd_trend
FROM derived.v_workspace_send_daily
WHERE date >= date_trunc('month', current_date)
GROUP BY workspace_id
ORDER BY sent_mtd DESC;

-- (E.3) dash.lens_campaign_performance__workspaces: replace its hard-coded 5-row CASE (stale short
--       names "Funding 1"…) with the normalizer, joined campaign workspace_name(slug) → current_name.
--       Full re-emit preserving the send/meeting FULL JOIN shape, swapping both CASE blocks for a
--       normalizer lookup keyed on the campaign's workspace_name (which is the slug).
CREATE OR REPLACE VIEW dash.lens_campaign_performance__workspaces AS
WITH camp AS (
  SELECT campaign_id, workspace_name,
         row_number() OVER (PARTITION BY campaign_id ORDER BY _loaded_at DESC) AS rn
  FROM main.raw_pipeline_campaigns
),
metrics AS (
  SELECT campaign_id, date, sent, unique_replies, unique_opportunities,
         row_number() OVER (PARTITION BY campaign_id, date ORDER BY _loaded_at DESC) AS rn
  FROM main.raw_pipeline_campaign_daily_metrics
),
sends AS (
  SELECT m.date,
         COALESCE(n.current_name,
                  NULLIF(c.workspace_name,''),
                  '(new / unmapped)')       AS workspace,
         m.sent, m.unique_replies, m.unique_opportunities
  FROM metrics m
  LEFT JOIN camp c ON c.campaign_id = m.campaign_id AND c.rn = 1
  LEFT JOIN core.v_workspace_norm n ON n.workspace_slug = c.workspace_name
  WHERE m.rn = 1 AND m.sent > 0
),
send_roll AS (
  SELECT date, workspace, sum(sent) AS sent, sum(unique_replies) AS replies,
         sum(unique_opportunities) AS opps
  FROM sends GROUP BY 1,2
),
camp_ws AS (
  SELECT campaign_id, arg_max(workspace_name, _loaded_at) AS workspace_name
  FROM main.raw_pipeline_campaigns GROUP BY 1
),
mtg_roll AS (
  SELECT CAST(mm.posted_at AS DATE) AS date,
         COALESCE(n.current_name, NULLIF(c.workspace_name,''), '(new / unmapped)') AS workspace,
         count_star() AS meetings
  FROM core.meeting mm
  LEFT JOIN camp_ws c USING (campaign_id)
  LEFT JOIN core.v_workspace_norm n ON n.workspace_slug = c.workspace_name
  WHERE mm.is_duplicate_of IS NULL
    AND lower(COALESCE(mm.raw_text,'')) NOT LIKE '%whatsapp%'
    AND lower(COALESCE(mm.raw_text,'')) NOT LIKE '%sms%'
    AND lower(COALESCE(mm.raw_text,'')) NOT LIKE '%sendivo%'
    AND lower(COALESCE(mm.raw_text,'')) NOT LIKE '%linkedin%'
    AND lower(COALESCE(mm.raw_text,'')) NOT LIKE '%sdr%'
  GROUP BY 1,2
)
SELECT COALESCE(s.date, t.date) AS date,
       COALESCE(s.workspace, t.workspace) AS workspace,
       COALESCE(s.sent,0) AS sent, COALESCE(s.replies,0) AS replies,
       COALESCE(s.opps,0) AS opps, COALESCE(t.meetings,0) AS meetings
FROM send_roll s
FULL JOIN mtg_roll t USING (date, workspace);
