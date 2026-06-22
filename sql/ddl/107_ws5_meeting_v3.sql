-- @gate: add
-- Depends on 104
-- ============================================================================
-- 107_ws5_meeting_v3.sql — WS5 v3 (core.meeting canonical rebuild)
-- core.meeting: canonical meeting_date (col A) + workspace (col O, alias-resolved)
--   + offer + program + sendivo_sub_account; canonical serving view; and the
--   LOCKSTEP migration of the 11 posted_at-bucketing/MTD views to meeting_date.
--
-- VERSION SLOT: 107 (RENUMBERED from the source design's "106"). Live
--   MAX(core.schema_version)=104 this session (103=WS1 workspace-name-normalization,
--   104=WS2 account_census). Intended slot = 107 per RECONCILED-DEPLOY-PLAN §3.
--   The NN_ prefix IS the schema version. The Schema-Moderator RE-CHECKS
--   SELECT max(version) FROM core.schema_version immediately before apply and
--   bumps the whole remaining WS-block by any delta if the nightly moved the floor
--   (C1/C7). apply_ddl_file PK-dedupes on version → a stale/taken version SILENTLY
--   NO-OPS the whole migration, so the free-slot check is mandatory (107 verified
--   FREE this session: 0 rows at version=107).
--
-- ############################################################################
-- ## BLOCKER RESOLVED [2026-06-21] — resolution (a): core.workspace_alias is  ##
-- ## now shipped SELF-CONTAINED inside THIS DDL (block 107.0a below: CREATE    ##
-- ## TABLE + full seed). WS1 (v103) deployed a DIFFERENT shape                ##
-- ## (core.workspace + core.workspace_slug_alias [slug<->id only, no label,   ##
-- ## no cm]); the label->slug/cm alias the WS5 generator/ASSERT 3 need did    ##
-- ## NOT exist. 107.0a creates it. Seed is derived 1:1 from the VERIFIED live ##
-- ## core.workspace(slug,name) map (see comments) — NOT a hand CASE in the    ##
-- ## generator (design RB3). The generator JOIN (alias_name = raw col-O label)##
-- ## and ASSERT 3/3b now resolve against this seeded table.                   ##
-- ## VERIFIED live this session: core.workspace_alias absent, version 107 free##
-- ## (MAX=104), seed re-validated against live core.workspace(slug,name).     ##
-- ############################################################################
--
-- HARD DEP: core.workspace_alias — now SELF-CONTAINED (block 107.0a). No WS1 dependency.
-- SOFT DEP: core.campaign.offer (WS7) — ALREADY LIVE this session (email-offer UPDATE
--   is safe to run; until populated, offer stays NULL, the chain is still complete).
-- Idempotent: ADD COLUMN IF NOT EXISTS; core.meeting is a pure rebuild (entities/meeting.py
--   repopulates these columns every run); the ALTERs just give the projection somewhere to land.
-- Gated via apply_ddl_file(version=107). Avoid the 03:30–05:45 UTC nightly write window.
-- ============================================================================

-- 107.0a core.workspace_alias — SELF-CONTAINED (resolution a). The label->slug/cm crosswalk WS5 needs.
--       KEY: alias_name = the RAW col-O label as Grace types it in the sheet (bare, e.g. 'Funding 2',
--       'Renaissance 1', 'Warm Leads'); the generator joins a.alias_name = f.workspace_name (the raw
--       label) — NO hand CASE (design RB3). canonical_current_name / warehouse_slug are taken 1:1 from
--       the VERIFIED live core.workspace(slug,name) map [2026-06-21], so workspace_canonical matches the
--       suffixed current names (ASSERT 3 GT). cm = the WORKSPACE-credited CM (D6); NULL = never credits.
--       Idempotent: CREATE TABLE IF NOT EXISTS + INSERT … WHERE NOT EXISTS (PK alias_name).
CREATE TABLE IF NOT EXISTS core.workspace_alias (
  alias_name             VARCHAR PRIMARY KEY,   -- raw col-O label as typed in Grace's sheet
  canonical_current_name VARCHAR,               -- = live core.workspace.name (suffixed current name)
  status                 VARCHAR,               -- 'active' | 'frozen' (historical/closed)
  warehouse_slug         VARCHAR,               -- = live core.workspace.slug
  instantly_uuid         VARCHAR,               -- workspace_id where known (audit; NULL ok)
  cm                     VARCHAR                 -- WORKSPACE-credited CM (D6); NULL = no CM credited
);

-- 107.0a seed — the 5 funding-CM workspaces + DFY + the 4 non-CM funding-program workspaces.
--   Mapping verified live against core.workspace(slug,name) [2026-06-21]:
--     renaissance-4=Funding 1 (Samuel)→SAMUEL · renaissance-5=Funding 2 (Ido)→IDO ·
--     prospects-power=Funding 3 (Leo)→LEO · koi-and-destroy=Funding 4 (Sam)→SAM ·
--     renaissance-2=Funding 5 (Eyver)→EYVER · renaissance-1=Renaissance 1 (Instantly)=DFY cm NULL ·
--     warm-leads=Warm leads cm NULL · equinox=RE Wholesale · outlook-1=Funding Canada ·
--     automated-applications=Funding UK · section-125-2=Section 125 (frozen-historical, D2).
--   ASSERT 3b invariant: ONLY the 5 funding slugs (renaissance-4/5, prospects-power, koi-and-destroy,
--   renaissance-2) carry a non-NULL cm; everything else (incl. DFY, warm-leads, RE/Canada/UK/125) cm NULL.
INSERT INTO core.workspace_alias (alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
SELECT * FROM (VALUES
  -- 5 funding-CM workspaces (cm credited) — alias_name is the bare label Grace types in col O
  ('Funding 1',    'Funding 1 (Samuel)',        'active','renaissance-4',  'cdae94c6-5a88-4614-92e2-09e28a073a2e','SAMUEL'),
  ('Funding 2',    'Funding 2 (Ido)',           'active','renaissance-5',  '88de6a7c-55db-4594-8851-ed7d56342a45','IDO'),
  ('Funding 3',    'Funding 3 (Leo)',           'active','prospects-power','d5ebf2bd-d7c8-4feb-8310-e57e6140e12a','LEO'),
  ('Funding 4',    'Funding 4 (Sam)',           'active','koi-and-destroy','6ab744f5-be81-4c5b-8333-c0c119a19b80','SAM'),
  ('Funding 5',    'Funding 5 (Eyver)',         'active','renaissance-2',  'f02d3d50-0e9f-4687-981d-6134e789baa4','EYVER'),
  -- DFY: a funding-program workspace but NOT a CM's — cm NULL (never credits IDO/anyone) (D6)
  ('Renaissance 1','Renaissance 1 (Instantly)', 'active','renaissance-1',  '587765d7-e9ed-4057-85d1-eca48bcc9384',NULL),
  -- warm-leads: own segment, cm NULL (the IDO-leak fix — must NOT credit a CM)
  ('Warm Leads',   'Warm leads',                'active','warm-leads',     '58ae9dc4-9bc0-46d6-beb2-a1dc3e99cbf5',NULL),
  -- 4 non-CM funding-program / frozen workspaces (keep workspace+offer; cm NULL)
  ('RE Wholesale', 'RE Wholesale',              'active','equinox',        '634b4eac-8903-48a6-9361-bf1d52a13476',NULL),
  ('Funding Canada','Funding Canada',           'active','outlook-1',      'ddbeb975-fafb-4412-ae8f-d3b478f6abff',NULL),
  ('Funding UK',   'Funding UK',                'active','automated-applications','7d4e8e68-db7c-427c-a5eb-9675c0d1f3e8',NULL),
  ('Section 125',  'Section 125',               'frozen','section-125-2',  '396288e0-48cc-456b-94f8-a49f093e90eb',NULL)
) v(alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
WHERE NOT EXISTS (SELECT 1 FROM core.workspace_alias a WHERE a.alias_name = v.alias_name);

-- 107.0 Supplemental workspace_alias seed for the col-O labels the 107.0a seed lacks (RB5).
--       ALL cm=NULL — none of these is a Funding CM's workspace, so they keep workspace+offer but NEVER
--       credit a CM (D6). NB on Max's workspace (slug the-gatekeepers, labels 'Max WS' + historical
--       'Funding 6'): col-O carries 8 EMAIL funding-form rows whose col-17 CM Grace typed as Samuel/Ido
--       — EXACTLY the mis-typed-CM leak class D6 kills. cm MUST be NULL here: (1) it is not a Funding
--       CM's workspace, (2) cm=MAX would trip ASSERT 3b (the-gatekeepers ∉ the funding-5 whitelist) AND
--       wrongly credit MAX for 8 mis-typed funding-form meetings. Pre-IPO/Max attribution, if ever
--       needed, is sourced OUTSIDE this sheet (C8), never fabricated from col-O here.
INSERT INTO core.workspace_alias (alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
SELECT * FROM (VALUES
  ('Sendivo (Renaissance 1)','Sendivo (Renaissance 1)','active','sendivo-renaissance-1',NULL,NULL),
  ('Sendivo (Renaissance 2)','Sendivo (Renaissance 2)','active','sendivo-renaissance-2',NULL,NULL),
  ('ISKRA','ISKRA','active','iskra',NULL,NULL),
  ('Max WS', 'Max''s workspace','active','the-gatekeepers','9e822ccc-549d-4d91-ac13-a9c313af8fd3',NULL),
  ('Funding 6','Max''s workspace','active','the-gatekeepers','9e822ccc-549d-4d91-ac13-a9c313af8fd3',NULL)
) v(alias_name, canonical_current_name, status, warehouse_slug, instantly_uuid, cm)
WHERE NOT EXISTS (SELECT 1 FROM core.workspace_alias a WHERE a.alias_name = v.alias_name);
-- NB: 'Warm Leads' moved to the 107.0a seed. Empty/NULL col-O label is handled in the generator
--     (routes to '(unmapped)' segment + a warning) — no alias row needed.

-- 107.1 Canonical business date (col A) + audit timestamp (col C).
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS meeting_date  DATE;       -- CANONICAL (col A) — day-bucket on THIS
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS submission_ts TIMESTAMP;  -- raw col C, audit only

-- 107.2 Workspace dimension (col O), alias-resolved (D6). workspace_name=sheet label;
--       workspace_slug + cm_workspace come from core.workspace_alias (NOT a hand CASE).
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS workspace_name VARCHAR;   -- raw col-O label
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS workspace_slug VARCHAR;   -- alias.warehouse_slug
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS workspace_canonical VARCHAR; -- alias.canonical_current_name
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS cm_workspace VARCHAR;     -- alias.cm (the WORKSPACE-credited CM; NULL=no CM)

-- 107.3 Offer + program + Sendivo sub-account (D7).
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS offer               VARCHAR; -- Business Funding|Pre-IPO|... (email via campaign.offer; SMS via sub-account)
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS program             VARCHAR; -- 'Funding' (default) | 'Pre-IPO' (SMS Ren2). Never substring-inferred.
ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS sendivo_sub_account VARCHAR; -- Renaissance 1|2|3 for SMS; NULL otherwise

CREATE INDEX IF NOT EXISTS ix_core_meeting_meeting_date ON core.meeting (meeting_date);
CREATE INDEX IF NOT EXISTS ix_core_meeting_ws_slug      ON core.meeting (workspace_slug);

-- 107.4 Canonical serving view (portal/dashboards READ this; never re-derive). All col-O labels
--       resolve via the alias join in the generator; this view exposes the resolved columns +
--       a reporting_segment. Section 125 -> frozen-historical (D2). cm_workspace = the only
--       CM-credit source (workspace-based per D6); raw m.cm kept as cm_raw for audit.
CREATE OR REPLACE VIEW core.v_meeting_canonical AS
SELECT
  m.meeting_id, m.source, m.source_event_id,
  m.meeting_date,                       -- CANONICAL (col A)
  m.posted_at, m.submission_ts,         -- audit (col C)
  m.channel, m.partner, m.partner_key,
  m.campaign_id, m.campaign_name_raw, m.match_method, m.match_confidence,
  m.cm                AS cm_raw,         -- raw sheet col-17 (audit; Grace types IDO on non-CM work)
  m.cm_workspace      AS cm,             -- WORKSPACE-credited CM (D6) — NULL for non-funding-CM workspaces
  m.workspace_name, m.workspace_slug, m.workspace_canonical,
  m.offer, m.program, m.sendivo_sub_account,
  CASE
    WHEN m.workspace_canonical = 'Section 125' THEN 'Section 125 (frozen)'   -- D2 frozen-historical
    WHEN m.workspace_slug IS NULL OR m.workspace_canonical IS NULL THEN '(unmapped)'
    ELSE m.workspace_canonical                                               -- e.g. 'Funding 2 (Ido)','Warm Leads'
  END AS reporting_segment,
  m.lead_email, m.advisor, m.advisor_name, m.advisor_partner, m.inbox_manager
FROM core.meeting m;

-- 107.5 LOCKSTEP MIGRATION — 11 views (9 day-bucket swap + 2 explicit MTD migration).
-- DEPLOY MECHANISM: the apply step regenerates each of the 9 day-bucket views from its LIVE
--   information_schema.views.view_definition with the broadened token substitution
--     regexp_replace(def, 'CAST\(([a-z_]+\.)?posted_at AS DATE\)', 'COALESCE(\1meeting_date, CAST(\1posted_at AS DATE))')
--   applied ONLY to the core.meeting leg. (\1 preserves the alias: m. / mm. / '' — RB1 fix.)
--   COALESCE keeps pre-cutover slack rows [meeting_date NULL] bucketing on posted_at — no data loss.
-- The 2 MTD views are migrated by the EXPLICIT bodies below (the swap does not touch date_trunc predicates).
-- ASSERT 5 (broadened) is the post-condition proving zero residual day-bucket on posted_at.
-- ALL 11 target views VERIFIED PRESENT in the live schema this session.

-- --- The 9 day-bucket views (swap, alias-preserving). Worked examples (highest-traffic) written in full: ---

CREATE OR REPLACE VIEW main.v_kpi_sms AS
  (SELECT p.metric_date AS date, p.campaign_id, p.campaign_name, p.sub_account_name,
          p.sent, p.delivered, p.replies, p.positive_replies AS opportunities,
          p.opt_outs, p.cost_usd, 0 AS meetings,
          (CAST(p.positive_replies AS DOUBLE) / nullif(p.delivered,0)) AS opp_rate,
          (CAST(p.replies AS DOUBLE) / nullif(p.delivered,0)) AS reply_rate
   FROM main.v_sms_campaign_performance AS p)
  UNION ALL
  (SELECT COALESCE(m.meeting_date, CAST(m.posted_at AS DATE)) AS date,   -- WAS CAST(m.posted_at AS DATE)
          NULL AS campaign_id, '(sms meetings)' AS campaign_name, NULL AS sub_account_name,
          0 AS sent, 0 AS delivered, 0 AS replies, 0 AS opportunities, 0 AS opt_outs,
          0.0 AS cost_usd, count_star() AS meetings, NULL AS opp_rate, NULL AS reply_rate
   FROM core.meeting AS m
   WHERE m.is_duplicate_of IS NULL AND m.source='sheet' AND m.channel='SMS'
   GROUP BY 1);

-- dash.lens_campaign_performance__workspaces: the mtg_roll CTE's `CAST(mm.posted_at AS DATE)` becomes
--   `COALESCE(mm.meeting_date, CAST(mm.posted_at AS DATE))` (alias mm preserved — the RB1 fix). The rest
--   of the body (send_roll/mtg_roll/FULL JOIN/workspace CASE) is regenerated unchanged from the live def.

-- --- The 2 EXPLICIT MTD migrations (date_trunc predicate → meeting_date) ---

-- dash.warehouse_overview__partners: migrate the MTD boolean to meeting_date.
CREATE OR REPLACE VIEW dash.warehouse_overview__partners AS
WITH em AS (
  SELECT CASE
           WHEN m.partner IN ('GreenBridge','GreenBridge Capital') THEN 'GreenBridge Capital'
           WHEN m.partner IN ('BTC','Big Think Capital')           THEN 'Big Think Capital'
           WHEN m.partner IN ('Qualifi','GoQualifi')               THEN 'GoQualifi'
           WHEN m.partner IN ('Llama','Llama Funding')             THEN 'Llama'
           WHEN m.partner IS NULL OR m.partner = ''                THEN '(unattributed)'
           ELSE m.partner END AS partner,
         (COALESCE(m.meeting_date, CAST(m.posted_at AS DATE))      -- WAS m.posted_at
            >= date_trunc('month', current_date)) AS is_mtd
  FROM core.meeting AS m
  WHERE CASE WHEN m.source='sheet' THEN m.channel='Email'
             ELSE NOT regexp_matches(lower(COALESCE(m.campaign_name_raw,'')||' '||COALESCE(m.raw_text,'')),
                                     'sendivo|\bsms\b|whatsapp|iskra') END
)
SELECT partner, count_star() FILTER (WHERE is_mtd) AS mtd_meetings, count_star() AS all_time_meetings
FROM em GROUP BY partner ORDER BY all_time_meetings DESC;

-- dash.warehouse_overview__kpi: migrate BOTH the MTD boundary predicates AND the record-day
--   CAST(posted_at AS DATE) buckets to meeting_date (regenerate from live def, replacing every
--   `m.posted_at`/`posted_at` in a core.meeting subquery with COALESCE(meeting_date,CAST(posted_at AS DATE))
--   in the >=/< MTD predicates AND the `CAST(posted_at AS DATE)` record-day GROUP BY). Full-rewrite
--   in the apply step from its live definition; not hand-typed here to avoid drift on the 6 scalar subqueries.

-- The remaining day-bucket views regenerated by the same alias-preserving swap:
--   dash.lens_campaign_performance__meetings, dash.lens_kpi__email (×2 casts),
--   derived.v_advisor_alltime, derived.v_inbox_manager_alltime, main.v_kpi_email,
--   main.v_omni_sms_performance, main.v_workspace_daily.
