-- Schema-gate Phase 1 — column_aliases seed (first curation pass). Version 85.
-- @gate: add
-- Depends on 84
--
-- The deterministic dupe authority. Each row says: "if an editor proposes column
-- <alias>, the canonical name for that concept is <canonical_name>." The gate then
-- WARNs (Phase 1) / BLOCKs (Phase 2) an ADD COLUMN that uses a known synonym when the
-- canonical already exists in the catalog.
--
-- GROUNDED IN THE LIVE SCHEMA (not invented): the canonicals below are the names that
-- actually exist today across the ~60 warehouse tables (verified via core.schema_catalog):
--   email, lead_email, workspace_id, campaign_id, domain, created_at, phone_e164,
--   mobile_e164, opportunities, emails_sent, ...
-- The aliases are the common synonyms an editor (or a Claude) is most likely to reach
-- for instead — the `email` vs `email_address` failure class from the 2026-06-18 meeting.
--
-- This is the SEED for Sam + Thomas to sign off (BUILD-SPEC §12.3). Append/edit freely:
-- the table is curated + append-only. scope='global' = any table; scope='schema.table'
-- to scope an alias to one table only.
--
-- Idempotent: ON CONFLICT (alias, scope) DO NOTHING — re-running setup_db won't dupe.
-- Additive only. Fully reversible (DELETE these rows / DROP core.column_aliases).

CREATE SCHEMA IF NOT EXISTS core;

INSERT INTO core.column_aliases (alias, canonical_name, scope, reason, added_by) VALUES
  -- ── email ─────────────────────────────────────────────────────────────────
  ('email_address',   'email',        'global', 'canonical is `email`',                 'schema-gate-seed'),
  ('emailaddress',    'email',        'global', 'canonical is `email`',                 'schema-gate-seed'),
  ('e_mail',          'email',        'global', 'canonical is `email`',                 'schema-gate-seed'),
  ('mail',            'email',        'global', 'canonical is `email`',                 'schema-gate-seed'),
  ('contact_email',   'lead_email',   'global', 'lead-level email is `lead_email`',     'schema-gate-seed'),
  ('lead_email_address','lead_email', 'global', 'lead-level email is `lead_email`',     'schema-gate-seed'),
  -- ── phone ─────────────────────────────────────────────────────────────────
  ('phone',           'phone_e164',   'global', 'phones stored E.164 -> `phone_e164`',  'schema-gate-seed'),
  ('phone_number',    'phone_e164',   'global', 'phones stored E.164 -> `phone_e164`',  'schema-gate-seed'),
  ('phonenumber',     'phone_e164',   'global', 'phones stored E.164 -> `phone_e164`',  'schema-gate-seed'),
  ('mobile',          'mobile_e164',  'global', 'mobiles stored E.164 -> `mobile_e164`','schema-gate-seed'),
  ('mobile_number',   'mobile_e164',  'global', 'mobiles stored E.164 -> `mobile_e164`','schema-gate-seed'),
  ('msisdn',          'phone_e164',   'global', 'canonical is `phone_e164`',            'schema-gate-seed'),
  -- ── workspace ─────────────────────────────────────────────────────────────
  ('ws_id',           'workspace_id', 'global', 'canonical is `workspace_id` (no abbrev)','schema-gate-seed'),
  ('workspaceid',     'workspace_id', 'global', 'canonical is `workspace_id`',          'schema-gate-seed'),
  ('org_id',          'workspace_id', 'global', 'a workspace IS the org unit here',     'schema-gate-seed'),
  ('workspace',       'workspace_id', 'global', 'reference the id, not a bare name',     'schema-gate-seed'),
  -- ── campaign ──────────────────────────────────────────────────────────────
  ('campaignid',      'campaign_id',  'global', 'canonical is `campaign_id`',           'schema-gate-seed'),
  ('cid',             'campaign_id',  'global', 'no cryptic abbrev — use `campaign_id`', 'schema-gate-seed'),
  ('camp_id',         'campaign_id',  'global', 'no abbrev — use `campaign_id`',         'schema-gate-seed'),
  -- ── domain ────────────────────────────────────────────────────────────────
  ('domain_name',     'domain',       'global', 'canonical is `domain`',                'schema-gate-seed'),
  ('hostname',        'domain',       'global', 'canonical is `domain` (or `mx_host`)',  'schema-gate-seed'),
  -- ── timestamps / dates ──────────────────────────────────────────────────────
  ('created',         'created_at',   'global', 'timestamps end `_at` -> `created_at`',  'schema-gate-seed'),
  ('createdat',       'created_at',   'global', 'snake_case -> `created_at`',           'schema-gate-seed'),
  ('create_date',     'created_at',   'global', 'timestamps end `_at` -> `created_at`',  'schema-gate-seed'),
  ('updated',         'updated_at',   'global', 'timestamps end `_at` -> `updated_at`',  'schema-gate-seed'),
  ('updatedat',       'updated_at',   'global', 'snake_case -> `updated_at`',           'schema-gate-seed'),
  ('modified_at',     'updated_at',   'global', 'canonical mutate-ts is `updated_at`',   'schema-gate-seed'),
  ('loaded_at',       '_loaded_at',   'global', 'warehouse audit cols keep the `_` prefix','schema-gate-seed'),
  ('synced_at',       '_loaded_at',   'global', 'warehouse load-ts is `_loaded_at`',     'schema-gate-seed'),
  -- ── counts / metrics ──────────────────────────────────────────────────────
  ('email_count',     'emails_sent',  'global', 'sent-volume canonical is `emails_sent`','schema-gate-seed'),
  ('sent_count',      'emails_sent',  'global', 'sent-volume canonical is `emails_sent`','schema-gate-seed'),
  ('opps',            'opportunities','global', 'no abbrev — use `opportunities`',       'schema-gate-seed'),
  ('opp_count',       'opportunities','global', 'no abbrev — use `opportunities`',       'schema-gate-seed'),
  ('num_opportunities','opportunities','global','prefer `opportunities` (count implied)', 'schema-gate-seed'),
  -- ── identity ──────────────────────────────────────────────────────────────
  ('uuid',            'id',           'global', 'primary key is `id` unless qualified',  'schema-gate-seed'),
  ('guid',            'id',           'global', 'primary key is `id` unless qualified',  'schema-gate-seed'),
  ('leadid',          'lead_id',      'global', 'snake_case -> `lead_id`',              'schema-gate-seed')
ON CONFLICT (alias, scope) DO NOTHING;
