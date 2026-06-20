-- Workstream F: core.funding_partner dimension (spec: funding-partner attribution).
-- Applied at schema version 25 by scripts/setup_db.py / orchestrator DDL applier.
--
-- One row per funding partner Renaissance books meetings to. This is a DESCRIPTIVE
-- dimension only: it stores reconciled aliases (the strings our data actually uses)
-- plus the commercial terms as LABELS. It is the canonical place to look up
-- "what does this partner pay us" — but it does NOT model revenue. Meetings stay flat
-- $145 in the dashboard (REVENUE_PER_MEETING); rev-share economics are deliberately
-- NOT computed anywhere yet (no settled-revenue source — bookmarked).
--
-- Alias reconciliation (verified 2026-05-31):
--   * core.meeting.partner (Slack-channel-sourced) uses SHORT forms: GreenBridge, BTC,
--     Qualifi, Llama.
--   * Darcy's im_bookings.partner (portal) uses LONG forms: GreenBridge Capital,
--     Big Think Capital, GoQualifi, Llama, DCX, Infusion, Clarify, Capfront.
--   The `aliases` array carries every observed string so both sources resolve to one key.
--
-- ⚠ core.meeting also contains partner='Jehoon' (219) and partner='Etay' (11). These look
--   like advisor/person NAMES, not partners, and they do NOT appear in im_bookings. We
--   deliberately do NOT create partner rows for them — flagged for Sam to confirm
--   (likely misattributed Slack posts). See sql/ddl/26 UPDATE: they will resolve to
--   partner_key = NULL, which is the correct "never silently wrong" behavior.
--
-- Type conventions match the rest of the warehouse:
--   text -> VARCHAR ; arrays -> VARCHAR[] ; bool -> BOOLEAN ; ts -> TIMESTAMPTZ.
--
-- Idempotent seed: INSERT ... ON CONFLICT (partner_key) DO NOTHING. Re-applying won't
-- duplicate. To change terms: DELETE the partner_key row, then re-INSERT.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.funding_partner (
  partner_key       VARCHAR PRIMARY KEY,    -- normalized slug (lowercase, underscores)
  display_name      VARCHAR NOT NULL,       -- canonical human name
  aliases           VARCHAR[] NOT NULL,     -- every observed string in our data (meeting + im_bookings)
  commercial_model  VARCHAR,                -- 'rev_share' | 'ppa' | 'ppa_plus_rev_share' | NULL (unknown)
  rev_share_pct     DOUBLE,                 -- our share of revenue, percent (e.g. 50.0). NULL if N/A or unknown.
  ppa_flag          BOOLEAN,                -- TRUE if a pay-per-appointment component exists. NULL if unknown.
  tier              VARCHAR,                -- 'anchor' | 'growing' | 'consistent' | 'new' | NULL
  notes             VARCHAR,
  active            BOOLEAN NOT NULL DEFAULT TRUE,
  _curated_at       TIMESTAMPTZ NOT NULL
);

-- --------------------------------------------------------------------------
-- Seed the known partners. Terms are descriptive labels only (no revenue math).
-- --------------------------------------------------------------------------
INSERT INTO core.funding_partner
  (partner_key, display_name, aliases, commercial_model, rev_share_pct, ppa_flag, tier, notes, active, _curated_at)
VALUES
  ('greenbridge', 'GreenBridge Capital',
   ['GreenBridge', 'GreenBridge Capital'],
   'rev_share', 50.0, FALSE, 'anchor',
   'Anchor partner; 50/50 rev share. Largest volume in both core.meeting (15,184) and im_bookings (17,148).',
   TRUE, now()),

  ('big_think', 'Big Think Capital',
   ['BTC', 'Big Think Capital'],
   'ppa_plus_rev_share', 10.0, TRUE, 'growing',
   'PPA (pay-per-appointment) plus 10% rev share. core.meeting alias is the short form "BTC".',
   TRUE, now()),

  ('goqualifi', 'GoQualifi',
   ['Qualifi', 'GoQualifi'],
   'ppa', NULL, TRUE, 'consistent',
   'PPA. core.meeting alias is "Qualifi"; im_bookings uses "GoQualifi". rev_share_pct N/A (PPA-only).',
   TRUE, now()),

  ('llama', 'Llama Funding',
   ['Llama', 'Llama Funding'],
   'ppa', NULL, TRUE, 'new',
   'PPA. New partner — first booking 2026-02-17 in both sources. rev_share_pct N/A (PPA-only).',
   TRUE, now()),

  -- ---- Partners present ONLY in Darcy''s im_bookings; commercial terms UNKNOWN. ----
  -- aliases = their name as it appears in im_bookings. Terms left NULL per brief.
  ('dcx', 'DCX',
   ['DCX'],
   NULL, NULL, NULL, NULL,
   'From Darcy im_bookings (161 rows). Commercial terms UNKNOWN — confirm with Sam.',
   TRUE, now()),

  ('infusion', 'Infusion',
   ['Infusion'],
   NULL, NULL, NULL, NULL,
   'From Darcy im_bookings (122 rows). Commercial terms UNKNOWN — confirm with Sam.',
   TRUE, now()),

  ('clarify', 'Clarify',
   ['Clarify'],
   NULL, NULL, NULL, NULL,
   'From Darcy im_bookings (54 rows). Commercial terms UNKNOWN — confirm with Sam.',
   TRUE, now()),

  ('capfront', 'Capfront',
   ['Capfront'],
   NULL, NULL, NULL, NULL,
   'From Darcy im_bookings (39 rows). Commercial terms UNKNOWN — confirm with Sam.',
   TRUE, now())
ON CONFLICT (partner_key) DO NOTHING;

-- NOTE on "Jehoon" / "Etay": intentionally NOT seeded. They appear in core.meeting.partner
-- but look like person/advisor names and are absent from im_bookings. They will resolve to
-- partner_key = NULL via sql/ddl/26 — correct "never silently wrong" behavior. Flag for Sam.
