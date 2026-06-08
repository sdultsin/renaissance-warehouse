-- Workstream F: core.funding_partner dimension (funding-partner attribution).
-- Applied at schema version 25 by scripts/setup_db.py / orchestrator DDL applier.
--
-- One row per funding partner the business books meetings to. This is a DESCRIPTIVE
-- dimension only: it stores reconciled aliases (the strings our data actually uses)
-- plus the commercial terms as LABELS. It is the canonical place to look up partner
-- terms — but it does NOT model revenue. Meetings stay flat at REVENUE_PER_MEETING in
-- the dashboard; rev-share economics are deliberately NOT computed anywhere yet.
--
-- Partner identities, aliases, and commercial terms are NOT inlined here. They are
-- loaded from an EXTERNAL, gitignored seed file (seed_data/funding_partner.csv) so
-- that partner names and commercial terms are not committed to a public repository.
-- The `aliases` column is pipe-delimited in the CSV and split into a list on load.
--
-- Alias reconciliation: meeting.partner (Slack-channel-sourced) and the bookings
-- portal use different spellings of the same partner; the aliases list carries every
-- observed string so both sources resolve to one key. Strings that match no alias
-- resolve to partner_key = NULL ("never silently wrong").
--
-- Idempotent: ON CONFLICT (partner_key) DO NOTHING. If the seed file is absent, the
-- table is created empty and the warehouse still builds.

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.funding_partner (
  partner_key       VARCHAR PRIMARY KEY,    -- normalized slug (lowercase, underscores)
  display_name      VARCHAR NOT NULL,       -- canonical human name
  aliases           VARCHAR[] NOT NULL,     -- every observed string in our data
  commercial_model  VARCHAR,                -- 'rev_share' | 'ppa' | 'ppa_plus_rev_share' | NULL (unknown)
  rev_share_pct     DOUBLE,                 -- our share of revenue, percent. NULL if N/A or unknown.
  ppa_flag          BOOLEAN,                -- TRUE if a pay-per-appointment component exists. NULL if unknown.
  tier              VARCHAR,                -- 'anchor' | 'growing' | 'consistent' | 'new' | NULL
  notes             VARCHAR,
  active            BOOLEAN NOT NULL DEFAULT TRUE,
  _curated_at       TIMESTAMPTZ NOT NULL
);

-- Seed from the external, gitignored file. Aliases are pipe-delimited -> list.
-- Guarded so a missing seed file leaves the table empty rather than erroring.
INSERT INTO core.funding_partner
  (partner_key, display_name, aliases, commercial_model, rev_share_pct, ppa_flag, tier, notes, active, _curated_at)
SELECT partner_key, display_name, string_split(aliases, '|'), commercial_model,
       rev_share_pct, ppa_flag, tier, notes, active, now()
FROM read_csv_auto('seed_data/funding_partner.csv', header=true, nullstr='')
WHERE (SELECT count(*) FROM glob('seed_data/funding_partner.csv')) > 0
ON CONFLICT (partner_key) DO NOTHING;
