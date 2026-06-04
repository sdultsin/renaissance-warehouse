-- Workstream F: add core.meeting.partner_key + backfill from core.funding_partner aliases.
-- Applied at schema version 26 by scripts/setup_db.py / orchestrator DDL applier.
--
-- core.meeting.partner is the raw Slack-channel-derived string (partnerA, BTC, Qualifi,
-- Llama, plus the likely-misattributed Jehoon/Etay, plus NULL). partner_key resolves it to
-- the canonical core.funding_partner.partner_key via the alias array. Unmatched partners
-- (Jehoon, Etay) and NULL partner both leave partner_key = NULL — that is correct.
--
-- DEPENDS ON sql/ddl/25_funding_partner.sql (must be applied + seeded first).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS (no-op if re-run); the UPDATE is a pure projection
-- of partner -> partner_key, safe to re-run any time (e.g. after meeting.py rebuilds the table
-- or after aliases change). Re-run this UPDATE after any nightly core.meeting rebuild if you
-- want partner_key kept fresh outside the orchestrator (see handoff note).

ALTER TABLE core.meeting ADD COLUMN IF NOT EXISTS partner_key VARCHAR;

-- Backfill: normalize core.meeting.partner through core.funding_partner.aliases.
-- list_contains() matches the exact observed string against the partner's alias array.
UPDATE core.meeting AS m
SET partner_key = fp.partner_key
FROM core.funding_partner fp
WHERE m.partner IS NOT NULL
  AND list_contains(fp.aliases, m.partner);

-- Defensive: any meeting whose partner did NOT match an alias (Jehoon, Etay, or a future
-- new partner not yet seeded) keeps partner_key = NULL. We do NOT guess.
-- (No statement needed — partner_key defaults to NULL — but documented for the reader.)

-- Optional index for partner-keyed rollups in the dashboard.
CREATE INDEX IF NOT EXISTS ix_core_meeting_partner_key ON core.meeting (partner_key);
