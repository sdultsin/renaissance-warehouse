-- 101_inbox_hub.sql  [2026-06-26]
-- core.inbox_hub — THE canonical, materialized inbox hub (fast table form of the live
-- view core.v_inbox_overview). One row per live inbox. Read THIS for instant lookups;
-- the view recomputes joins on every query (~5-7s), this table is a plain scan (ms).
--
-- Refreshed every nightly run by entities/inbox_hub.py (derived phase, AFTER the
-- Instantly feeders fill). This DDL just declares the shape (mirrors the view) + the
-- self-describing comments so any client/cloud that introspects the schema knows what
-- this is and that it supersedes hitting the Instantly API for inbox data.
--
-- ADDITIVE: brand-new table; nothing existing is touched.
-- @gate: add
-- Depends on 99
CREATE TABLE IF NOT EXISTS core.inbox_hub AS SELECT * FROM core.v_inbox_overview LIMIT 0;

COMMENT ON TABLE core.inbox_hub IS
  'CANONICAL INBOX HUB — the one true list of every live sending inbox (~433k), one row '
  'each, with identity, workspace, provider, status, lifecycle stage, ALL lifecycle dates '
  '(created/connected/warmup-start/first+last cold send/paused/retired), tags, batch/RG, '
  'and campaign membership. QUERY THIS (or the live view core.v_inbox_overview it '
  'materializes) for ANY inbox lookup instead of the Instantly API. Refreshed nightly. '
  'Created 2026-06-26.';

COMMENT ON VIEW core.v_inbox_overview IS
  'Live view behind the CANONICAL INBOX HUB (materialized nightly as core.inbox_hub). '
  'One row per live inbox with all identity/status/provider/tags/dates/campaigns. Use '
  'core.inbox_hub for fast reads; this view for always-current (per last nightly snapshot).';
