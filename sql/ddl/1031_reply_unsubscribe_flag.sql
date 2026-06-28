-- @gate: add
-- Depends on 43
-- Reply-pipe remediation (2026-06-28): add a row-level `is_unsubscribe` flag to core.reply.
-- Applied by setup_db.py in its OWN connection, BEFORE the orchestrator opens its writer
-- (ALTER ADD COLUMN + the bulk INSERT in f_reply_canonical.py in the same DuckDB connection
--  trips an internal ColumnData::Append assertion, so the column-add must be split out — same
--  pattern as 48_reply_eaccount.sql).
--
-- Why: a lead asking to be removed ("unsubscribe me", "stop emailing me", "take me off your
-- list") is a HUMAN action but NOT a positive reply. Folding it into the human-reply count
-- poisons human-reply-rate / positive-rate KPIs. is_auto_reply (OOO/autoresponder/bounce) and
-- is_unsubscribe (lead removal request) are distinct non-human/non-positive categories; this
-- column lets downstream KPIs net both out. Additive only.

ALTER TABLE core.reply ADD COLUMN IF NOT EXISTS is_unsubscribe BOOLEAN;
