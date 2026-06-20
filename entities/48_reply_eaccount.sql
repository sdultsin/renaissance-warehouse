-- Spec 16 follow-up — add the sending inbox (eaccount) to core.reply. Version 48.
-- Applied by setup_db.py in its OWN connection, BEFORE the orchestrator opens its writer.
-- (ALTER ADD COLUMN + bulk INSERT in the same DuckDB connection trips an internal
--  ColumnData::Append assertion, so the column-add must happen separately — see
--  entities/reply_canonical.py.)
-- Additive only.

ALTER TABLE core.reply ADD COLUMN IF NOT EXISTS eaccount VARCHAR;
