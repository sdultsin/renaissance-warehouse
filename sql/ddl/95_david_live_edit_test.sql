-- @gate: add
-- Depends on 83
-- 95_david_live_edit_test.sql — TEMPORARY live-edit verification artifact.
--
-- WHAT: derived.david_live_edit_test is a zero-row-dependency view that returns a single
-- static marker row. It references no base tables, so it cannot affect any other consumer.
--
-- WHY: one-off proof that the editor (david@renaissancegrowth.io) can author -> gate ->
-- apply-now a change LIVE. Additive; not in the gate's required_schema, so it can never
-- block a snapshot promote. Will be dropped immediately after verification (see follow-up DDL).
CREATE OR REPLACE VIEW derived.david_live_edit_test AS
SELECT 'david-live-edit-test' AS marker, 'ddl-95' AS source;
