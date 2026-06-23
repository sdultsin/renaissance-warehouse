-- @gate: add
-- Depends on 1005
-- THROWAWAY: validates the pull_request auto-trigger of two-key-automerge.yml. Never merged.
CREATE OR REPLACE VIEW core.v_ci_trigger_smoke AS SELECT 1 AS ok;
