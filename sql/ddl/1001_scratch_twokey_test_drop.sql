-- @gate: drop
-- Two-key auto-merge LIVE verification (DESTRUCTIVE path). Safe to delete.
-- Drops a throwaway scratch view to prove the destructive class is HELD (author-intent confirm),
-- never auto-merged. [2026-06-22]
DROP VIEW IF EXISTS core.scratch_twokey_test;
