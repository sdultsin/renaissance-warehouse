-- Superseded by 15_cost_seed_v2.sql.
--
-- Cost reference data is no longer inlined into version-controlled SQL. It is
-- loaded from an external, gitignored seed file (see 15_cost_seed_v2.sql).
-- This file is retained only to preserve the migration version sequence; it is
-- intentionally a no-op so that already-initialized databases keep their
-- recorded schema_version=14 without re-running anything.

SELECT 1;
