-- @gate: create core.review_learnings (self-updating reviewer memory) + seed the 2026-06-26 dup-index lesson [W3]
-- Depends on
-- ============================================================================
-- W3 — LEARNINGS MEMORY: core.review_learnings
--   The reviewer READS recent rows at review start; the post-apply integrity gate
--   APPENDS one row on every caught incident. Append-mostly; `superseded` retires a
--   lesson without deletion. Additive + idempotent (IF NOT EXISTS / seed guarded).
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS core;

CREATE SEQUENCE IF NOT EXISTS core.review_learnings_id_seq START 1;

CREATE TABLE IF NOT EXISTS core.review_learnings (
    id              BIGINT PRIMARY KEY DEFAULT nextval('core.review_learnings_id_seq'),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    -- taxonomy so the read-query can prioritise the most relevant lessons
    category        VARCHAR NOT NULL,            -- e.g. 'index', 'rename', 'not_null', 'sync', 'type'
    -- human-readable lesson (the post-mortem in one paragraph)
    lesson          VARCHAR NOT NULL,
    -- the enforceable RULE the reviewer should apply going forward (imperative, testable)
    rule_text       VARCHAR NOT NULL,
    -- a minimal correct/incorrect DDL exemplar the LLM can pattern-match against
    example_ddl     VARCHAR,
    -- provenance: what incident/date taught this (free text or an id ref)
    source_incident VARCHAR,
    -- did the gate CATCH it or did it ESCAPE (post-mortem) — drives weighting/alerting
    outcome         VARCHAR NOT NULL DEFAULT 'caught'   -- 'caught' | 'escaped'
                    CHECK (outcome IN ('caught', 'escaped')),
    severity        VARCHAR NOT NULL DEFAULT 'block'    -- 'block' | 'warn' | 'info'
                    CHECK (severity IN ('block', 'warn', 'info')),
    -- retire a lesson without deleting it (keeps the audit trail immutable)
    superseded      BOOLEAN NOT NULL DEFAULT FALSE,
    superseded_by   BIGINT,                              -- id of the replacement lesson
    -- how many times the reviewer has cited this lesson in a verdict (telemetry)
    times_cited     BIGINT NOT NULL DEFAULT 0
);

-- read path is "recent active lessons", so index the sort/filter columns
CREATE INDEX IF NOT EXISTS ix_review_learnings_active
    ON core.review_learnings (superseded, created_at);
CREATE INDEX IF NOT EXISTS ix_review_learnings_category
    ON core.review_learnings (category);

-- ============================================================================
-- SEED — the 2026-06-26 lesson (idempotent: only inserts if not already seeded).
-- ============================================================================
INSERT INTO core.review_learnings
    (category, lesson, rule_text, example_ddl, source_incident, outcome, severity)
SELECT
    'index',
    'On 2026-06-26 an index "rename" was shipped as a bare CREATE UNIQUE INDEX IF '
    || 'NOT EXISTS under a new name without DROPping the old index. Because IF NOT '
    || 'EXISTS keys off the NEW name, the old unique index on _key survived, leaving '
    || 'TWO unique indexes on the same (_key) column. The nightly upsert (which '
    || 'DELETE-then-INSERTs by _key) then FATALed with "Failed to delete all rows '
    || 'from index" because it could not consistently maintain both unique indexes. '
    || 'Live evidence: 9 of 12 raw_pipeline_* tables still carry a duplicate '
    || 'ux_*_key + uxk_* unique-on-_key pair from exactly this pattern.',
    'An index rename MUST be DROP-old + CREATE-new (expand/contract): in the SAME '
    || 'migration, DROP INDEX IF EXISTS <old_name> AND CREATE [UNIQUE] INDEX '
    || '<new_name>. NEVER ship a standalone CREATE [UNIQUE] INDEX IF NOT EXISTS as a '
    || '"rename" — IF NOT EXISTS matches on the new name and silently leaves the old '
    || 'index in place, yielding two unique indexes on _key that FATAL the nightly '
    || 'upsert. BLOCK any raw_pipeline_* change that adds a UNIQUE index on (_key) '
    || 'when one already exists (see the duplicate-unique-index live-schema flag) '
    || 'unless the same change DROPs the prior one.',
    '-- WRONG (leaves two unique indexes on _key -> nightly upsert FATAL):'   || chr(10)
    || 'CREATE UNIQUE INDEX IF NOT EXISTS uxk_raw_pipeline_campaigns'         || chr(10)
    || '    ON raw_pipeline_campaigns(_key);'                                  || chr(10)
    || '-- RIGHT (expand/contract — drop old, then create new, atomically):'  || chr(10)
    || 'DROP INDEX IF EXISTS ux_raw_pipeline_campaigns_key;'                   || chr(10)
    || 'CREATE UNIQUE INDEX uxk_raw_pipeline_campaigns'                        || chr(10)
    || '    ON raw_pipeline_campaigns(_key);',
    '2026-06-26 nightly-upsert FATAL: duplicate unique index on raw_pipeline_*._key '
    || '(Failed to delete all rows from index)',
    'escaped',
    'block'
WHERE NOT EXISTS (
    SELECT 1 FROM core.review_learnings
    WHERE category = 'index'
      AND source_incident LIKE '2026-06-26 nightly-upsert FATAL%'
);
