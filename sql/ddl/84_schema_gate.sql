-- Schema-gate Phase 1 — the queryable schema contract + issue ledger. Version 84.
-- @gate: add
-- Depends on 00
--
-- This is the "review agent / middleman for the DuckDB warehouse" from the
-- 2026-06-18 Data Analysis Sync. It stops an uncoordinated column rename/move/drop
-- from silently breaking one of the ~100 syncs, and stops Claude-driven edits from
-- drifting into semantic dupes (`email` vs `email_address`).
--
-- The whole "dynamic, updated daily, takes + posts issues" requirement is satisfied
-- by these five DuckDB tables (NOT a *brain.md — the runtime reads nothing from prose):
--   core.schema_catalog    — live truth of every (schema, table, column) + canonical_name + status
--   core.schema_consumers  — who reads each column (file:line), tagged by confidence
--   core.column_aliases    — canonical_name <-> synonym, the deterministic dupe authority
--   core.schema_issue      — the "take + post issues" ledger appended by the gate + nightly drift
--   core.schema_gate_pass  — checksum-bound PASS records the apply-tooth consults
--
-- All additive. No ALTER/DROP/RENAME of any pre-existing table or view. Fully reversible
-- (DROP these 5 tables to undo). Rebuilt nightly by entities/schema_manifest.py (graft A).
--
-- PHASE 1 IS WARN-ONLY: the gate writes rows here but NEVER blocks a DDL apply, NEVER
-- fails the nightly, NEVER blocks any of the 4 editors. Flipping to BLOCK is a separate
-- Sam decision (see deliverables/2026-06-18-db-review-agent/BUILD-SPEC.md §9, §12).

CREATE SCHEMA IF NOT EXISTS core;

-- ── core.schema_catalog ──────────────────────────────────────────────────────
-- The live truth of every column. Grain = (table_schema, table_name, column_name).
-- Rebuilt nightly from information_schema by entities/schema_manifest.py. Rot-proof:
-- it can never disagree with the live DB for longer than one nightly (the drift check
-- in warehouse_qa.py catches any gap in between).
CREATE TABLE IF NOT EXISTS core.schema_catalog (
    table_schema    VARCHAR NOT NULL,
    table_name      VARCHAR NOT NULL,
    column_name     VARCHAR NOT NULL,
    data_type       VARCHAR,
    ordinal_position INTEGER,
    is_nullable     BOOLEAN,
    -- canonical_name: the agreed name for this CONCEPT. Defaults to column_name; an
    -- alias curation pass points synonyms at their canonical (via core.column_aliases).
    canonical_name  VARCHAR,
    -- status: active | deprecated | renaming (during an expand/contract migration).
    status          VARCHAR NOT NULL DEFAULT 'active',
    object_type     VARCHAR,            -- 'BASE TABLE' | 'VIEW'
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes           VARCHAR,
    PRIMARY KEY (table_schema, table_name, column_name)
);

-- ── core.schema_consumers ────────────────────────────────────────────────────
-- "Who reads this column." Grain = (column ref, consumer file:line). Rebuilt nightly:
-- static sqlglot parse over .sql + SQL-in-Python literals, UNION the declared registry,
-- UNION fail-closed 'assumed' rows for files that did not fully parse.
CREATE TABLE IF NOT EXISTS core.schema_consumers (
    table_schema  VARCHAR,             -- schema of the consumed column (may be NULL if unqualified)
    table_name    VARCHAR,             -- table of the consumed column (NULL when only the col name resolved)
    column_name   VARCHAR NOT NULL,    -- the column this consumer reads
    consumer_file VARCHAR NOT NULL,    -- repo-relative path of the consuming file
    consumer_line INTEGER,             -- best-effort line number (NULL for declared/assumed)
    -- confidence: 'static' (sqlglot resolved it) | 'declared' (`# @consumes:` marker)
    --             | 'assumed' (file did not fully parse -> fail-closed, treat as possible consumer)
    confidence    VARCHAR NOT NULL DEFAULT 'static',
    -- rename_resilient: TRUE when the consumer is column-name-agnostic by design
    -- (e.g. refresh_sync_registry picks from a priority list) -> no lineage needed.
    rename_resilient BOOLEAN NOT NULL DEFAULT FALSE,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    notes         VARCHAR
);
-- A column may be read by many files; a file may read many columns. No PK; the
-- nightly rebuild DELETEs static/assumed rows then re-derives (declared rows persist).
CREATE INDEX IF NOT EXISTS idx_schema_consumers_col
    ON core.schema_consumers (column_name);
CREATE INDEX IF NOT EXISTS idx_schema_consumers_tbl
    ON core.schema_consumers (table_schema, table_name, column_name);

-- ── core.column_aliases ──────────────────────────────────────────────────────
-- The deterministic dupe authority. canonical_name <-> synonym. Curated, append-only.
-- `email_address` -> canonical `email`. The gate BLOCKs (Phase 2) / WARNs (Phase 1) an
-- ADD COLUMN whose name is a known synonym of an existing canonical column.
CREATE TABLE IF NOT EXISTS core.column_aliases (
    alias          VARCHAR NOT NULL,   -- the synonym, snake_case (e.g. 'email_address')
    canonical_name VARCHAR NOT NULL,   -- the agreed canonical (e.g. 'email')
    -- scope: 'global' (any table) | a specific 'schema.table' the alias is scoped to.
    scope          VARCHAR NOT NULL DEFAULT 'global',
    reason         VARCHAR,
    added_by       VARCHAR,
    added_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (alias, scope)
);

-- ── core.schema_issue ────────────────────────────────────────────────────────
-- The "take issues + post them" ledger. One row per finding. Appended by the gate
-- (author-time) and the nightly drift/contract checks. Queryable by every editor's
-- Claude: `SELECT * FROM core.schema_issue WHERE status='open'`.
CREATE TABLE IF NOT EXISTS core.schema_issue (
    issue_id      BIGINT,             -- monotonic; assigned by the writer (max+1)
    -- rule: R1..R6 (see BUILD-SPEC §9) or 'DRIFT' / 'CONTRACT' (nightly graft B).
    rule          VARCHAR NOT NULL,
    -- severity: 'Error' | 'Warn' | 'Info'. In Phase 1, EVEN 'Error' rows are advisory
    -- (logged, never blocking) — severity is the eventual block tier, recorded now.
    severity      VARCHAR NOT NULL,
    -- classification (Atlas migrate-lint taxonomy) when applicable:
    -- DESTRUCTIVE | BREAKING-RENAME | DATA-DEPENDENT | LOCK-REWRITE | DUPE | NAMING | INTENT
    classification VARCHAR,
    table_schema  VARCHAR,
    table_name    VARCHAR,
    column_name   VARCHAR,
    ddl_file      VARCHAR,            -- the file that triggered it (author-time)
    detail        VARCHAR NOT NULL,   -- human-readable: what + the exact remedy
    consumers     VARCHAR,            -- JSON array of file:line consumers (impact list)
    -- status: 'open' | 'waived' | 'resolved'
    status        VARCHAR NOT NULL DEFAULT 'open',
    waived_by     VARCHAR,
    waived_reason VARCHAR,
    phase_mode    VARCHAR NOT NULL DEFAULT 'warn',  -- 'warn' (Phase 1) | 'block' (Phase 2)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_schema_issue_status ON core.schema_issue (status);
CREATE INDEX IF NOT EXISTS idx_schema_issue_rule   ON core.schema_issue (rule);

-- ── core.schema_gate_pass ────────────────────────────────────────────────────
-- What the apply tooth consults. Written by the gate on PASS. Checksum-bound: the
-- sha256 is of the DDL FILE CONTENT, so an editor can't pass the gate on one version
-- then quietly edit the file (the hash won't match at apply time).
--
-- PHASE 1: the apply tooth READS this but only WARNs on a miss — it NEVER refuses an
-- apply. So un-gated DDL from Thomas/Sam/Darcy/David still applies normally and the
-- nightly never breaks. (Phase 2 flips the miss to a hard refuse.)
CREATE TABLE IF NOT EXISTS core.schema_gate_pass (
    version       INTEGER NOT NULL,
    sql_file      VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    -- verdict at gate time: 'pass' | 'pass-with-warn' (warnings present but no hard rule)
    verdict       VARCHAR NOT NULL DEFAULT 'pass',
    gated_by      VARCHAR,
    gate_version  VARCHAR,            -- schema_gate.py version that produced this pass
    issue_count   INTEGER DEFAULT 0,  -- # rows written to schema_issue for this file
    passed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (version, content_sha256)
);
