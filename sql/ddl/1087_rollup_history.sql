-- @gate: add
-- Depends on 00
-- core.provider_history / deliverability_history / batch_history — daily AGGREGATE snapshots that give the
-- Renaissance Data Hub its over-time graphs for Providers, Deliverability, and Batches (the dimensions the
-- census does NOT keep per day). Written by the rollup_history entity (canonical phase) once per nightly
-- run from core.v_inbox_overview: ONE aggregate row per provider / per workspace / per batch — ~150 rows a
-- night, ~55k rows/year total. Tiny by construction (aggregates, never per-inbox), so it cannot clog the DB.
-- Forward-only: provider(supplier), deliverability, and batch are not date-partitioned anywhere else, so
-- history begins the day this ships. NEVER trimmed. Idempotent per snapshot_date (the entity DELETEs today's
-- rows then re-INSERTs), so a same-day re-run replaces cleanly — no PK / ART-abort risk (plain append).
CREATE TABLE IF NOT EXISTS core.provider_history (
    snapshot_date   DATE NOT NULL,
    provider        VARCHAR,      -- supplier grouping (Google Panel split out via tags), matches the Hub Providers view
    n_total         BIGINT,
    n_live          BIGINT,
    n_warming       BIGINT,
    n_disc          BIGINT,
    n_banned        BIGINT,
    n_domains       BIGINT,
    live_capacity   DOUBLE,       -- sum(daily_limit) over Live inboxes
    _loaded_at      TIMESTAMPTZ,
    _run_id         VARCHAR
);
CREATE TABLE IF NOT EXISTS core.deliverability_history (
    snapshot_date   DATE NOT NULL,
    workspace_slug  VARCHAR,
    n_total         BIGINT,
    spf_pct         DOUBLE,
    dkim_pct        DOUBLE,
    dmarc_pct       DOUBLE,
    mx_pct          DOUBLE,
    blacklisted     BIGINT,       -- curated blacklist count (v_inbox_overview.blacklisted)
    _loaded_at      TIMESTAMPTZ,
    _run_id         VARCHAR
);
CREATE TABLE IF NOT EXISTS core.batch_history (
    snapshot_date   DATE NOT NULL,
    batch_key       VARCHAR,
    n_total         BIGINT,
    n_live          BIGINT,
    n_warming       BIGINT,
    n_disc          BIGINT,
    pct_live        DOUBLE,       -- % of the batch that has ever gone live
    _loaded_at      TIMESTAMPTZ,
    _run_id         VARCHAR
);
